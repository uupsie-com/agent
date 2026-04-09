package discovery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Resource struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
	Kind      string `json:"kind"`
}

type Inventory struct {
	Namespaces []string   `json:"namespaces"`
	Resources  []Resource `json:"resources"`
}

type Discovery struct {
	clientset  *kubernetes.Clientset
	apiURL     string
	apiToken   string
	httpClient *http.Client
}

func New(apiURL, apiToken string) (*Discovery, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("building in-cluster config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating kubernetes client: %w", err)
	}

	return &Discovery{
		clientset:  clientset,
		apiURL:     apiURL,
		apiToken:   apiToken,
		httpClient: &http.Client{Timeout: 15 * time.Second},
	}, nil
}

// Collect gathers the current cluster inventory.
func (d *Discovery) Collect(ctx context.Context) (*Inventory, error) {
	inv := &Inventory{}

	// Namespaces
	nsList, err := d.clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing namespaces: %w", err)
	}
	for _, ns := range nsList.Items {
		inv.Namespaces = append(inv.Namespaces, ns.Name)
	}

	// For each namespace, collect deployments, pods, services
	for _, ns := range inv.Namespaces {
		deploys, err := d.clientset.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			log.Printf("[discovery] failed to list deployments in %s: %v", ns, err)
			continue
		}
		for _, dep := range deploys.Items {
			inv.Resources = append(inv.Resources, Resource{
				Name:      dep.Name,
				Namespace: ns,
				Kind:      "deployment",
			})
		}

		pods, err := d.clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			log.Printf("[discovery] failed to list pods in %s: %v", ns, err)
			continue
		}
		for _, pod := range pods.Items {
			inv.Resources = append(inv.Resources, Resource{
				Name:      pod.Name,
				Namespace: ns,
				Kind:      "pod",
			})
		}

		services, err := d.clientset.CoreV1().Services(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			log.Printf("[discovery] failed to list services in %s: %v", ns, err)
			continue
		}
		for _, svc := range services.Items {
			inv.Resources = append(inv.Resources, Resource{
				Name:      svc.Name,
				Namespace: ns,
				Kind:      "service",
			})
		}
	}

	// Nodes (cluster-scoped)
	nodes, err := d.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Printf("[discovery] failed to list nodes: %v", err)
	} else {
		for _, node := range nodes.Items {
			inv.Resources = append(inv.Resources, Resource{
				Name: node.Name,
				Kind: "node",
			})
		}
	}

	return inv, nil
}

// Report collects inventory and sends it to the API.
func (d *Discovery) Report(ctx context.Context) error {
	inv, err := d.Collect(ctx)
	if err != nil {
		return fmt.Errorf("collecting inventory: %w", err)
	}

	payload, err := json.Marshal(inv)
	if err != nil {
		return fmt.Errorf("marshaling inventory: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", d.apiURL+"/api/v1/agent/inventory", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+d.apiToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("sending inventory: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("API returned %d: %s", resp.StatusCode, string(body))
	}

	log.Printf("[discovery] reported %d namespaces, %d resources", len(inv.Namespaces), len(inv.Resources))
	return nil
}

// Start runs inventory reporting on startup and then every interval.
func (d *Discovery) Start(ctx context.Context, interval time.Duration) {
	// Initial report
	if err := d.Report(ctx); err != nil {
		log.Printf("[discovery] initial report failed: %v", err)
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := d.Report(ctx); err != nil {
					log.Printf("[discovery] report failed: %v", err)
				}
			}
		}
	}()
}
