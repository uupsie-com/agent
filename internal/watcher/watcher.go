package watcher

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/uupsie-com/agent/internal/config"
	"github.com/uupsie-com/agent/internal/reporter"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Manager manages informers for a set of monitors grouped by namespace.
type Manager struct {
	clientset *kubernetes.Clientset
	reporter  *reporter.Reporter

	mu          sync.Mutex
	monitors    map[string]config.Monitor        // keyed by monitor ID
	stopChs     map[string]chan struct{}           // keyed by namespace (informers)
	serviceCtxs map[string]context.CancelFunc     // keyed by monitor ID (service probes)
}

func NewManager(rep *reporter.Reporter) (*Manager, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("building in-cluster config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating kubernetes client: %w", err)
	}

	return &Manager{
		clientset:   clientset,
		reporter:    rep,
		monitors:    make(map[string]config.Monitor),
		stopChs:     make(map[string]chan struct{}),
		serviceCtxs: make(map[string]context.CancelFunc),
	}, nil
}

// Reconcile updates the set of watched resources based on the config from the API.
func (m *Manager) Reconcile(monitors []config.Monitor) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Separate monitors into informer-based and probe-based
	newMonitors := make(map[string]config.Monitor)
	needNamespaces := make(map[string]bool)
	newServiceMonitors := make(map[string]config.Monitor)

	for _, mon := range monitors {
		newMonitors[mon.ID] = mon
		if mon.Type == "k8s_service" {
			newServiceMonitors[mon.ID] = mon
			continue
		}
		ns := mon.Config["namespace"]
		if ns == "" {
			ns = "default"
		}
		needNamespaces[ns] = true
	}
	m.monitors = newMonitors

	// --- Informer lifecycle (pod, deployment, node) ---

	// Stop informers for namespaces no longer needed
	for ns, stopCh := range m.stopChs {
		if !needNamespaces[ns] {
			log.Printf("[watcher] stopping informers for namespace %s", ns)
			close(stopCh)
			delete(m.stopChs, ns)
		}
	}

	// Start informers for new namespaces
	for ns := range needNamespaces {
		if _, exists := m.stopChs[ns]; exists {
			continue
		}
		log.Printf("[watcher] starting informers for namespace %s", ns)
		stopCh := make(chan struct{})
		m.stopChs[ns] = stopCh
		m.startInformers(ns, stopCh)
	}

	// --- Service probe lifecycle ---

	// Stop probes for removed service monitors
	for id, cancel := range m.serviceCtxs {
		if _, exists := newServiceMonitors[id]; !exists {
			cancel()
			delete(m.serviceCtxs, id)
		}
	}

	// Start probes for new service monitors
	for id, mon := range newServiceMonitors {
		if _, exists := m.serviceCtxs[id]; exists {
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		m.serviceCtxs[id] = cancel
		go m.runServiceCheck(ctx, mon)
	}
}

func (m *Manager) startInformers(namespace string, stopCh chan struct{}) {
	factory := informers.NewSharedInformerFactoryWithOptions(
		m.clientset,
		30*time.Second,
		informers.WithNamespace(namespace),
	)

	podInformer := factory.Core().V1().Pods().Informer()
	podInformer.AddEventHandler(newPodHandler(m))

	deployInformer := factory.Apps().V1().Deployments().Informer()
	deployInformer.AddEventHandler(newDeploymentHandler(m))

	nodeInformer := factory.Core().V1().Nodes().Informer()
	nodeInformer.AddEventHandler(newNodeHandler(m))

	factory.Start(stopCh)
}

// findMonitors returns monitors matching the given type, namespace, and resource name.
func (m *Manager) findMonitors(monitorType, namespace, name string) []config.Monitor {
	m.mu.Lock()
	defer m.mu.Unlock()

	var matches []config.Monitor
	for _, mon := range m.monitors {
		if mon.Type != monitorType {
			continue
		}
		monNs := mon.Config["namespace"]
		if monNs == "" {
			monNs = "default"
		}
		monName := mon.Config["name"]
		if monNs == namespace && monName == name {
			matches = append(matches, mon)
		}
	}
	return matches
}

func (m *Manager) reportStatus(monitorID, status string, errMsg *string) {
	m.reporter.Report(reporter.CheckResult{
		MonitorID:    monitorID,
		Status:       status,
		ErrorMessage: errMsg,
		CheckedAt:    time.Now().UTC().Format(time.RFC3339),
	})
}

// Stop shuts down all informers and service probes.
func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for ns, stopCh := range m.stopChs {
		log.Printf("[watcher] stopping informers for namespace %s", ns)
		close(stopCh)
	}
	m.stopChs = make(map[string]chan struct{})

	for id, cancel := range m.serviceCtxs {
		log.Printf("[watcher] stopping service probe for monitor %s", id)
		cancel()
	}
	m.serviceCtxs = make(map[string]context.CancelFunc)
}
