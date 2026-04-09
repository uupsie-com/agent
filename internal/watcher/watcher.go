package watcher

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/uupsie/agent/internal/config"
	"github.com/uupsie/agent/internal/reporter"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Manager manages informers for a set of monitors grouped by namespace.
type Manager struct {
	clientset *kubernetes.Clientset
	reporter  *reporter.Reporter

	mu       sync.Mutex
	monitors map[string]config.Monitor // keyed by monitor ID
	stopChs  map[string]chan struct{}   // keyed by namespace
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
		clientset: clientset,
		reporter:  rep,
		monitors:  make(map[string]config.Monitor),
		stopChs:   make(map[string]chan struct{}),
	}, nil
}

// Reconcile updates the set of watched resources based on the config from the API.
func (m *Manager) Reconcile(monitors []config.Monitor) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Build new monitor map and required namespaces
	newMonitors := make(map[string]config.Monitor)
	needNamespaces := make(map[string]bool)
	for _, mon := range monitors {
		newMonitors[mon.ID] = mon
		ns := mon.Config["namespace"]
		if ns == "" {
			ns = "default"
		}
		needNamespaces[ns] = true
	}
	m.monitors = newMonitors

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
}

func (m *Manager) startInformers(namespace string, stopCh chan struct{}) {
	factory := informers.NewSharedInformerFactoryWithOptions(
		m.clientset,
		30*time.Second,
		informers.WithNamespace(namespace),
	)

	// Start all relevant informers — handlers evaluate health on events
	podInformer := factory.Core().V1().Pods().Informer()
	podInformer.AddEventHandler(newPodHandler(m))

	deployInformer := factory.Apps().V1().Deployments().Informer()
	deployInformer.AddEventHandler(newDeploymentHandler(m))

	serviceInformer := factory.Core().V1().Services().Informer()
	serviceInformer.AddEventHandler(newServiceHandler(m))

	nodeInformer := factory.Core().V1().Nodes().Informer()
	nodeInformer.AddEventHandler(newNodeHandler(m))

	factory.Start(stopCh)
}

// findMonitor returns monitors matching the given type, namespace, and resource name.
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

// Stop shuts down all informers.
func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for ns, stopCh := range m.stopChs {
		log.Printf("[watcher] stopping informers for namespace %s", ns)
		close(stopCh)
	}
	m.stopChs = make(map[string]chan struct{})
}
