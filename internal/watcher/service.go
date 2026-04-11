package watcher

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/uupsie-com/agent/internal/config"
	"github.com/uupsie-com/agent/internal/reporter"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// runServiceCheck actively probes a K8s service via TCP at the configured interval.
func (m *Manager) runServiceCheck(ctx context.Context, mon config.Monitor) {
	namespace := mon.ConfigString("namespace")
	if namespace == "" {
		namespace = "default"
	}
	name := mon.ConfigString("name")
	port := mon.ConfigString("port")
	if port == "" {
		port = "80"
	}

	interval := 60 * time.Second
	if mon.IntervalSeconds > 0 {
		interval = time.Duration(mon.IntervalSeconds) * time.Second
	}

	timeout := 10 * time.Second
	if mon.TimeoutSeconds > 0 {
		timeout = time.Duration(mon.TimeoutSeconds) * time.Second
	}

	// Resolve ClusterIP once; re-resolve on failure
	clusterIP := m.resolveClusterIP(ctx, namespace, name)
	if clusterIP != "" {
		log.Printf("[service] resolved %s/%s to ClusterIP %s", namespace, name, clusterIP)
	}

	logName := fmt.Sprintf("%s.%s:%s", name, namespace, port)
	log.Printf("[service] starting TCP probe for %s (every %s, timeout %s)", logName, interval, timeout)

	// Check immediately on start
	m.probeService(ctx, mon.ID, namespace, name, port, clusterIP, timeout)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[service] stopping TCP probe for %s", logName)
			return
		case <-ticker.C:
			m.probeService(ctx, mon.ID, namespace, name, port, clusterIP, timeout)
		}
	}
}

func (m *Manager) resolveClusterIP(ctx context.Context, namespace, name string) string {
	svc, err := m.clientset.CoreV1().Services(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		log.Printf("[service] failed to resolve ClusterIP for %s/%s: %v", namespace, name, err)
		return ""
	}
	if svc.Spec.ClusterIP == "" || svc.Spec.ClusterIP == "None" {
		log.Printf("[service] %s/%s is headless (no ClusterIP), falling back to DNS", namespace, name)
		return ""
	}
	return svc.Spec.ClusterIP
}

func (m *Manager) getEndpointCounts(ctx context.Context, namespace, name string) (ready int, total int) {
	endpoints, err := m.clientset.CoreV1().Endpoints(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return 0, 0
	}
	for _, subset := range endpoints.Subsets {
		ready += len(subset.Addresses)
		total += len(subset.Addresses) + len(subset.NotReadyAddresses)
	}
	return ready, total
}

func (m *Manager) probeService(ctx context.Context, monitorID, namespace, name, port, clusterIP string, timeout time.Duration) {
	// Get endpoint counts regardless of probe result
	readyPods, totalPods := m.getEndpointCounts(ctx, namespace, name)
	metadata := map[string]any{
		"ready_endpoints": readyPods,
		"total_endpoints": totalPods,
	}

	// Use ClusterIP if available, fall back to DNS
	address := fmt.Sprintf("%s:%s", clusterIP, port)
	if clusterIP == "" {
		address = fmt.Sprintf("%s.%s.svc.cluster.local:%s", name, namespace, port)
	}

	start := time.Now()
	conn, err := net.DialTimeout("tcp", address, timeout)
	elapsed := time.Since(start)
	responseTime := float64(elapsed.Microseconds()) / 1000.0 // sub-ms precision

	if err != nil {
		errMsg := fmt.Sprintf("TCP connect to %s/%s:%s failed: %v", namespace, name, port, err)
		log.Printf("[service] %s", errMsg)
		m.reporter.Report(reporter.CheckResult{
			MonitorID:      monitorID,
			Status:         "down",
			ResponseTimeMs: &responseTime,
			ErrorMessage:   &errMsg,
			Metadata:       metadata,
			CheckedAt:      time.Now().UTC().Format(time.RFC3339),
		})
		return
	}
	conn.Close()

	m.reporter.Report(reporter.CheckResult{
		MonitorID:      monitorID,
		Status:         "up",
		ResponseTimeMs: &responseTime,
		Metadata:       metadata,
		CheckedAt:      time.Now().UTC().Format(time.RFC3339),
	})
}
