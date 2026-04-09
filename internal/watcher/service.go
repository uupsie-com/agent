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

	address := fmt.Sprintf("%s.%s.svc.cluster.local:%s", name, namespace, port)

	interval := 60 * time.Second
	if mon.IntervalSeconds > 0 {
		interval = time.Duration(mon.IntervalSeconds) * time.Second
	}

	timeout := 10 * time.Second
	if mon.TimeoutSeconds > 0 {
		timeout = time.Duration(mon.TimeoutSeconds) * time.Second
	}

	log.Printf("[service] starting TCP probe for %s (every %s, timeout %s)", address, interval, timeout)

	// Check immediately on start
	m.probeService(ctx, mon.ID, namespace, name, address, timeout)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[service] stopping TCP probe for %s", address)
			return
		case <-ticker.C:
			m.probeService(ctx, mon.ID, namespace, name, address, timeout)
		}
	}
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

func (m *Manager) probeService(ctx context.Context, monitorID, namespace, name, address string, timeout time.Duration) {
	// Get endpoint counts regardless of probe result
	readyPods, totalPods := m.getEndpointCounts(ctx, namespace, name)
	metadata := map[string]any{
		"ready_endpoints": readyPods,
		"total_endpoints": totalPods,
	}

	start := time.Now()
	conn, err := net.DialTimeout("tcp", address, timeout)
	responseTime := int(time.Since(start).Milliseconds())
	if responseTime == 0 {
		responseTime = 1 // sub-ms connects report as 1ms minimum
	}

	if err != nil {
		errMsg := fmt.Sprintf("TCP connect to %s failed: %v", address, err)
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
