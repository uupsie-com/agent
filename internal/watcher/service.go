package watcher

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/uupsie-com/agent/internal/config"
	"github.com/uupsie-com/agent/internal/reporter"
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
	m.probeService(mon.ID, address, timeout)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[service] stopping TCP probe for %s", address)
			return
		case <-ticker.C:
			m.probeService(mon.ID, address, timeout)
		}
	}
}

func (m *Manager) probeService(monitorID, address string, timeout time.Duration) {
	start := time.Now()
	conn, err := net.DialTimeout("tcp", address, timeout)
	responseTime := int(time.Since(start).Milliseconds())

	if err != nil {
		errMsg := fmt.Sprintf("TCP connect to %s failed: %v", address, err)
		log.Printf("[service] %s", errMsg)
		m.reporter.Report(reporter.CheckResult{
			MonitorID:      monitorID,
			Status:         "down",
			ResponseTimeMs: &responseTime,
			ErrorMessage:   &errMsg,
			CheckedAt:      time.Now().UTC().Format(time.RFC3339),
		})
		return
	}
	conn.Close()

	m.reporter.Report(reporter.CheckResult{
		MonitorID:      monitorID,
		Status:         "up",
		ResponseTimeMs: &responseTime,
		CheckedAt:      time.Now().UTC().Format(time.RFC3339),
	})
}
