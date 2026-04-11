package reporter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

type CheckResult struct {
	MonitorID      string   `json:"monitor_id"`
	Status         string   `json:"status"`
	ResponseTimeMs *float64 `json:"response_time_ms"`
	ErrorMessage   *string  `json:"error_message"`
	Metadata       any      `json:"metadata,omitempty"`
	CheckedAt      string   `json:"checked_at"`
}

type Reporter struct {
	apiURL     string
	apiToken   string
	httpClient *http.Client

	mu      sync.Mutex
	pending []CheckResult
}

func New(apiURL, apiToken string) *Reporter {
	return &Reporter{
		apiURL:   apiURL,
		apiToken: apiToken,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// Report queues a check result and flushes immediately.
func (r *Reporter) Report(result CheckResult) {
	r.mu.Lock()
	r.pending = append(r.pending, result)
	batch := r.pending
	r.pending = nil
	r.mu.Unlock()

	if err := r.flush(batch); err != nil {
		log.Printf("[reporter] failed to send results: %v", err)
		// Re-queue on failure
		r.mu.Lock()
		r.pending = append(batch, r.pending...)
		r.mu.Unlock()
	}
}

func (r *Reporter) flush(batch []CheckResult) error {
	if len(batch) == 0 {
		return nil
	}

	payload, err := json.Marshal(map[string]any{"results": batch})
	if err != nil {
		return fmt.Errorf("marshaling results: %w", err)
	}

	req, err := http.NewRequest("POST", r.apiURL+"/api/v1/agent/check-results", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+r.apiToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("sending results: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("API returned %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// SendHeartbeat sends a keepalive to the API.
func (r *Reporter) SendHeartbeat() error {
	payload, _ := json.Marshal(map[string]string{
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})

	req, err := http.NewRequest("POST", r.apiURL+"/api/v1/agent/heartbeat", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("creating heartbeat request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+r.apiToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("sending heartbeat: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("heartbeat returned %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// StartHeartbeat runs a heartbeat loop every interval.
func (r *Reporter) StartHeartbeat(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		// Send immediately on start
		if err := r.SendHeartbeat(); err != nil {
			log.Printf("[reporter] heartbeat failed: %v", err)
		}

		for range ticker.C {
			if err := r.SendHeartbeat(); err != nil {
				log.Printf("[reporter] heartbeat failed: %v", err)
			}
		}
	}()
}
