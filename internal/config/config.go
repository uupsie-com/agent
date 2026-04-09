package config

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Monitor struct {
	ID              string            `json:"id"`
	Type            string            `json:"type"`
	Name            string            `json:"name"`
	Config          map[string]any    `json:"config"`
	Namespace       string            `json:"namespace"`
	Resource        string            `json:"resource"`
	IntervalSeconds int               `json:"interval_seconds"`
	TimeoutSeconds  int               `json:"timeout_seconds"`
}
// ConfigString returns a config value as a string, or empty string if missing/wrong type.
func (m Monitor) ConfigString(key string) string {
	v, ok := m.Config[key]
	if !ok {
		return ""
	}
	switch s := v.(type) {
	case string:
		return s
	case float64:
		return fmt.Sprintf("%v", s)
	default:
		return fmt.Sprintf("%v", s)
	}
}
type AgentConfig struct {
	Monitors []Monitor `json:"monitors"`
}

type Client struct {
	apiURL     string
	apiToken   string
	httpClient *http.Client
}

func NewClient(apiURL, apiToken string) *Client {
	return &Client{
		apiURL:   apiURL,
		apiToken: apiToken,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *Client) FetchConfig() (*AgentConfig, error) {
	req, err := http.NewRequest("GET", c.apiURL+"/api/v1/agent/config", nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiToken)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching config: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("config endpoint returned %d: %s", resp.StatusCode, string(body))
	}

	var cfg AgentConfig
	if err := json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decoding config: %w", err)
	}
	return &cfg, nil
}
