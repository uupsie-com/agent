package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/uupsie-com/agent/internal/config"
	"github.com/uupsie-com/agent/internal/reporter"
	"github.com/uupsie-com/agent/internal/watcher"
)

func main() {
	apiURL := os.Getenv("AGENT_API_URL")
	apiToken := os.Getenv("AGENT_API_TOKEN")

	if apiURL == "" || apiToken == "" {
		fmt.Fprintln(os.Stderr, "AGENT_API_URL and AGENT_API_TOKEN must be set")
		os.Exit(1)
	}

	log.Printf("agent starting — reporting to %s", apiURL)

	// Config client
	cfgClient := config.NewClient(apiURL, apiToken)

	// Reporter (sends check results + heartbeats)
	rep := reporter.New(apiURL, apiToken)
	rep.StartHeartbeat(30 * time.Second)

	// Watcher manager (Kubernetes informers)
	mgr, err := watcher.NewManager(rep)
	if err != nil {
		log.Fatalf("failed to create watcher manager: %v", err)
	}

	// Initial config fetch
	cfg, err := cfgClient.FetchConfig()
	if err != nil {
		log.Fatalf("failed to fetch initial config: %v", err)
	}
	log.Printf("loaded %d monitors from API", len(cfg.Monitors))
	mgr.Reconcile(cfg.Monitors)

	// Config refresh loop (every 5 minutes)
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			newCfg, err := cfgClient.FetchConfig()
			if err != nil {
				log.Printf("[config] refresh failed: %v", err)
				continue
			}
			log.Printf("[config] refreshed: %d monitors", len(newCfg.Monitors))
			mgr.Reconcile(newCfg.Monitors)
		}
	}()

	// Wait for shutdown signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("shutting down...")
	mgr.Stop()
}
