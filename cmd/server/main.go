package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"marine-survey-payload-window-orchestrator/internal/api"
	"marine-survey-payload-window-orchestrator/internal/domain"
	"marine-survey-payload-window-orchestrator/internal/execution"
	"marine-survey-payload-window-orchestrator/internal/persistence"
)

func main() {
	cfg := loadConfig()
	store, err := persistence.OpenSQLite(cfg.db)
	if err != nil {
		log.Fatal(err)
	}
	store.Close()
	clock := domain.RealClock{}
	report := api.RunStartupRecovery(store, clock)
	if !report.Ready {
		log.Printf("startup recovery marked readiness failed: %s", report.Reason)
	}
	scheduler := execution.NewScheduler(store, store, clock, time.Second)
	go scheduler.Run(context.Background())
	server := api.NewServer(store, clock)
	log.Printf("marine survey payload window orchestrator listening on %s using %s", cfg.addr, cfg.db)
	log.Fatal(http.ListenAndServe(cfg.addr, server.Routes()))
}
