package main

import (
	"flag"
	"os"
)

type config struct {
	addr string
	db   string
}

func loadConfig() config {
	addr := flag.String("addr", envDefault("ORCHESTRATOR_ADDR", ":8080"), "HTTP listen address")
	db := flag.String("db", envDefault("ORCHESTRATOR_DB", "orchestrator.db"), "SQLite database path")
	flag.Parse()
	return config{addr: *addr, db: *db}
}

func envDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
