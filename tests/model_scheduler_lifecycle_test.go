package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"marine-survey-payload-window-orchestrator/internal/domain"
	"marine-survey-payload-window-orchestrator/internal/execution"
	"marine-survey-payload-window-orchestrator/internal/persistence"
)

func TestModel_ProductionSchedulerLifecycle(t *testing.T) {
	cases := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "run exits when its caller cancels",
			run: func(t *testing.T) {
				store := persistence.NewMemoryStore()
				scheduler := execution.NewScheduler(store, store, domain.RealClock{}, time.Hour)
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan struct{})
				go func() {
					scheduler.Run(ctx)
					close(done)
				}()
				cancel()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("Scheduler.Run did not exit promptly after context cancellation")
				}
			},
		},
		{
			name: "production server keeps ticking SQLite batches",
			run: func(t *testing.T) {
				root := filepath.Dir(filepath.Dir(currentFile(t)))
				executable := filepath.Join(t.TempDir(), "orchestrator")
				if runtime.GOOS == "windows" {
					executable += ".exe"
				}
				build := exec.Command("go", "build", "-o", executable, "./cmd/server")
				build.Dir = root
				if output, err := build.CombinedOutput(); err != nil {
					t.Fatalf("build production server: %v\n%s", err, output)
				}

				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				addr := listener.Addr().String()
				if err := listener.Close(); err != nil {
					t.Fatal(err)
				}

				dbPath := filepath.Join(t.TempDir(), "live.db")
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var logs bytes.Buffer
				server := exec.CommandContext(ctx, executable, "-addr", addr, "-db", dbPath)
				server.Stdout = &logs
				server.Stderr = &logs
				if err := server.Start(); err != nil {
					t.Fatal(err)
				}
				exited := make(chan error, 1)
				go func() { exited <- server.Wait() }()
				defer func() {
					cancel()
					select {
					case <-exited:
					case <-time.After(3 * time.Second):
						t.Errorf("production server did not stop; logs:\n%s", logs.String())
					}
				}()

				client := &http.Client{Timeout: 500 * time.Millisecond}
				baseURL := "http://" + addr
				readyDeadline := time.Now().Add(10 * time.Second)
				for {
					response, requestErr := client.Get(baseURL + "/health/live")
					if requestErr == nil {
						_ = response.Body.Close()
						if response.StatusCode == http.StatusOK {
							break
						}
					}
					select {
					case err := <-exited:
						t.Fatalf("production server exited during startup: %v\n%s", err, logs.String())
					default:
					}
					if time.Now().After(readyDeadline) {
						t.Fatalf("production server did not become live; logs:\n%s", logs.String())
					}
					time.Sleep(25 * time.Millisecond)
				}

				store, err := persistence.OpenSQLite(dbPath)
				if err != nil {
					t.Fatal(err)
				}
				batchID := "inserted-after-startup-recovery"
				event, err := store.AppendEvent(batchID, "BATCH_RESERVED", map[string]string{"source": "acceptance-test"})
				if err != nil {
					_ = store.Close()
					t.Fatal(err)
				}
				batch := domain.TrialBatch{
					ID:           batchID,
					State:        domain.StateReserved,
					Version:      7,
					LastEventSeq: event.AggregateSeq,
					Window: domain.TimeRange{
						Start: time.Now().Add(-10 * time.Minute),
						End:   time.Now().Add(-5 * time.Minute),
					},
				}
				if err := store.SaveBatch(batch); err != nil {
					_ = store.Close()
					t.Fatal(err)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}

				type batchResponse struct {
					Batch  domain.TrialBatch   `json:"batch"`
					Events []domain.AuditEvent `json:"events"`
				}
				advanceDeadline := time.Now().Add(4 * time.Second)
				for {
					response, requestErr := client.Get(fmt.Sprintf("%s/api/v1/batches/%s", baseURL, batchID))
					var got batchResponse
					if requestErr == nil {
						decodeErr := json.NewDecoder(response.Body).Decode(&got)
						_ = response.Body.Close()
						if response.StatusCode == http.StatusOK && decodeErr == nil && got.Batch.State == domain.StateAborted {
							if got.Batch.Version != 8 || got.Batch.TerminationReason != "MISSED_START_WINDOW" || got.Batch.TerminatedAt == nil {
								t.Fatalf("persisted aborted batch is incomplete: %#v", got.Batch)
							}
							if got.Batch.LastEventSeq != 2 || len(got.Events) != 2 || got.Events[1].EventType != "SCHEDULE_ABORTED" || got.Events[1].AggregateSeq != 2 {
								t.Fatalf("missing persisted scheduling event: batch=%#v events=%#v", got.Batch, got.Events)
							}
							break
						}
					}
					if time.Now().After(advanceDeadline) {
						t.Fatalf("live production scheduler never aborted the expired RESERVED batch; last response=%#v requestErr=%v logs:\n%s", got, requestErr, logs.String())
					}
					time.Sleep(50 * time.Millisecond)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, tc.run)
	}
}
