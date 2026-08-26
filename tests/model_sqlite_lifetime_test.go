package tests

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"marine-survey-payload-window-orchestrator/internal/domain"
	"marine-survey-payload-window-orchestrator/internal/persistence"
)

func TestModel_SQLiteConnectionRemainsUsableForServerLifetime(t *testing.T) {
	root := filepath.Dir(filepath.Dir(currentFile(t)))
	serverName := "orchestrator-server"
	if runtime.GOOS == "windows" {
		serverName += ".exe"
	}
	serverPath := filepath.Join(t.TempDir(), serverName)
	build := exec.Command("go", "build", "-o", serverPath, "./cmd/server")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build server: %v\n%s", err, output)
	}

	dbPath := filepath.Join(t.TempDir(), "service.db")
	store, err := persistence.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("create sqlite database: %v", err)
	}
	wantResource := domain.ResourceSpec{ResourceID: "survey-winch", Capacity: 3, Unit: "channels"}
	store.SetResource(wantResource)
	if err := store.Ready(); err != nil {
		t.Fatalf("seed sqlite database: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close seeded sqlite database: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve server address: %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release server address: %v", err)
	}

	server := exec.Command(serverPath, "-addr", addr, "-db", dbPath)
	if err := server.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() {
		_ = server.Process.Kill()
		_ = server.Wait()
	})

	client := &http.Client{Timeout: 2 * time.Second}
	baseURL := "http://" + addr
	deadline := time.Now().Add(10 * time.Second)
	for {
		response, requestErr := client.Get(baseURL + "/health/live")
		if requestErr == nil {
			_ = response.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("server did not begin listening: %v", requestErr)
		}
		time.Sleep(25 * time.Millisecond)
	}

	cases := []struct {
		name  string
		delay time.Duration
		path  string
		check func(int, []byte) error
	}{
		{
			name: "ready_after_startup_recovery",
			path: "/health/ready",
			check: func(status int, body []byte) error {
				if status != http.StatusOK {
					return fmt.Errorf("status = %d, want 200; body=%s", status, body)
				}
				return nil
			},
		},
		{
			name: "resource_endpoint_reads_persisted_data",
			path: "/api/v1/resources",
			check: func(status int, body []byte) error {
				if status != http.StatusOK {
					return fmt.Errorf("status = %d, want 200; body=%s", status, body)
				}
				var payload struct {
					Resources map[string]domain.ResourceSpec `json:"resources"`
				}
				if err := json.Unmarshal(body, &payload); err != nil {
					return fmt.Errorf("decode response: %w", err)
				}
				if got, ok := payload.Resources[wantResource.ResourceID]; !ok || got != wantResource {
					return fmt.Errorf("persisted resource = %+v (present=%v), want %+v", got, ok, wantResource)
				}
				return nil
			},
		},
		{
			name:  "ready_after_scheduler_tick",
			delay: 1200 * time.Millisecond,
			path:  "/health/ready",
			check: func(status int, body []byte) error {
				if status != http.StatusOK {
					return fmt.Errorf("status = %d, want 200; body=%s", status, body)
				}
				return nil
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			time.Sleep(tc.delay)
			response, err := client.Get(baseURL + tc.path)
			if err != nil {
				t.Fatalf("GET %s: %v", tc.path, err)
			}
			body, readErr := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if readErr != nil {
				t.Fatalf("read %s response: %v", tc.path, readErr)
			}
			if err := tc.check(response.StatusCode, body); err != nil {
				t.Fatal(err)
			}
		})
	}
}
