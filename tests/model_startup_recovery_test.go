package tests

import (
	"path/filepath"
	"testing"
	"time"

	"marine-survey-payload-window-orchestrator/internal/api"
	"marine-survey-payload-window-orchestrator/internal/domain"
	"marine-survey-payload-window-orchestrator/internal/persistence"
)

func TestModel_StartupRecoveryReplaysAuthoritativeEventStream(t *testing.T) {
	base := time.Date(2026, 4, 12, 8, 0, 0, 0, time.UTC)
	cases := []struct {
		name       string
		eventTypes []string
		wantState  domain.BatchState
		offline    bool
	}{
		{
			name:       "running event is ahead of reserved projection",
			eventTypes: []string{"BATCH_RESERVED", "BATCH_RUNNING"},
			wantState:  domain.StateRunning,
		},
		{
			name:       "degraded event is ahead of reserved projection",
			eventTypes: []string{"BATCH_RESERVED", "BATCH_RUNNING", "BATCH_DEGRADED"},
			wantState:  domain.StateDegraded,
			offline:    true,
		},
		{
			name:       "completed event is ahead of reserved projection",
			eventTypes: []string{"BATCH_RESERVED", "BATCH_RUNNING", "BATCH_COMPLETED"},
			wantState:  domain.StateCompleted,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "state.db")
			store, err := persistence.OpenSQLite(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			batch := domain.TrialBatch{
				ID:           "batch-crash-window",
				State:        domain.StateReserved,
				LastEventSeq: 1,
				Window: domain.TimeRange{
					Start: base,
					End:   base.Add(10 * time.Minute),
				},
			}
			if err := store.SaveBatch(batch); err != nil {
				t.Fatal(err)
			}
			for _, eventType := range tc.eventTypes {
				if _, err := store.AppendEvent(batch.ID, eventType, map[string]string{"state": eventType}); err != nil {
					t.Fatal(err)
				}
			}
			if tc.offline {
				if err := store.SaveLiveness(domain.DeviceLiveness{
					BatchID:        batch.ID,
					ResourceID:     "optional-recorder",
					LastReceivedAt: base,
					Timeout:        time.Minute,
					Critical:       false,
				}); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}

			reopened, err := persistence.OpenSQLite(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			report := api.RunStartupRecovery(reopened, domain.NewManualClock(base.Add(5*time.Minute)))
			if !report.Ready || reopened.Ready() != nil {
				t.Fatalf("startup recovery did not become ready: report=%#v readyErr=%v", report, reopened.Ready())
			}

			got, ok := reopened.GetBatch(batch.ID)
			if !ok {
				t.Fatal("batch missing after startup recovery")
			}
			if got.State != tc.wantState {
				t.Fatalf("batch projection state = %s, want authoritative event state %s", got.State, tc.wantState)
			}
			events := reopened.EventsAfter(batch.ID, 0)
			if len(events) != len(tc.eventTypes) {
				t.Fatalf("recovery appended a duplicate transition: got %d events, want %d", len(events), len(tc.eventTypes))
			}
			if got.LastEventSeq != events[len(events)-1].AggregateSeq {
				t.Fatalf("batch last_event_seq = %d, public event tail = %d", got.LastEventSeq, events[len(events)-1].AggregateSeq)
			}
			if events[len(events)-1].EventType != tc.eventTypes[len(tc.eventTypes)-1] {
				t.Fatalf("public event tail = %s, want %s", events[len(events)-1].EventType, tc.eventTypes[len(tc.eventTypes)-1])
			}
		})
	}
}
