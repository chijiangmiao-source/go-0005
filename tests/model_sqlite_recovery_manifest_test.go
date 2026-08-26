package tests

import (
	"path/filepath"
	"testing"
	"time"

	"marine-survey-payload-window-orchestrator/internal/domain"
	"marine-survey-payload-window-orchestrator/internal/persistence"
)

func TestModel_SQLiteRecoveryFinalManifestIncludesTerminalEvent(t *testing.T) {
	base := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	cases := []struct {
		name          string
		state         domain.BatchState
		started       bool
		wantState     domain.BatchState
		wantEventType string
	}{
		{name: "running batch completes", state: domain.StateRunning, started: true, wantState: domain.StateCompleted, wantEventType: "RECOVERY_COMPLETED"},
		{name: "reserved batch aborts", state: domain.StateReserved, wantState: domain.StateAborted, wantEventType: "RECOVERY_ABORTED"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "recovery.db")
			store, err := persistence.OpenSQLite(dbPath)
			if err != nil {
				t.Fatal(err)
			}

			batch := domain.TrialBatch{
				ID:            "batch-terminal-recovery",
				ApplicationID: "application-1",
				OrbitDigest:   "orbit-digest",
				SeaDigest:     "sea-digest",
				Window:        domain.TimeRange{Start: base, End: base.Add(10 * time.Minute)},
				State:         tc.state,
				Version:       3,
				Resources: []domain.ResourceRequirement{{
					ResourceID: "recorder", Quantity: 1, Timeout: 2 * time.Minute,
				}},
			}
			if tc.started {
				startedAt := base
				batch.StartedAt = &startedAt
			}
			seed, err := store.AppendEvent(batch.ID, "BATCH_PERSISTED_BEFORE_OUTAGE", map[string]string{"state": string(tc.state)})
			if err != nil {
				t.Fatal(err)
			}
			batch.LastEventSeq = seed.AggregateSeq
			if err := store.SaveBatch(batch); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}

			clock := domain.NewManualClock(base.Add(11 * time.Minute))
			store, err = persistence.OpenSQLite(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			report := store.Recover(clock)
			if !report.Ready || report.BatchesAdvanced != 1 {
				t.Fatalf("terminal recovery failed: %#v", report)
			}
			got, ok := store.GetBatch(batch.ID)
			if !ok {
				t.Fatal("recovered batch not found")
			}
			events := store.EventsAfter(batch.ID, 0)
			if got.State != tc.wantState {
				t.Fatalf("state = %s, want %s", got.State, tc.wantState)
			}
			if len(events) != 2 || events[len(events)-1].EventType != tc.wantEventType {
				t.Fatalf("terminal event timeline = %#v, want final event %s", events, tc.wantEventType)
			}
			if got.LastEventSeq != events[len(events)-1].AggregateSeq {
				t.Fatalf("LastEventSeq = %d, terminal event sequence = %d", got.LastEventSeq, events[len(events)-1].AggregateSeq)
			}
			wantDigest, err := domain.FinalManifestDigest(got, events, store.Liveness(batch.ID))
			if err != nil {
				t.Fatal(err)
			}
			if got.FinalManifestDigest != wantDigest {
				t.Fatalf("final manifest digest = %s, want digest over recovered terminal state and complete event timeline %s", got.FinalManifestDigest, wantDigest)
			}
			stableDigest := got.FinalManifestDigest
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}

			for restart := 1; restart <= 2; restart++ {
				store, err = persistence.OpenSQLite(dbPath)
				if err != nil {
					t.Fatal(err)
				}
				report = store.Recover(clock)
				if !report.Ready || report.BatchesAdvanced != 0 {
					t.Fatalf("restart %d unexpectedly advanced terminal batch: %#v", restart, report)
				}
				got, ok = store.GetBatch(batch.ID)
				if !ok || got.FinalManifestDigest != stableDigest || got.LastEventSeq != 2 {
					t.Fatalf("restart %d changed finalized batch: %#v", restart, got)
				}
				if events = store.EventsAfter(batch.ID, 0); len(events) != 2 {
					t.Fatalf("restart %d changed terminal event timeline: %#v", restart, events)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
