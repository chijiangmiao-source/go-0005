package tests

import (
	"path/filepath"
	"testing"
	"time"

	"marine-survey-payload-window-orchestrator/internal/api"
	"marine-survey-payload-window-orchestrator/internal/domain"
	"marine-survey-payload-window-orchestrator/internal/persistence"
)

func TestD09SQLiteRecoveryReopensPersistedBatch(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	dbPath := filepath.Join(t.TempDir(), "state.db")
	store, err := persistence.OpenSQLite(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	batch := domain.TrialBatch{ID: "batch-recover", State: domain.StateReserved, Window: domain.TimeRange{Start: base, End: base.Add(10 * time.Minute)}, Resources: []domain.ResourceRequirement{{ResourceID: "antenna", Critical: true, Timeout: time.Minute}}}
	if err := store.SaveBatch(batch); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendEvent(batch.ID, "BATCH_RESERVED", batch); err != nil {
		t.Fatal(err)
	}
	_ = store.Close()
	reopened, err := persistence.OpenSQLite(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	report := api.RunStartupRecovery(reopened, domain.NewManualClock(base.Add(11*time.Minute)))
	if !report.Ready || report.BatchesAdvanced != 1 {
		t.Fatalf("D09 expected recovery advance, got %#v", report)
	}
	got, ok := reopened.GetBatch(batch.ID)
	if !ok || got.State != domain.StateAborted {
		t.Fatalf("expected missed start abort after recovery, got %#v ok=%v", got, ok)
	}
}

func TestRecoveryAcrossReservedRunningDegradedIsDeterministic(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, state := range []domain.BatchState{domain.StateReserved, domain.StateRunning, domain.StateDegraded} {
		store := persistence.NewMemoryStore()
		batch := domain.TrialBatch{ID: string(state), State: state, Window: domain.TimeRange{Start: base, End: base.Add(10 * time.Minute)}}
		if state != domain.StateReserved {
			batch.StartedAt = &base
		}
		if err := store.SaveBatch(batch); err != nil {
			t.Fatal(err)
		}
		report1 := store.Recover(domain.NewManualClock(base.Add(10 * time.Minute)))
		got1, _ := store.GetBatch(batch.ID)
		report2 := store.Recover(domain.NewManualClock(base.Add(10 * time.Minute)))
		got2, _ := store.GetBatch(batch.ID)
		if !report1.Ready || !report2.Ready || got1.State != got2.State {
			t.Fatalf("recovery was not deterministic for %s: %#v %#v", state, got1, got2)
		}
	}
}

func TestEventSequenceGapFailsReadiness(t *testing.T) {
	events := []domain.AuditEvent{{AggregateID: "b", AggregateSeq: 1}, {AggregateID: "b", AggregateSeq: 3}}
	if err := persistence.VerifyEventSequence(events); err == nil {
		t.Fatal("expected sequence gap error")
	}
}

func TestFinalManifestDigestStableAcrossRestarts(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	batch := domain.TrialBatch{ID: "done", State: domain.StateCompleted, Window: domain.TimeRange{Start: base, End: base.Add(10 * time.Minute)}, TerminationReason: "WINDOW_END", LastEventSeq: 1}
	events := []domain.AuditEvent{{AggregateID: "done", AggregateSeq: 1, EventType: "BATCH_COMPLETED", PayloadDigest: "p", CanonicalDigest: "c"}}
	first, err := domain.FinalManifestDigest(batch, events, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := domain.FinalManifestDigest(batch, events, nil)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("manifest digest changed across restart simulation: %s != %s", first, second)
	}
}
