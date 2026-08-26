package tests

import (
	"testing"
	"time"

	"marine-survey-payload-window-orchestrator/internal/domain"
	"marine-survey-payload-window-orchestrator/internal/execution"
	"marine-survey-payload-window-orchestrator/internal/persistence"
)

func TestD06IllegalStateTransitionHasNoSideEffect(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	batch := domain.TrialBatch{ID: "b", State: domain.StateReserved, Version: 7, Window: domain.TimeRange{Start: base, End: base.Add(10 * time.Minute)}}
	_, err := execution.NewMachine(domain.NewManualClock(base)).Transition(batch, domain.StateCompleted, "illegal")
	if err == nil {
		t.Fatal("D06 expected illegal RESERVED to COMPLETED rejection")
	}
	if batch.State != domain.StateReserved || batch.Version != 7 {
		t.Fatalf("transition mutated input: %#v", batch)
	}
}

func TestReservedRunningCompletedPath(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := domain.NewManualClock(base)
	m := execution.NewMachine(clock)
	batch := domain.TrialBatch{ID: "b", State: domain.StateReserved, Window: domain.TimeRange{Start: base, End: base.Add(10 * time.Minute)}}
	running, changed, _ := m.Advance(batch, []domain.DeviceLiveness{{BatchID: "b", ResourceID: "antenna", Critical: true, Timeout: time.Minute, LastReceivedAt: base}})
	if !changed || running.State != domain.StateRunning || running.StartedAt == nil {
		t.Fatalf("expected running start, got %#v changed=%v", running, changed)
	}
	clock.Set(base.Add(10 * time.Minute))
	done, changed, reason := m.Advance(running, nil)
	if !changed || done.State != domain.StateCompleted || reason != "WINDOW_END" {
		t.Fatalf("expected completed at window end, got %#v %s", done, reason)
	}
}

func TestReservedWithMissingOptionalStartsDegraded(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	batch := domain.TrialBatch{ID: "b", State: domain.StateReserved, Window: domain.TimeRange{Start: base, End: base.Add(10 * time.Minute)}}
	live := []domain.DeviceLiveness{{BatchID: "b", ResourceID: "recorder", Critical: false, Timeout: time.Minute}}
	next, changed, reason := execution.NewMachine(domain.NewManualClock(base)).Advance(batch, live)
	if !changed || next.State != domain.StateDegraded || reason != "OPTIONAL_DEVICE_MISSING" {
		t.Fatalf("expected degraded for missing optional, got %#v %s", next, reason)
	}
}

func TestOptionalDeviceLossAndRecovery(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := domain.NewManualClock(base.Add(2 * time.Minute))
	m := execution.NewMachine(clock)
	batch := domain.TrialBatch{ID: "b", State: domain.StateRunning, Window: domain.TimeRange{Start: base, End: base.Add(10 * time.Minute)}}
	live := []domain.DeviceLiveness{{BatchID: "b", ResourceID: "recorder", Critical: false, Timeout: time.Minute, LastReceivedAt: base}}
	degraded, changed, _ := m.Advance(batch, live)
	if !changed || degraded.State != domain.StateDegraded {
		t.Fatalf("expected optional timeout degraded, got %#v", degraded)
	}
	live[0].LastReceivedAt = clock.Now()
	recovered, changed, reason := m.Advance(degraded, live)
	if !changed || recovered.State != domain.StateRunning || reason != "OPTIONAL_DEVICE_RECOVERED" {
		t.Fatalf("expected optional recovery to running, got %#v %s", recovered, reason)
	}
}

func TestD08TimeoutEqualityOnlineAndOverageAbortsCritical(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := domain.NewManualClock(base.Add(time.Minute))
	m := execution.NewMachine(clock)
	batch := domain.TrialBatch{ID: "b", State: domain.StateRunning, Window: domain.TimeRange{Start: base, End: base.Add(10 * time.Minute)}}
	live := []domain.DeviceLiveness{{BatchID: "b", ResourceID: "antenna", Critical: true, Timeout: time.Minute, LastReceivedAt: base}}
	if next, changed, _ := m.Advance(batch, live); changed || next.State != domain.StateRunning {
		t.Fatalf("D08 equality timeout should remain online, got %#v", next)
	}
	clock.Advance(time.Microsecond)
	next, changed, reason := m.Advance(batch, live)
	if !changed || next.State != domain.StateAborted || reason != "CRITICAL_DEVICE_TIMEOUT" {
		t.Fatalf("D08 expected critical timeout abort, got %#v %s", next, reason)
	}
}

func TestWindowEndWinsOverSameInstantTimeout(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	batch := domain.TrialBatch{ID: "b", State: domain.StateRunning, StartedAt: &base, Window: domain.TimeRange{Start: base, End: base.Add(10 * time.Minute)}}
	live := []domain.DeviceLiveness{{BatchID: "b", ResourceID: "antenna", Critical: true, Timeout: time.Minute, LastReceivedAt: base.Add(9 * time.Minute)}}
	next, changed, reason := execution.NewMachine(domain.NewManualClock(batch.Window.End)).Advance(batch, live)
	if !changed || next.State != domain.StateCompleted || reason != "WINDOW_END" {
		t.Fatalf("window end must win over timeout, got %#v %s", next, reason)
	}
}

func TestSchedulerPersistsOneTransitionEvent(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store := persistence.NewMemoryStore()
	batch := domain.TrialBatch{ID: "sched", State: domain.StateReserved, Window: domain.TimeRange{Start: base, End: base.Add(10 * time.Minute)}, Resources: []domain.ResourceRequirement{{ResourceID: "antenna", Critical: true, Timeout: time.Minute}}}
	if err := store.SaveBatch(batch); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveLiveness(domain.DeviceLiveness{BatchID: "sched", ResourceID: "antenna", Critical: true, Timeout: time.Minute, LastReceivedAt: base}); err != nil {
		t.Fatal(err)
	}
	records := execution.NewScheduler(store, store, domain.NewManualClock(base), time.Hour).Tick()
	if len(records) != 1 || records[0].To != domain.StateRunning {
		t.Fatalf("expected one scheduler transition to running, got %#v", records)
	}
}
