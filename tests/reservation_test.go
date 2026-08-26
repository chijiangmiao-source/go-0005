package tests

import (
	"sync"
	"testing"
	"time"

	"marine-survey-payload-window-orchestrator/internal/domain"
	"marine-survey-payload-window-orchestrator/internal/persistence"
	"marine-survey-payload-window-orchestrator/internal/reservation"
)

func TestD04OverlappingReservationsAccumulateCapacity(t *testing.T) {
	store := persistence.NewMemoryStore()
	store.SetResource(domain.ResourceSpec{ResourceID: "recorder", Capacity: 2, Unit: "channel"})
	reserver := reservation.NewReserver(store)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	req := []domain.ResourceRequirement{{ResourceID: "recorder", Quantity: 1, Timeout: time.Minute}}
	for _, id := range []string{"b1", "b2"} {
		if _, conflict, err := reserver.Reserve(id, domain.TimeRange{Start: base, End: base.Add(10 * time.Minute)}, req); err != nil || conflict != nil {
			t.Fatalf("reserve %s failed conflict=%v err=%v", id, conflict, err)
		}
	}
	if _, conflict, err := reserver.Reserve("b3", domain.TimeRange{Start: base.Add(time.Minute), End: base.Add(3 * time.Minute)}, req); err != nil || conflict == nil {
		t.Fatalf("D04 expected accumulated capacity conflict, got conflict=%v err=%v", conflict, err)
	}
}

func TestAdjacentWindowsDoNotConflict(t *testing.T) {
	store := persistence.NewMemoryStore()
	reserver := reservation.NewReserver(store)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	req := []domain.ResourceRequirement{{ResourceID: "antenna", Quantity: 1, Critical: true, Timeout: time.Minute}}
	if _, conflict, _ := reserver.Reserve("a", domain.TimeRange{Start: base, End: base.Add(10 * time.Minute)}, req); conflict != nil {
		t.Fatal(conflict)
	}
	if _, conflict, err := reserver.Reserve("b", domain.TimeRange{Start: base.Add(10 * time.Minute), End: base.Add(20 * time.Minute)}, req); err != nil || conflict != nil {
		t.Fatalf("adjacent window should be allowed conflict=%v err=%v", conflict, err)
	}
}

func TestConcurrentApplicationsOnlyOneGetsRemainingCapacity(t *testing.T) {
	store := persistence.NewMemoryStore()
	reserver := reservation.NewReserver(store)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	req := []domain.ResourceRequirement{{ResourceID: "antenna", Quantity: 1, Critical: true, Timeout: time.Minute}}
	var wg sync.WaitGroup
	success := 0
	conflicts := 0
	var mu sync.Mutex
	for _, id := range []string{"c1", "c2"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			_, conflict, err := reserver.Reserve(id, domain.TimeRange{Start: base, End: base.Add(10 * time.Minute)}, req)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if conflict == nil {
				success++
			} else {
				conflicts++
			}
		}(id)
	}
	wg.Wait()
	if success != 1 || conflicts != 1 {
		t.Fatalf("expected one success and one conflict, got success=%d conflicts=%d", success, conflicts)
	}
}

func TestD05InjectedReservationFaultLeavesNoOrphan(t *testing.T) {
	store := persistence.NewMemoryStore()
	reserver := reservation.NewReserverWithFaults(store, reservation.FaultInjector{Point: reservation.FaultAfterReservation})
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	req := []domain.ResourceRequirement{{ResourceID: "antenna", Quantity: 1, Critical: true, Timeout: time.Minute}}
	if _, _, err := reserver.Reserve("fault", domain.TimeRange{Start: base, End: base.Add(10 * time.Minute)}, req); err != reservation.ErrInjectedCommit {
		t.Fatalf("expected injected commit error, got %v", err)
	}
	if got := store.Reservations(); len(got) != 0 {
		t.Fatalf("expected rollback to remove orphan reservations, got %#v", got)
	}
}
