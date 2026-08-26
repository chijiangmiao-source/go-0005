package tests

import (
	"errors"
	"testing"
	"time"

	"marine-survey-payload-window-orchestrator/internal/domain"
	"marine-survey-payload-window-orchestrator/internal/persistence"
	"marine-survey-payload-window-orchestrator/internal/reservation"
)

type reservationSaveFailureStore struct {
	*persistence.MemoryStore
	failAt    int
	saveCalls int
}

func (s *reservationSaveFailureStore) SaveReservation(res domain.Reservation) error {
	s.saveCalls++
	if s.failAt > 0 && s.saveCalls == s.failAt {
		return errors.New("injected reservation save failure")
	}
	return s.MemoryStore.SaveReservation(res)
}

func TestModel_ReservationBatchIsAtomicOnPersistenceFailure(t *testing.T) {
	cases := []struct {
		name       string
		failAtSave int
		fault      reservation.FaultPoint
	}{
		{name: "second reservation save fails", failAtSave: 2},
		{name: "later reservation save fails", failAtSave: 3},
		{name: "commit stage fails after every save", fault: reservation.FaultAfterReservation},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &reservationSaveFailureStore{
				MemoryStore: persistence.NewMemoryStore(),
				failAt:      tc.failAtSave,
			}
			store.SetResource(domain.ResourceSpec{ResourceID: "decoder", Capacity: 1, Unit: "channel"})
			window := domain.TimeRange{
				Start: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
				End:   time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC),
			}
			requirements := []domain.ResourceRequirement{
				{ResourceID: "antenna", Quantity: 1, Timeout: time.Minute},
				{ResourceID: "recorder", Quantity: 1, Timeout: time.Minute},
				{ResourceID: "decoder", Quantity: 1, Timeout: time.Minute},
			}

			reserver := reservation.NewReserverWithFaults(store, reservation.FaultInjector{Point: tc.fault})
			if _, conflict, err := reserver.Reserve("failed-batch", window, requirements); err == nil || conflict != nil {
				t.Fatalf("expected persistence error without a capacity conflict, conflict=%v err=%v", conflict, err)
			}
			if got := store.Reservations(); len(got) != 0 {
				t.Fatalf("failed batch left active reservations: %#v", got)
			}

			store.failAt = 0
			retry := reservation.NewReserver(store)
			got, conflict, err := retry.Reserve("retry-batch", window, requirements)
			if err != nil || conflict != nil || len(got) != len(requirements) {
				t.Fatalf("retry should acquire the fully released window: reservations=%d conflict=%v err=%v", len(got), conflict, err)
			}
		})
	}
}
