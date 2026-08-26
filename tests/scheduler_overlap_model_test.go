package tests

import (
	"sync"
	"testing"
	"time"

	"marine-survey-payload-window-orchestrator/internal/domain"
	"marine-survey-payload-window-orchestrator/internal/execution"
)

type overlapProbeStore struct {
	mu          sync.Mutex
	batch       domain.TrialBatch
	getCalls    int
	events      []domain.AuditEvent
	firstGet    chan struct{}
	secondGet   chan struct{}
	releaseGets chan struct{}
}

func (s *overlapProbeStore) GetBatch(string) (domain.TrialBatch, bool) {
	s.mu.Lock()
	snapshot := s.batch
	s.getCalls++
	call := s.getCalls
	s.mu.Unlock()

	if call == 1 {
		close(s.firstGet)
	}
	if call == 2 {
		close(s.secondGet)
	}
	<-s.releaseGets
	return snapshot, true
}

func (s *overlapProbeStore) UpdateBatch(batch domain.TrialBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batch = batch
	return nil
}

func (s *overlapProbeStore) Liveness(string) []domain.DeviceLiveness { return nil }

func (s *overlapProbeStore) AppendEvent(aggregateID, eventType string, _ any) (domain.AuditEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	event := domain.AuditEvent{AggregateID: aggregateID, AggregateSeq: len(s.events) + 1, EventType: eventType}
	s.events = append(s.events, event)
	return event, nil
}

func (s *overlapProbeStore) EventsAfter(aggregateID string, afterSeq int) []domain.AuditEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	var events []domain.AuditEvent
	for _, event := range s.events {
		if event.AggregateID == aggregateID && event.AggregateSeq > afterSeq {
			events = append(events, event)
		}
	}
	return events
}

type overlapProbeLister struct {
	mu         sync.Mutex
	calls      int
	secondList chan struct{}
}

func (l *overlapProbeLister) OpenBatchIDs() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	if l.calls == 2 {
		close(l.secondList)
	}
	return []string{"batch-overlap"}
}

func TestModel_SchedulerSerializesOverlappingTicks(t *testing.T) {
	cases := []struct {
		name       string
		startState domain.BatchState
	}{
		{name: "same batch cannot be read by a second tick while the first tick is unfinished", startState: domain.StateReserved},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			store := &overlapProbeStore{
				batch: domain.TrialBatch{
					ID:      "batch-overlap",
					State:   tc.startState,
					Window:  domain.TimeRange{Start: now, End: now.Add(10 * time.Minute)},
					Version: 0,
				},
				firstGet:    make(chan struct{}),
				secondGet:   make(chan struct{}),
				releaseGets: make(chan struct{}),
			}
			lister := &overlapProbeLister{secondList: make(chan struct{})}
			scheduler := execution.NewScheduler(store, lister, domain.NewManualClock(now), time.Hour)

			results := make(chan []execution.TransitionRecord, 2)
			go func() { results <- scheduler.Tick() }()
			select {
			case <-store.firstGet:
			case <-time.After(2 * time.Second):
				t.Fatal("first tick did not begin reading the batch")
			}

			go func() { results <- scheduler.Tick() }()
			select {
			case <-lister.secondList:
			case <-time.After(2 * time.Second):
				close(store.releaseGets)
				t.Fatal("second tick did not begin")
			}

			overlapped := false
			select {
			case <-store.secondGet:
				overlapped = true
			case <-time.After(250 * time.Millisecond):
			}
			close(store.releaseGets)

			var transitionCount int
			for i := 0; i < 2; i++ {
				select {
				case records := <-results:
					transitionCount += len(records)
				case <-time.After(2 * time.Second):
					t.Fatal("scheduler tick did not finish after the store was released")
				}
			}

			store.mu.Lock()
			batch := store.batch
			events := append([]domain.AuditEvent(nil), store.events...)
			store.mu.Unlock()

			if overlapped {
				t.Fatal("second tick read the same batch before the first tick completed its update")
			}
			if transitionCount != 1 || len(events) != 1 {
				t.Fatalf("expected one successful transition and one event, got transitions=%d events=%d", transitionCount, len(events))
			}
			if batch.State != domain.StateRunning || batch.Version != 1 || batch.LastEventSeq != 1 {
				t.Fatalf("batch state/version/event sequence diverged: %#v", batch)
			}
			if events[0].EventType != "SCHEDULE_RUNNING" || events[0].AggregateSeq != batch.LastEventSeq {
				t.Fatalf("event timeline does not match the batch: batch=%#v events=%#v", batch, events)
			}
		})
	}
}
