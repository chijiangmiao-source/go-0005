package execution

import (
	"context"
	"sync"
	"time"

	"marine-survey-payload-window-orchestrator/internal/domain"
)

type BatchStore interface {
	GetBatch(id string) (domain.TrialBatch, bool)
	UpdateBatch(domain.TrialBatch) error
	Liveness(batchID string) []domain.DeviceLiveness
	AppendEvent(aggregateID, eventType string, payload any) (domain.AuditEvent, error)
	EventsAfter(aggregateID string, afterSeq int) []domain.AuditEvent
}

type BatchLister interface {
	OpenBatchIDs() []string
}

type Scheduler struct {
	store   BatchStore
	lister  BatchLister
	machine *Machine
	period  time.Duration

	mu       sync.Mutex
	inflight map[string]*sync.Mutex
}

func NewScheduler(store BatchStore, lister BatchLister, clock domain.Clock, period time.Duration) *Scheduler {
	if period <= 0 {
		period = time.Second
	}
	return &Scheduler{store: store, lister: lister, machine: NewMachine(clock), period: period, inflight: map[string]*sync.Mutex{}}
}

func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.period)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			go s.Tick()
		}
	}
}

func (s *Scheduler) Tick() []TransitionRecord {
	ids := s.lister.OpenBatchIDs()
	records := make([]TransitionRecord, 0, len(ids))
	for _, id := range ids {
		if record, changed := s.AdvanceBatch(id); changed {
			records = append(records, record)
		}
	}
	return records
}

// batchMutex returns a dedicated lock for a single batch. Overlapping
// scheduler periods (each Tick runs in its own goroutine) can otherwise
// advance the same batch concurrently: both read the same stale state,
// each appends a transition event, and the second UpdateBatch upserts over
// the same Version, leaving two events against a single version bump.
// Serializing per batch keeps the background cycle from advancing the same
// batch twice in parallel while still allowing distinct batches to advance
// concurrently.
func (s *Scheduler) batchMutex(batchID string) *sync.Mutex {
	s.mu.Lock()
	mu, ok := s.inflight[batchID]
	if !ok {
		mu = &sync.Mutex{}
		s.inflight[batchID] = mu
	}
	s.mu.Unlock()
	return mu
}

func (s *Scheduler) AdvanceBatch(batchID string) (TransitionRecord, bool) {
	mu := s.batchMutex(batchID)
	mu.Lock()
	defer mu.Unlock()
	batch, ok := s.store.GetBatch(batchID)
	if !ok || batch.State.Terminal() {
		return TransitionRecord{}, false
	}
	live := s.store.Liveness(batchID)
	next, changed, reason := s.machine.Advance(batch, live)
	if !changed {
		return TransitionRecord{}, false
	}
	event, err := s.store.AppendEvent(batchID, "SCHEDULE_"+string(next.State), map[string]string{"reason": reason})
	if err != nil {
		return TransitionRecord{BatchID: batchID, From: batch.State, To: batch.State, Reason: err.Error()}, false
	}
	next.LastEventSeq = event.AggregateSeq
	if next.State == domain.StateCompleted || next.State == domain.StateAborted {
		next.FinalManifestDigest, _ = domain.FinalManifestDigest(next, s.store.EventsAfter(batchID, 0), live)
	}
	if err := s.store.UpdateBatch(next); err != nil {
		return TransitionRecord{BatchID: batchID, From: batch.State, To: batch.State, Reason: err.Error()}, false
	}
	return TransitionRecord{BatchID: batchID, From: batch.State, To: next.State, Reason: reason, EventSeq: event.AggregateSeq}, true
}
