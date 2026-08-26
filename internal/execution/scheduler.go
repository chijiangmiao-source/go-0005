package execution

import (
	"context"
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
}

func NewScheduler(store BatchStore, lister BatchLister, clock domain.Clock, period time.Duration) *Scheduler {
	if period <= 0 {
		period = time.Second
	}
	return &Scheduler{store: store, lister: lister, machine: NewMachine(clock), period: period}
}

func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.period)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.Tick()
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

func (s *Scheduler) AdvanceBatch(batchID string) (TransitionRecord, bool) {
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
	if err := s.store.UpdateBatch(next); err != nil {
		return TransitionRecord{BatchID: batchID, From: batch.State, To: batch.State, Reason: err.Error()}, false
	}
	if next.State == domain.StateCompleted || next.State == domain.StateAborted {
		next.FinalManifestDigest, _ = domain.FinalManifestDigest(next, s.store.EventsAfter(batchID, 0), live)
	}
	return TransitionRecord{BatchID: batchID, From: batch.State, To: next.State, Reason: reason, EventSeq: event.AggregateSeq}, true
}
