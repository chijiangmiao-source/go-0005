package persistence

import (
	"fmt"

	"marine-survey-payload-window-orchestrator/internal/domain"
	"marine-survey-payload-window-orchestrator/internal/execution"
)

func (s *MemoryStore) Recover(clock domain.Clock) RecoveryReport {
	s.mu.Lock()
	ids := make([]string, 0, len(s.events))
	for id := range s.events {
		ids = append(ids, id)
	}
	report := RecoveryReport{Ready: true, CheckedAt: domain.NormalizeTime(clock.Now()), Aggregates: len(ids)}
	for _, id := range ids {
		events := append([]domain.AuditEvent(nil), s.events[id]...)
		report.Events += len(events)
		if err := VerifyEventSequence(events); err != nil {
			s.readyErr = err
			report.Ready = false
			report.Reason = err.Error()
			s.mu.Unlock()
			return report
		}
	}
	open := make([]domain.TrialBatch, 0, len(s.batches))
	for _, batch := range s.batches {
		if !batch.State.Terminal() {
			open = append(open, batch)
		}
	}
	s.mu.Unlock()

	machine := execution.NewMachine(clock)
	for _, batch := range open {
		live := s.Liveness(batch.ID)
		next, changed, reason := machine.Advance(batch, live)
		if !changed {
			continue
		}
		event, err := s.AppendEvent(batch.ID, "RECOVERY_"+string(next.State), map[string]string{"reason": reason})
		if err != nil {
			s.SetReadyError(err)
			report.Ready = false
			report.Reason = err.Error()
			return report
		}
		next.LastEventSeq = event.AggregateSeq
		if next.State.Terminal() {
			next.FinalManifestDigest, _ = domain.FinalManifestDigest(next, s.EventsAfter(next.ID, 0), live)
		}
		if err := s.UpdateBatch(next); err != nil {
			err = fmt.Errorf("recover batch %s: %w", next.ID, err)
			s.SetReadyError(err)
			report.Ready = false
			report.Reason = err.Error()
			return report
		}
		report.BatchesAdvanced++
	}
	s.SetReadyError(nil)
	return report
}
