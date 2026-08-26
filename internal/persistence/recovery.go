package persistence

import (
	"encoding/json"
	"fmt"
	"time"

	"marine-survey-payload-window-orchestrator/internal/domain"
	"marine-survey-payload-window-orchestrator/internal/execution"
)

type RecoveryReport struct {
	Ready           bool      `json:"ready"`
	CheckedAt       time.Time `json:"checked_at"`
	Aggregates      int       `json:"aggregates"`
	Events          int       `json:"events"`
	BatchesAdvanced int       `json:"batches_advanced"`
	Reason          string    `json:"reason,omitempty"`
}

func (s *SQLiteStore) Recover(clock domain.Clock) RecoveryReport {
	s.mu.Lock()
	defer s.mu.Unlock()
	report := RecoveryReport{Ready: true, CheckedAt: domain.NormalizeTime(clock.Now())}
	ids, err := s.aggregateIDsLocked()
	if err != nil {
		return s.failRecoveryLocked(report, err)
	}
	for _, id := range ids {
		events, err := s.eventsLocked(id)
		if err != nil {
			return s.failRecoveryLocked(report, err)
		}
		if err := VerifyEventSequence(events); err != nil {
			return s.failRecoveryLocked(report, err)
		}
		report.Aggregates++
		report.Events += len(events)
	}
	machine := execution.NewMachine(clock)
	batches, err := s.batchesLocked()
	if err != nil {
		return s.failRecoveryLocked(report, err)
	}
	for _, batch := range batches {
		// The event stream is authoritative. A crash may have persisted a
		// state event for this batch before the batch row was updated, so the
		// row read above can be behind the recorded events. Replay the
		// continuous event stream to rebuild the true projection before any
		// timed action is derived; otherwise a stale row would drive Advance
		// to emit a duplicate or out-of-order recovery transition.
		events, err := s.eventsLocked(batch.ID)
		if err != nil {
			return s.failRecoveryLocked(report, err)
		}
		if state, ok := ReplayProjection(events); ok {
			if state != batch.State {
				// The row is behind the event stream: a crash persisted a
				// state transition event before the batch row was updated.
				// Rebuild the true projection from the authoritative events.
				batch.State = state
				batch.LastEventSeq = lastEventSeqOf(events)
				if state.Terminal() && batch.FinalManifestDigest == "" {
					batch.FinalManifestDigest, _ = domain.FinalManifestDigest(batch, events, nil)
				}
				if err := s.saveBatchLocked(batch); err != nil {
					return s.failRecoveryLocked(report, err)
				}
				report.BatchesAdvanced++
			} else if lastEventSeqOf(events) > batch.LastEventSeq {
				// The state already matches, but the row's event pointer is
				// behind a non-state event (e.g. telemetry acknowledgement).
				// Catch the pointer up without counting a state advance.
				batch.LastEventSeq = lastEventSeqOf(events)
				if err := s.saveBatchLocked(batch); err != nil {
					return s.failRecoveryLocked(report, err)
				}
			}
		}
		if batch.State.Terminal() {
			continue
		}
		live, err := s.livenessLocked(batch.ID)
		if err != nil {
			return s.failRecoveryLocked(report, err)
		}
		next, changed, reason := machine.Advance(batch, live)
		if !changed {
			continue
		}
		if reason == "WINDOW_END" {
			next.FinalManifestDigest, _ = domain.FinalManifestDigest(next, s.eventsAfterLocked(next.ID, 0), live)
		}
		event, err := s.appendEventLocked(batch.ID, "RECOVERY_"+string(next.State), map[string]string{"reason": reason}, report.CheckedAt)
		if err != nil {
			return s.failRecoveryLocked(report, err)
		}
		next.LastEventSeq = event.AggregateSeq
		if err := s.saveBatchLocked(next); err != nil {
			return s.failRecoveryLocked(report, err)
		}
		report.BatchesAdvanced++
	}
	body, _ := json.Marshal(report)
	_, err = s.db.Exec(`insert into recovery_checkpoints(id,projected_at,last_result) values(1,?,?)
		on conflict(id) do update set projected_at=excluded.projected_at,last_result=excluded.last_result`,
		micros(report.CheckedAt), string(body))
	if err != nil {
		return s.failRecoveryLocked(report, err)
	}
	s.readyErr = nil
	return report
}

func (s *SQLiteStore) failRecoveryLocked(report RecoveryReport, err error) RecoveryReport {
	report.Ready = false
	report.Reason = err.Error()
	s.readyErr = err
	body, _ := json.Marshal(report)
	_, _ = s.db.Exec(`insert into recovery_checkpoints(id,projected_at,last_result) values(1,?,?)
		on conflict(id) do update set projected_at=excluded.projected_at,last_result=excluded.last_result`,
		micros(report.CheckedAt), string(body))
	return report
}

func (s *SQLiteStore) aggregateIDsLocked() ([]string, error) {
	rows, err := s.db.Query(`select distinct aggregate_id from audit_events order by aggregate_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *SQLiteStore) eventsLocked(id string) ([]domain.AuditEvent, error) {
	rows, err := s.db.Query(`select json_body from audit_events where aggregate_id=? order by aggregate_seq`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.AuditEvent
	for rows.Next() {
		var body string
		var event domain.AuditEvent
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(body), &event); err != nil {
			return nil, fmt.Errorf("decode event %s: %w", id, err)
		}
		out = append(out, event)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) batchesLocked() ([]domain.TrialBatch, error) {
	rows, err := s.db.Query(`select json_body from batches order by id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.TrialBatch
	for rows.Next() {
		var body string
		var batch domain.TrialBatch
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(body), &batch); err != nil {
			return nil, err
		}
		out = append(out, batch)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) livenessLocked(batchID string) ([]domain.DeviceLiveness, error) {
	rows, err := s.db.Query(`select json_body from liveness where batch_id=? order by resource_id`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.DeviceLiveness
	for rows.Next() {
		var body string
		var live domain.DeviceLiveness
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(body), &live); err != nil {
			return nil, err
		}
		out = append(out, live)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) eventsAfterLocked(id string, after int) []domain.AuditEvent {
	events, err := s.eventsLocked(id)
	if err != nil {
		return nil
	}
	out := events[:0]
	for _, event := range events {
		if event.AggregateSeq > after {
			out = append(out, event)
		}
	}
	return out
}
