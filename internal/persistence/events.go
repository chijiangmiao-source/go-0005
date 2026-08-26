package persistence

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"marine-survey-payload-window-orchestrator/internal/domain"
)

func (s *SQLiteStore) appendEventLocked(aggregateID, eventType string, payload any, occurredAt time.Time) (domain.AuditEvent, error) {
	payloadDigest, err := domain.DigestCanonical(payload)
	if err != nil {
		return domain.AuditEvent{}, err
	}
	var lastSeq int
	var previous string
	row := s.db.QueryRow(`select aggregate_seq, canonical_digest from audit_events where aggregate_id=? order by aggregate_seq desc limit 1`, aggregateID)
	if err := row.Scan(&lastSeq, &previous); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return domain.AuditEvent{}, err
	}
	event := domain.AuditEvent{
		AggregateID:    aggregateID,
		AggregateSeq:   lastSeq + 1,
		EventType:      eventType,
		PayloadDigest:  payloadDigest,
		OccurredAt:     domain.NormalizeTime(occurredAt),
		PreviousDigest: previous,
	}
	event.CanonicalDigest, err = eventDigest(event)
	if err != nil {
		return domain.AuditEvent{}, err
	}
	body, err := json.Marshal(event)
	if err != nil {
		return domain.AuditEvent{}, err
	}
	_, err = s.db.Exec(`insert into audit_events(aggregate_id,aggregate_seq,event_type,payload_digest,previous_digest,canonical_digest,occurred_at,json_body)
		values(?,?,?,?,?,?,?,?)`,
		event.AggregateID, event.AggregateSeq, event.EventType, event.PayloadDigest, event.PreviousDigest, event.CanonicalDigest, micros(event.OccurredAt), string(body))
	return event, err
}

func eventDigest(event domain.AuditEvent) (string, error) {
	return domain.DigestCanonical(struct {
		AggregateID   string `json:"aggregate_id"`
		AggregateSeq  int    `json:"aggregate_seq"`
		EventType     string `json:"event_type"`
		PayloadDigest string `json:"payload_digest"`
		Previous      string `json:"previous_digest"`
	}{
		AggregateID:   event.AggregateID,
		AggregateSeq:  event.AggregateSeq,
		EventType:     event.EventType,
		PayloadDigest: event.PayloadDigest,
		Previous:      event.PreviousDigest,
	})
}

func VerifyEventSequence(events []domain.AuditEvent) error {
	previous := ""
	for i, event := range events {
		expectedSeq := i + 1
		if event.AggregateSeq != expectedSeq {
			return fmt.Errorf("event sequence gap for %s: got %d want %d", event.AggregateID, event.AggregateSeq, expectedSeq)
		}
		if event.PreviousDigest != previous {
			return fmt.Errorf("event chain mismatch for %s seq %d", event.AggregateID, event.AggregateSeq)
		}
		digest, err := eventDigest(event)
		if err != nil {
			return err
		}
		if event.CanonicalDigest != digest {
			return fmt.Errorf("event digest mismatch for %s seq %d", event.AggregateID, event.AggregateSeq)
		}
		previous = event.CanonicalDigest
	}
	return nil
}
