package persistence

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"marine-survey-payload-window-orchestrator/internal/domain"
)

type IdempotencyRecord struct {
	Key           string `json:"key"`
	RequestDigest string `json:"request_digest"`
	Status        int    `json:"status"`
	Body          []byte `json:"body"`
}

type MemoryStore struct {
	mu           sync.Mutex
	nextOrbit    int
	nextSea      int
	nextApp      int
	nextBatch    int
	readyErr     error
	resources    map[string]domain.ResourceSpec
	orbits       map[string]domain.OrbitSnapshot
	seas         map[string]domain.SeaSnapshot
	apps         map[string]domain.Application
	batches      map[string]domain.TrialBatch
	reservations []domain.Reservation
	liveness     map[string]domain.DeviceLiveness
	inbox        map[string]inboxRecord
	idempotency  map[string]IdempotencyRecord
	events       map[string][]domain.AuditEvent
}

type inboxRecord struct {
	digest string
	result domain.TelemetryResult
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		resources: map[string]domain.ResourceSpec{
			"antenna":  {ResourceID: "antenna", Capacity: 1, Unit: "dish"},
			"recorder": {ResourceID: "recorder", Capacity: 2, Unit: "channel"},
		},
		orbits:      map[string]domain.OrbitSnapshot{},
		seas:        map[string]domain.SeaSnapshot{},
		apps:        map[string]domain.Application{},
		batches:     map[string]domain.TrialBatch{},
		liveness:    map[string]domain.DeviceLiveness{},
		inbox:       map[string]inboxRecord{},
		idempotency: map[string]IdempotencyRecord{},
		events:      map[string][]domain.AuditEvent{},
	}
}

func (s *MemoryStore) Ready() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readyErr
}

func (s *MemoryStore) SetReadyError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readyErr = err
}

func (s *MemoryStore) SetResource(spec domain.ResourceSpec) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resources[spec.ResourceID] = spec
}

func (s *MemoryStore) Resources() map[string]domain.ResourceSpec {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]domain.ResourceSpec, len(s.resources))
	for k, v := range s.resources {
		out[k] = v
	}
	return out
}

func (s *MemoryStore) SaveOrbit(snapshot domain.OrbitSnapshot) (domain.OrbitSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var err error
	snapshot, err = snapshot.WithVerifiedDigest()
	if err != nil {
		return snapshot, err
	}
	if snapshot.ID == "" {
		s.nextOrbit++
		snapshot.ID = fmt.Sprintf("orbit-%d", s.nextOrbit)
	}
	s.orbits[snapshot.ID] = snapshot
	return snapshot, nil
}

func (s *MemoryStore) SaveSea(snapshot domain.SeaSnapshot) (domain.SeaSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var err error
	snapshot, err = snapshot.WithVerifiedDigest()
	if err != nil {
		return snapshot, err
	}
	if snapshot.ID == "" {
		s.nextSea++
		snapshot.ID = fmt.Sprintf("sea-%d", s.nextSea)
	}
	s.seas[snapshot.ID] = snapshot
	return snapshot, nil
}

func (s *MemoryStore) Orbit(id string) (domain.OrbitSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id != "" {
		v, ok := s.orbits[id]
		return v, ok
	}
	for _, v := range s.orbits {
		return v, true
	}
	return domain.OrbitSnapshot{}, false
}

func (s *MemoryStore) Sea(id string) (domain.SeaSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id != "" {
		v, ok := s.seas[id]
		return v, ok
	}
	for _, v := range s.seas {
		return v, true
	}
	return domain.SeaSnapshot{}, false
}

func (s *MemoryStore) SaveApplication(app domain.Application) (domain.Application, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if app.Request.ID == "" {
		s.nextApp++
		app.Request.ID = fmt.Sprintf("app-%d", s.nextApp)
	}
	if app.CreatedAt.IsZero() {
		app.CreatedAt = domain.NormalizeTime(time.Now())
	}
	s.apps[app.Request.ID] = app
	return app, nil
}

func (s *MemoryStore) Application(id string) (domain.Application, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	app, ok := s.apps[id]
	return app, ok
}

func (s *MemoryStore) AllocateBatchID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextBatch++
	return fmt.Sprintf("batch-%d", s.nextBatch)
}

func (s *MemoryStore) SaveBatch(batch domain.TrialBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if batch.ID == "" {
		return errors.New("batch id is required")
	}
	s.batches[batch.ID] = batch
	return nil
}

func (s *MemoryStore) GetBatch(id string) (domain.TrialBatch, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	batch, ok := s.batches[id]
	return batch, ok
}

func (s *MemoryStore) UpdateBatch(batch domain.TrialBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.batches[batch.ID]; !ok {
		return errors.New("batch not found")
	}
	s.batches[batch.ID] = batch
	return nil
}

func (s *MemoryStore) Reservations() []domain.Reservation {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.Reservation, len(s.reservations))
	copy(out, s.reservations)
	return out
}

func (s *MemoryStore) SaveReservation(res domain.Reservation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reservations = append(s.reservations, res)
	return nil
}

func (s *MemoryStore) ReleaseBatchReservations(batchID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.reservations[:0]
	for _, res := range s.reservations {
		if res.BatchID == batchID {
			continue
		}
		out = append(out, res)
	}
	s.reservations = out
	return nil
}

func (s *MemoryStore) Liveness(batchID string) []domain.DeviceLiveness {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []domain.DeviceLiveness{}
	for _, live := range s.liveness {
		if live.BatchID == batchID {
			out = append(out, live)
		}
	}
	return out
}

func (s *MemoryStore) GetLiveness(batchID, resourceID string) (domain.DeviceLiveness, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	live, ok := s.liveness[batchID+"|"+resourceID]
	return live, ok
}

func (s *MemoryStore) SaveLiveness(live domain.DeviceLiveness) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.liveness[live.BatchID+"|"+live.ResourceID] = live
	return nil
}

func (s *MemoryStore) Inbox(messageID string) (string, domain.TelemetryResult, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.inbox[messageID]
	return rec.digest, rec.result, ok
}

func (s *MemoryStore) SaveInbox(messageID string, digest string, result domain.TelemetryResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inbox[messageID] = inboxRecord{digest: digest, result: result}
}

func (s *MemoryStore) AppendEvent(aggregateID, eventType string, payload any) (domain.AuditEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	digest, err := domain.DigestCanonical(payload)
	if err != nil {
		return domain.AuditEvent{}, err
	}
	events := s.events[aggregateID]
	prev := ""
	if len(events) > 0 {
		prev = events[len(events)-1].CanonicalDigest
	}
	event := domain.AuditEvent{AggregateID: aggregateID, AggregateSeq: len(events) + 1, EventType: eventType, PayloadDigest: digest, OccurredAt: domain.NormalizeTime(time.Now()), PreviousDigest: prev}
	event.CanonicalDigest, err = domain.DigestCanonical(struct {
		AggregateID   string `json:"aggregate_id"`
		AggregateSeq  int    `json:"aggregate_seq"`
		EventType     string `json:"event_type"`
		PayloadDigest string `json:"payload_digest"`
		Previous      string `json:"previous_digest"`
	}{event.AggregateID, event.AggregateSeq, event.EventType, event.PayloadDigest, event.PreviousDigest})
	if err != nil {
		return domain.AuditEvent{}, err
	}
	s.events[aggregateID] = append(events, event)
	return event, nil
}

func (s *MemoryStore) EventsAfter(aggregateID string, afterSeq int) []domain.AuditEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := s.events[aggregateID]
	out := []domain.AuditEvent{}
	for _, event := range events {
		if event.AggregateSeq > afterSeq {
			out = append(out, event)
		}
	}
	return out
}

func (s *MemoryStore) OpenBatchIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []string{}
	for id, batch := range s.batches {
		if !batch.State.Terminal() {
			out = append(out, id)
		}
	}
	return out
}

func (s *MemoryStore) Idempotency(key string) (IdempotencyRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.idempotency[key]
	return rec, ok
}

func (s *MemoryStore) SaveIdempotency(key, requestDigest string, status int, body []byte) IdempotencyRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored := append([]byte(nil), body...)
	rec := IdempotencyRecord{Key: key, RequestDigest: requestDigest, Status: status, Body: stored}
	s.idempotency[key] = rec
	return rec
}
