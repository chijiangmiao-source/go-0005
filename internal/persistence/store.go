package persistence

import "marine-survey-payload-window-orchestrator/internal/domain"

type Store interface {
	Ready() error
	SetResource(domain.ResourceSpec)
	Resources() map[string]domain.ResourceSpec
	SaveOrbit(domain.OrbitSnapshot) (domain.OrbitSnapshot, error)
	SaveSea(domain.SeaSnapshot) (domain.SeaSnapshot, error)
	Orbit(id string) (domain.OrbitSnapshot, bool)
	Sea(id string) (domain.SeaSnapshot, bool)
	SaveApplication(domain.Application) (domain.Application, error)
	Application(id string) (domain.Application, bool)
	AllocateBatchID() string
	SaveBatch(domain.TrialBatch) error
	GetBatch(id string) (domain.TrialBatch, bool)
	UpdateBatch(domain.TrialBatch) error
	Reservations() []domain.Reservation
	SaveReservation(domain.Reservation) error
	ReleaseBatchReservations(batchID string) error
	Liveness(batchID string) []domain.DeviceLiveness
	GetLiveness(batchID, resourceID string) (domain.DeviceLiveness, bool)
	SaveLiveness(domain.DeviceLiveness) error
	Inbox(messageID string) (digest string, result domain.TelemetryResult, ok bool)
	SaveInbox(messageID string, digest string, result domain.TelemetryResult)
	AppendEvent(aggregateID, eventType string, payload any) (domain.AuditEvent, error)
	EventsAfter(aggregateID string, afterSeq int) []domain.AuditEvent
	Idempotency(key string) (IdempotencyRecord, bool)
	SaveIdempotency(key, requestDigest string, status int, body []byte) IdempotencyRecord
	OpenBatchIDs() []string
}

type Recoverable interface {
	Recover(clock domain.Clock) RecoveryReport
}
