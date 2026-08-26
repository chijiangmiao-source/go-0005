package telemetry

import (
	"errors"
	"sync"

	"marine-survey-payload-window-orchestrator/internal/domain"
	"marine-survey-payload-window-orchestrator/internal/execution"
)

var (
	ErrMessageConflict = errors.New("telemetry message digest conflict")
	ErrStaleSequence   = errors.New("telemetry device sequence is not newer than last accepted sequence")
	ErrUnknownDevice   = errors.New("telemetry resource does not belong to batch")
)

type Store interface {
	Inbox(messageID string) (digest string, result domain.TelemetryResult, ok bool)
	SaveInbox(messageID string, digest string, result domain.TelemetryResult)
	GetBatch(id string) (domain.TrialBatch, bool)
	UpdateBatch(domain.TrialBatch) error
	Liveness(batchID string) []domain.DeviceLiveness
	GetLiveness(batchID, resourceID string) (domain.DeviceLiveness, bool)
	SaveLiveness(domain.DeviceLiveness) error
	AppendEvent(aggregateID, eventType string, payload any) (domain.AuditEvent, error)
	EventsAfter(aggregateID string, afterSeq int) []domain.AuditEvent
}

type Receiver struct {
	mu      sync.Mutex
	store   Store
	clock   domain.Clock
	machine *execution.Machine
}

func NewReceiver(store Store, clock domain.Clock, machine *execution.Machine) *Receiver {
	return &Receiver{store: store, clock: clock, machine: machine}
}

func (r *Receiver) Receive(batchID string, hb domain.TelemetryHeartbeat) (domain.TelemetryResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if decision := ValidateHeartbeatShape(hb); !decision.Accepted {
		return domain.TelemetryResult{Accepted: false, Reason: decision.Reason}, ErrUnknownDevice
	}
	digest, err := NewDigest(hb)
	if err != nil {
		return domain.TelemetryResult{}, err
	}
	if oldDigest, oldResult, ok := r.store.Inbox(hb.MessageID); ok {
		if oldDigest != digest {
			return domain.TelemetryResult{Accepted: false, Reason: "MESSAGE_DIGEST_CONFLICT"}, ErrMessageConflict
		}
		oldResult.Replayed = true
		return oldResult, nil
	}
	batch, ok := r.store.GetBatch(batchID)
	if !ok {
		result := domain.TelemetryResult{Accepted: false, Reason: "UNKNOWN_BATCH"}
		r.store.SaveInbox(hb.MessageID, digest, result)
		return result, nil
	}
	if batch.State.Terminal() {
		result := domain.TelemetryResult{Accepted: false, BatchState: batch.State, Reason: "TERMINAL_BATCH"}
		r.store.SaveInbox(hb.MessageID, digest, result)
		return result, nil
	}
	req, ok := requirement(batch, hb.ResourceID)
	if !ok {
		result := domain.TelemetryResult{Accepted: false, BatchState: batch.State, Reason: "UNKNOWN_DEVICE"}
		r.store.SaveInbox(hb.MessageID, digest, result)
		return result, ErrUnknownDevice
	}
	live, exists := r.store.GetLiveness(batchID, hb.ResourceID)
	if exists && hb.DeviceSeq <= live.LastDeviceSeq {
		result := domain.TelemetryResult{Accepted: false, BatchState: batch.State, Reason: "STALE_DEVICE_SEQ"}
		r.store.SaveInbox(hb.MessageID, digest, result)
		return result, ErrStaleSequence
	}
	now := domain.NormalizeTime(r.clock.Now())
	live = domain.DeviceLiveness{BatchID: batchID, ResourceID: hb.ResourceID, LastDeviceSeq: hb.DeviceSeq, LastObservedAt: domain.NormalizeTime(hb.ObservedAt), LastReceivedAt: now, Timeout: req.Timeout, Critical: req.Critical}
	if err := r.store.SaveLiveness(live); err != nil {
		return domain.TelemetryResult{}, err
	}
	event, err := r.store.AppendEvent(batchID, "TELEMETRY_ACCEPTED", hb)
	if err != nil {
		return domain.TelemetryResult{}, err
	}
	result := domain.TelemetryResult{Accepted: true, BatchState: batch.State, EventSeq: event.AggregateSeq}
	liveSet := r.store.Liveness(batchID)
	next, changed, reason := r.machine.Advance(batch, liveSet)
	if changed {
		stateEvent, err := r.store.AppendEvent(batchID, "BATCH_"+string(next.State), map[string]string{"reason": reason})
		if err != nil {
			return domain.TelemetryResult{}, err
		}
		next.LastEventSeq = stateEvent.AggregateSeq
		if next.State.Terminal() {
			next.FinalManifestDigest, _ = domain.FinalManifestDigest(next, r.store.EventsAfter(batchID, 0), liveSet)
		}
		if err := r.store.UpdateBatch(next); err != nil {
			return domain.TelemetryResult{}, err
		}
		batch = next
	}
	// Persist the inbox record only after the state machine has run, so the
	// replayed result carries the same post-transition batch_state that the
	// first response returned to the caller.
	result.BatchState = batch.State
	r.store.SaveInbox(hb.MessageID, digest, result)
	return result, nil
}

func requirement(batch domain.TrialBatch, resourceID string) (domain.ResourceRequirement, bool) {
	for _, req := range batch.Resources {
		if req.ResourceID == resourceID {
			return req, true
		}
	}
	return domain.ResourceRequirement{}, false
}
