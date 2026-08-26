package execution

import (
	"time"

	"marine-survey-payload-window-orchestrator/internal/domain"
)

type Machine struct {
	clock domain.Clock
}

func NewMachine(clock domain.Clock) *Machine {
	return &Machine{clock: clock}
}

func (m *Machine) Transition(batch domain.TrialBatch, next domain.BatchState, reason string) (domain.TrialBatch, error) {
	if err := domain.ValidateTransition(batch.State, next); err != nil {
		return batch, err
	}
	now := domain.NormalizeTime(m.clock.Now())
	batch.State = next
	batch.Version++
	if next == domain.StateRunning && batch.StartedAt == nil {
		batch.StartedAt = &now
	}
	if next == domain.StateCompleted || next == domain.StateAborted {
		batch.TerminatedAt = &now
		batch.TerminationReason = reason
	}
	return batch, nil
}

func (m *Machine) Advance(batch domain.TrialBatch, live []domain.DeviceLiveness) (domain.TrialBatch, bool, string) {
	if batch.State.Terminal() {
		return batch, false, ""
	}
	now := domain.NormalizeTime(m.clock.Now())
	if !now.Before(batch.Window.End) {
		if batch.StartedAt != nil || batch.State == domain.StateRunning || batch.State == domain.StateDegraded {
			next, err := m.Transition(batch, domain.StateCompleted, "WINDOW_END")
			return next, err == nil, "WINDOW_END"
		}
		next, err := m.Transition(batch, domain.StateAborted, "MISSED_START_WINDOW")
		return next, err == nil, "MISSED_START_WINDOW"
	}
	criticalOffline, optionalOffline := livenessGaps(now, live)
	if criticalOffline {
		next, err := m.Transition(batch, domain.StateAborted, "CRITICAL_DEVICE_TIMEOUT")
		return next, err == nil, "CRITICAL_DEVICE_TIMEOUT"
	}
	if optionalOffline && batch.State == domain.StateReserved {
		next, err := m.Transition(batch, domain.StateDegraded, "OPTIONAL_DEVICE_MISSING")
		return next, err == nil, "OPTIONAL_DEVICE_MISSING"
	}
	if optionalOffline && batch.State == domain.StateRunning {
		next, err := m.Transition(batch, domain.StateDegraded, "OPTIONAL_DEVICE_TIMEOUT")
		return next, err == nil, "OPTIONAL_DEVICE_TIMEOUT"
	}
	if !optionalOffline && batch.State == domain.StateDegraded {
		next, err := m.Transition(batch, domain.StateRunning, "OPTIONAL_DEVICE_RECOVERED")
		return next, err == nil, "OPTIONAL_DEVICE_RECOVERED"
	}
	if batch.State == domain.StateReserved {
		next, err := m.Transition(batch, domain.StateRunning, "WINDOW_STARTED")
		return next, err == nil, "WINDOW_STARTED"
	}
	return batch, false, ""
}

func livenessGaps(now time.Time, live []domain.DeviceLiveness) (critical bool, optional bool) {
	for _, l := range live {
		if l.Timeout <= 0 || l.LastReceivedAt.IsZero() {
			if l.Critical {
				critical = true
			} else {
				optional = true
			}
			continue
		}
		if now.Sub(domain.NormalizeTime(l.LastReceivedAt)) > l.Timeout {
			if l.Critical {
				critical = true
			} else {
				optional = true
			}
		}
	}
	return critical, optional
}
