package telemetry

import (
	"time"

	"marine-survey-payload-window-orchestrator/internal/domain"
)

type SequenceDecision struct {
	Accepted bool   `json:"accepted"`
	Reason   string `json:"reason,omitempty"`
}

func ValidateHeartbeatShape(hb domain.TelemetryHeartbeat) SequenceDecision {
	if hb.MessageID == "" {
		return SequenceDecision{Reason: "MESSAGE_ID_REQUIRED"}
	}
	if hb.ResourceID == "" {
		return SequenceDecision{Reason: "RESOURCE_ID_REQUIRED"}
	}
	if hb.DeviceSeq <= 0 {
		return SequenceDecision{Reason: "DEVICE_SEQ_REQUIRED"}
	}
	if hb.ObservedAt.IsZero() {
		return SequenceDecision{Reason: "OBSERVED_AT_REQUIRED"}
	}
	return SequenceDecision{Accepted: true}
}

func IsOnline(now time.Time, live domain.DeviceLiveness) bool {
	if live.Timeout <= 0 || live.LastReceivedAt.IsZero() {
		return false
	}
	return domain.NormalizeTime(now).Sub(domain.NormalizeTime(live.LastReceivedAt)) <= live.Timeout
}

func NewDigest(hb domain.TelemetryHeartbeat) (string, error) {
	return domain.DigestCanonical(hb)
}
