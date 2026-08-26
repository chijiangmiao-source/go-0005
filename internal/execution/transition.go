package execution

import "marine-survey-payload-window-orchestrator/internal/domain"

type TransitionRecord struct {
	BatchID  string            `json:"batch_id"`
	From     domain.BatchState `json:"from"`
	To       domain.BatchState `json:"to"`
	Reason   string            `json:"reason"`
	EventSeq int               `json:"event_seq"`
}

func CompletionPriority(nowReachedEnd bool, criticalTimeout bool) string {
	if nowReachedEnd {
		return "WINDOW_END"
	}
	if criticalTimeout {
		return "CRITICAL_DEVICE_TIMEOUT"
	}
	return ""
}

func StatusEventName(state domain.BatchState) string {
	return "BATCH_" + string(state)
}
