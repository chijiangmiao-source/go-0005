package api

import (
	"net/http"

	"marine-survey-payload-window-orchestrator/internal/domain"
	"marine-survey-payload-window-orchestrator/internal/persistence"
)

func RunStartupRecovery(store persistence.Store, clock domain.Clock) persistence.RecoveryReport {
	if recoverable, ok := store.(persistence.Recoverable); ok {
		return recoverable.Recover(clock)
	}
	return persistence.RecoveryReport{Ready: true, CheckedAt: domain.NormalizeTime(clock.Now())}
}

func (s *Server) writeReadiness(w http.ResponseWriter) {
	if err := s.store.Ready(); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"ready": "false", "reason": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ready": true})
}
