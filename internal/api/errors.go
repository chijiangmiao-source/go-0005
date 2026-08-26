package api

import (
	"errors"
	"net/http"

	"marine-survey-payload-window-orchestrator/internal/telemetry"
)

type codedError struct {
	status int
	code   string
}

func mapTelemetryError(err error) codedError {
	switch {
	case err == nil:
		return codedError{status: http.StatusOK}
	case errors.Is(err, telemetry.ErrMessageConflict):
		return codedError{status: http.StatusConflict, code: "MESSAGE_DIGEST_CONFLICT"}
	case errors.Is(err, telemetry.ErrStaleSequence):
		return codedError{status: http.StatusUnprocessableEntity, code: "STALE_DEVICE_SEQ"}
	case errors.Is(err, telemetry.ErrUnknownDevice):
		return codedError{status: http.StatusUnprocessableEntity, code: "UNKNOWN_DEVICE"}
	default:
		return codedError{status: http.StatusServiceUnavailable, code: "TELEMETRY_WRITE_FAILED"}
	}
}
