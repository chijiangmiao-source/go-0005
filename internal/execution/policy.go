package execution

import (
	"time"

	"marine-survey-payload-window-orchestrator/internal/domain"
)

type DeviceStatus struct {
	ResourceID string `json:"resource_id"`
	Critical   bool   `json:"critical"`
	Online     bool   `json:"online"`
	OverdueBy  int64  `json:"overdue_by_us"`
}

func EvaluateDevices(now time.Time, live []domain.DeviceLiveness) []DeviceStatus {
	now = domain.NormalizeTime(now)
	out := make([]DeviceStatus, 0, len(live))
	for _, item := range live {
		status := DeviceStatus{ResourceID: item.ResourceID, Critical: item.Critical, Online: true}
		if item.LastReceivedAt.IsZero() || item.Timeout <= 0 {
			status.Online = false
		} else if gap := now.Sub(domain.NormalizeTime(item.LastReceivedAt)); gap > item.Timeout {
			status.Online = false
			status.OverdueBy = int64((gap - item.Timeout) / time.Microsecond)
		}
		out = append(out, status)
	}
	return out
}

func AnyOffline(status []DeviceStatus) (critical bool, optional bool) {
	for _, item := range status {
		if item.Online {
			continue
		}
		if item.Critical {
			critical = true
		} else {
			optional = true
		}
	}
	return critical, optional
}
