package reservation

import "marine-survey-payload-window-orchestrator/internal/domain"

type UsageSlice struct {
	ResourceID string           `json:"resource_id"`
	Window     domain.TimeRange `json:"window"`
	Used       int              `json:"used"`
	Capacity   int              `json:"capacity"`
}

func CalculateUsage(resourceID string, window domain.TimeRange, reservations []domain.Reservation, specs map[string]domain.ResourceSpec) UsageSlice {
	slice := UsageSlice{ResourceID: resourceID, Window: window.Normalize()}
	if spec, ok := specs[resourceID]; ok {
		slice.Capacity = spec.Capacity
	}
	for _, held := range reservations {
		if held.Released || held.ResourceID != resourceID || !held.Window.Overlaps(window) {
			continue
		}
		slice.Used += held.Quantity
	}
	return slice
}

func BuildReservations(batchID string, window domain.TimeRange, reqs []domain.ResourceRequirement) []domain.Reservation {
	out := make([]domain.Reservation, 0, len(reqs))
	for i, req := range reqs {
		out = append(out, domain.Reservation{
			ID:         reservationID(batchID, req.ResourceID, i+1),
			BatchID:    batchID,
			ResourceID: req.ResourceID,
			Window:     window.Normalize(),
			Quantity:   req.Quantity,
			Critical:   req.Critical,
			Timeout:    req.Timeout,
		})
	}
	return out
}

func reservationID(batchID, resourceID string, ordinal int) string {
	return batchID + "-" + resourceID + "-" + itoa(ordinal)
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
