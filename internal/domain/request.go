package domain

import (
	"errors"
	"sort"
	"time"
)

var (
	ErrMissingSnapshot = errors.New("snapshot id is required")
	ErrMissingResource = errors.New("at least one resource requirement is required")
	ErrInvalidQuantity = errors.New("resource quantity must be positive")
	ErrInvalidTimeout  = errors.New("resource timeout must be positive")
)

func (r ApplicationRequest) Normalize() ApplicationRequest {
	r.Window = r.Window.Normalize()
	for i := range r.Posture {
		r.Posture[i].At = NormalizeTime(r.Posture[i].At)
	}
	sort.SliceStable(r.Posture, func(i, j int) bool { return r.Posture[i].At.Before(r.Posture[j].At) })
	return r
}

func (r ApplicationRequest) ValidateShape() error {
	if r.OrbitSnapshotID == "" || r.SeaSnapshotID == "" {
		return ErrMissingSnapshot
	}
	if err := r.Window.ValidateTrialWindow(); err != nil {
		return err
	}
	if len(r.Resources) == 0 {
		return ErrMissingResource
	}
	seen := map[string]bool{}
	for _, req := range r.Resources {
		if req.Quantity <= 0 {
			return ErrInvalidQuantity
		}
		if req.Timeout <= 0 {
			return ErrInvalidTimeout
		}
		if req.ResourceID == "" || seen[req.ResourceID] {
			return ErrMissingResource
		}
		seen[req.ResourceID] = true
	}
	return nil
}

func NewLivenessSeed(batchID string, req ResourceRequirement, at time.Time) DeviceLiveness {
	now := NormalizeTime(at)
	return DeviceLiveness{
		BatchID:        batchID,
		ResourceID:     req.ResourceID,
		LastDeviceSeq:  0,
		LastObservedAt: now,
		LastReceivedAt: time.Time{},
		Timeout:        req.Timeout,
		Critical:       req.Critical,
	}
}
