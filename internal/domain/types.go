package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
)

const Microsecond = time.Microsecond

var (
	ErrInvalidWindow = errors.New("invalid half-open trial window")
	ErrUnknownState  = errors.New("unknown batch state")
)

type TimeRange struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

func (r TimeRange) Normalize() TimeRange {
	return TimeRange{Start: NormalizeTime(r.Start), End: NormalizeTime(r.End)}
}

func NormalizeTime(t time.Time) time.Time {
	return t.UTC().Truncate(Microsecond)
}

func (r TimeRange) ValidateTrialWindow() error {
	r = r.Normalize()
	if !r.Start.Before(r.End) {
		return ErrInvalidWindow
	}
	d := r.End.Sub(r.Start)
	if d < 2*time.Minute || d > 120*time.Minute {
		return ErrInvalidWindow
	}
	return nil
}

func (r TimeRange) ContainsWindow(inner TimeRange) bool {
	r, inner = r.Normalize(), inner.Normalize()
	return !r.Start.After(inner.Start) && !inner.End.After(r.End)
}

func (r TimeRange) Overlaps(other TimeRange) bool {
	r, other = r.Normalize(), other.Normalize()
	return r.Start.Before(other.End) && other.Start.Before(r.End)
}

type SourceMeta struct {
	Source     string    `json:"source"`
	Version    string    `json:"version"`
	SourceTime time.Time `json:"source_time"`
}

type PostureSample struct {
	At       time.Time `json:"at"`
	RollDeg  float64   `json:"roll_deg"`
	PitchDeg float64   `json:"pitch_deg"`
}

type EnvelopeSample struct {
	At          time.Time `json:"at"`
	RollMinDeg  float64   `json:"roll_min_deg"`
	RollMaxDeg  float64   `json:"roll_max_deg"`
	PitchMinDeg float64   `json:"pitch_min_deg"`
	PitchMaxDeg float64   `json:"pitch_max_deg"`
}

type SeaSample struct {
	At                     time.Time `json:"at"`
	SignificantWaveHeightM float64   `json:"significant_wave_height_m"`
	WindSpeedMS            float64   `json:"wind_speed_m_s"`
	HeaveM                 float64   `json:"heave_m"`
}

type OrbitSnapshot struct {
	ID                         string           `json:"id"`
	Source                     SourceMeta       `json:"source"`
	Valid                      TimeRange        `json:"valid"`
	Envelope                   []EnvelopeSample `json:"envelope"`
	MaxAngularRateDegPerSecond float64          `json:"max_angular_rate_deg_per_second"`
	Digest                     string           `json:"digest"`
}

type SeaSnapshot struct {
	ID           string        `json:"id"`
	Source       SourceMeta    `json:"source"`
	Valid        TimeRange     `json:"valid"`
	Samples      []SeaSample   `json:"samples"`
	MaxSampleGap time.Duration `json:"max_sample_gap"`
	Digest       string        `json:"digest"`
}

type SeaLimits struct {
	MaxWaveHeightM float64 `json:"max_wave_height_m"`
	MaxWindSpeedMS float64 `json:"max_wind_speed_m_s"`
	MaxHeaveM      float64 `json:"max_heave_m"`
}

type ResourceRequirement struct {
	ResourceID string        `json:"resource_id"`
	Quantity   int           `json:"quantity"`
	Critical   bool          `json:"critical"`
	Timeout    time.Duration `json:"timeout"`
}

type ApplicationRequest struct {
	ID                         string                `json:"id"`
	IdempotencyDigest          string                `json:"idempotency_digest"`
	OrbitSnapshotID            string                `json:"orbit_snapshot_id"`
	SeaSnapshotID              string                `json:"sea_snapshot_id"`
	Window                     TimeRange             `json:"window"`
	Posture                    []PostureSample       `json:"posture"`
	MaxAngularRateDegPerSecond float64               `json:"max_angular_rate_deg_per_second"`
	SeaLimits                  SeaLimits             `json:"sea_limits"`
	Resources                  []ResourceRequirement `json:"resources"`
	Priority                   int                   `json:"priority"`
}

type Application struct {
	Request         ApplicationRequest `json:"request"`
	State           BatchState         `json:"state"`
	Decisions       []Decision         `json:"decisions"`
	RejectionReason string             `json:"rejection_reason,omitempty"`
	CreatedAt       time.Time          `json:"created_at"`
}

type Decision struct {
	Code    string    `json:"code"`
	Message string    `json:"message"`
	Passed  bool      `json:"passed"`
	At      time.Time `json:"at,omitempty"`
}

type ResourceSpec struct {
	ResourceID string `json:"resource_id"`
	Capacity   int    `json:"capacity"`
	Unit       string `json:"unit"`
}

type Reservation struct {
	ID         string        `json:"id"`
	BatchID    string        `json:"batch_id"`
	ResourceID string        `json:"resource_id"`
	Window     TimeRange     `json:"window"`
	Quantity   int           `json:"quantity"`
	Critical   bool          `json:"critical"`
	Timeout    time.Duration `json:"timeout"`
	Released   bool          `json:"released"`
}

type TrialBatch struct {
	ID                  string                `json:"id"`
	ApplicationID       string                `json:"application_id"`
	OrbitDigest         string                `json:"orbit_digest"`
	SeaDigest           string                `json:"sea_digest"`
	Window              TimeRange             `json:"window"`
	State               BatchState            `json:"state"`
	Version             int                   `json:"version"`
	StartedAt           *time.Time            `json:"started_at,omitempty"`
	TerminatedAt        *time.Time            `json:"terminated_at,omitempty"`
	TerminationReason   string                `json:"termination_reason,omitempty"`
	LastEventSeq        int                   `json:"last_event_seq"`
	FinalManifestDigest string                `json:"final_manifest_digest,omitempty"`
	Resources           []ResourceRequirement `json:"resources"`
}

type TelemetryHeartbeat struct {
	MessageID  string             `json:"message_id"`
	ResourceID string             `json:"resource_id"`
	DeviceSeq  int64              `json:"device_seq"`
	ObservedAt time.Time          `json:"observed_at"`
	Metrics    map[string]float64 `json:"metrics"`
}

type TelemetryResult struct {
	Accepted   bool       `json:"accepted"`
	Replayed   bool       `json:"replayed"`
	BatchState BatchState `json:"batch_state"`
	EventSeq   int        `json:"event_seq"`
	Reason     string     `json:"reason,omitempty"`
}

type DeviceLiveness struct {
	BatchID        string        `json:"batch_id"`
	ResourceID     string        `json:"resource_id"`
	LastDeviceSeq  int64         `json:"last_device_seq"`
	LastObservedAt time.Time     `json:"last_observed_at"`
	LastReceivedAt time.Time     `json:"last_received_at"`
	Timeout        time.Duration `json:"timeout"`
	Critical       bool          `json:"critical"`
}

type AuditEvent struct {
	AggregateID     string    `json:"aggregate_id"`
	AggregateSeq    int       `json:"aggregate_seq"`
	EventType       string    `json:"event_type"`
	PayloadDigest   string    `json:"payload_digest"`
	OccurredAt      time.Time `json:"occurred_at"`
	PreviousDigest  string    `json:"previous_digest"`
	CanonicalDigest string    `json:"canonical_digest"`
}

func DigestCanonical(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
