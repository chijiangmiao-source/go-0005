package tests

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"marine-survey-payload-window-orchestrator/internal/api"
	"marine-survey-payload-window-orchestrator/internal/domain"
	"marine-survey-payload-window-orchestrator/internal/persistence"
)

type publicHarness struct {
	base  time.Time
	clock *domain.ManualClock
	store *persistence.MemoryStore
	srv   *httptest.Server
}

func newPublicHarness(t *testing.T) *publicHarness {
	t.Helper()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := domain.NewManualClock(base)
	store := persistence.NewMemoryStore()
	srv := httptest.NewServer(api.NewServer(store, clock).Routes())
	t.Cleanup(srv.Close)
	return &publicHarness{base: base, clock: clock, store: store, srv: srv}
}

func publicFixture(base time.Time) (domain.ApplicationRequest, domain.OrbitSnapshot, domain.SeaSnapshot) {
	window := domain.TimeRange{Start: base, End: base.Add(10 * time.Minute)}
	orbit := domain.OrbitSnapshot{
		Source:                     domain.SourceMeta{Source: "public-tle", Version: "frozen", SourceTime: base.Add(-time.Hour)},
		Valid:                      domain.TimeRange{Start: base.Add(-time.Minute), End: base.Add(20 * time.Minute)},
		MaxAngularRateDegPerSecond: 1,
		Envelope: []domain.EnvelopeSample{
			{At: base, RollMinDeg: -2, RollMaxDeg: 2, PitchMinDeg: -2, PitchMaxDeg: 2},
			{At: base.Add(5 * time.Minute), RollMinDeg: -2, RollMaxDeg: 2, PitchMinDeg: -2, PitchMaxDeg: 2},
			{At: base.Add(10 * time.Minute), RollMinDeg: -2, RollMaxDeg: 2, PitchMinDeg: -2, PitchMaxDeg: 2},
		},
	}
	sea := domain.SeaSnapshot{
		Source:       domain.SourceMeta{Source: "public-buoy", Version: "frozen", SourceTime: base.Add(-time.Hour)},
		Valid:        domain.TimeRange{Start: base.Add(-time.Minute), End: base.Add(20 * time.Minute)},
		MaxSampleGap: 10 * time.Minute,
		Samples: []domain.SeaSample{
			{At: base, SignificantWaveHeightM: 2, WindSpeedMS: 9, HeaveM: 1},
			{At: base.Add(5 * time.Minute), SignificantWaveHeightM: 2, WindSpeedMS: 9, HeaveM: 1},
			{At: base.Add(10 * time.Minute), SignificantWaveHeightM: 2, WindSpeedMS: 9, HeaveM: 1},
		},
	}
	app := domain.ApplicationRequest{
		Window:                     window,
		Posture:                    []domain.PostureSample{{At: base, RollDeg: 0, PitchDeg: 0}, {At: base.Add(5 * time.Minute), RollDeg: 1, PitchDeg: 1}, {At: base.Add(10 * time.Minute), RollDeg: 1, PitchDeg: 1}},
		MaxAngularRateDegPerSecond: 1,
		SeaLimits:                  domain.SeaLimits{MaxWaveHeightM: 2, MaxWindSpeedMS: 9, MaxHeaveM: 1},
		Resources:                  []domain.ResourceRequirement{{ResourceID: "antenna", Quantity: 1, Critical: true, Timeout: time.Minute}},
	}
	return app, orbit, sea
}

func registerPublicSnapshots(t *testing.T, h *publicHarness, app domain.ApplicationRequest, orbit domain.OrbitSnapshot, sea domain.SeaSnapshot) domain.ApplicationRequest {
	t.Helper()
	orbit, err := h.store.SaveOrbit(orbit)
	if err != nil {
		t.Fatal(err)
	}
	sea, err = h.store.SaveSea(sea)
	if err != nil {
		t.Fatal(err)
	}
	app.OrbitSnapshotID = orbit.ID
	app.SeaSnapshotID = sea.ID
	return app
}

func postJSON(t *testing.T, url string, key string, body any) (*http.Response, []byte) {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	out := new(bytes.Buffer)
	_, _ = out.ReadFrom(res.Body)
	return res, out.Bytes()
}

type simStep struct {
	Kind       string
	MessageID  string
	ResourceID string
	Seq        int64
	Advance    time.Duration
}

func runTelemetryScript(t *testing.T, h *publicHarness, batchID string, steps []simStep) []domain.TelemetryResult {
	t.Helper()
	var out []domain.TelemetryResult
	for _, step := range steps {
		switch step.Kind {
		case "Advance":
			h.clock.Advance(step.Advance)
		case "Concurrent":
			var wg sync.WaitGroup
			results := make([]domain.TelemetryResult, 2)
			for i := range results {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					results[idx] = sendHeartbeat(t, h, batchID, step.MessageID, step.ResourceID, step.Seq)
				}(i)
			}
			wg.Wait()
			out = append(out, results...)
		default:
			out = append(out, sendHeartbeat(t, h, batchID, step.MessageID, step.ResourceID, step.Seq))
		}
	}
	return out
}

func sendHeartbeat(t *testing.T, h *publicHarness, batchID, messageID, resourceID string, seq int64) domain.TelemetryResult {
	t.Helper()
	hb := domain.TelemetryHeartbeat{MessageID: messageID, ResourceID: resourceID, DeviceSeq: seq, ObservedAt: h.clock.Now(), Metrics: map[string]float64{"temp_c": 12}}
	res, body := postJSON(t, h.srv.URL+"/api/v1/batches/"+batchID+"/telemetry/heartbeats", "", hb)
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusUnprocessableEntity && res.StatusCode != http.StatusConflict {
		t.Fatalf("unexpected telemetry status %d: %s", res.StatusCode, body)
	}
	var result domain.TelemetryResult
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatal(err)
	}
	return result
}
