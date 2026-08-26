package tests

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"marine-survey-payload-window-orchestrator/internal/api"
	"marine-survey-payload-window-orchestrator/internal/constraints"
	"marine-survey-payload-window-orchestrator/internal/domain"
	"marine-survey-payload-window-orchestrator/internal/execution"
	"marine-survey-payload-window-orchestrator/internal/persistence"
	"marine-survey-payload-window-orchestrator/internal/reservation"
	"marine-survey-payload-window-orchestrator/internal/telemetry"
)

func TestConstraintFoundation(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	app, orbit, sea := fixtureRequest(base)
	decisions := constraints.NewValidator().Validate(app, orbit, sea)
	for _, d := range decisions {
		if !d.Passed {
			t.Fatalf("expected valid fixture, got %s: %s", d.Code, d.Message)
		}
	}
	app.MaxAngularRateDegPerSecond = 0.001
	decisions = constraints.NewValidator().Validate(app, orbit, sea)
	if !hasFailed(decisions, "D03_ANGULAR_RATE") {
		t.Fatalf("expected angular rate decision, got %#v", decisions)
	}
}

func TestReservationFoundation(t *testing.T) {
	store := persistence.NewMemoryStore()
	reserver := reservation.NewReserver(store)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	req := []domain.ResourceRequirement{{ResourceID: "antenna", Quantity: 1, Critical: true, Timeout: time.Minute}}
	if _, conflict, err := reserver.Reserve("batch-1", domain.TimeRange{Start: base, End: base.Add(10 * time.Minute)}, req); err != nil || conflict != nil {
		t.Fatalf("first reservation failed: conflict=%v err=%v", conflict, err)
	}
	if _, conflict, err := reserver.Reserve("batch-2", domain.TimeRange{Start: base.Add(10 * time.Minute), End: base.Add(20 * time.Minute)}, req); err != nil || conflict != nil {
		t.Fatalf("adjacent half-open reservation failed: conflict=%v err=%v", conflict, err)
	}
	if _, conflict, err := reserver.Reserve("batch-3", domain.TimeRange{Start: base.Add(5 * time.Minute), End: base.Add(12 * time.Minute)}, req); err != nil || conflict == nil {
		t.Fatalf("expected overlap conflict, got conflict=%v err=%v", conflict, err)
	}
}

func TestExecutionAndTelemetryFoundation(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := domain.NewManualClock(base)
	store := persistence.NewMemoryStore()
	batch := domain.TrialBatch{ID: "batch-1", Window: domain.TimeRange{Start: base, End: base.Add(10 * time.Minute)}, State: domain.StateReserved, Resources: []domain.ResourceRequirement{{ResourceID: "antenna", Quantity: 1, Critical: true, Timeout: time.Minute}}}
	if err := store.SaveBatch(batch); err != nil {
		t.Fatal(err)
	}
	machine := execution.NewMachine(clock)
	if _, err := machine.Transition(batch, domain.StateCompleted, "illegal"); err == nil {
		t.Fatal("expected illegal transition to fail")
	}
	receiver := telemetry.NewReceiver(store, clock, machine)
	hb := domain.TelemetryHeartbeat{MessageID: "m1", ResourceID: "antenna", DeviceSeq: 1, ObservedAt: base}
	first, err := receiver.Receive("batch-1", hb)
	if err != nil || !first.Accepted || first.BatchState != domain.StateRunning {
		t.Fatalf("first heartbeat failed: result=%#v err=%v", first, err)
	}
	again, err := receiver.Receive("batch-1", hb)
	if err != nil || !again.Replayed {
		t.Fatalf("expected replay: result=%#v err=%v", again, err)
	}
	hb.MessageID = "m2"
	if result, err := receiver.Receive("batch-1", hb); err != telemetry.ErrStaleSequence || result.Accepted {
		t.Fatalf("expected stale sequence rejection: result=%#v err=%v", result, err)
	}
	clock.Set(base.Add(time.Minute))
	live := store.Liveness("batch-1")
	next, changed, reason := machine.Advance(mustBatch(t, store, "batch-1"), live)
	if changed || next.State != domain.StateRunning || reason != "" {
		t.Fatalf("timeout equality should remain online: state=%s changed=%v reason=%s", next.State, changed, reason)
	}
	clock.Advance(time.Microsecond)
	next, changed, reason = machine.Advance(next, live)
	if !changed || next.State != domain.StateAborted || reason != "CRITICAL_DEVICE_TIMEOUT" {
		t.Fatalf("expected critical timeout abort: state=%s changed=%v reason=%s", next.State, changed, reason)
	}
}

func TestAPIAndWebFoundation(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	appReq, orbit, sea := fixtureRequest(base)
	store := persistence.NewMemoryStore()
	orbit, _ = store.SaveOrbit(orbit)
	sea, _ = store.SaveSea(sea)
	appReq.OrbitSnapshotID = orbit.ID
	appReq.SeaSnapshotID = sea.ID
	srv := httptest.NewServer(api.NewServer(store, domain.NewManualClock(base)).Routes())
	defer srv.Close()
	root, err := http.Get(srv.URL + "/")
	if err != nil || root.StatusCode != http.StatusOK {
		t.Fatalf("root failed: status=%v err=%v", status(root), err)
	}
	body, _ := json.Marshal(appReq)
	firstCode, firstBody := postApplication(t, srv.URL, "same-key", body)
	secondCode, secondBody := postApplication(t, srv.URL, "same-key", body)
	if firstCode != http.StatusCreated || secondCode != firstCode || !bytes.Equal(firstBody, secondBody) {
		t.Fatalf("expected byte-identical idempotent response: first=%d second=%d", firstCode, secondCode)
	}
	appReq.Priority = 9
	changed, _ := json.Marshal(appReq)
	conflictCode, _ := postApplication(t, srv.URL, "same-key", changed)
	if conflictCode != http.StatusConflict {
		t.Fatalf("expected idempotency conflict, got %d", conflictCode)
	}
}

func fixtureRequest(base time.Time) (domain.ApplicationRequest, domain.OrbitSnapshot, domain.SeaSnapshot) {
	window := domain.TimeRange{Start: base, End: base.Add(10 * time.Minute)}
	orbit := domain.OrbitSnapshot{
		Valid:                      domain.TimeRange{Start: base.Add(-time.Minute), End: base.Add(20 * time.Minute)},
		MaxAngularRateDegPerSecond: 1,
		Envelope: []domain.EnvelopeSample{
			{At: base, RollMinDeg: -2, RollMaxDeg: 2, PitchMinDeg: -2, PitchMaxDeg: 2},
			{At: base.Add(10 * time.Minute), RollMinDeg: -2, RollMaxDeg: 2, PitchMinDeg: -2, PitchMaxDeg: 2},
		},
	}
	sea := domain.SeaSnapshot{
		Valid:        domain.TimeRange{Start: base.Add(-time.Minute), End: base.Add(20 * time.Minute)},
		MaxSampleGap: 10 * time.Minute,
		Samples: []domain.SeaSample{
			{At: base, SignificantWaveHeightM: 2, WindSpeedMS: 9, HeaveM: 1},
			{At: base.Add(10 * time.Minute), SignificantWaveHeightM: 2, WindSpeedMS: 9, HeaveM: 1},
		},
	}
	app := domain.ApplicationRequest{
		Window:                     window,
		Posture:                    []domain.PostureSample{{At: base, RollDeg: 0, PitchDeg: 0}, {At: base.Add(10 * time.Minute), RollDeg: 1, PitchDeg: 1}},
		MaxAngularRateDegPerSecond: 1,
		SeaLimits:                  domain.SeaLimits{MaxWaveHeightM: 2, MaxWindSpeedMS: 9, MaxHeaveM: 1},
		Resources:                  []domain.ResourceRequirement{{ResourceID: "antenna", Quantity: 1, Critical: true, Timeout: time.Minute}},
	}
	return app, orbit, sea
}

func hasFailed(decisions []domain.Decision, code string) bool {
	for _, d := range decisions {
		if d.Code == code && !d.Passed {
			return true
		}
	}
	return false
}

func mustBatch(t *testing.T, store *persistence.MemoryStore, id string) domain.TrialBatch {
	t.Helper()
	batch, ok := store.GetBatch(id)
	if !ok {
		t.Fatalf("batch %s not found", id)
	}
	return batch
}

func postApplication(t *testing.T, baseURL, key string, body []byte) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, baseURL+"/api/v1/applications", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	got, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return res.StatusCode, got
}

func status(res *http.Response) int {
	if res == nil {
		return 0
	}
	return res.StatusCode
}
