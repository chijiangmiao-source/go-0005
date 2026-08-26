package tests

import (
	"testing"
	"time"

	"marine-survey-payload-window-orchestrator/internal/constraints"
)

func TestD01HalfOpenWindowCoverageAndAdjacentBoundary(t *testing.T) {
	app, orbit, sea := publicFixture(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	decisions := constraints.NewValidator().Validate(app, orbit, sea)
	if failed, ok := constraints.FirstFailure(decisions); ok {
		t.Fatalf("fixture rejected at adjacent half-open boundary: %#v", failed)
	}
	app.Window.Start = app.Window.End
	if !hasFailed(constraints.NewValidator().Validate(app, orbit, sea), "D01_WINDOW_RANGE") {
		t.Fatal("D01 expected invalid empty half-open window")
	}
}

func TestD02PostureInterpolationEqualsEnvelope(t *testing.T) {
	app, orbit, sea := publicFixture(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	app.Posture[1].RollDeg = 2
	app.Posture[1].PitchDeg = 2
	if failed, ok := constraints.FirstFailure(constraints.NewValidator().Validate(app, orbit, sea)); ok {
		t.Fatalf("D02 equality at orbit envelope should pass: %#v", failed)
	}
}

func TestD03AngularRateOverLimit(t *testing.T) {
	app, orbit, sea := publicFixture(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	app.Posture[1].RollDeg = 400
	if !hasFailed(constraints.NewValidator().Validate(app, orbit, sea), "D03_ANGULAR_RATE") {
		t.Fatal("D03 expected derived angular rate rejection")
	}
}

func TestSeaThresholdEqualityAndTinyExcess(t *testing.T) {
	app, orbit, sea := publicFixture(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if failed, ok := constraints.FirstFailure(constraints.NewValidator().Validate(app, orbit, sea)); ok {
		t.Fatalf("threshold equality should pass: %#v", failed)
	}
	sea.Samples[1].HeaveM += 0.001
	if !hasFailed(constraints.NewValidator().Validate(app, orbit, sea), "SEA_THRESHOLD") {
		t.Fatal("expected sea threshold rejection for tiny excess")
	}
}

func TestSnapshotGapRejectsExtrapolation(t *testing.T) {
	app, orbit, sea := publicFixture(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	orbit.Envelope = orbit.Envelope[:2]
	orbit.Envelope[1].At = app.Window.End.Add(-time.Minute)
	if !hasFailed(constraints.NewValidator().Validate(app, orbit, sea), "NO_EXTRAPOLATION") {
		t.Fatal("expected no-extrapolation failure when snapshot samples stop early")
	}
}
