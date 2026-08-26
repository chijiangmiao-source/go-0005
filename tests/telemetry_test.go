package tests

import (
	"testing"
	"time"
)

func TestD07SameMessageIDAndDigestReplays(t *testing.T) {
	h := newPublicHarness(t)
	app, orbit, sea := publicFixture(h.base)
	app = registerPublicSnapshots(t, h, app, orbit, sea)
	_, body := postJSON(t, h.srv.URL+"/api/v1/applications", "telemetry-replay", app)
	batchID := decodeBatchID(t, body)
	results := runTelemetryScript(t, h, batchID, []simStep{{Kind: "Send", MessageID: "m1", ResourceID: "antenna", Seq: 1}, {Kind: "Duplicate", MessageID: "m1", ResourceID: "antenna", Seq: 1}})
	if !results[0].Accepted || !results[1].Replayed {
		t.Fatalf("D07 expected accepted then replay, got %#v", results)
	}
}

func TestSameTelemetryIDDifferentDigestConflicts(t *testing.T) {
	h := newPublicHarness(t)
	app, orbit, sea := publicFixture(h.base)
	app = registerPublicSnapshots(t, h, app, orbit, sea)
	_, body := postJSON(t, h.srv.URL+"/api/v1/applications", "telemetry-conflict", app)
	batchID := decodeBatchID(t, body)
	_ = sendHeartbeat(t, h, batchID, "same", "antenna", 1)
	result := sendHeartbeat(t, h, batchID, "same", "antenna", 2)
	if result.Accepted || result.Reason != "MESSAGE_DIGEST_CONFLICT" {
		t.Fatalf("expected digest conflict, got %#v", result)
	}
}

func TestNewTelemetryIDWithStaleSequenceRejected(t *testing.T) {
	h := newPublicHarness(t)
	app, orbit, sea := publicFixture(h.base)
	app = registerPublicSnapshots(t, h, app, orbit, sea)
	_, body := postJSON(t, h.srv.URL+"/api/v1/applications", "telemetry-stale", app)
	batchID := decodeBatchID(t, body)
	_ = sendHeartbeat(t, h, batchID, "s1", "antenna", 2)
	result := sendHeartbeat(t, h, batchID, "s2", "antenna", 1)
	if result.Accepted || result.Reason != "STALE_DEVICE_SEQ" {
		t.Fatalf("expected stale sequence rejection, got %#v", result)
	}
}

func TestConcurrentDuplicateTelemetryRefreshesOnce(t *testing.T) {
	h := newPublicHarness(t)
	app, orbit, sea := publicFixture(h.base)
	app = registerPublicSnapshots(t, h, app, orbit, sea)
	_, body := postJSON(t, h.srv.URL+"/api/v1/applications", "telemetry-concurrent", app)
	batchID := decodeBatchID(t, body)
	results := runTelemetryScript(t, h, batchID, []simStep{{Kind: "Concurrent", MessageID: "cx", ResourceID: "antenna", Seq: 1}})
	accepted := 0
	replayed := 0
	for _, result := range results {
		if result.Accepted {
			accepted++
		}
		if result.Replayed {
			replayed++
		}
	}
	if accepted != 2 || replayed != 1 {
		t.Fatalf("expected one original and one replay, got %#v", results)
	}
	if events := h.store.EventsAfter(batchID, 0); len(events) != 3 {
		t.Fatalf("expected reserve, telemetry, state events only once each, got %d", len(events))
	}
}

func TestTelemetryShapeValidationRejectsBadHeartbeat(t *testing.T) {
	h := newPublicHarness(t)
	h.clock.Set(h.base.Add(time.Second))
	result := sendHeartbeat(t, h, "missing", "", "antenna", 1)
	if result.Accepted {
		t.Fatal("expected missing message id rejection")
	}
}

func TestTelemetrySimulatorAdvanceDirective(t *testing.T) {
	h := newPublicHarness(t)
	before := h.clock.Now()
	runTelemetryScript(t, h, "none", []simStep{{Kind: "Advance", Advance: time.Minute}})
	if got := h.clock.Now(); got.Sub(before) != time.Minute {
		t.Fatalf("expected manual clock advance, got %s", got.Sub(before))
	}
}
