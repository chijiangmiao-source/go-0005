package tests

import (
	"encoding/json"
	"net/http"
	"testing"

	"marine-survey-payload-window-orchestrator/internal/domain"
)

func TestModel_TelemetryReplayPublishesFinalBatchState(t *testing.T) {
	type batchView struct {
		Batch    domain.TrialBatch       `json:"batch"`
		Liveness []domain.DeviceLiveness `json:"liveness"`
		Events   []domain.AuditEvent     `json:"events"`
	}

	setup := func(t *testing.T, key string) (*publicHarness, string) {
		t.Helper()
		h := newPublicHarness(t)
		app, orbit, sea := publicFixture(h.base)
		app = registerPublicSnapshots(t, h, app, orbit, sea)
		res, body := postJSON(t, h.srv.URL+"/api/v1/applications", key, app)
		if res.StatusCode != http.StatusCreated {
			t.Fatalf("create batch: status=%d body=%s", res.StatusCode, body)
		}
		return h, decodeBatchID(t, body)
	}

	getBatch := func(t *testing.T, h *publicHarness, batchID string) batchView {
		t.Helper()
		res, err := http.Get(h.srv.URL + "/api/v1/batches/" + batchID)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("get batch: status=%d", res.StatusCode)
		}
		var view batchView
		if err := json.NewDecoder(res.Body).Decode(&view); err != nil {
			t.Fatal(err)
		}
		return view
	}

	cases := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "identical replay returns the published transition result",
			run: func(t *testing.T) {
				h, batchID := setup(t, "model-replay-final-state")
				if initial := getBatch(t, h, batchID); initial.Batch.State != domain.StateReserved {
					t.Fatalf("precondition: batch state=%s, want %s", initial.Batch.State, domain.StateReserved)
				}

				first := sendHeartbeat(t, h, batchID, "critical-heartbeat", "antenna", 1)
				afterFirst := getBatch(t, h, batchID)
				replay := sendHeartbeat(t, h, batchID, "critical-heartbeat", "antenna", 1)
				afterReplay := getBatch(t, h, batchID)

				if !first.Accepted || first.Replayed {
					t.Fatalf("first heartbeat result=%#v", first)
				}
				if !replay.Accepted || !replay.Replayed {
					t.Fatalf("replayed heartbeat result=%#v", replay)
				}
				if first.BatchState != domain.StateRunning || replay.BatchState != first.BatchState {
					t.Fatalf("batch states: first=%s replay=%s, want both %s", first.BatchState, replay.BatchState, domain.StateRunning)
				}
				if first.EventSeq == 0 || replay.EventSeq != first.EventSeq {
					t.Fatalf("event sequences: first=%d replay=%d", first.EventSeq, replay.EventSeq)
				}
				if afterFirst.Batch.State != first.BatchState || afterReplay.Batch.State != first.BatchState {
					t.Fatalf("queried states: first=%s replay=%s, response=%s", afterFirst.Batch.State, afterReplay.Batch.State, first.BatchState)
				}
				if len(afterReplay.Events) != len(afterFirst.Events) || afterReplay.Batch.LastEventSeq != afterFirst.Batch.LastEventSeq {
					t.Fatalf("replay changed audit stream: before=(%d,%d) after=(%d,%d)", len(afterFirst.Events), afterFirst.Batch.LastEventSeq, len(afterReplay.Events), afterReplay.Batch.LastEventSeq)
				}
				telemetryEvents, runningEvents, matchingSeq := 0, 0, false
				for _, event := range afterReplay.Events {
					switch event.EventType {
					case "TELEMETRY_ACCEPTED":
						telemetryEvents++
						matchingSeq = matchingSeq || event.AggregateSeq == first.EventSeq
					case "BATCH_RUNNING":
						runningEvents++
					}
				}
				if telemetryEvents != 1 || runningEvents != 1 || !matchingSeq {
					t.Fatalf("queried events inconsistent with result: telemetry=%d running=%d matching_event_seq=%v", telemetryEvents, runningEvents, matchingSeq)
				}
				if len(afterReplay.Liveness) != 1 || afterReplay.Liveness[0].LastDeviceSeq != 1 {
					t.Fatalf("replay changed liveness: %#v", afterReplay.Liveness)
				}
			},
		},
		{
			name: "same message id with a different digest conflicts",
			run: func(t *testing.T) {
				h, batchID := setup(t, "model-replay-conflict")
				_ = sendHeartbeat(t, h, batchID, "digest-key", "antenna", 1)
				before := getBatch(t, h, batchID)
				conflict := sendHeartbeat(t, h, batchID, "digest-key", "antenna", 2)
				after := getBatch(t, h, batchID)
				if conflict.Accepted || conflict.Replayed || conflict.Reason != "MESSAGE_DIGEST_CONFLICT" {
					t.Fatalf("conflicting heartbeat result=%#v", conflict)
				}
				if len(after.Events) != len(before.Events) || after.Batch.LastEventSeq != before.Batch.LastEventSeq {
					t.Fatalf("digest conflict changed audit stream: before=%d after=%d", len(before.Events), len(after.Events))
				}
			},
		},
		{
			name: "new message id with a stale device sequence is rejected",
			run: func(t *testing.T) {
				h, batchID := setup(t, "model-replay-stale-sequence")
				_ = sendHeartbeat(t, h, batchID, "newer-sequence", "antenna", 2)
				before := getBatch(t, h, batchID)
				stale := sendHeartbeat(t, h, batchID, "different-message", "antenna", 1)
				after := getBatch(t, h, batchID)
				if stale.Accepted || stale.Replayed || stale.Reason != "STALE_DEVICE_SEQ" {
					t.Fatalf("stale heartbeat result=%#v", stale)
				}
				if len(after.Events) != len(before.Events) || after.Batch.LastEventSeq != before.Batch.LastEventSeq {
					t.Fatalf("stale heartbeat changed audit stream: before=%d after=%d", len(before.Events), len(after.Events))
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, tc.run)
	}
}
