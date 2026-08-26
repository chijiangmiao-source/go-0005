package tests

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"marine-survey-payload-window-orchestrator/internal/api"
	"marine-survey-payload-window-orchestrator/internal/domain"
	"marine-survey-payload-window-orchestrator/internal/execution"
	"marine-survey-payload-window-orchestrator/internal/persistence"
)

func TestModel_SchedulerTerminalManifestMatchesPublicEventStream(t *testing.T) {
	base := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name         string
		now          time.Time
		lastReceived time.Time
		wantState    domain.BatchState
		wantReason   string
	}{
		{
			name:         "window end completes with final manifest",
			now:          base.Add(10 * time.Minute),
			lastReceived: base.Add(9 * time.Minute),
			wantState:    domain.StateCompleted,
			wantReason:   "WINDOW_END",
		},
		{
			name:         "critical timeout aborts with final manifest",
			now:          base.Add(2 * time.Minute),
			lastReceived: base,
			wantState:    domain.StateAborted,
			wantReason:   "CRITICAL_DEVICE_TIMEOUT",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := persistence.NewMemoryStore()
			batchID := "terminal-batch"
			batch := domain.TrialBatch{
				ID:            batchID,
				ApplicationID: "survey-application",
				OrbitDigest:   "orbit-digest",
				SeaDigest:     "sea-digest",
				Window:        domain.TimeRange{Start: base, End: base.Add(10 * time.Minute)},
				State:         domain.StateRunning,
				Version:       2,
				StartedAt:     &base,
				Resources: []domain.ResourceRequirement{{
					ResourceID: "antenna", Quantity: 1, Critical: true, Timeout: time.Minute,
				}},
			}
			if err := store.SaveBatch(batch); err != nil {
				t.Fatal(err)
			}
			initial, err := store.AppendEvent(batchID, "SCHEDULE_RUNNING", map[string]string{"reason": "WINDOW_STARTED"})
			if err != nil {
				t.Fatal(err)
			}
			batch.LastEventSeq = initial.AggregateSeq
			if err := store.UpdateBatch(batch); err != nil {
				t.Fatal(err)
			}
			if err := store.SaveLiveness(domain.DeviceLiveness{
				BatchID: batchID, ResourceID: "antenna", LastDeviceSeq: 17,
				LastObservedAt: tc.lastReceived, LastReceivedAt: tc.lastReceived,
				Timeout: time.Minute, Critical: true,
			}); err != nil {
				t.Fatal(err)
			}

			clock := domain.NewManualClock(tc.now)
			records := execution.NewScheduler(store, store, clock, time.Hour).Tick()
			if len(records) != 1 || records[0].To != tc.wantState || records[0].Reason != tc.wantReason {
				t.Fatalf("unexpected scheduler result: %#v", records)
			}

			srv := httptest.NewServer(api.NewServer(store, clock).Routes())
			defer srv.Close()
			batchResponse, err := http.Get(srv.URL + "/api/v1/batches/" + batchID)
			if err != nil {
				t.Fatal(err)
			}
			defer batchResponse.Body.Close()
			if batchResponse.StatusCode != http.StatusOK {
				t.Fatalf("batch query returned %d", batchResponse.StatusCode)
			}
			var public struct {
				Batch    domain.TrialBatch       `json:"batch"`
				Liveness []domain.DeviceLiveness `json:"liveness"`
				Events   []domain.AuditEvent     `json:"events"`
			}
			if err := json.NewDecoder(batchResponse.Body).Decode(&public); err != nil {
				t.Fatal(err)
			}

			eventsResponse, err := http.Get(srv.URL + "/api/v1/batches/" + batchID + "/events")
			if err != nil {
				t.Fatal(err)
			}
			defer eventsResponse.Body.Close()
			if eventsResponse.StatusCode != http.StatusOK {
				t.Fatalf("events query returned %d", eventsResponse.StatusCode)
			}
			var eventFeed struct {
				Events []domain.AuditEvent `json:"events"`
			}
			if err := json.NewDecoder(eventsResponse.Body).Decode(&eventFeed); err != nil {
				t.Fatal(err)
			}

			if public.Batch.State != tc.wantState || public.Batch.TerminationReason != tc.wantReason {
				t.Fatalf("public batch terminal fields mismatch: %#v", public.Batch)
			}
			if len(eventFeed.Events) != 2 || eventFeed.Events[1].EventType != "SCHEDULE_"+string(tc.wantState) {
				t.Fatalf("terminal event missing from public feed: %#v", eventFeed.Events)
			}
			if !reflect.DeepEqual(public.Events, eventFeed.Events) {
				t.Fatalf("batch and event queries disagree: batch=%#v feed=%#v", public.Events, eventFeed.Events)
			}
			lastEvent := eventFeed.Events[len(eventFeed.Events)-1]
			if public.Batch.LastEventSeq != lastEvent.AggregateSeq {
				t.Fatalf("last_event_seq=%d, terminal event seq=%d", public.Batch.LastEventSeq, lastEvent.AggregateSeq)
			}
			wantDigest, err := domain.FinalManifestDigest(public.Batch, eventFeed.Events, public.Liveness)
			if err != nil {
				t.Fatal(err)
			}
			if public.Batch.FinalManifestDigest == "" || public.Batch.FinalManifestDigest != wantDigest {
				t.Fatalf("final_manifest_digest=%q, want digest of public terminal events and liveness %q", public.Batch.FinalManifestDigest, wantDigest)
			}
		})
	}
}
