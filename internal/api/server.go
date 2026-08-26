package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

	"marine-survey-payload-window-orchestrator/internal/constraints"
	"marine-survey-payload-window-orchestrator/internal/domain"
	"marine-survey-payload-window-orchestrator/internal/execution"
	"marine-survey-payload-window-orchestrator/internal/persistence"
	"marine-survey-payload-window-orchestrator/internal/reservation"
	"marine-survey-payload-window-orchestrator/internal/telemetry"
)

type Server struct {
	store     persistence.Store
	clock     domain.Clock
	validator *constraints.Validator
	reserver  *reservation.Reserver
	receiver  *telemetry.Receiver
}

func NewServer(store persistence.Store, clock domain.Clock) *Server {
	machine := execution.NewMachine(clock)
	return &Server{store: store, clock: clock, validator: constraints.NewValidator(), reserver: reservation.NewReserver(store), receiver: telemetry.NewReceiver(store, clock, machine)}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/static/", s.handleStatic)
	mux.HandleFunc("/health/live", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]bool{"live": true})
	})
	mux.HandleFunc("/health/ready", s.handleReady)
	mux.HandleFunc("/api/v1/resources", s.handleResources)
	mux.HandleFunc("/api/v1/orbit-snapshots", s.handleOrbitSnapshots)
	mux.HandleFunc("/api/v1/sea-snapshots", s.handleSeaSnapshots)
	mux.HandleFunc("/api/v1/applications", s.handleApplications)
	mux.HandleFunc("/api/v1/applications/", s.handleApplication)
	mux.HandleFunc("/api/v1/batches/", s.handleBatch)
	return mux
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	http.ServeFileFS(w, r, staticFiles, "static/index.html")
}

func (s *Server) handleStatic(w http.ResponseWriter, r *http.Request) {
	http.ServeFileFS(w, r, staticFiles, strings.TrimPrefix(r.URL.Path, "/"))
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	s.writeReadiness(w)
}

func (s *Server) handleOrbitSnapshots(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var snapshot domain.OrbitSnapshot
	if !decodeJSON(w, r, &snapshot) {
		return
	}
	created, err := s.store.SaveOrbit(snapshot)
	if err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"reason": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (s *Server) handleSeaSnapshots(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var snapshot domain.SeaSnapshot
	if !decodeJSON(w, r, &snapshot) {
		return
	}
	created, err := s.store.SaveSea(snapshot)
	if err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"reason": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (s *Server) handleApplications(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"reason": "IDEMPOTENCY_KEY_REQUIRED"})
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"reason": "BODY_READ_FAILED"})
		return
	}
	digest, err := domain.DigestJSONBytes(body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"reason": "REQUEST_DIGEST_FAILED"})
		return
	}
	if rec, ok := s.store.Idempotency(key); ok {
		if rec.RequestDigest != digest {
			writeJSON(w, http.StatusConflict, map[string]string{"reason": "IDEMPOTENCY_DIGEST_CONFLICT"})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(rec.Status)
		_, _ = w.Write(rec.Body)
		return
	}
	var req domain.ApplicationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"reason": "INVALID_JSON"})
		return
	}
	req = req.Normalize()
	req.IdempotencyDigest = digest
	status, response := s.processApplication(req)
	payload, _ := json.Marshal(response)
	s.store.SaveIdempotency(key, digest, status, payload)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(payload)
}

func (s *Server) processApplication(req domain.ApplicationRequest) (int, any) {
	if err := req.ValidateShape(); err != nil {
		return http.StatusUnprocessableEntity, map[string]string{"reason": err.Error()}
	}
	orbit, ok := s.store.Orbit(req.OrbitSnapshotID)
	if !ok {
		return http.StatusUnprocessableEntity, map[string]string{"reason": "ORBIT_SNAPSHOT_REQUIRED"}
	}
	sea, ok := s.store.Sea(req.SeaSnapshotID)
	if !ok {
		return http.StatusUnprocessableEntity, map[string]string{"reason": "SEA_SNAPSHOT_REQUIRED"}
	}
	decisions := s.validator.Validate(req, orbit, sea)
	state := domain.StateValidated
	reason := ""
	if failed, ok := constraints.FirstFailure(decisions); ok {
		state = domain.StateRejected
		reason = failed.Code
	}
	app, _ := s.store.SaveApplication(domain.Application{Request: req, State: state, Decisions: decisions, RejectionReason: reason, CreatedAt: s.clock.Now()})
	if state == domain.StateRejected {
		_, _ = s.store.AppendEvent(app.Request.ID, "APPLICATION_REJECTED", map[string]string{"reason": reason})
		return http.StatusUnprocessableEntity, map[string]any{"application_id": app.Request.ID, "state": state, "reason": reason, "decisions": decisions}
	}
	batchID := s.store.AllocateBatchID()
	reservations, conflict, err := s.reserver.Reserve(batchID, req.Window, req.Resources)
	if err != nil {
		return http.StatusServiceUnavailable, map[string]string{"reason": err.Error()}
	}
	if conflict != nil {
		_, _ = s.store.SaveApplication(domain.Application{Request: app.Request, State: domain.StateRejected, Decisions: decisions, RejectionReason: "RESOURCE_CONFLICT", CreatedAt: app.CreatedAt})
		_, _ = s.store.AppendEvent(app.Request.ID, "APPLICATION_REJECTED", conflict)
		return http.StatusConflict, map[string]any{"application_id": app.Request.ID, "state": domain.StateRejected, "reason": "RESOURCE_CONFLICT", "conflict": conflict}
	}
	batch := domain.TrialBatch{ID: batchID, ApplicationID: app.Request.ID, OrbitDigest: orbit.Digest, SeaDigest: sea.Digest, Window: req.Window.Normalize(), State: domain.StateReserved, Version: 1, Resources: req.Resources}
	_ = s.store.SaveBatch(batch)
	for _, resource := range req.Resources {
		_ = s.store.SaveLiveness(domain.NewLivenessSeed(batchID, resource, s.clock.Now()))
	}
	event, _ := s.store.AppendEvent(batchID, "BATCH_RESERVED", reservations)
	batch.LastEventSeq = event.AggregateSeq
	_ = s.store.UpdateBatch(batch)
	return http.StatusCreated, map[string]any{"application_id": app.Request.ID, "batch_id": batchID, "state": batch.State, "event_seq": event.AggregateSeq}
}

func (s *Server) handleApplication(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/applications/")
	if r.Method != http.MethodGet || id == "" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	app, ok := s.store.Application(id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, app)
}

func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/batches/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	batchID := parts[0]
	if len(parts) == 2 && parts[1] == "events" {
		after, _ := strconv.Atoi(r.URL.Query().Get("after_seq"))
		writeJSON(w, http.StatusOK, map[string]any{"events": s.store.EventsAfter(batchID, after)})
		return
	}
	if len(parts) == 3 && parts[1] == "telemetry" && parts[2] == "heartbeats" {
		s.handleHeartbeat(w, r, batchID)
		return
	}
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	batch, ok := s.store.GetBatch(batchID)
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"batch": batch, "liveness": s.store.Liveness(batchID), "events": s.store.EventsAfter(batchID, 0)})
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request, batchID string) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var hb domain.TelemetryHeartbeat
	if !decodeJSON(w, r, &hb) {
		return
	}
	result, err := s.receiver.Receive(batchID, hb)
	if err != nil && !result.Accepted {
		mapped := mapTelemetryError(err)
		if result.Reason == "" {
			result.Reason = mapped.code
		}
		writeJSON(w, mapped.status, result)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"reason": "INVALID_JSON"})
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
