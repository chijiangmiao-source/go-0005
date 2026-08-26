package persistence

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"marine-survey-payload-window-orchestrator/internal/domain"
	_ "modernc.org/sqlite"
)

type SQLiteStore struct {
	db       *sql.DB
	mu       sync.Mutex
	readyErr error
	path     string
}

func OpenSQLite(path string) (*SQLiteStore, error) {
	if path == "" {
		path = "orchestrator.db"
	}
	if dir := filepath.Dir(path); dir == "." || dir == "" {
		path = filepath.Clean(path)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &SQLiteStore{db: db, path: path}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *SQLiteStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *SQLiteStore) Ready() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readyErr
}

func (s *SQLiteStore) SetResource(spec domain.ResourceSpec) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`insert into resources(resource_id,capacity,unit) values(?,?,?)
		on conflict(resource_id) do update set capacity=excluded.capacity, unit=excluded.unit`, spec.ResourceID, spec.Capacity, spec.Unit)
	s.readyErr = err
}

func (s *SQLiteStore) Resources() map[string]domain.ResourceSpec {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`select resource_id,capacity,unit from resources order by resource_id`)
	if err != nil {
		s.readyErr = err
		return map[string]domain.ResourceSpec{}
	}
	defer rows.Close()
	out := map[string]domain.ResourceSpec{}
	for rows.Next() {
		var spec domain.ResourceSpec
		if err := rows.Scan(&spec.ResourceID, &spec.Capacity, &spec.Unit); err == nil {
			out[spec.ResourceID] = spec
		}
	}
	return out
}

func (s *SQLiteStore) SaveOrbit(snapshot domain.OrbitSnapshot) (domain.OrbitSnapshot, error) {
	return saveSnapshot(s, snapshot, "orbit_snapshots", "orbit", verifyOrbitSnapshot, setOrbitSnapshotID)
}

func (s *SQLiteStore) SaveSea(snapshot domain.SeaSnapshot) (domain.SeaSnapshot, error) {
	return saveSnapshot(s, snapshot, "sea_snapshots", "sea", verifySeaSnapshot, setSeaSnapshotID)
}

type snapshotVerifier[T any] func(T) (snapshotRow[T], error)
type snapshotIDSetter[T any] func(T, string) T

type snapshotRow[T any] struct {
	value  T
	id     string
	digest string
	valid  domain.TimeRange
}

func saveSnapshot[T any](store *SQLiteStore, snapshot T, table, prefix string, verify snapshotVerifier[T], setID snapshotIDSetter[T]) (T, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	row, err := verify(snapshot)
	if err != nil {
		return row.value, err
	}
	if row.id == "" {
		row.id = store.nextIDLocked(prefix)
		row.value = setID(row.value, row.id)
	}
	if err := store.insertSnapshotLocked(table, row.id, row.digest, row.valid, row.value); err != nil {
		return row.value, err
	}
	return row.value, nil
}

func verifyOrbitSnapshot(snapshot domain.OrbitSnapshot) (snapshotRow[domain.OrbitSnapshot], error) {
	verified, err := snapshot.WithVerifiedDigest()
	return snapshotRow[domain.OrbitSnapshot]{
		value:  verified,
		id:     verified.ID,
		digest: verified.Digest,
		valid:  verified.Valid,
	}, err
}

func verifySeaSnapshot(snapshot domain.SeaSnapshot) (snapshotRow[domain.SeaSnapshot], error) {
	verified, err := snapshot.WithVerifiedDigest()
	return snapshotRow[domain.SeaSnapshot]{
		value:  verified,
		id:     verified.ID,
		digest: verified.Digest,
		valid:  verified.Valid,
	}, err
}

func setOrbitSnapshotID(snapshot domain.OrbitSnapshot, id string) domain.OrbitSnapshot {
	snapshot.ID = id
	return snapshot
}

func setSeaSnapshotID(snapshot domain.SeaSnapshot, id string) domain.SeaSnapshot {
	snapshot.ID = id
	return snapshot
}

func (s *SQLiteStore) insertSnapshotLocked(table, id, digest string, valid domain.TimeRange, snapshot any) error {
	body, err := encode(snapshot)
	if err != nil {
		return err
	}
	query := fmt.Sprintf(`insert into %s(id,digest,valid_start,valid_end,json_body) values(?,?,?,?,?)`, table)
	_, err = s.db.Exec(query, id, digest, micros(valid.Start), micros(valid.End), body)
	return err
}

func (s *SQLiteStore) Orbit(id string) (domain.OrbitSnapshot, bool) {
	var out domain.OrbitSnapshot
	ok := s.oneJSON(`select json_body from orbit_snapshots where (?='' or id=?) order by rowid limit 1`, []any{id, id}, &out)
	return out, ok
}

func (s *SQLiteStore) Sea(id string) (domain.SeaSnapshot, bool) {
	var out domain.SeaSnapshot
	ok := s.oneJSON(`select json_body from sea_snapshots where (?='' or id=?) order by rowid limit 1`, []any{id, id}, &out)
	return out, ok
}

func (s *SQLiteStore) SaveApplication(app domain.Application) (domain.Application, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if app.Request.ID == "" {
		app.Request.ID = s.nextIDLocked("app")
	}
	if app.CreatedAt.IsZero() {
		app.CreatedAt = domain.NormalizeTime(time.Now())
	}
	body, err := encode(app)
	if err != nil {
		return app, err
	}
	_, err = s.db.Exec(`insert into applications(id,state,rejection_reason,created_at,json_body)
		values(?,?,?,?,?)
		on conflict(id) do update set state=excluded.state,rejection_reason=excluded.rejection_reason,json_body=excluded.json_body`,
		app.Request.ID, string(app.State), app.RejectionReason, micros(app.CreatedAt), body)
	return app, err
}

func (s *SQLiteStore) Application(id string) (domain.Application, bool) {
	var out domain.Application
	ok := s.oneJSON(`select json_body from applications where id=?`, []any{id}, &out)
	return out, ok
}

func (s *SQLiteStore) AllocateBatchID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextIDLocked("batch")
}

func (s *SQLiteStore) SaveBatch(batch domain.TrialBatch) error {
	if batch.ID == "" {
		return errors.New("batch id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveBatchLocked(batch)
}

func (s *SQLiteStore) GetBatch(id string) (domain.TrialBatch, bool) {
	var out domain.TrialBatch
	ok := s.oneJSON(`select json_body from batches where id=?`, []any{id}, &out)
	return out, ok
}

func (s *SQLiteStore) UpdateBatch(batch domain.TrialBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var exists int
	if err := s.db.QueryRow(`select count(*) from batches where id=?`, batch.ID).Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		return errors.New("batch not found")
	}
	return s.saveBatchLocked(batch)
}

func (s *SQLiteStore) Reservations() []domain.Reservation {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`select json_body from reservations where released=0 order by id`)
	if err != nil {
		s.readyErr = err
		return nil
	}
	defer rows.Close()
	out := []domain.Reservation{}
	for rows.Next() {
		var body string
		var res domain.Reservation
		if rows.Scan(&body) == nil && json.Unmarshal([]byte(body), &res) == nil {
			out = append(out, res)
		}
	}
	return out
}

func (s *SQLiteStore) SaveReservation(res domain.Reservation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	body, err := encode(res)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`insert into reservations(id,batch_id,resource_id,start_us,end_us,quantity,released,json_body)
		values(?,?,?,?,?,?,?,?)`, res.ID, res.BatchID, res.ResourceID, micros(res.Window.Start), micros(res.Window.End), res.Quantity, boolInt(res.Released), body)
	return err
}

func (s *SQLiteStore) ReleaseBatchReservations(batchID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`delete from reservations where batch_id=?`, batchID)
	return err
}

func (s *SQLiteStore) Liveness(batchID string) []domain.DeviceLiveness {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`select json_body from liveness where batch_id=? order by resource_id`, batchID)
	if err != nil {
		s.readyErr = err
		return nil
	}
	defer rows.Close()
	var out []domain.DeviceLiveness
	for rows.Next() {
		var body string
		var live domain.DeviceLiveness
		if rows.Scan(&body) == nil && json.Unmarshal([]byte(body), &live) == nil {
			out = append(out, live)
		}
	}
	return out
}

func (s *SQLiteStore) GetLiveness(batchID, resourceID string) (domain.DeviceLiveness, bool) {
	var out domain.DeviceLiveness
	ok := s.oneJSON(`select json_body from liveness where batch_id=? and resource_id=?`, []any{batchID, resourceID}, &out)
	return out, ok
}

func (s *SQLiteStore) SaveLiveness(live domain.DeviceLiveness) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	body, err := encode(live)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`insert into liveness(batch_id,resource_id,last_device_seq,last_received_at,json_body)
		values(?,?,?,?,?)
		on conflict(batch_id,resource_id) do update set last_device_seq=excluded.last_device_seq,last_received_at=excluded.last_received_at,json_body=excluded.json_body`,
		live.BatchID, live.ResourceID, live.LastDeviceSeq, micros(live.LastReceivedAt), body)
	return err
}

func (s *SQLiteStore) Inbox(messageID string) (string, domain.TelemetryResult, bool) {
	var digest, body string
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.db.QueryRow(`select digest,result_body from telemetry_inbox where message_id=?`, messageID).Scan(&digest, &body)
	if err != nil {
		return "", domain.TelemetryResult{}, false
	}
	var result domain.TelemetryResult
	if json.Unmarshal([]byte(body), &result) != nil {
		return "", domain.TelemetryResult{}, false
	}
	return digest, result, true
}

func (s *SQLiteStore) SaveInbox(messageID string, digest string, result domain.TelemetryResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	body, err := encode(result)
	if err != nil {
		s.readyErr = err
		return
	}
	_, s.readyErr = s.db.Exec(`insert into telemetry_inbox(message_id,digest,result_body)
		values(?,?,?) on conflict(message_id) do nothing`, messageID, digest, body)
}

func (s *SQLiteStore) AppendEvent(aggregateID, eventType string, payload any) (domain.AuditEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendEventLocked(aggregateID, eventType, payload, domain.NormalizeTime(time.Now()))
}

func (s *SQLiteStore) EventsAfter(aggregateID string, afterSeq int) []domain.AuditEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`select json_body from audit_events where aggregate_id=? and aggregate_seq>? order by aggregate_seq`, aggregateID, afterSeq)
	if err != nil {
		s.readyErr = err
		return nil
	}
	defer rows.Close()
	var out []domain.AuditEvent
	for rows.Next() {
		var body string
		var ev domain.AuditEvent
		if rows.Scan(&body) == nil && json.Unmarshal([]byte(body), &ev) == nil {
			out = append(out, ev)
		}
	}
	return out
}

func (s *SQLiteStore) Idempotency(key string) (IdempotencyRecord, bool) {
	var rec IdempotencyRecord
	ok := s.oneJSON(`select json_body from idempotency where key=?`, []any{key}, &rec)
	return rec, ok
}

func (s *SQLiteStore) SaveIdempotency(key, requestDigest string, status int, body []byte) IdempotencyRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := IdempotencyRecord{Key: key, RequestDigest: requestDigest, Status: status, Body: append([]byte(nil), body...)}
	jsonBody, err := encode(rec)
	if err != nil {
		s.readyErr = err
		return rec
	}
	_, s.readyErr = s.db.Exec(`insert into idempotency(key,request_digest,status,response_body,json_body)
		values(?,?,?,?,?) on conflict(key) do nothing`, key, requestDigest, status, body, jsonBody)
	return rec
}

func (s *SQLiteStore) OpenBatchIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`select id from batches where state not in ('REJECTED','COMPLETED','ABORTED') order by start_us,id`)
	if err != nil {
		s.readyErr = err
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil {
			out = append(out, id)
		}
	}
	return out
}

func (s *SQLiteStore) saveBatchLocked(batch domain.TrialBatch) error {
	body, err := encode(batch)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`insert into batches(id,application_id,state,version,start_us,end_us,last_event_seq,manifest_digest,json_body)
		values(?,?,?,?,?,?,?,?,?)
		on conflict(id) do update set state=excluded.state,version=excluded.version,last_event_seq=excluded.last_event_seq,manifest_digest=excluded.manifest_digest,json_body=excluded.json_body`,
		batch.ID, batch.ApplicationID, string(batch.State), batch.Version, micros(batch.Window.Start), micros(batch.Window.End), batch.LastEventSeq, batch.FinalManifestDigest, body)
	return err
}

func (s *SQLiteStore) oneJSON(query string, args []any, dst any) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	var body string
	if err := s.db.QueryRow(query, args...).Scan(&body); err != nil {
		return false
	}
	return json.Unmarshal([]byte(body), dst) == nil
}

func (s *SQLiteStore) nextIDLocked(prefix string) string {
	var value int64
	_, _ = s.db.Exec(`insert into counters(name,value) values(?,0) on conflict(name) do nothing`, prefix)
	_ = s.db.QueryRow(`update counters set value=value+1 where name=? returning value`, prefix).Scan(&value)
	return fmt.Sprintf("%s-%d", prefix, value)
}

func encode(v any) (string, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func micros(t time.Time) int64 { return domain.NormalizeTime(t).UnixMicro() }

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
