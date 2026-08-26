package persistence

func (s *SQLiteStore) migrate() error {
	statements := []string{
		`pragma journal_mode = wal`,
		`pragma synchronous = normal`,
		`pragma foreign_keys = on`,
		`create table if not exists counters(
			name text primary key,
			value integer not null
		)`,
		`create table if not exists resources(
			resource_id text primary key,
			capacity integer not null check(capacity > 0),
			unit text not null
		)`,
		`create table if not exists orbit_snapshots(
			id text primary key,
			digest text not null unique,
			valid_start integer not null,
			valid_end integer not null,
			json_body text not null
		)`,
		`create table if not exists sea_snapshots(
			id text primary key,
			digest text not null unique,
			valid_start integer not null,
			valid_end integer not null,
			json_body text not null
		)`,
		`create table if not exists applications(
			id text primary key,
			state text not null,
			rejection_reason text not null default '',
			created_at integer not null,
			json_body text not null
		)`,
		`create table if not exists batches(
			id text primary key,
			application_id text,
			state text not null,
			version integer not null,
			start_us integer not null,
			end_us integer not null,
			last_event_seq integer not null default 0,
			manifest_digest text not null default '',
			json_body text not null
		)`,
		`create table if not exists reservations(
			id text primary key,
			batch_id text not null,
			resource_id text not null,
			start_us integer not null,
			end_us integer not null,
			quantity integer not null,
			released integer not null default 0,
			json_body text not null
		)`,
		`create index if not exists reservations_resource_window on reservations(resource_id,start_us,end_us)`,
		`create table if not exists liveness(
			batch_id text not null,
			resource_id text not null,
			last_device_seq integer not null,
			last_received_at integer not null,
			json_body text not null,
			primary key(batch_id, resource_id)
		)`,
		`create table if not exists telemetry_inbox(
			message_id text primary key,
			digest text not null,
			result_body text not null
		)`,
		`create table if not exists audit_events(
			aggregate_id text not null,
			aggregate_seq integer not null,
			event_type text not null,
			payload_digest text not null,
			previous_digest text not null,
			canonical_digest text not null,
			occurred_at integer not null,
			json_body text not null,
			primary key(aggregate_id, aggregate_seq)
		)`,
		`create table if not exists idempotency(
			key text primary key,
			request_digest text not null,
			status integer not null,
			response_body blob not null,
			json_body text not null
		)`,
		`create table if not exists recovery_checkpoints(
			id integer primary key check(id = 1),
			projected_at integer not null,
			last_result text not null
		)`,
		`insert into resources(resource_id,capacity,unit) values('antenna',1,'dish')
			on conflict(resource_id) do nothing`,
		`insert into resources(resource_id,capacity,unit) values('recorder',2,'channel')
			on conflict(resource_id) do nothing`,
		`insert into resources(resource_id,capacity,unit) values('downlink',1,'link')
			on conflict(resource_id) do nothing`,
	}
	for _, stmt := range statements {
		if _, err := s.db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
