CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE runs (
 boot_id TEXT PRIMARY KEY, observer_id TEXT NOT NULL, started_at INTEGER NOT NULL,
 last_seen INTEGER NOT NULL, stopped_at INTEGER, release TEXT NOT NULL,
 upstream_revision TEXT NOT NULL, fingerprint TEXT NOT NULL, state TEXT NOT NULL,
 drops INTEGER NOT NULL DEFAULT 0, clock_jumps INTEGER NOT NULL DEFAULT 0,
 unclean INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE sessions (
 session_id TEXT PRIMARY KEY, boot_id TEXT NOT NULL REFERENCES runs(boot_id),
 peer_id TEXT NOT NULL, ip TEXT NOT NULL, port INTEGER NOT NULL,
 client_name TEXT NOT NULL, advertised_version TEXT, capabilities TEXT NOT NULL,
 started_at INTEGER NOT NULL, ready_at INTEGER NOT NULL, start_mono INTEGER NOT NULL,
 ended_at INTEGER, end_mono INTEGER, reason TEXT NOT NULL DEFAULT ''
);
CREATE INDEX sessions_time ON sessions(started_at);
CREATE INDEX sessions_peer ON sessions(peer_id, started_at);
CREATE TABLE momentums (
 hash TEXT PRIMARY KEY, height INTEGER NOT NULL, previous_hash TEXT NOT NULL,
 timestamp INTEGER NOT NULL, producer TEXT NOT NULL, protocol_version INTEGER NOT NULL,
 verification TEXT NOT NULL, source TEXT NOT NULL, first_seen INTEGER NOT NULL,
 last_seen INTEGER NOT NULL, reference_checked_at INTEGER, signer_conflict INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX momentums_height ON momentums(height);
CREATE INDEX momentums_time ON momentums(timestamp);
CREATE INDEX momentums_producer ON momentums(producer, timestamp);
CREATE TABLE arrivals (
 id INTEGER PRIMARY KEY, observer_id TEXT NOT NULL, boot_id TEXT NOT NULL,
 session_id TEXT NOT NULL REFERENCES sessions(session_id), peer_id TEXT NOT NULL,
 hash TEXT NOT NULL, kind TEXT NOT NULL, arrived_at INTEGER NOT NULL, arrived_utc TEXT NOT NULL,
 arrived_ns INTEGER NOT NULL, monotonic_ns INTEGER NOT NULL, duplicates INTEGER NOT NULL DEFAULT 0,
 eligible INTEGER NOT NULL, exclusion TEXT NOT NULL,
 UNIQUE(boot_id, session_id, hash, kind)
);
CREATE INDEX arrivals_time ON arrivals(arrived_at);
CREATE INDEX arrivals_hash ON arrivals(hash, arrived_at);
CREATE INDEX arrivals_session ON arrivals(session_id);
CREATE TABLE reference_checks (
 height INTEGER NOT NULL, source TEXT NOT NULL, hash TEXT NOT NULL,
 observed_at INTEGER NOT NULL, PRIMARY KEY(height, source)
);
CREATE TABLE pillar_snapshots (
 id INTEGER PRIMARY KEY, producer TEXT NOT NULL, name TEXT NOT NULL,
 source TEXT NOT NULL, observed_at INTEGER NOT NULL
);
CREATE INDEX pillar_snapshots_producer ON pillar_snapshots(producer, observed_at);
CREATE TABLE gaps (
 id INTEGER PRIMARY KEY, boot_id TEXT NOT NULL, kind TEXT NOT NULL,
 observed_at INTEGER NOT NULL, detail TEXT NOT NULL
);
CREATE INDEX gaps_time ON gaps(observed_at);
PRAGMA user_version=1;
