CREATE TABLE association_hours (
 hour INTEGER PRIMARY KEY, revision INTEGER NOT NULL DEFAULT 1,
 computed_revision INTEGER NOT NULL DEFAULT 0, computed_at INTEGER,
 exclusions TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE association_parts (
 hour INTEGER NOT NULL REFERENCES association_hours(hour) ON DELETE CASCADE,
 cohort TEXT NOT NULL, producer TEXT NOT NULL, peer TEXT NOT NULL, kind TEXT NOT NULL,
 boot_id TEXT NOT NULL, payload TEXT NOT NULL,
 PRIMARY KEY(hour,cohort,producer,peer,kind)
);
CREATE INDEX association_parts_identity ON association_parts(cohort,producer,peer,kind,hour);
CREATE INDEX association_parts_boot ON association_parts(boot_id,hour);
CREATE INDEX association_parts_producer ON association_parts(producer,hour);
CREATE INDEX association_parts_peer ON association_parts(peer,hour);
CREATE TABLE association_evidence (
 hour INTEGER NOT NULL REFERENCES association_hours(hour) ON DELETE CASCADE,
 hash TEXT NOT NULL, PRIMARY KEY(hour,hash)
);
CREATE INDEX association_evidence_hash ON association_evidence(hash);
INSERT INTO association_hours(hour) SELECT DISTINCT arrived_at/3600000*3600 FROM arrivals;
CREATE TRIGGER association_arrival_insert AFTER INSERT ON arrivals BEGIN
 INSERT INTO association_hours(hour) VALUES(NEW.arrived_at/3600000*3600)
 ON CONFLICT(hour) DO UPDATE SET revision=revision+1;
 UPDATE association_hours SET revision=revision+1 WHERE hour=(
 SELECT min(arrived_at)/3600000*3600 FROM arrivals WHERE hash=NEW.hash AND boot_id=NEW.boot_id AND kind=NEW.kind);
END;
CREATE TRIGGER association_arrival_resolved AFTER UPDATE OF eligible ON arrivals
 WHEN NEW.eligible<>OLD.eligible BEGIN
 UPDATE association_hours SET revision=revision+1 WHERE hour IN (NEW.arrived_at/3600000*3600,
 (SELECT min(arrived_at)/3600000*3600 FROM arrivals WHERE hash=NEW.hash AND boot_id=NEW.boot_id AND kind=NEW.kind));
END;
CREATE TRIGGER association_header_update AFTER UPDATE OF verification,signer_conflict ON momentums
 WHEN NEW.verification<>OLD.verification OR NEW.signer_conflict<>OLD.signer_conflict BEGIN
 UPDATE association_hours SET revision=revision+1 WHERE hour IN
 (SELECT DISTINCT arrived_at/3600000*3600 FROM arrivals WHERE hash=NEW.hash UNION SELECT hour FROM association_evidence WHERE hash=NEW.hash);
END;
CREATE TRIGGER association_session_end AFTER UPDATE OF ended_at ON sessions
 WHEN NEW.ended_at IS NOT OLD.ended_at BEGIN
 UPDATE association_hours SET revision=revision+1
 WHERE hour*1000<=NEW.ended_at AND (hour+3600)*1000>NEW.ended_at-10000;
END;
CREATE TRIGGER association_unsafe_run AFTER UPDATE OF drops,clock_jumps,unclean ON runs
 WHEN NEW.drops<>OLD.drops OR NEW.clock_jumps<>OLD.clock_jumps OR NEW.unclean<>OLD.unclean BEGIN
 UPDATE association_hours SET revision=revision+1 WHERE hour IN
 (SELECT hour FROM association_parts WHERE boot_id=NEW.boot_id UNION
 SELECT DISTINCT arrived_at/3600000*3600 FROM arrivals WHERE boot_id=NEW.boot_id);
END;
PRAGMA user_version=2;
