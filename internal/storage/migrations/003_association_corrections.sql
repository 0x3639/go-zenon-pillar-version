-- Schema 2 is already deployed. Preserve its evidence and upgrade forward.
CREATE TABLE association_session_refs (
 hour INTEGER NOT NULL REFERENCES association_hours(hour) ON DELETE CASCADE,
 cohort TEXT NOT NULL,
 session_id TEXT NOT NULL REFERENCES sessions(session_id),
 PRIMARY KEY(hour,cohort,session_id)
);
CREATE INDEX association_session_refs_session ON association_session_refs(session_id);
INSERT OR IGNORE INTO association_session_refs(hour,cohort,session_id)
 SELECT p.hour,p.cohort,j.value FROM association_parts p,json_each(p.payload,'$.cohortSessionIds') j
 JOIN sessions s ON s.session_id=j.value;
ALTER TABLE association_hours ADD COLUMN retry_after INTEGER NOT NULL DEFAULT 0;
ALTER TABLE association_hours ADD COLUMN failures INTEGER NOT NULL DEFAULT 0;
ALTER TABLE association_hours ADD COLUMN last_error TEXT NOT NULL DEFAULT '';
CREATE TRIGGER association_revision_retry AFTER UPDATE OF revision ON association_hours
 WHEN NEW.revision<>OLD.revision BEGIN
 UPDATE association_hours SET retry_after=0,failures=0,last_error='' WHERE hour=NEW.hour;
END;
-- association-2 changes denominators. Withhold all old results until replayed;
-- expired raw history becomes an explicit exclusion, never a mixed-method row.
UPDATE association_hours SET revision=revision+1;
PRAGMA user_version=3;
