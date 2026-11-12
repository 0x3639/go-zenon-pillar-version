# Operating the collector

## Deployment and upgrade

Use one Git-based Docker Compose application, `/compose.yaml`, internal HTTP port
8080, and the existing `observer-data:/data` volume. Do not deploy `compose.local.yaml`
or `compose.preview.yaml` remotely. The user performs the Coolify deployment.
Changing the domain does not change the observer identity.

For 0.1 → 0.2, preserve `OBSERVER_ID`, the volume, peer seeds, and traffic budgets.
Set `RUN_DURATION=0s`; the previous `65m` value remains finite. Service mode is fixed
in the normal Compose definition. SQLite schema 1 is created next to the old files.
Legacy JSONL/reports remain untouched and are not imported. The same identity and
unexpired traffic reservations are reused. There are no writes to Observatory
containers, storage, identity mappings, or discovery configuration.

For 0.2 → 0.3.0, SQLite advances to schema 2. For 0.3.0 → 0.3.1, it advances
to schema 3; schema 1 can also upgrade directly. Back up before changing schemas.
See [corrections and replay](association-corrections.md).

Allow an active feasibility experiment to finish before deploying this update.
Use four configured public peers on distinct IPs for the next pilot. A two-seed
configuration remains a two-peer observation even if MAX_PEERS is four.

A separate host gives the strongest isolation. Prefer a separate Coolify build
server or build the image elsewhere and deploy a registry digest. Source builds
on a shared server consume resources outside the observer's runtime limits.

## States and containment

| Boundary | Behavior |
| --- | --- |
| Process | One non-root container; one CPU; 512 MiB hard memory; no additional swap; 64 pids |
| Storage | SQLite WAL; one write connection and bounded 2,000-record capture queue; batches at most 100 records / 250 ms |
| Records | At most 8 KiB per queued record; headers and metadata only |
| Network | Four peers by default, ceiling eight; one dial; one peer per IP; no inbound P2P |
| Traffic | 250 MiB received / 50 MiB sent per conservative rolling hour; state survives restart |
| Cache | Ten minutes / at most 20,000 arrival records; 2,048 checked headers; 128 pending resolutions |
| Data | 2 GiB total by default; SQLite main-file ceiling 75% of budget; sidecars and backups count |
| Filesystem | Pause below max(1 GiB, 10% free) or above 90% data budget |
| Logs | Compact status every 30 seconds; Docker rotation 10 MiB × 3 |
| Shutdown | 20-second container grace; cancel network work, drain writer, checkpoint, stop HTTP |
| Restart | `on-failure:3`; dependency outages do not terminate the process |

`observing` means a fresh reference permits collection; check `connectedPeers` and
recent momentum counts as well. `reference_degraded` retains existing peer sessions
but waits for a fresh reference before dialing more. Network calls have deadlines.
`traffic_paused` closes sessions and stops new reference intake during the persisted
61-minute cooldown; it resumes conservatively after expiry. This is not a host
bandwidth quota. RPC traffic is bounded separately, not charged to P2P counters.

`storage_paused` closes peers while preserving HTTP access to prior results where
possible. Maintenance can recover space and resume collection. SQLite writer or
budget-persistence failures fail closed until an operator fixes the cause and
restarts. Queue saturation/cache overflow yields `capture_paused`, visible drop
counts and failed readiness; restart only after inspecting the cause. Clock jumps
are recorded and invalidate capture eligibility for the run.
`associationSummariesAvailable` means current-method summary rows exist and chain
continuity is resolved. It does not mean any row meets the 100-sample/three-day
threshold. `rankingsAvailable` in data quality and `rankingAvailable` in status
remain deprecated aliases with exactly the same meaning; use each row’s
`associationStatus` and holdout result to interpret it.

`/healthz` is liveness; `/readyz` is initialization/write readiness. External RPC
outages alone do not fail readiness. The status snapshot is marked stale after
30 seconds without refresh. HTTP remains separate from peer I/O and uses bounded
queries. API readiness is not proof that momentums are being collected.

Verify effective limits on the actual Coolify container; expected memory and total
swap values are both 536870912, NanoCpus 1000000000, pids 64, read-only root, dropped
capabilities and no-new-privileges. Also verify the `/data` mount survived the upgrade.
The app cannot certify a remote container's effective settings itself.

## Reconciliation and retention

A preferred healthy RPC supplies fresh checked headers at a ten-second target.
A separate worker checks up to 180 recent heights, in batches of at most 30, against
two available configured endpoints about once a minute. The operation has a
45-second deadline; there is no unbounded backfill. RPC records are limited to
256 KiB per response. Pillar registration snapshots are refreshed every five minutes,
at most four pages of 100. Source failure is visible; old source timestamps remain.

Conflicting hashes are retained as disputed. An agreed hash can replace a prior
branch at the same height without deleting that branch. A missing shared continuity
anchor marks continuity unresolved; that flag persists across restarts and requires
operator review. There is no automatic reset or full-chain repair endpoint.
Hash agreement is not consensus validation or proof of independent RPC operators.
Conflicting signing identities for a hash remain disputed and are never attributed.

Raw arrivals, names and gaps are retained seven days (configurable 1–7).
Hourly association summaries and their supporting headers/sessions are retained
up to 90 days, within the same storage budget. Dated name labels are embedded in
summaries. Runs remain while their sessions are retained. Maintenance deletes expired rows in batches of 1,000, checkpoints WAL, and
reclaims at most 128 free pages per incremental-vacuum pass. At 80% budget it runs
every ten seconds instead of every minute. It never removes identity, traffic state,
backups, or legacy experiment evidence. No full VACUUM is used. Expired raw data cannot be recomputed later. Association rows expose
`rawEvidenceAvailable`; invalidated expired summaries are withheld. See
[association analysis](association-analysis.md) for schema-2 upgrade, background
replay, query rules and diagnostics.

## Backup and rollback

In the observer container Terminal, create a consistent online database backup:

```sh
observer backup backup-2026-11-07.sqlite
```

This uses [SQLite's online backup API](https://www.sqlite.org/backup.html) in small steps, checks the result, refuses to
overwrite files, and enforces the total data budget. The filename must be new and
end in `.sqlite`. It resides under `/data` and counts against the budget. Copy a
completed backup to private external storage, then remove that backup explicitly
when appropriate. Never copy only the live `observer.sqlite` file.

A database backup does not include `identity.key` or `limits.json`. For full recovery,
stop the observer and take a private snapshot of its entire volume, preserving
ownership (10001:10001) and restrictive permissions. The snapshot contains a private
P2P key. Restore only with the observer stopped. Do not delete state files to reset
a traffic cooldown. Never share this volume between two running observers.

Before a schema upgrade, preserve a full consistent snapshot and record the image
digest. Newer unknown schema versions are refused. Roll back with the compatible
image plus its matching snapshot; do not point an older image at a newer database.
The old 0.1 experiment does not read SQLite and cannot expose this API.

## Pilot acceptance

The four-peer 24–48 hour remote pilot is still required. Record at least two healthy
sessions for 90% of minutes, at least 1,000 distinct checked live momentums and zero
unreported drops. Sample effective CPU/memory, traffic, logs, database/WAL growth,
and existing Observatory health if the host is shared. Verify observer stop/restart
leaves that application unaffected. Do not interpret a short local check as this
pilot, or a 48-hour pilot as the three-day association-analysis threshold.
