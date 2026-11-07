# Operating the feasibility experiment

## Resource boundary

The normal Compose application uses only its own image, network and volume.
No Observatory volume, key, environment or container is shared. No public
port is needed. A separate host is the strongest eventual deployment boundary.
The local development override mounts only this project's data directory.

| Limit | Implementation |
| --- | --- |
| Connections | Default 4; ceiling 8; at most one per IP; one dial/handshake at a time |
| Runtime | Default 65 minutes; allowed 1 second–2 hours; no automatic restart |
| Memory/CPU | 512 MiB hard container limit; 320 MiB Go target; 1 CPU; 64 process limit |
| Metadata | 2,000-record nonblocking queue; 8 KiB per record; every failed enqueue counted |
| Arrivals | At most 20,000 unique session/hash/kind records per boot |
| Header cache | 2,048 headers; bounded FIFO eviction; no full payload cache |
| Hash resolution | 128 pending requests; one RPC worker; resolution requests spaced at least 1 second apart |
| Evidence files | At most 64 MiB per boot, plus small report/limit/key files |
| Storage | 2 GiB default total directory budget (1–2 GiB supported); pause at 90%; no pruning in milestone 1 |
| Free disk | Pause below max(1 GiB, 10% of containing filesystem) |
| Traffic | 250 MiB received / 50 MiB sent per conservative rolling hour |
| Logs | Docker json-file rotation, 10 MiB each, 3 files |

Traffic accounting wraps the TCP connection before encryption: authentication,
Hello, status, ping/pong, discarded transactions, requests and all other peer
traffic count. RPC traffic is separately bounded by call frequency, timeout
and response size; it is not included in the P2P hourly counter.

Reservations are persisted and synced **before** socket I/O. Shared 64 KiB
credits are charged to one-minute buckets, retaining the current minute and
the previous 60 minutes. Unused credits are conservatively discarded at minute
boundaries and on restart. Consequently reserved usage can exceed actual traffic
and pauses may occur early. Actual byte totals are also reported per boot.
Traffic is not intentionally permitted beyond the reserved budget. On a breaker,
all sessions close and the run ends; `pausedUntil` survives restart for 61 minutes.
A network read already in flight is bounded to 32 KiB per session and its bytes
have already been reserved. These safeguards are not host bandwidth quotas.
Reservation acknowledgements have a 250 ms deadline. A stalled persistence
operation prevents further credit grants and additional persistence workers;
the observer closes sessions and reports `storage_paused`. No credit is granted
for a timed-out write, even if the underlying filesystem later completes it.

The report contains peak Go heap and Linux process maximum RSS. Container memory
also includes other charges, so inspect effective settings and measured usage:

```sh
docker inspect zenon-p2p-observer-observer-1 \
  --format '{{.HostConfig.Memory}} {{.HostConfig.MemorySwap}} {{.HostConfig.NanoCpus}} {{.HostConfig.ReadonlyRootfs}}'
docker stats --no-stream zenon-p2p-observer-observer-1
```

The expected memory and swap-total values are both 536870912; NanoCpus is
1000000000. Check these on the real deployment too. Go's memory target alone
does not enforce a host limit.

## Stop, restart and recovery

```sh
docker compose stop --timeout 20 observer
docker compose up -d observer
```

Use the same Compose overrides when operating a local run. SIGTERM closes
connections, waits for workers, drains the bounded metadata queue, syncs evidence
and writes the final report. The Compose stop allowance is 20 seconds. Disk
operations can still block on a failing filesystem; the runtime's stop deadline
is the final boundary, and an interrupted run may lack a final report.

The new run uses a new boot ID and evidence file, but the same persisted peer
identity and traffic/cooldown state. Monotonic times from different boot IDs must
never be compared. Existing evidence is not replayed into new counts. Durable
cross-restart analysis belongs to the SQLite milestone.

A failed startup does not pass the milestone. A run with `final: false` may
have been interrupted and must not be treated as a completed test. Check both
the report and logs. External network or RPC failure causes bounded retries
while the process remains alive until the configured experiment duration.

If storage is unsafe, the experiment exits without deleting old data or its
identity. After inspecting and moving completed evidence to a separate location,
restart explicitly. Do not delete `identity.key` or `limits.json` to bypass a
pause. File permissions and the exclusive process lock prevent accidental
identity rotation and concurrent writers. A malformed state file fails closed.

## Backup and retention

Milestone 1 uses append-only JSONL evidence and atomic JSON reports/state, not
SQLite. Stop the observer before copying a complete directory snapshot so its
identity, limits and completed evidence are consistent. Keep backups private:
they include the P2P private key. Local snapshots count against the data budget;
prefer a separately configured external destination. Restore to a stopped
observer with restrictive ownership and permissions.

There is no automated retention yet. No seven-day/90-day retention claim is
made for this release. Preserve evidence used by a feasibility report; later
SQLite retention, online backups, migrations and rollback compatibility must
be implemented and tested before the 24–48 hour Coolify pilot.

## Deployment handoff

The user creates the new remote Git repository and Coolify application. Build
and pilot locally first. This milestone is a finite command-line experiment,
not a continuously serving production API. Do not configure Coolify to restart
it unconditionally or report its lack of HTTP service as a health failure.
The later service will use port 8080 and independent health/readiness routes.
Build immutable release images off the production host, supply the chosen
registry/domain, and verify effective container limits at that stage.
