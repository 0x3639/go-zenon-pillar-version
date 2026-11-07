# API contract, schema 1.0

HTTP listens on `0.0.0.0:8080`; Coolify supplies HTTPS. The root returns service
information, not a dashboard. This public observation API has no administrative
routes or authentication.

Every API response includes `schemaVersion`, `observerId`, `generatedAt`,
`windowStart`, `windowEnd`, `methodVersion`, `dataQuality` and `result`.
`generatedAt` is the operational snapshot time, refreshed about every ten seconds.
Arrival and reference timestamps retain their original observation times. Failed
RPC refreshes never advance the last successful reference timestamp. Status counts
cover ten minutes of in-memory arrivals; historical queries read SQLite.

## Collections

`/api/v1/peers` returns `result.data` session rows with full peer ID, endpoint, raw
client name, nullable advertised version, capabilities, version observation time,
connection interval and boot ID. Reconnecting or changing IP/version creates a new
session. After an unclean restart, old sessions close at their last heartbeat and
are marked `uncleanShutdown`; the actual disconnect instant is unknown.

`/api/v1/momentums` returns checked metadata. Producer addresses derive from checked
signing keys and are not consensus certifications. Conflicting signer claims for
one hash produce a null address and `signerConflict: true`. Verification states
are `signature_checked`, `rpc_observed`, `rpc_agreed`, and `disputed`. Unresolved
announcements appear only in arrival evidence.

`/api/v1/momentums/{hash}/arrivals` returns original UTC nanosecond timestamps,
boot-relative monotonic times, event category, duplicate counts, capture eligibility
and exclusion, session version/IP and current header verification. RPC resolution
never changes arrival clocks. `eligible` records the original capture/warmup checks;
`rankingEligible` is false in this release, including for subsequently disputed
headers. Duplicate counts saturate at 65,535 and are lower bounds after cache eviction.

`/api/v1/pillars/{address}` returns `nameSnapshots`, `momentums`, their separate next
offsets, and empty association/assertion arrays. `associationAnalysisAvailable` is
false. Names are dated snapshots, not retrospective producing-address assignments.

`/api/v1/associations` returns `available: false`, empty `data`, and
`reason: association_analysis_not_implemented`. Comparable denominators, cohorts,
ties, delay distributions, three-day thresholds and holdouts are not implemented.
No 90-day aggregate query is advertised. Producer software versions remain null.

## Parameters

| Parameter | Bounds |
| --- | --- |
| `from`, `to` | RFC3339 instants; half-open interval, at most 24 hours within raw retention |
| `limit` | Default 100, maximum 500 |
| `offset` | Default 0, maximum 100000; narrow the window beyond this |
| `producer` | Zenon address; filters momentums |
| `peerId` | Lowercase full 128-character key; filters sessions |
| `eventKind` | Supported kind; filters arrivals |

The default interval is 24 hours ending at a 30-second boundary, so newest data
can be absent for up to 30 seconds. Supply an explicit `to` for fresher observations.
Keep both dates fixed while paging with `result.nextOffset`. Offset paging is not
a snapshot: concurrent late resolution can change results; consumers should dedup
by durable identity. Session queries include overlapping sessions. Momentum windows
use slot time; arrivals and name snapshots use observation time. Duplicate, unknown
and malformed parameters are rejected.

```sh
curl 'https://observer.example/api/v1/momentums?limit=25'
curl 'https://observer.example/api/v1/peers?limit=100'
curl 'https://observer.example/api/v1/momentums/HASH/arrivals?limit=100'
```

## Health, quality and limits

`/healthz` reports process liveness. `/readyz` returns 503 during initialization,
stale status, unsafe storage, capture loss, and shutdown. RPC/peer outages alone
do not make the API unready. Traffic cooldown remains readable and ready, with
capture visibly paused. Consult `/api/v1/status` for collection health.

Data quality includes drops/clock jumps in overlapping runs, interrupted runs,
retention and analysis availability. Run-level counts conservatively cover the
entire overlapping run, not exact subwindows. Missing observations do not imply
offline peers/pillars. Legacy feasibility JSONL is not imported.

GET/HEAD/OPTIONS only; bodies are rejected. URI limit 4096 bytes, header budget
16 KiB (plus Go's small parser allowance), response limit 1 MiB, 64 connections,
eight concurrent API requests, four database readers, two-second SQL and three-second
request work deadlines. A shared 20 requests/second token bucket permits burst 40;
health checks bypass it. Retry 429/503 with backoff. Reduce page size on 413.
SQL error details and environment values are not exposed.

Collections support ETags and `Cache-Control: public, max-age=30`; status, health
and errors use `no-store`. HEAD has no body. CORS uses an exact origin allowlist
such as `https://0x3639.com`. Credentials and wildcard origins are unsupported;
CORS is browser policy, not authorization.
