# Zenon P2P Observer

Repository: [0x3639/go-zenon-pillar-version](https://github.com/0x3639/go-zenon-pillar-version).

An independent, bounded Go observer for the Zenon P2P network. This repository
implements **milestone 1: protocol feasibility** from [SPEC.md](SPEC.md). It
does not run a full node, sync chain history, operate a validator, access a
wallet, or change the existing Observatory.

The [November 7 feasibility experiment](docs/feasibility-report.md) passed:
two simultaneous public-peer sessions for over an hour, 337 checked live
momentums seen from both peers, and zero reported capture drops. Durable
collection and the site's read-only API are the next milestone.

The first experiment maintains two or four outbound connections, negotiates
encrypted RLPx, Hello and Zenon mainnet `eth/61` status, and records per-peer
momentum arrivals. It resolves producer addresses by recomputing hashes and
checking Ed25519 signatures. RPC fetch times never replace P2P arrival times.

**Relay observations do not identify a pillar's IP or software version.**
Advertised versions belong to peer sessions. This release provides no ownership
inference, association rankings, HTTP API, database, or dashboard.

## Build and test

All dependencies and Go code run in containers. No Node tooling is required.
Docker must be available. From the repository root:

```sh
./scripts/check.sh
docker build -t zenon-p2p-observer:feasibility .
docker run --rm --network none zenon-p2p-observer:feasibility version
```

The check script downloads pinned dependencies in a non-root container, then
disables networking for the race suite, compatibility test, static checks and
build. The Go toolchain image is pinned by digest. `go.mod` pins go-zenon to
`bb39e31c608dc397a91b98b21b096bca99a9f4d6`; `go.sum` pins dependency content.
The release and upstream revision are included in every run report and in
`observer version`. Standard Go build metadata records resolved modules.

The full upstream `p2p.Server` has a reproducible race in its own lifecycle.
The explicit `upstream_integration` test therefore runs separately without the
race detector. It verifies interoperability with that exact upstream server.
The default race suite covers the observer's packages, including concurrent
encrypted handshakes. The production observer does not instantiate the upstream
server, discovery scheduler or protocol manager. See [testing.md](docs/testing.md).

## Run the bounded experiment

```sh
cp .env.example .env
# Set OBSERVER_ID and PEER_SEEDS in .env.
docker compose up -d --build
docker compose logs --tail 30
```

Supply seeds as a comma-separated list of
`enode://<128-character-public-key>@<public-IP-or-hostname>:<TCP-port>`.
Ports are required. At most 32 candidates are accepted. Choose known public
peers from an existing candidate roster; this program has no scanner or
automatic discovery-feed importer. Candidate sources are untrusted: addresses
are validated, DNS is resolved immediately before dialing, the socket endpoint
is checked, and both transport and Hello identities must match the seed key.

The default run ends after 65 minutes. Set `MAX_PEERS=2` for the initial
feasibility test; four is the configuration default and eight is the ceiling.
One dial/handshake runs at a time. A successful roster remains fixed; only
failed sessions are replaced. Restarting preserves the dedicated key,
traffic reservations and per-peer reconnect cooldowns.

No port is published, no inbound P2P socket is opened, and there is no HTTP
listener in milestone 1. Compose uses a dedicated volume, one non-root replica,
a read-only root filesystem, dropped capabilities, 512 MiB memory, one CPU,
no extra swap allowance, and rotated logs. The experiment does not restart
automatically after completion, overload, or an unsafe storage condition.

For a local run using a workspace directory on macOS (rather than Docker's
internal disk), build with `./scripts/check.sh`, create `.work/run-data`, and
use `docker compose -f compose.yaml -f compose.local.yaml up -d --build`.
The local override's `user` must match the owner of that directory; the supplied
value is `501:20`. It uses the compiled local binary. The ordinary Dockerfile
builds from source and is the release build path.

## Evidence and acceptance

Each run writes these files under `/data`:

| File | Purpose |
| --- | --- |
| `identity.key` | Dedicated persisted secp256k1 identity, mode 0600; never publish |
| `limits.json` | Persisted rolling traffic reservations and reconnect cooldowns |
| `evidence-<boot-id>.jsonl` | Bounded metadata evidence; no full account bodies |
| `report-<boot-id>.json` | Atomically refreshed operational and acceptance report |
| `observer.lock` | Exclusive volume ownership lock |

Read a report without exposing the key:

```sh
docker compose exec -T observer sh -c 'cat /data/report-*.json'
# For the local override:
python3 scripts/summarize.py .work/run-data/report-*.json
```

`milestone1Passed` requires two distinct peer identities on distinct IPs with
at least **60 continuous overlapping minutes**, at least **100 distinct checked
live momentums seen from two peers**, zero reported capture drops, zero storage
writer failures, and no detected clock discontinuity. The 100-momentum condition
is deliberately stricter than a total counted from just one peer. An early
report remains provisional until `final: true`. A status handshake or successful
Hello alone never passes the test.

Full broadcasts and hash announcements have separate event kinds. Requested
responses and unsolicited unmatched response packets are retained but excluded
from live unsolicited counts. Five seconds of warmup and a two-minute live age
window apply. Slot time is not a producer's transmission time. Every arrival
has UTC and a boot-relative monotonic timestamp, and each peer's earliest
arrival survives deduplication. Resolution records retain that original time.

See [interpretation.json](docs/interpretation.json), [protocol.md](docs/protocol.md)
and [operations.md](docs/operations.md) for measurement and resource limits.

After the run ends, `python3 scripts/audit-evidence.py <report-file>
<evidence-file>` independently checks recorded counts, session overlap, and
preservation of original P2P timestamps across resolution updates. It does not
repeat signature verification from the stripped metadata or validate consensus.

## Scope after feasibility

Milestone 2 adds SQLite/WAL, migrations, durable raw-event deduplication,
bounded two-source reconciliation, reorganization handling, retention and
health/status endpoints. Later milestones add the isolated Coolify pilot,
coverage-adjusted association statistics, the read-only API/OpenAPI schema,
and optional dashboard integration. None of those milestones is implied by
a passing protocol experiment. The user creates the remote repository and
performs Coolify deployment; this work does not deploy or alter that service.

The intended API consumer is the existing **go-zenon-version** site at
<https://0x3639.com/go-zenon-version/>. Its observer requests will use a separate
API domain and fail independently of the existing discovery panels. The future
CORS allowlist should include the site's origin `https://0x3639.com`, not the
page path. No browser access or API route is enabled in this milestone.

The full requested design remains in [SPEC.md](SPEC.md). Licensing and adapted
source notices are in [LICENSE](LICENSE) and [third_party/NOTICE.md](third_party/NOTICE.md).
