# Validation scope

Run `./scripts/check.sh`. Dependency installation, compilation, third-party
code and tests execute inside containers, with tests offline. No Node packages
or host Go execution are needed.

The default `go test -race ./...` suite checks:

- encrypted legacy RLPx/Hello negotiation, peer identity mismatch and ping framing;
- a valid encrypted oversized-frame header rejected before reading its payload;
- momentum hashing, producer derivation and invalid Ed25519 signatures;
- wrong network, genesis and capability protocol version;
- malformed, excessive, nested and trailing RLP data;
- public IPv4/IPv6 validation, mapped metadata IPs and non-public destinations;
- per-session earliest arrival preservation and later hash resolution;
- exclusion of requested responses and failure of single-peer acceptance;
- metadata queue saturation and oversized-record drops;
- identity persistence and rejection of overly permissive key files;
- traffic budget/breaker and reconnect cooldown persistence across restart;
- conservative rolling traffic bucket expiry and reconnect delay bounds.

`go test -tags upstream_integration ./internal/runner` additionally starts the
exact pinned upstream `p2p.Server` on loopback inside the test container. It
negotiates mainnet status and sends signed full payloads and hash announcements
to the observer. This fixture is deliberately separate from the race suite:
the upstream Server's `running` field races in `server.go` around Start/Stop and
its dial task loop. That server is never constructed in the production observer.
The adapter's encrypted transport tests still run with the race detector.

`go vet ./...` checks the observer source. The adapted upstream public-key
literal uses named fields to satisfy current Go vet; this does not alter the
transport semantics. A seeded fuzz target is provided for bounded full-payload
decoding. Longer fuzzing can run with `go test -fuzz FuzzFull -fuzztime 30s
./internal/protocol` inside the same offline test container.

A unit test pass is not a public-network feasibility pass. Only an actual run
with final evidence, two continuously overlapping sessions for an hour and
100 checked live momentums observed independently from both peers establishes
milestone 1. The report must also disclose disconnects, capture gaps, bytes
and peak memory. The completed local experiment and its build-provenance limits
are documented in `docs/feasibility-report.md`, with the original metadata and
resource evidence.

The collector suite additionally tests SQLite schema creation/newer-schema rejection,
transaction rollback, raw retention, consistent backups, restart recovery, historical
session versions, exact arrival clocks through deferred resolution, RPC disagreement
and continuity loss, request methods/ranges/origins/size bounds, ETags, and API liveness
while RPC is unavailable or storage intake is paused. The four-peer local smoke check
and its provenance are recorded in `docs/collector-validation.md`.

The association suite tests warmup and ten-second cohort membership, single-peer
exclusion, duplicate deliveries, missing deliveries in denominators, five-millisecond
ties, requested-response exclusion, exact delay statistics, general-fast and
pillar-specific relays, chronological holdouts and unstable/negative findings.
SQLite/API tests cover schema-1 migration with clocks unchanged, reorg invalidation,
cross-hour late resolution, clean restart, unsafe-run revocation, supporting evidence
retention, 90-day expiry, filters and multi-day query bounds. A bounded public sample
is replayed offline; it is not a representative production association assessment.

Still pending: production server isolation, remote rollback exercise, complete
Coolify pilot acceptance, and assessment of the full deployed multi-day results. Local
tests and a short smoke check do not establish these acceptance gates.
