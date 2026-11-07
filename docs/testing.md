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

Not yet tested or implemented: SQLite migrations/recovery, two-source disputes,
reorganizations, retention, HTTP limits, multi-day cohort statistics, holdouts,
production server isolation and the 24–48 hour Coolify pilot. These are later
acceptance gates in SPEC.md, not implied successes.
