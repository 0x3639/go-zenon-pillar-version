# Association release validation — November 11, 2026 (Central)

Release 0.3.0-associations passed the container-only `scripts/check.sh` workflow:
all package tests with the race detector, the separately tagged pinned-upstream
fixture, static checks, and compilation. The final source also built through the
production Dockerfile; a network-disabled invocation reported the expected release.
The image runs as UID/GID 10001:10001. Compose configuration validation passed.

The tests cover ten-second session membership, warmup, single-peer exclusion,
missing deliveries, duplicate arrivals, five-millisecond ties, separate event kinds,
IP/version/cohort changes, exact delay summaries, weighted baselines, universally
fast relays, pillar-specific relays, negative/unstable holdouts, and threshold labels.
Database/API checks cover migration from schema 1 with original clocks intact,
retention, clean restart, unsafe-run exclusion, reorg invalidation before serving,
late cross-hour resolution, deep disputes after raw expiry, continuity suspension,
pagination/filtering, complete-hour ranges, cancellation and truthful status.

A bounded compatibility replay downloaded 24 recent RPC-agreed headers and their
190 arrival records from the public observer for November 11, 23:00–24:00 UTC.
Queries included the following ten seconds for boundary comparison windows.
The replay produced 168 producer/peer/category rows: 168 eligible peer comparisons
and 125 eligible deliveries. Every row remained `insufficient_data`, with null
producer software version and insufficient holdout evidence. The sample is selected
by recency, small, and intentionally unsuitable for a production association claim.
The retained source response is in `evidence/2026-11-11-associations/public-sample.json`.

A synthetic seven-day database contained 115,584 hourly parts (86 producers, four
peers, two event categories). A 100-row query took about **0.30 seconds** in one
benchmark iteration under one CPU, 512 MiB memory and no additional swap allowance.
The Go benchmark reported **59,220,512 bytes allocated per operation**; that is total
allocation, not peak resident memory. Timing is a local sample, not a production
latency guarantee or a 90-day capacity claim. The benchmark is reproducible as
`BenchmarkAssociationWeek` inside the pinned toolchain container.

No production database was migrated, service redeployed, or site integration
changed during validation. Full historical results and later holdout interpretation
remain to be assessed after the user deploys this release. This work does not
certify the outstanding remote resource/isolation/rollback pilot checks.

The machine-readable validation record includes hashes of the tested source files
and the final local image identity. Documentation-only additions followed the image
build; runtime source did not change afterward.
