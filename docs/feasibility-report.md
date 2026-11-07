# Protocol feasibility report — November 7, 2026

**Milestone 1 passed for the measured protocol experiment.** Two public peers
completed encrypted transport, Hello and Zenon mainnet status, then remained
connected together for **60.66 minutes**.
Both peers supplied unsolicited observations for **337 distinct checked live
momentums**. This establishes that a bounded observer can receive and attribute
live signed momentum headers without running a full chain.

It does **not** establish which go-zenon release produced a momentum. The
software names below describe relays, and no ownership, pillar-name or producer
software assertion has been created. The intended future API consumer is
[go-zenon-version](https://0x3639.com/go-zenon-version/); no HTTP API or site
integration is included in this milestone.

## Measured run

- Observer: `local-feasibility-20261107`; boot: `8e304370b4a0f72ab3b219f90f32d18e`.
- UTC interval: `2026-11-07T14:38:16.522560869Z` to `2026-11-07T15:38:58.5914791Z`.
- Two-session overlap: 3639.892 seconds.
- Distinct checked live momentums seen from both peers: 337.
- Signing producer addresses observed: 80; registered names were not inferred.
- Reported capture drops: 0; clock discontinuities: 0.
- Remote disconnects during observation: zero. Both sessions ended when the
  experiment was deliberately stopped after passing the one-hour requirement.
- Clean shutdown command completed in 0.147 seconds; container exit code 0.

| Remote socket | Advertised client name | Peer key prefix |
| --- | --- | --- |
| `51.81.209.53:35995` | `v0.0.8 znn-node` | `0316e6152bed5773…` |
| `23.95.73.26:35995` | `v0.0.9 znn-node` | `072792b916d53179…` |

Full public keys, session intervals, status reference headers and disconnect
messages are retained in [the final report](evidence/2026-11-07/report.json).
The two addresses represent sampling diversity, not proven independent operators.

| Recorded event kind | Unique session/hash/kind records |
| --- | ---: |
| Full broadcasts | 172 |
| Hash announcements | 660 |
| Requested responses | 447 |

Requested responses were excluded from unsolicited live counts. The metadata
consistency audit independently reproduced the totals and session overlap, and
verified that all **448 resolution updates** retained the original UTC and
monotonic arrival times. Hash/signature verification occurred during collection;
the stripped metadata cannot independently repeat full-payload verification.

## Resource observations

This was a local macOS/Colima Linux arm64 experiment using a dedicated workspace
bind directory. It was not a Coolify production pilot. CPU, memory, traffic,
queue, file and runtime limits remained bounded.

| Measurement | Observed result |
| --- | ---: |
| P2P ingress, including encrypted framing and handshakes | 377,348 bytes |
| P2P egress, including requests, status and keepalive | 106,054 bytes |
| Conservative reserved ingress / egress at finish | 3,997,696 / 3,997,696 bytes |
| Maximum process RSS reported by Linux | 19,427,328 bytes (18.53 MiB) |
| Highest sampled Go heap | 3,465,536 bytes |
| Container memory peak read before shutdown | 14,241,792 bytes |
| Highest one-minute Docker CPU sample | 1.87% of one CPU |
| Observer data directory after restart smoke test | 1,497,326 bytes |
| Raw evidence JSONL | 1,471,334 bytes |

Process RSS, container memory accounting and sampled Go heap measure different
things. Resource samples began about 16 minutes into the run; sampled CPU is
not an absolute peak. Docker network totals include RPC, DNS and network overhead
and therefore exceed the P2P socket counters. The conservative reservations
include unused credits and should not be mistaken for actual bytes transferred.

Effective container limits were verified as 536,870,912 bytes of memory,
no additional swap allowance, one CPU, 64 processes, a read-only root filesystem,
all capabilities dropped, and rotated 10 MiB logs with three files. Container
out-of-memory counters and CPU throttling counters were zero at the final check.
The traffic breaker did not activate; overshoot was therefore **not measured**
in this run. Synthetic tests check reservation/breaker persistence.

The first attempted launch refused intake because Docker's internal disk had
only 4.4 GiB free on a 59 GiB filesystem, below the 10% reserve. The successful
run used the separate workspace data directory. At the user's request, unused
Docker build cache was removed, restoring approximately 22 GiB of free space
before subsequent observer builds. Existing containers, images and volumes were
not removed by that cleanup.

## Checks and restart recovery

The final source passed container-only race tests, `go vet`, a clean Docker
build, and the pinned-upstream compatibility fixture. A 30-second decoder fuzz
run executed 1,812,800 inputs without failure. The full upstream Server fixture
has a known lifecycle race and runs separately without the race detector; the
observer does not instantiate that Server. See [testing.md](testing.md).

A separate 15-second smoke run of the **final source-built image** reconnected
to both peers, retained the same public peer identity and 59 still-current
traffic buckets, kept the original evidence files, and reported no capture
drops or clock jumps. Older traffic buckets expired normally. This short run
is not presented as a second hour-long acceptance test.

The hour-long measurement used the initial feasibility binary:
`c4a7cf6b05203c74c2f7ebf374fdfee220abf811685d740e92c851e08acf95cf`.
The handoff source commit is `67624fae62f24b6c147c5484a6eb6ff657b1e722`. Changes made while that binary
was running were limited to persistence containment, expired-credit and shutdown
edge cases, bounded state reads, static-check cleanup, and the repository module
rename. Protocol message handling remained unchanged. The final image received
tests and the short restart check; **it was not rerun for another hour**.

## Evidence and next gate

[Evidence manifest](evidence/2026-11-07/manifest.json),
[metadata audit](evidence/2026-11-07/evidence-audit.json),
[compressed arrivals](evidence/2026-11-07/arrivals.jsonl.gz),
[resource samples](evidence/2026-11-07/resource-samples.jsonl),
[effective container settings](evidence/2026-11-07/container-observations.json),
[restart result](evidence/2026-11-07/restart-result.json), and
[test results](evidence/2026-11-07/test-results.txt) are committed with this report.
No private peer identity or local configuration is included.

Next: milestone 2 adds durable SQLite collection, bounded multi-source
reconciliation and reorganization handling, retention, and the read-only API.
The later four-peer, 24–48 hour Coolify pilot and three-day association threshold
remain outstanding. No production server was deployed or modified, and no
producer-version attribution or association ranking has passed an acceptance gate.
