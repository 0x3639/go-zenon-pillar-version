# Collector/API validation — November 7, 2026

Release `0.2.0-collector` passed containerized Go race tests, the separately tagged
pinned upstream interoperability fixture, `go vet`, compilation, and a source-based
Docker build. [Machine-readable results and source hashes](evidence/2026-11-07-collector/validation.json)
identify the exact final source and local Linux image.

Two short local four-peer checks exercised real encrypted sessions and the API.
The four-minute candidate run (16:33:03–16:37:03 UTC) ended with 23 distinct checked
live momentums, four active peers before shutdown, and zero capture drops. RPCs
agreed over 180 heights and dated pillar snapshots loaded successfully. This image
preceded the final UTC-window normalization and conservative anchor-read fixes;
its [log](evidence/2026-11-07-collector/candidate-run.log) is labeled as candidate evidence.

The final release image was rebuilt after those fixes and run against the same
database at 16:38:37 UTC. It reconnected to four peers and retained the exact UTC and
monotonic timestamps of ten sampled arrival records, old closed sessions, the same
identity, and every unexpired traffic reservation bucket. It collected five new
checked momentums before the explicit stop at 16:39:54 UTC, with zero drops.
Shutdown took 0.163 seconds, exited zero, and was not OOM-killed.

Liveness/readiness, peer/header/arrival/pillar responses, allowed-origin CORS, ETag
304 responses and the container health command were exercised through localhost.
An online SQLite backup completed and passed its integrity check. Unit tests also
cover failed transactions, preservation of conflicting hashes/signers, restart
recovery, retention, newer-schema refusal, RPC disagreement and missing continuity,
half-open UTC query windows, methods/range/body/response limits, and API availability
under RPC failure or storage pressure. See [test output](evidence/2026-11-07-collector/checks.txt).

Effective container limits were verified: 512 MiB memory and total swap, one CPU,
64 pids, read-only root, all capabilities dropped, no-new-privileges, and rotated
logs. The local preview used a dedicated workspace data directory and localhost
port 18080, with the workstation UID override. The normal release retains its
non-root UID 10001 and dedicated named volume. Resource samples are point samples,
not a peak measurement or a production capacity claim.

The initial smoke check revealed that the local shared filesystem reports an I/O
block size different from its allocation fragment size. Storage accounting now uses
the latter, matching `df`; the minimum free-space rule remains max(1 GiB, 10%).

No remote deployment, remote rollback exercise, 24–48 hour Coolify pilot, three-day
association validation, or existing-site integration is claimed. The user's running
feasibility deployment and the existing Observatory were not changed.
