# Association corrections — 0.3.1

This release addresses the CodeRabbit findings on PR #2, which shipped as 0.3.0.
It preserves raw observations and upgrades schema 2 forward to schema 3. Schema 1
can also upgrade directly. The analysis method is now `association-2`; API schema
1.1 is unchanged and includes additional diagnostics.

## Corrected behavior

- A checked live momentum announced only by an ineligible warmup session counts as
  a missed delivery for each otherwise eligible peer. Unknown or historical slots
  do not establish these comparisons. Missing deliveries no longer disappear from
  the denominator when every cohort member misses them; delays remain null.
- Deterministic run/session/arrival/part bounds explicitly exclude the affected
  hour and advance processing. A transient failure gets a persisted 30-second to
  15-minute retry delay so it cannot monopolize the oldest-hour queue. Part writes
  still use a 500-millisecond transaction and publish atomically. New evidence
  clears the retry delay. `retryingHours` and `excludedHoursByReason` expose the result.
- Summary retention references exact session IDs, including cohort members that
  never delivered. Other sessions from the same process boot can expire normally.
- Pillar requests with no complete hour return null association window bounds and
  `no_complete_hours`, rather than a reversed range.
- Cohort durations are computed in one sweep per boot. Each migration has its own
  two-minute transactional deadline, separate from startup connection checks.
- `associationSummariesAvailable` describes whether summary rows exist, not whether
  a row meets presentation thresholds. The old ranking availability names remain
  documented, deprecated aliases. Historical schema documentation and the replay/
  late-resolution assertions are corrected; read margins use the shared window.

## Upgrade

Create a consistent backup in the current container before upgrading:

```sh
observer backup pre-0.3.1-2026-11-11.sqlite
```

Keep the volume and existing configuration. Deploy 0.3.1 through the same Compose
application. The migration initializes session references from stored cohorts and
marks prior summaries for recomputation. Pending old-method results are withheld.
Expired raw history becomes an explicit exclusion; it is not silently relabeled
as the corrected method. Returning to an older release requires the corresponding
schema-compatible backup with the service stopped.

The unclean-run rule remains conservative: a run marked interrupted is excluded
from comparisons, even though its raw records remain until normal retention.
The running observer reported one interrupted run after the earlier redeployment;
0.3.0 processed 104 hours but returned no association rows, with 63,509 excluded
momentum/category comparisons. These corrections do not establish which windows
of that interrupted run are safe to rank. Operator review of persisted evidence
and the pre-upgrade backup is needed before changing that interpretation.

## Validation

Regression tests cover the reproduced missing-denominator case, rejection of old
warmup anchors, the supported eight-peer/three-cohort part bound, progress after
transient write failures, exact session retention/expiry, schema-2 reference
migration and method invalidation, null time ranges, and preservation of duration
results against an independent reference calculation. Existing race, protocol,
SQLite, API and upstream compatibility checks run through `scripts/check.sh`.

The prior migration probe used a synthetic schema-1 database with 450,000 arrivals
(241.1 MiB) under one CPU and 512 MiB. Its schema-1-to-2 migration took about 18 ms; deliberately
interrupting the migration left schema 1 intact. This was a local synthetic check,
not the production backup or a guarantee about remote storage performance.

The final container race suite, tagged upstream fixture, static checks, build,
production image/version and Compose validation all passed. The public replay is
pinned to 190 arrivals, four sessions, 168 rows, 168 eligible peer comparisons and
125 deliveries; all rows still have insufficient data.

Under one CPU, 512 MiB and no additional swap, the 2,048-session duration sweep
took 0.636 ms. A 100-row query over 115,584 synthetic hourly parts took 0.219 s,
allocating 59,220,592 bytes in total during the operation. These are single local
ARM64 runs, not production latency guarantees; allocated bytes are not peak memory.
Source hashes and image identity are recorded in the
[validation manifest](evidence/2026-11-11-association-corrections/validation.json).
The original 0.3.0 evidence remains unchanged.
