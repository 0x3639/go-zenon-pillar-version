# Association analysis, method association-1

Release 0.3.0 adds relay comparisons, hourly summaries retained for 90 days, and
chronological holdouts. It does not identify a relay as the producer or establish
which software a pillar runs. All `producerSoftwareVersion` values remain null.
An `observed_relay_association` label is a sample-size label, including negative
results; it is not a positive identity finding.

## Upgrade and first results

In the current Coolify container Terminal, create a consistent backup before
upgrading:

```sh
observer backup pre-associations-2026-11-11.sqlite
```

Keep the volume, observer ID, peer seeds, and environment settings. Deploy the
0.3.0 source through the existing Compose application. Port 8080 and the public
API domain remain the same. Responses advertise `schemaVersion=1.1` and
`methodVersion=association-1`; raw `rankingEligible` is now null with an explicit
window-dependent reason. Clients that strictly check schema versions need to accept
1.1. SQLite migrates schema 1 to 2 without rewriting arrival
clocks or changing the P2P identity. To return to 0.2.0, restore the compatible
backup with the service stopped; 0.2.0 cannot open schema 2.

The worker automatically replays retained complete hours, oldest first. No manual
analysis command is required. It processes one hour at a time with a five-second
work deadline and a one-second pause between successful hours. Idle/error retries
wait 15 seconds. Storage pressure pauses analysis. API reads and collection continue
independently; analysis errors appear as `result.analysisError` in status.

Check `/api/v1/status`: `dataQuality.analysisState` is `building`, `ready`,
`awaiting_complete_hour`, or `chain_continuity_unresolved`. Completed/pending-hour
counts describe all retained completed hours. The association endpoint reports its
own window's `processedHours`, `pendingHours`, `missingHours`, exclusions and
`computedAt`. Missing hours are unknown coverage, not zero observations or proof
that any peer or pillar was offline. Dirty hours are withheld until recomputed.

`GET /api/v1/associations?limit=25` returns the last seven complete days by default,
ending at the latest completed UTC hour with a two-minute reconciliation grace.
Use fixed `from` and `to` dates on UTC-hour boundaries for paging. The range can be
up to 90 days within aggregate retention. Filter by `producer`, `peerId`, and either
`eventKind=hash_announcement` or `eventKind=full_broadcast`. Narrow the dates or page
size if a query reaches a work/response bound. A fresh deployment has no preexisting
summaries for observations that already expired; backups do not get imported
implicitly. Only still-retained raw observations are replayed.

## Comparison rules

1. Group original arrivals by boot, momentum hash, and event kind. Require
   `rpc_agreed`, an unambiguous checked signing address, and original live eligibility.
   Requested responses and historical/unresolved events never participate.
2. Anchor the ten-second window at the first original observation of that event
   category, even if that sender itself is in warmup. Compare only sessions past
   five-second warmup at the anchor and continuously present through the window.
   Require at least two distinct peer identities on distinct IPs. Later arrivals
   outside this window are missing deliveries for this comparison.
3. Deduplicate each peer's delivery. Relative delay is the monotonic arrival minus
   the earliest eligible arrival in the same boot/category/cohort. Count all peers
   within five milliseconds of that minimum as ties. A tie gives no strict first
   win; `tieCount` and `tieShare` are separate. Millisecond output does not establish
   sub-millisecond measurement accuracy.
4. Every cohort member contributes to `eligibleCount`, including members that did
   not deliver. `sampleCount` equals this comparable denominator. `receivedCount`
   counts deliveries; coverage is received/eligible. Strict first share is
   first/eligible. Missing deliveries contribute no delay value. Median uses the
   middle value (mean of the two middle values for even counts); p95 uses nearest
   rank. Both use exact retained monotonic differences, reported in milliseconds.
5. A cohort ID hashes the boot and sorted eligible session IDs. Reconnects, changes
   to IP/version, or restarts split cohorts. Rows never pool different cohorts or
   event kinds. `activeCompetitorCount` excludes the row's peer. Daily breakdowns
   show the same denominators. Dates are UTC dates of first observation.
6. For each hour/cohort/event/peer, compare that producer with all other producers.
   The expected baseline is the other-producer strict-first share weighted by the
   target producer's eligible samples in that hour. `baselineMatchedEligibleCount`
   identifies samples with a baseline; `excessFirstArrivalShare` compares actual
   and expected share on that matched subset only. Filtering the API by producer
   does not alter the stored baseline. A universally fast relay has no advantage
   over its own other-producer baseline.

`firstObservedAt`/`lastObservedAt` bound comparable momentum observations, including
comparisons with a missing delivery from this peer. `connectedDurationSeconds` is
the exact cohort's eligible anchor-time duration within contributing hours, not
lifetime peer uptime or a per-minute proof of network delivery. A dated pillar-name
snapshot is a display label; it is not proof of a historical producing assignment.

Capture drops, clock discontinuities, or an unclean shutdown conservatively exclude
an entire affected boot. Current collection records do not establish exact safe
subwindows in those runs. Clean prior runs remain usable. Unresolved chain continuity
suspends all rankings. Competing hashes/signers are excluded; later verification
changes invalidate affected summaries and rebuild them when raw evidence remains.

## Presentation threshold and holdout

A row requires 100 comparable samples across at least three UTC days for
`associationStatus=observed_relay_association`. All other rows are `insufficient_data`.
This is a conservative presentation threshold, not a statistical significance test.

The fixed query window is split at its chronological midpoint, rounded down to an
hour. The earlier and later halves each publish measurements and matched baselines.
The holdout result remains `insufficient_data` unless the overall threshold passes
and each half has at least 20 eligible samples, 20 other-producer eligible samples,
and baseline coverage for at least 80% of its eligible samples. Otherwise:

- `persistent_positive_pattern`: both halves exceed their matched baseline by more
  than five percentage points;
- `no_persistent_specific_advantage`: neither half exceeds that margin;
- `unstable_pattern`: only one half exceeds it.

These are descriptive, predeclared heuristics. No ownership probability, statistical
confidence, confirmed-producer label, or producer-version inference is provided.
Changing query dates changes the holdout split; retain fixed dates when validating
an early hypothesis on later data. One observer location cannot remove routing bias.

## Retention and bounds

Hourly sufficient statistics retain counts, exact delay samples, baseline counts,
cohort sessions, historical IP/version segments and dated name labels for 90 days.
Daily views are derived from those hours. Supporting headers and sessions remain
while their summaries reference them. `rawEvidenceAvailable=false` means some
contributing hours are outside raw retention; exact historical recomputation is unavailable.
A detected deeper dispute invalidates summaries using retained headers. If replay
requires expired arrivals, the hour is withheld with `raw_history_expired`.

The existing total 2 GiB storage budget, 75% SQLite-main-file cap and free-space
checks apply to these summaries too; 90 days is a retention ceiling, not a guaranteed
capacity. The worker reads at most 20,000 arrivals, 2,048 sessions and 128 runs per
hour and writes at most 4,096 parts in a transaction capped at 500 milliseconds.
Queries read at most 200,000 hourly parts and one million delay samples, with one concurrent association
query, a five-second work deadline and the existing 1 MiB response limit. Raw queries
retain their two-second SQL deadline; association HTTP requests have a seven-second
overall deadline. No unbounded export or new administrative HTTP route is added.
