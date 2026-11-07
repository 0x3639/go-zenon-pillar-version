#!/usr/bin/env python3
"""Check recorded evidence consistency; this does not revalidate consensus."""
import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("report", type=Path)
parser.add_argument("evidence", type=Path)
args = parser.parse_args()
report = json.loads(args.report.read_text())
if not report.get("final"):
    raise SystemExit("Wait for a final report before auditing the completed experiment")
arrivals = {}
sessions = {}
first = {}
resolved = 0
with args.evidence.open() as stream:
    for line in stream:
        if len(line.encode()) > 8192:
            raise SystemExit("Oversized evidence record")
        record = json.loads(line)
        kind = record.get("type")
        if kind in ("session_started", "session_ended"):
            session = record["session"]
            sessions[session["sessionId"]] = session
        elif kind in ("arrival", "arrival_resolved"):
            assert record["bootId"] == report["bootId"], "mixed boot IDs"
            key = (record["sessionId"], record["hash"], record["eventKind"])
            stamp = (record["arrivedAt"], record["monotonicNs"])
            if kind == "arrival":
                assert key not in arrivals, "duplicate first-arrival record"
                first[key] = stamp
            else:
                assert key in arrivals, "resolution without original arrival"
                assert first[key] == stamp, "resolution changed original P2P arrival time"
                resolved += 1
            if record["header"] is not None:
                assert record["header"]["hash"] == record["hash"], "header/hash mismatch"
                assert record["header"]["producerAddress"], "resolved header lacks producer"
            if record["eventKind"] in ("requested_response", "historical_or_unresolved"):
                assert not record["eligible"], "response entered unsolicited live counts"
            arrivals[key] = record
            assert len(arrivals) <= 20000, "arrival capacity exceeded"

peers_by_hash = defaultdict(set)
for record in arrivals.values():
    session = sessions[record["sessionId"]]
    assert record["peerId"] == session["peerId"], "session identity mismatch"
    if record["eligible"]:
        assert record["header"] is not None, "unresolved event is eligible"
        peers_by_hash[record["hash"]].add(record["peerId"])
counts = Counter(record["eventKind"] for record in arrivals.values())
two_peer_count = sum(len(peers) >= 2 for peers in peers_by_hash.values())
assert dict(counts) == report["arrivalsByKind"], "event counts disagree"
assert len(peers_by_hash) == report["distinctCheckedLiveMomentums"], "live counts disagree"
assert two_peer_count == report["distinctCheckedLiveMomentumsSeenFromTwoPeers"], "peer intersection disagrees"

overlap = 0
values = list(sessions.values())
for index, left in enumerate(values):
    assert left["disconnectedAt"] is not None, "session lacks final interval"
    for right in values[index + 1:]:
        if left["peerId"] == right["peerId"] or left["ip"] == right["ip"]:
            continue
        start = max(left["statusCompletedMonoNs"], right["statusCompletedMonoNs"])
        end = min(left["disconnectedMonoNs"], right["disconnectedMonoNs"])
        overlap = max(overlap, end - start)
assert abs(overlap / 1e9 - report["maxTwoSessionOverlapSeconds"]) < 1e-6, "session overlap disagrees"
passed = (overlap >= 3600 * 10**9 and two_peer_count >= 100
          and report["captureDrops"] == 0 and report["clockJumps"] == 0
          and not report["storageError"])
assert passed == report["milestone1Passed"], "acceptance result disagrees"
print(json.dumps({
    "evidenceConsistencyPassed": True,
    "bootId": report["bootId"],
    "uniqueArrivalRecords": len(arrivals),
    "resolutionUpdatesPreservingOriginalTime": resolved,
    "arrivalsByKind": dict(counts),
    "distinctCheckedLiveMomentumsSeenFromTwoPeers": two_peer_count,
    "continuousTwoSessionOverlapSeconds": overlap / 1e9,
    "milestone1Passed": passed,
    "scope": "Recorded metadata consistency; signatures were checked during collection, not rechecked from stripped headers.",
}, indent=2))
