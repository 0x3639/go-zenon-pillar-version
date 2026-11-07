#!/usr/bin/env python3
"""Print a small report using only Python's standard library. Never reads keys."""
import json
import sys
from pathlib import Path

FIELDS = (
    "observerId", "bootId", "windowStart", "generatedAt", "final",
    "milestone1Passed", "maxTwoSessionOverlapSeconds",
    "distinctCheckedLiveMomentums", "distinctCheckedLiveMomentumsSeenFromTwoPeers",
    "arrivalsByKind", "captureDrops", "clockJumps", "peakRSSBytes",
    "peakHeapBytes", "traffic", "captureState", "referenceError",
)

for filename in sys.argv[1:]:
    path = Path(filename)
    if not path.name.startswith("report-") or path.suffix != ".json":
        raise SystemExit("Pass only report-<boot-id>.json files")
    if path.stat().st_size > 2 * 1024 * 1024:
        raise SystemExit("Report exceeds size limit")
    report = json.loads(path.read_text())
    print(json.dumps({key: report.get(key) for key in FIELDS}, indent=2))
