#!/usr/bin/env python3
"""Finite resource sampler for the local experiment; Python standard library only."""
import argparse
import datetime as dt
import json
from pathlib import Path
import subprocess
import time

parser = argparse.ArgumentParser()
parser.add_argument("--data-dir", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--container", default="zenon-p2p-observer-observer-1")
parser.add_argument("--max-seconds", type=int, default=7200)
args = parser.parse_args()
deadline = time.monotonic() + min(max(args.max_seconds, 1), 7200)

with args.output.open("a") as output:
    while time.monotonic() < deadline:
        reports = sorted(args.data_dir.glob("report-*.json"), key=lambda p: p.stat().st_mtime)
        if not reports:
            raise SystemExit("No observer report is available")
        path = reports[-1]
        if path.stat().st_size > 2 * 1024 * 1024:
            raise SystemExit("Report exceeds sampler size bound")
        report = json.loads(path.read_text())
        stats = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}", args.container],
            check=True, capture_output=True, text=True, timeout=15,
        )
        sample = {
            "sampledAt": dt.datetime.now(dt.timezone.utc).isoformat(),
            "bootId": report["bootId"],
            "reportAt": report["generatedAt"],
            "final": report["final"],
            "overlapSeconds": report["maxTwoSessionOverlapSeconds"],
            "checkedFromTwoPeers": report["distinctCheckedLiveMomentumsSeenFromTwoPeers"],
            "drops": report["captureDrops"],
            "clockJumps": report["clockJumps"],
            "container": json.loads(stats.stdout) if stats.stdout.strip() else None,
        }
        line = json.dumps(sample)
        output.write(line + "\n")
        output.flush()
        print(line, flush=True)
        if report["final"]:
            break
        time.sleep(min(60, max(0, deadline - time.monotonic())))
    else:
        raise SystemExit("Sampler deadline reached before a final report")
