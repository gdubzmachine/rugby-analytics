#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ingest_all_rugby_teams.py

Runs ingest_league_teams_from_events.py for every rugby league we track.
"""

import subprocess
import sys
import os
import time

ROOT = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(ROOT)
PYTHON = os.path.join(PROJECT_ROOT, ".venv", "Scripts", "python.exe")

TARGET_SCRIPT = os.path.join(ROOT, "ingest_league_teams_from_events.py")
if not os.path.exists(TARGET_SCRIPT):
    print(f"[ERROR] ingest_league_teams_from_events.py not found at {TARGET_SCRIPT}")
    sys.exit(1)

# All priority rugby leagues (Div 1 + International)
RUGBY_LEAGUES = [
    4446, 5370, 5069, 5166, 5167, 5070, 5168, 5480, 5479,
    5170, 5169, 5701, 5165, 5512, 5695, 5418, 4550,
    5037, 4985, 4986, 4983, 4574, 4714, 5082, 5563, 5682
]

print(f"Running TEAM INGEST for {len(RUGBY_LEAGUES)} rugby leagues...\n")

for league_id in RUGBY_LEAGUES:
    print(f"=== Ingesting TEAMS for TSDB league {league_id} ===")

    cmd = [
        PYTHON,
        TARGET_SCRIPT,
        "--tsdb-league-id", str(league_id),
        "--write-csv",
        "-v",
    ]

    try:
        result = subprocess.run(cmd, capture_output=False)
        if result.returncode != 0:
            print(f"!!! ingest FAILED for league {league_id}")
        else:
            print(f"✔ ingest SUCCESS for league {league_id}")

    except Exception as exc:
        print(f"!!! ingest CRASH for league {league_id}: {exc}")

    # Sleep between runs to avoid API rate limit
    time.sleep(5)

print("\n[DONE] Team ingestion completed.")
