#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ingest_urc_teams.py
-------------------

Fetch all URC teams from TheSportsDB and ingest them into Postgres.

- Uses shared tsdb_client from scr/ingest/tsdb_client.py
- Discovers URC teams by scanning events for the current season
- Inserts/updates teams table using tsdb_team_id
- Optionally writes CSV snapshot

Usage:
  python scripts/ingest_urc_teams.py --write-csv -v
"""

import os
import sys
import csv
from typing import Dict, Any, List, Optional

# Load project root so "scr" package works
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Now import the shared TSDB client
try:
    from scr.ingest.tsdb_client import (
        get_league_meta,
        get_current_season_label,
        get_events_for_season_rugby,
        get_team_details,
    )
except Exception as exc:
    print(f"[ERROR] Failed to import scr.ingest.tsdb_client: {exc}")
    sys.exit(1)

# DB
try:
    import psycopg2
    from psycopg2.extras import DictCursor
except ImportError:
    print("Missing dependency psycopg2. Run: pip install psycopg2-binary")
    sys.exit(1)

# DB connection helper
try:
    from db.connection import get_db_connection
except Exception:
    get_db_connection = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_conn():
    if get_db_connection:
        return get_db_connection()
    
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set and no db.connection helper found.")
    return psycopg2.connect(dsn)


def _write_csv_snapshot(teams: List[Dict[str, Any]], filename: str) -> str:
    os.makedirs("data", exist_ok=True)
    path = os.path.join("data", filename)

    cols = ["tsdb_team_id", "name", "short_name", "country", "badge_url"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for t in teams:
            w.writerow(t)

    return path


def _upsert_team(cur, t: Dict[str, Any], verbose: bool=False):
    """Insert or update a team using tsdb_team_id as unique key."""

    cur.execute("SELECT team_id FROM teams WHERE tsdb_team_id = %s", (t["tsdb_team_id"],))
    row = cur.fetchone()

    if row:
        team_id = row[0]
        if verbose:
            print(f"  [UPDATE] team_id={team_id} : {t['name']}")
        cur.execute("""
            UPDATE teams SET
              name = %s,
              short_name = %s,
              abbreviation = %s,
              country = %s,
              updated_at = NOW()
            WHERE team_id = %s
        """, (
            t["name"],
            t["short_name"],
            t["short_name"],
            t["country"],
            team_id
        ))
        return "update"

    # INSERT
    if verbose:
        print(f"  [INSERT] {t['name']}")

    cur.execute("""
        INSERT INTO teams (
            name,
            short_name,
            abbreviation,
            country,
            tsdb_team_id,
            created_at,
            updated_at
        ) VALUES (%s,%s,%s,%s,%s,NOW(),NOW())
    """, (
        t["name"],
        t["short_name"],
        t["short_name"],
        t["country"],
        t["tsdb_team_id"],
    ))
    return "insert"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Ingest URC teams into Postgres.")
    parser.add_argument("--league-id", default="4446", help="TSDB league id (default URC=4446)")
    parser.add_argument("--write-csv", action="store_true", help="Write CSV snapshot")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    league_id = args.league_id
    verbose = args.verbose

    # --- Fetch league info ---
    league_meta = get_league_meta(league_id)
    league_name = league_meta.get("strLeague", "Unknown League")
    print(f"[INFO] Target League: {league_name} (id={league_id})")

    # --- Find current season ---
    current_season = get_current_season_label(league_id)
    print(f"[INFO] Current Season: {current_season}")

    # --- Get events to discover ALL team IDs ---
    events = get_events_for_season_rugby(league_id, current_season)
    print(f"[INFO] Retrieved {len(events)} events")

    team_ids: set[str] = set()
    for e in events:
        if e.get("idHomeTeam"):
            team_ids.add(e["idHomeTeam"])
        if e.get("idAwayTeam"):
            team_ids.add(e["idAwayTeam"])

    team_ids = sorted(team_ids)
    print(f"[INFO] Found {len(team_ids)} unique team IDs: {team_ids}")

    # --- Fetch full team metadata ---
    teams: List[Dict[str, Any]] = []
    for tid in team_ids:
        t = get_team_details(tid)
        if not t:
            print(f"[WARN] No team data for idTeam={tid}")
            continue

        teams.append({
            "tsdb_team_id": tid,
            "name": t.get("strTeam"),
            "short_name": t.get("strTeamShort") or t.get("strAlternate") or t.get("strTeam"),
            "country": t.get("strCountry"),
            "badge_url": t.get("strTeamBadge"),
        })

    # --- Optional CSV export ---
    if args.write_csv:
        fname = f"urc_teams_{current_season}.csv"
        path = _write_csv_snapshot(teams, fname)
        print(f"[OK] Wrote CSV snapshot: {path}")

    # --- Write to DB ---
    conn = _get_conn()
    cur = conn.cursor(cursor_factory=DictCursor)

    inserted = updated = 0
    try:
        for t in teams:
            res = _upsert_team(cur, t, verbose=verbose)
            if res == "insert":
                inserted += 1
            else:
                updated += 1

        conn.commit()
        print(f"[DONE] Teams ingested. Inserted={inserted}, Updated={updated}")

    except Exception as exc:
        conn.rollback()
        print(f"[ERROR] DB failure: {exc}")
        raise

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
