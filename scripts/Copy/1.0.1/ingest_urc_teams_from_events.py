#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""
ingest_urc_teams_from_events.py
--------------------------------

Goal:
  Get ALL URC teams (16) into `teams` by deriving team IDs from the schedule
  and then enriching each team with `lookupteam.php`.

Why this method:
  TheSportsDB v1 doesn't reliably serve rugby leagues via `lookup_all_teams.php?id={league}`.
  But `eventsseason.php?id={league}&s={season}` returns full fixtures with idHomeTeam/idAwayTeam.
  We take the unique team IDs from there and fetch each team via `lookupteam.php?id={idTeam}`.

Now modular:
  - All TheSportsDB HTTP logic lives in scr.ingest.tsdb_client
  - This script only orchestrates:
       * find team IDs via events
       * fetch teams via lookupteam
       * upsert into DB

Inputs / Config:
  - .env: THESPORTSDB_API_KEY (your premium key)
  - Default URC league idLeague = 4446
  - Will scan the current season (and then previous seasons if needed) until it finds 16 teams.

DB assumptions:
  Table `teams` has:
    team_id (PK),
    name TEXT,
    short_name TEXT NULL,
    abbreviation TEXT NULL,
    country TEXT NULL,
    espn_team_id TEXT NULL UNIQUE,
    tsdb_team_id TEXT NULL UNIQUE,
    created_at, updated_at TIMESTAMPTZ.

Usage:
  python .\scripts\ingest_urc_teams_from_events.py -v --write-csv
"""

import os
import sys
import csv
import time
from typing import Any, Dict, List, Optional, Set, Tuple

# DB
try:
    import psycopg2
    from psycopg2.extras import DictCursor
except ImportError:
    print("Missing dependency: psycopg2 (pip install psycopg2-binary)", file=sys.stderr)
    sys.exit(1)

# Try to use your existing db helper
try:
    from db.connection import get_db_connection  # type: ignore
except Exception:
    get_db_connection = None  # type: ignore

# Shared TSDB client
from scr.ingest import tsdb_client


# -------------------------
# Env helpers
# -------------------------
def _load_dotenv_if_available() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        pass


# -------------------------
# Utilities
# -------------------------
def _previous_season_label(label: str) -> str:
    s = (label or "").strip()
    # 'YYYY-YYYY' → subtract 1 from start
    if len(s) >= 9 and s[4] in "-/":
        try:
            start_year = int(s[:4])
            prev_start = start_year - 1
            prev_end = prev_start + 1
            return f"{prev_start}-{prev_end}"
        except Exception:
            pass
    # Fallback: numeric year
    try:
        y = int(s[:4])
        return str(y - 1)
    except Exception:
        return s


def _best_short_name(team: Dict[str, Any]) -> Optional[str]:
    short = (team.get("strTeamShort") or "").strip()
    alt = (team.get("strAlternate") or "").strip()
    name = (team.get("strTeam") or "").strip()
    if short:
        return short
    if alt:
        return alt
    return name.split()[0] if name else None


def _best_abbrev(team: Dict[str, Any]) -> Optional[str]:
    short = (team.get("strTeamShort") or "").strip()
    return short if 2 <= len(short) <= 5 else None


def _ensure_data_dir() -> str:
    out = os.path.join(os.getcwd(), "data")
    os.makedirs(out, exist_ok=True)
    return out


def _write_csv_snapshot(teams: List[Dict[str, Any]], league_id: str) -> str:
    path = os.path.join(_ensure_data_dir(), f"urc_teams_from_events_{league_id}.csv")
    cols = ["idTeam", "strTeam", "strTeamShort", "strAlternate", "strCountry", "strSport"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for t in teams:
            w.writerow({
                "idTeam": t.get("idTeam"),
                "strTeam": t.get("strTeam"),
                "strTeamShort": t.get("strTeamShort"),
                "strAlternate": t.get("strAlternate"),
                "strCountry": t.get("strCountry"),
                "strSport": t.get("strSport"),
            })
    return path


# -------------------------
# DB helpers
# -------------------------
def _get_conn():
    if get_db_connection is not None:
        return get_db_connection()  # type: ignore
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("No db.connection.get_db_connection() and no DATABASE_URL; set one or add the helper.")
    return psycopg2.connect(dsn)


def _ensure_tsdb_team_id_column(cur, verbose: bool = False) -> None:
    """
    Make sure teams.tsdb_team_id exists ( UNIQUE ). If not, add it.
    """
    cur.execute("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='teams' AND column_name='tsdb_team_id'
        LIMIT 1;
    """)
    if cur.fetchone():
        return
    if verbose:
        print("[INFO] Adding teams.tsdb_team_id column (TEXT UNIQUE)")
    cur.execute("ALTER TABLE teams ADD COLUMN IF NOT EXISTS tsdb_team_id TEXT;")
    # Add a unique index if not present
    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_indexes
                WHERE schemaname='public'
                  AND tablename='teams'
                  AND indexname='uniq_teams_tsdb_team_id'
            ) THEN
                CREATE UNIQUE INDEX uniq_teams_tsdb_team_id ON teams(tsdb_team_id);
            END IF;
        END; $$;
    """)


def _upsert_team(cur, team: Dict[str, Any], verbose: bool = False) -> str:
    """
    Upsert precedence:
      1) by tsdb_team_id (idTeam)
      2) else by LOWER(name) match, attach tsdb_team_id
      3) else insert

    Returns: "inserted" | "updated_by_tsdb_id" | "matched_by_name"
    """
    tsdb_id = (team.get("idTeam") or "").strip()
    if not tsdb_id:
        raise ValueError("Missing idTeam")

    name = (team.get("strTeam") or "").strip()
    short_name = _best_short_name(team)
    abbrev = _best_abbrev(team)
    country = (team.get("strCountry") or "").strip() or None

    # 1) by tsdb_team_id
    cur.execute("SELECT team_id FROM teams WHERE tsdb_team_id = %s", (tsdb_id,))
    row = cur.fetchone()
    if row:
        team_id = row[0]
        if verbose:
            print(f"  [DB] update team_id={team_id} via tsdb_team_id={tsdb_id}")
        cur.execute(
            """
            UPDATE teams
               SET name = %s,
                   short_name = %s,
                   abbreviation = %s,
                   country = %s,
                   updated_at = NOW()
             WHERE team_id = %s
            """,
            (name, short_name, abbrev, country, team_id),
        )
        return "updated_by_tsdb_id"

    # 2) by name (attach tsdb id)
    cur.execute(
        "SELECT team_id FROM teams WHERE LOWER(name) = LOWER(%s) LIMIT 1",
        (name,),
    )
    row = cur.fetchone()
    if row:
        team_id = row[0]
        if verbose:
            print(f"  [DB] attach tsdb_team_id={tsdb_id} to existing team_id={team_id} (by name='{name}')")
        cur.execute(
            """
            UPDATE teams
               SET tsdb_team_id = %s,
                   short_name = COALESCE(short_name, %s),
                   abbreviation = COALESCE(abbreviation, %s),
                   country = COALESCE(country, %s),
                   updated_at = NOW()
             WHERE team_id = %s
            """,
            (tsdb_id, short_name, abbrev, country, team_id),
        )
        return "matched_by_name"

    # 3) insert
    if verbose:
        print(f"  [DB] insert new team '{name}' (tsdb_team_id={tsdb_id})")
    cur.execute(
        """
        INSERT INTO teams (
            name, short_name, abbreviation, country,
            espn_team_id, tsdb_team_id, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s,
            NULL, %s, NOW(), NOW()
        )
        """,
        (name, short_name, abbrev, country, tsdb_id),
    )
    return "inserted"


# -------------------------
# Main workflow
# -------------------------
def main() -> None:
    import argparse

    _load_dotenv_if_available()

    parser = argparse.ArgumentParser(description="Ingest all URC teams by deriving from eventsseason (reliable).")
    parser.add_argument("--league-id", default="4446", help="TheSportsDB idLeague (URC=4446)")
    parser.add_argument(
        "--api-key",
        default=os.getenv("THESPORTSDB_API_KEY", "1"),
        help="TheSportsDB v1 key (default: THESPORTSDB_API_KEY or '1').",
    )
    parser.add_argument("--sleep-seconds", type=float, default=0.4, help="Sleep between team lookups (default 0.4)")
    parser.add_argument(
        "--max-seasons-scan",
        type=int,
        default=3,
        help="If fewer than 16 found, scan more previous seasons (default 3)",
    )
    parser.add_argument("--write-csv", action="store_true", help="Write CSV snapshot to ./data")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    api_key = args.api_key
    league_id = str(args.league_id)
    sleep_s = max(args.sleep_seconds, 0.0)
    verbose = args.verbose

    if verbose:
        shown = api_key if len(api_key) <= 4 else api_key[:2] + "***" + api_key[-2:]
        print(f"[INFO] Using TSDB v1 key '{shown}'  league_id={league_id}")

    sess = tsdb_client._session_with_default_headers()  # reuse shared helper

    # 1) league meta to get strCurrentSeason
    league_meta = tsdb_client.get_league_meta(
        league_id, api_key=api_key, session=sess, verbose=verbose
    )
    league_name = (league_meta.get("strLeague") or f"league-{league_id}").strip()
    current_season = (league_meta.get("strCurrentSeason") or "").strip()
    if not current_season:
        raise SystemExit("No strCurrentSeason on league; cannot proceed.")

    if verbose:
        print(f"[INFO] League: {league_name}  Current season: {current_season}")

    # 2) collect unique team ids from current season, else walk back
    target_count = 16  # URC teams
    seasons_checked: List[str] = []
    team_ids: Set[str] = set()
    team_names_for_fallback: Set[str] = set()

    season = current_season
    for i in range(max(1, args.max_seasons_scan + 1)):  # include current + N previous
        seasons_checked.append(season)
        events = tsdb_client.get_events_for_season_rugby(
            league_id, season, api_key=api_key, session=sess, verbose=verbose
        )
        if verbose:
            print(f"[INFO] season={season} -> {len(events)} rugby events")
        for e in events:
            hid = (e.get("idHomeTeam") or "").strip()
            aid = (e.get("idAwayTeam") or "").strip()
            if hid:
                team_ids.add(hid)
            if aid:
                team_ids.add(aid)
            ht = (e.get("strHomeTeam") or "").strip()
            at = (e.get("strAwayTeam") or "").strip()
            if ht:
                team_names_for_fallback.add(ht)
            if at:
                team_names_for_fallback.add(at)
        if len(team_ids) >= target_count:
            break
        season = _previous_season_label(season)

    if verbose:
        print(f"[INFO] Unique team IDs found: {len(team_ids)} → {sorted(team_ids)}")
        print(f"[INFO] Seasons checked: {seasons_checked}")

    if len(team_ids) == 0:
        raise SystemExit("No team IDs found from eventsseason. Check API key and league_id.")

    # 3) fetch full team objects
    enriched: List[Dict[str, Any]] = []
    for idx, tid in enumerate(sorted(team_ids), start=1):
        if verbose:
            print(f"[INFO] ({idx}/{len(team_ids)}) lookupteam id={tid}")
        t = tsdb_client.lookup_team(tid, api_key=api_key, session=sess, verbose=verbose)
        if t:
            enriched.append(t)
        else:
            if verbose:
                print(f"[WARN] lookupteam failed for id={tid}", file=sys.stderr)
        if sleep_s:
            time.sleep(sleep_s)

    if verbose:
        names = ", ".join([t.get("strTeam", "") for t in enriched])
        print(f"[INFO] Successfully fetched {len(enriched)} teams: {names}")

    # 4) optional CSV snapshot
    if args.write_csv:
        path = _write_csv_snapshot(enriched, league_id)
        print(f"[OK] Wrote CSV snapshot: {path}")

    # 5) upsert into DB
    conn = _get_conn()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=DictCursor)

    try:
        _ensure_tsdb_team_id_column(cur, verbose=verbose)

        inserted = 0
        updated_by_tsdb_id = 0
        matched_by_name = 0

        for t in enriched:
            outcome = _upsert_team(cur, t, verbose=verbose)
            if outcome == "inserted":
                inserted += 1
            elif outcome == "updated_by_tsdb_id":
                updated_by_tsdb_id += 1
            elif outcome == "matched_by_name":
                matched_by_name += 1

        conn.commit()
        print(
            f"[OK] URC teams upsert complete -> "
            f"inserted={inserted}, updated_by_tsdb_id={updated_by_tsdb_id}, matched_by_name={matched_by_name}"
        )

    except Exception as exc:
        conn.rollback()
        print(f"[ERROR] Upsert failed; rolled back: {exc}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
