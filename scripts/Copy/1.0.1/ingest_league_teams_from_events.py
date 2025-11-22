#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ingest_league_teams_from_events.py
Fetches teams for one or more rugby leagues by scanning match events,
upserts them into `teams`, and links them into `league_team_seasons`.

USAGE EXAMPLE (from C:\\rugby-analytics):

    python -m scripts.ingest_league_teams_from_events ^
        --tsdb-leagues 4446 ^
        --limit-seasons-back 10 ^
        --write-csv -v

Where:
  - 4446 is the TheSportsDB league id (URC).
"""

import os
import sys
import csv
import time
from typing import List, Dict, Any, Set, Optional, Tuple

import psycopg2
from psycopg2.extras import DictCursor

# --- import your tsdb_client correctly ---
try:
    from scr.ingest.tsdb_client import (
        get_league_meta,
        get_current_season_label,
        get_events_for_season_rugby,
        get_team_details,  # uses TSDB lookupteam.php under the hood
    )
except Exception as e:
    print("[ERROR] Could not import scr.ingest.tsdb_client:", e, file=sys.stderr)
    sys.exit(1)

# Optional DB helper
try:
    from db.connection import get_db_connection  # type: ignore
except Exception:
    get_db_connection = None  # type: ignore


# ----------------------------------------------------------------------
# DB CONNECTION
# ----------------------------------------------------------------------
def get_conn():
    """Connects to DB using your helper OR DATABASE_URL."""
    if get_db_connection is not None:
        return get_db_connection()

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "DATABASE_URL not set and db.connection.get_db_connection() missing."
        )
    return psycopg2.connect(dsn)


# ----------------------------------------------------------------------
# TEAM SCHEMA INTROSPECTION (avoid missing columns like 'sport')
# ----------------------------------------------------------------------
_TEAM_COLUMNS: Optional[Set[str]] = None


def _get_team_columns(cur) -> Set[str]:
    global _TEAM_COLUMNS
    if _TEAM_COLUMNS is not None:
        return _TEAM_COLUMNS

    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'teams'
        """
    )
    _TEAM_COLUMNS = {row[0] for row in cur.fetchall()}
    return _TEAM_COLUMNS


# ----------------------------------------------------------------------
# HELPERS
# ----------------------------------------------------------------------
def previous_season_label(label: str) -> str:
    """Given '2025-2026' returns '2024-2025'."""
    s = label.strip()
    if "-" in s:
        start = int(s.split("-")[0])
        return f"{start - 1}-{start}"
    # fallback: treat as year string
    y = int(s[:4])
    return str(y - 1)


def ensure_team_in_db(cur, t: Dict[str, Any], verbose: bool = False) -> int:
    """
    Insert/update a team based on tsdb_team_id, using ONLY columns that exist
    in your `teams` table (detected via information_schema).

    Returns: team_id
    """
    cols = _get_team_columns(cur)

    tsdb_id = t.get("idTeam")
    name = (t.get("strTeam") or "").strip()
    short = (t.get("strTeamShort") or "").strip()
    alt = (t.get("strAlternate") or "").strip()
    country = (t.get("strCountry") or "").strip()
    sport = (t.get("strSport") or "Rugby").strip()

    if not tsdb_id or not name:
        raise ValueError("Team object missing idTeam or strTeam")

    # Make sure we actually have tsdb_team_id in the schema
    if "tsdb_team_id" not in cols:
        raise RuntimeError(
            "teams.tsdb_team_id column does not exist. "
            "Please add it or adjust this script."
        )

    # 1) Try to find existing team
    cur.execute(
        "SELECT team_id FROM teams WHERE tsdb_team_id = %s",
        (tsdb_id,),
    )
    row = cur.fetchone()

    # ----------------------------------
    # UPDATE path
    # ----------------------------------
    if row:
        team_id = row[0]
        if verbose:
            print(f"  [UPDATE] team_id={team_id}: {name}")

        update_fragments = []
        params = []

        if "name" in cols:
            update_fragments.append("name = %s")
            params.append(name)

        if "short_name" in cols:
            update_fragments.append("short_name = %s")
            params.append(short or alt or name)

        if "country" in cols:
            update_fragments.append("country = %s")
            params.append(country)

        if "sport" in cols:
            update_fragments.append("sport = %s")
            params.append(sport)

        # If nothing to update, just return
        if not update_fragments:
            return team_id

        sql = f"UPDATE teams SET {', '.join(update_fragments)} WHERE team_id = %s"
        params.append(team_id)
        cur.execute(sql, tuple(params))
        return team_id

    # ----------------------------------
    # INSERT path
    # ----------------------------------
    if verbose:
        print(f"  [INSERT] {name} (tsdb_team_id={tsdb_id})")

    insert_cols: List[str] = []
    placeholders: List[str] = []
    params: List[Any] = []

    if "name" in cols:
        insert_cols.append("name")
        placeholders.append("%s")
        params.append(name)

    if "short_name" in cols:
        insert_cols.append("short_name")
        placeholders.append("%s")
        params.append(short or alt or name)

    if "country" in cols:
        insert_cols.append("country")
        placeholders.append("%s")
        params.append(country)

    if "sport" in cols:
        insert_cols.append("sport")
        placeholders.append("%s")
        params.append(sport)

    # tsdb_team_id is essential
    insert_cols.append("tsdb_team_id")
    placeholders.append("%s")
    params.append(tsdb_id)

    if not insert_cols:
        raise RuntimeError("No insertable columns detected for teams table.")

    sql = f"""
        INSERT INTO teams ({', '.join(insert_cols)})
        VALUES ({', '.join(placeholders)})
        RETURNING team_id
    """
    cur.execute(sql, tuple(params))
    team_id = cur.fetchone()[0]
    return team_id


def _resolve_db_league_id(cur, tsdb_league_id: str) -> Optional[int]:
    """
    Resolve internal league_id from leagues.tsdb_league_id.
    """
    cur.execute(
        """
        SELECT league_id
        FROM leagues
        WHERE tsdb_league_id = %s
        """,
        (tsdb_league_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return row[0]


def _resolve_db_season_ids(
    cur,
    db_league_id: int,
    season_labels: List[str],
) -> Dict[str, int]:
    """
    Map TSDB season labels -> seasons.season_id in your DB.
    Tries tsdb_season_key, falls back to label.
    """
    out: Dict[str, int] = {}
    for s in season_labels:
        cur.execute(
            """
            SELECT season_id
            FROM seasons
            WHERE league_id = %s
              AND (tsdb_season_key = %s OR label = %s)
            ORDER BY tsdb_season_key IS NULL, year DESC
            LIMIT 1
            """,
            (db_league_id, s, s),
        )
        row = cur.fetchone()
        if row:
            out[s] = row[0]
    return out


def _link_league_team_seasons(
    cur,
    db_league_id: int,
    per_season_team_ids: Dict[str, Set[str]],
    season_label_to_id: Dict[str, int],
    verbose: bool = False,
) -> None:
    """
    Ensure league_team_seasons is populated for (league, season, team).
    Uses a safe INSERT ... WHERE NOT EXISTS pattern (no ON CONFLICT needed).
    """
    for season_label, tsdb_team_ids in per_season_team_ids.items():
        season_id = season_label_to_id.get(season_label)
        if not season_id:
            if verbose:
                print(
                    f"[WARN] No DB season_id found for season label {season_label!r}; "
                    "skipping league_team_seasons rows."
                )
            continue

        if verbose:
            print(
                f"[INFO] Linking league_team_seasons for league_id={db_league_id}, "
                f"season_id={season_id} ({season_label}), "
                f"{len(tsdb_team_ids)} teams"
            )

        for tsdb_tid in tsdb_team_ids:
            cur.execute(
                "SELECT team_id FROM teams WHERE tsdb_team_id = %s",
                (tsdb_tid,),
            )
            row = cur.fetchone()
            if not row:
                continue
            team_id = row[0]

            # INSERT only if not exists (works even without PK/unique)
            cur.execute(
                """
                INSERT INTO league_team_seasons (league_id, season_id, team_id)
                SELECT %s, %s, %s
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM league_team_seasons
                    WHERE league_id = %s
                      AND season_id = %s
                      AND team_id = %s
                )
                """,
                (db_league_id, season_id, team_id, db_league_id, season_id, team_id),
            )


# ----------------------------------------------------------------------
# MAIN INGEST
# ----------------------------------------------------------------------
def ingest_teams_for_league(
    tsdb_league_id: int,
    limit_back: int,
    write_csv: bool,
    verbose: bool,
):
    """
    Main ingest for a single TSDB league id.
    """
    conn = get_conn()
    cur = conn.cursor(cursor_factory=DictCursor)

    tsdb_league_str = str(tsdb_league_id)

    # 1) League metadata from TSDB (for logging)
    meta = get_league_meta(tsdb_league_str, verbose=verbose)
    league_name = meta.get("strLeague") or f"league_{tsdb_league_str}"

    if verbose:
        print(f"[TSDB] League {tsdb_league_str}: {league_name}")

    # 2) Resolve DB league_id (if present)
    db_league_id = _resolve_db_league_id(cur, tsdb_league_str)
    if db_league_id is None:
        print(
            f"[WARN] No league row found in DB for tsdb_league_id={tsdb_league_str}. "
            "Teams will be inserted, but league_team_seasons cannot be populated.",
            file=sys.stderr,
        )

    # 3) Determine seasons to fetch from TSDB
    current = get_current_season_label(tsdb_league_str, verbose=verbose)
    if verbose:
        print(f"[TSDB] Current season for league {tsdb_league_str}: {current}")

    seasons: List[str] = []
    s = current
    for _ in range(limit_back):
        seasons.append(s)
        s = previous_season_label(s)

    # 4) Map TSDB season labels to DB seasons (if league exists)
    season_label_to_id: Dict[str, int] = {}
    if db_league_id is not None:
        season_label_to_id = _resolve_db_season_ids(cur, db_league_id, seasons)
        if verbose:
            print(
                "[INFO] Resolved DB seasons:",
                {k: season_label_to_id.get(k) for k in seasons},
            )

    # 5) Fetch events and collect team ids, per-season
    all_team_ids: Set[str] = set()
    per_season_team_ids: Dict[str, Set[str]] = {}

    for season_label in seasons:
        if verbose:
            print(f"[INFO] Fetching events for season={season_label}")

        try:
            events = get_events_for_season_rugby(
                tsdb_league_str,
                season_label,
                verbose=verbose,
            )
        except Exception as e:
            print(f"[WARN] Failed fetching season {season_label}: {e}", file=sys.stderr)
            events = []

        season_set = per_season_team_ids.setdefault(season_label, set())

        for ev in events:
            home = ev.get("idHomeTeam")
            away = ev.get("idAwayTeam")

            if home:
                all_team_ids.add(home)
                season_set.add(home)
            if away:
                all_team_ids.add(away)
                season_set.add(away)

        # Sleep between season calls to reduce 429
        time.sleep(1.2)

    if verbose:
        print(
            f"[INFO] Discovered {len(all_team_ids)} unique team ids "
            f"for league {league_name}"
        )

    # 6) Lookup each team with TSDB and upsert into DB
    for tid in sorted(all_team_ids):
        if not tid:
            continue
        tdata = get_team_details(str(tid), verbose=verbose)
        if not tdata:
            if verbose:
                print(f"[WARN] Team lookup failed for {tid}")
            continue

        ensure_team_in_db(cur, tdata, verbose=verbose)
        conn.commit()  # commit after each team to keep things safe

        # delay to reduce 429
        time.sleep(0.8)

    # 7) Link into league_team_seasons (if we know league + seasons in DB)
    if db_league_id is not None and season_label_to_id:
        if verbose:
            print("[INFO] Populating league_team_seasons…")
        _link_league_team_seasons(
            cur,
            db_league_id=db_league_id,
            per_season_team_ids=per_season_team_ids,
            season_label_to_id=season_label_to_id,
            verbose=verbose,
        )
        conn.commit()

    # 8) Write CSV if requested
    if write_csv:
        outdir = os.path.join(os.getcwd(), "data")
        os.makedirs(outdir, exist_ok=True)
        path = os.path.join(outdir, f"teams_league_{tsdb_league_str}.csv")

        if verbose:
            print(f"[INFO] Writing CSV: {path}")

        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["tsdb_team_id"])
            for tid in sorted(all_team_ids):
                w.writerow([tid])

    cur.close()
    conn.close()


# ----------------------------------------------------------------------
# CLI ENTRY
# ----------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Ingest teams for rugby leagues by scanning events, "
            "upserting into teams, and linking league_team_seasons."
        )
    )
    parser.add_argument(
        "--tsdb-leagues",
        nargs="+",
        required=True,
        help="One or more TSDB league IDs (e.g. 4446 5167 4714)",
    )
    parser.add_argument(
        "--limit-seasons-back",
        type=int,
        default=8,
        help="How many seasons back to scan (default 8)",
    )
    parser.add_argument(
        "--write-csv",
        action="store_true",
        help="Write CSV of discovered team IDs",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
    )
    args = parser.parse_args()

    for idx, lid in enumerate(args.tsdb_leagues, start=1):
        print(f"\n=== [{idx}/{len(args.tsdb_leagues)}] LEAGUE {lid} ===")
        ingest_teams_for_league(
            tsdb_league_id=int(lid),
            limit_back=args.limit_seasons_back,
            write_csv=args.write_csv,
            verbose=args.verbose,
        )
        print(f"=== DONE league {lid} ===\n")
        time.sleep(3)  # avoid hammering API


if __name__ == "__main__":
    main()
