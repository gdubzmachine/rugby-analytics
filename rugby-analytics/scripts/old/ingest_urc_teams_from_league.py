#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""
ingest_urc_teams_from_league.py
-------------------------------

Fetch **all teams** for the United Rugby Championship (or any TheSportsDB league)
and upsert them into your `teams` table.

Uses TheSportsDB V1 API:

    THESPORTSDB_API_KEY  (from .env, e.g. your premium key)

Default target league: URC = idLeague 4446.

What it does
============

1. Calls:
       lookup_all_teams.php?id={idLeague}
   which returns all teams registered for that league.

2. Filters to rugby teams (strSport starts with "Rugby").

3. For each team, maps TSDB → DB:

   - teams.tsdb_team_id  ← idTeam         (PRIMARY external key)
   - teams.name          ← strTeam
   - teams.short_name    ← strTeamShort or strAlternate or derived
   - teams.abbreviation  ← strTeamShort (if short) or NULL
   - teams.country       ← strCountry

4. Upsert logic:

   - First, look up by tsdb_team_id.
       If found → UPDATE basic fields (name, short_name, abbreviation, country).
   - If not found, look for an existing row by name (case-insensitive).
       If found → UPDATE that row and set tsdb_team_id.
   - Else → INSERT a new team row.

5. Optional CSV snapshot:
       ./data/urc_teams_from_league.csv

Assumptions about your schema
=============================

`teams` table has at least:

- team_id       (PK, BIGSERIAL)
- name          (TEXT)
- short_name    (TEXT, nullable)
- abbreviation  (TEXT, nullable)
- country       (TEXT, nullable)
- espn_team_id  (TEXT, nullable, UNIQUE)
- tsdb_team_id  (TEXT, nullable, UNIQUE)  <-- this script uses this
- created_at    (TIMESTAMPTZ)
- updated_at    (TIMESTAMPTZ)

DB connection
=============

Preferred: your existing helper in rugby-analytics/db:

    from db.connection import get_db_connection
    conn = get_db_connection()

Fallback: standard DATABASE_URL:

    export DATABASE_URL=postgres://user:pass@host:port/dbname

Usage
=====

# URC (default league_id=4446), verbose + CSV snapshot
python .\scripts\ingest_urc_teams_from_league.py -v --write-csv

# Other league (e.g. 4550 = European Rugby Champions Cup)
python .\scripts\ingest_urc_teams_from_league.py --league-id 4550 -v

"""

import os
import sys
import csv
import time
from typing import Any, Dict, List, Optional

# --- HTTP --------------------------------------------------------------------
try:
    import requests
except ImportError:
    print("Missing dependency: requests (pip install requests)", file=sys.stderr)
    sys.exit(1)

# --- DB ----------------------------------------------------------------------
try:
    import psycopg2
    from psycopg2.extras import DictCursor
except ImportError:
    print("Missing dependency: psycopg2 (pip install psycopg2)", file=sys.stderr)
    sys.exit(1)

# Try to use your existing db connection helper, if present
try:
    from db.connection import get_db_connection  # type: ignore
except Exception:
    get_db_connection = None  # type: ignore


# --- Env helpers -------------------------------------------------------------
def _load_dotenv_if_available() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        pass


def _tsdb_base(api_key: str) -> str:
    return f"https://www.thesportsdb.com/api/v1/json/{api_key}"


def _session_with_retries() -> requests.Session:
    s = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=0)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _get_with_backoff(
    session: requests.Session,
    url: str,
    params: Dict[str, Any],
    max_retries: int = 4,
    verbose: bool = False,
) -> requests.Response:
    delay = 0.8
    for attempt in range(1, max_retries + 1):
        resp = session.get(url, params=params, timeout=45)
        if resp.status_code == 429:
            if verbose:
                print(
                    f"[WARN] 429 Too Many Requests (attempt {attempt}/{max_retries}); "
                    f"sleeping {delay:.1f}s…",
                    file=sys.stderr,
                )
            time.sleep(delay)
            delay *= 1.75
            continue
        resp.raise_for_status()
        return resp
    resp.raise_for_status()
    return resp


# --- Small utils -------------------------------------------------------------
def _ensure_data_dir() -> str:
    out = os.path.join(os.getcwd(), "data")
    os.makedirs(out, exist_ok=True)
    return out


def _write_teams_csv(teams: List[Dict[str, Any]], league_id: str) -> str:
    out_dir = _ensure_data_dir()
    fname = f"urc_teams_from_league_{league_id}.csv"
    path = os.path.join(out_dir, fname)

    cols = [
        "idTeam",
        "strTeam",
        "strTeamShort",
        "strAlternate",
        "strCountry",
        "strSport",
    ]

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


def _best_short_name(team: Dict[str, Any]) -> Optional[str]:
    """
    Decide a good short_name for this team.
    Priority:
        1) strTeamShort if present
        2) strAlternate if present
        3) First word of strTeam
    """
    t_short = (team.get("strTeamShort") or "").strip()
    t_alt = (team.get("strAlternate") or "").strip()
    t_name = (team.get("strTeam") or "").strip()

    if t_short:
        return t_short
    if t_alt:
        return t_alt
    if t_name:
        return t_name.split()[0]
    return None


def _best_abbreviation(team: Dict[str, Any]) -> Optional[str]:
    """
    Make a simple abbreviation:
        - If strTeamShort is 2–5 chars, use it.
        - Else None.
    """
    t_short = (team.get("strTeamShort") or "").strip()
    if 2 <= len(t_short) <= 5:
        return t_short
    return None


# --- TheSportsDB league → teams ---------------------------------------------
def _lookup_all_teams_for_league(
    session: requests.Session,
    api_key: str,
    league_id: str,
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    """
    Use:
        lookup_all_teams.php?id={idLeague}

    Then filter to rugby teams and dedupe by idTeam.
    """
    url = f"{_tsdb_base(api_key)}/lookup_all_teams.php"
    resp = _get_with_backoff(session, url, {"id": league_id}, verbose=verbose)
    data = resp.json() or {}
    teams = data.get("teams") or []

    # Filter to rugby
    rugby_teams: List[Dict[str, Any]] = []
    seen_ids = set()

    for t in teams:
        sport = (t.get("strSport") or "").lower()
        if not sport.startswith("rugby"):
            continue
        tid = (t.get("idTeam") or "").strip()
        if not tid or tid in seen_ids:
            continue
        seen_ids.add(tid)
        rugby_teams.append(t)

    return rugby_teams


# --- DB helpers --------------------------------------------------------------
def _get_conn():
    """
    Get a psycopg2 connection, preferring your db.connection helper if available.
    """
    if get_db_connection is not None:
        return get_db_connection()  # type: ignore

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "No db.connection.get_db_connection and no DATABASE_URL set. "
            "Either create db/connection.py with get_db_connection(), or set DATABASE_URL."
        )
    return psycopg2.connect(dsn)


def _upsert_team(
    cur,
    team: Dict[str, Any],
    verbose: bool = False,
) -> str:
    """
    Upsert a single team row.

    Returns:
        "inserted", "updated_by_tsdb_id", or "matched_by_name"
    """
    tsdb_team_id = (team.get("idTeam") or "").strip()
    if not tsdb_team_id:
        raise ValueError("Team missing idTeam / tsdb_team_id")

    name = (team.get("strTeam") or "").strip()
    short_name = _best_short_name(team)
    abbreviation = _best_abbreviation(team)
    country = (team.get("strCountry") or "").strip() or None

    # 1) Try lookup by tsdb_team_id
    cur.execute(
        "SELECT team_id FROM teams WHERE tsdb_team_id = %s",
        (tsdb_team_id,),
    )
    row = cur.fetchone()
    if row:
        team_id = row[0]
        if verbose:
            print(f"  [DB] update team_id={team_id} (by tsdb_team_id={tsdb_team_id})")
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
            (
                name,
                short_name,
                abbreviation,
                country,
                team_id,
            ),
        )
        return "updated_by_tsdb_id"

    # 2) Try lookup by name (case-insensitive) to attach tsdb id
    cur.execute(
        """
        SELECT team_id
        FROM teams
        WHERE LOWER(name) = LOWER(%s)
        LIMIT 1
        """,
        (name,),
    )
    row = cur.fetchone()
    if row:
        team_id = row[0]
        if verbose:
            print(f"  [DB] attach tsdb_team_id={tsdb_team_id} to existing team_id={team_id} (by name='{name}')")
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
            (
                tsdb_team_id,
                short_name,
                abbreviation,
                country,
                team_id,
            ),
        )
        return "matched_by_name"

    # 3) Insert new row
    if verbose:
        print(f"  [DB] insert new team tsdb_team_id={tsdb_team_id}, name='{name}'")

    cur.execute(
        """
        INSERT INTO teams (
            name,
            short_name,
            abbreviation,
            country,
            espn_team_id,
            tsdb_team_id,
            created_at,
            updated_at
        ) VALUES (
            %s, %s, %s, %s,
            NULL,
            %s,
            NOW(), NOW()
        )
        """,
        (
            name,
            short_name,
            abbreviation,
            country,
            tsdb_team_id,
        ),
    )
    return "inserted"


# --- Main --------------------------------------------------------------------
def main() -> None:
    import argparse

    _load_dotenv_if_available()

    parser = argparse.ArgumentParser(
        description="Ingest all URC teams (or any league's teams) from TheSportsDB into Postgres."
    )
    parser.add_argument(
        "--league-id",
        default="4446",
        help="TheSportsDB idLeague (default: URC = 4446)",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("THESPORTSDB_API_KEY", "752396"),
        help="TheSportsDB V1 API key (default: THESPORTSDB_API_KEY or '752396').",
    )
    parser.add_argument(
        "--write-csv",
        action="store_true",
        help="Also write a CSV snapshot to ./data.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose logging.",
    )
    args = parser.parse_args()

    api_key = args.api_key
    league_id = str(args.league_id)
    verbose = args.verbose

    if verbose:
        shown = api_key if len(api_key) <= 4 else api_key[:2] + "***" + api_key[-2:]
        print(f"[INFO] Using TheSportsDB V1 key: '{shown}'")
        print(f"[INFO] Target TSDB league_id={league_id} (URC by default)")

    # HTTP session
    sess = _session_with_retries()

    # 1) Fetch teams for league
    teams = _lookup_all_teams_for_league(sess, api_key, league_id, verbose=verbose)
    if verbose:
        print(f"[INFO] Retrieved {len(teams)} rugby team(s) for league_id={league_id}")

    if not teams:
        raise SystemExit("No rugby teams returned for that league_id; check league_id and API key.")

    # Optional: CSV snapshot
    if args.write_csv:
        path = _write_teams_csv(teams, league_id)
        print(f"[OK] Wrote CSV snapshot: {path}")

    # 2) Upsert into DB
    conn = _get_conn()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=DictCursor)

    inserted = 0
    updated_by_tsdb_id = 0
    matched_by_name = 0

    try:
        for idx, t in enumerate(teams, start=1):
            if verbose:
                print(
                    f"[INFO] ({idx}/{len(teams)}) tsdb_team_id={t.get('idTeam')} "
                    f"name='{t.get('strTeam')}'"
                )
            outcome = _upsert_team(cur, t, verbose=verbose)
            if outcome == "inserted":
                inserted += 1
            elif outcome == "updated_by_tsdb_id":
                updated_by_tsdb_id += 1
            elif outcome == "matched_by_name":
                matched_by_name += 1

        conn.commit()
        print(
            f"[OK] Teams upsert complete -> "
            f"inserted={inserted}, updated_by_tsdb_id={updated_by_tsdb_id}, matched_by_name={matched_by_name}"
        )

    except Exception as exc:
        conn.rollback()
        print(f"[ERROR] Upsert failed, rolled back transaction: {exc}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
