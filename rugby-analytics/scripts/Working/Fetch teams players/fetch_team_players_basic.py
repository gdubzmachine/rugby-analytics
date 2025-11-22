#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
fetch_team_players_basic.py
---------------------------

Given a TheSportsDB team ID, fetch that team's squad list and upsert
basic player info into your `players` table.

Step 1 in your player pipeline:
- Just get: player ID, name, nationality, date of birth, position string.
- We'll later add a separate script to enrich per-player details.

What it does
------------
1. Calls v1 /lookup_all_players.php?id={team_id}.
2. Filters results (optionally) to rugby via strSport.
3. Ensures players.tsdb_player_id BIGINT + unique index exist.
4. Upserts into `players` table:
     - update by tsdb_player_id
     - else attach tsdb_player_id to an existing row with same full_name
     - else insert
5. Writes CSV snapshot to ./data/team_{team_id}_players.csv.

Assumptions
-----------
- DB connection module `db` exists in rugby-analytics/db and exposes one of:
    get_connection(), get_db(), connect()
  Otherwise, DATABASE_URL or PG* env vars are used.

- players table has at least:
    full_name, first_name, last_name, date_of_birth, nationality
  plus whatever other columns you already defined.

Environment
-----------
- .env in project root (python-dotenv optional).
- THESPORTSDB_API_KEY in .env (defaults to "1" if missing).

Usage
-----
1) Find a team ID in your DB (for example):
       SELECT name, tsdb_team_id FROM teams WHERE name = 'Leinster';

2) Run:
       python .\scripts\fetch_team_players_basic.py --team-id 123456 --team-name "Leinster" --write-csv -v

"""

import os
import re
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

# --- DB: psycopg v3 preferred, fallback to psycopg2 --------------------------
try:
    import psycopg  # v3
    _PSYCOPG_VERSION = 3
except ImportError:
    try:
        import psycopg2 as psycopg  # v2
        _PSYCOPG_VERSION = 2
    except ImportError:
        print(
            "Missing dependency: psycopg or psycopg2-binary\n"
            "Install one of:\n"
            "  pip install psycopg[binary]\n"
            "  or\n"
            "  pip install psycopg2-binary",
            file=sys.stderr,
        )
        sys.exit(1)


# --- Utils -------------------------------------------------------------------
def _load_dotenv_if_available() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        # dotenv is optional
        pass


def _clean(x: Optional[str]) -> Optional[str]:
    if x is None:
        return None
    x = x.strip()
    return x or None


def _split_name(full_name: str) -> (Optional[str], Optional[str]):
    """
    Simple split: last token = last_name, rest = first_name.
    """
    parts = full_name.split()
    if not parts:
        return None, None
    if len(parts) == 1:
        return parts[0], None
    first_name = " ".join(parts[:-1])
    last_name = parts[-1]
    return first_name, last_name


# --- TheSportsDB helpers -----------------------------------------------------
def _tsdb_base(api_key: str) -> str:
    return f"https://www.thesportsdb.com/api/v1/json/{api_key}"


def _fetch_team_players(api_key: str, team_id: str, verbose: bool = False) -> List[Dict[str, Any]]:
    """
    Use lookup_all_players.php?id={team_id} to fetch full squad list.
    """
    url = f"{_tsdb_base(api_key)}/lookup_all_players.php"
    resp = requests.get(url, params={"id": team_id}, timeout=30)
    resp.raise_for_status()
    payload = resp.json() or {}
    players = payload.get("player") or []

    # Optionally filter to rugby if mixed-sport club:
    rugby_players: List[Dict[str, Any]] = []
    for p in players:
        sport = (p.get("strSport") or "").lower()
        if sport and not sport.startswith("rugby"):
            # skip non-rugby variants like football/soccer if they ever appear
            continue
        rugby_players.append(p)

    if verbose:
        print(f"[INFO] TheSportsDB returned {len(players)} players, using {len(rugby_players)} rugby players")

    return rugby_players


# --- DB helpers --------------------------------------------------------------
def _get_db_connection():
    """
    Prefer your local db module (rugby-analytics/db) if present, else use env.
    """
    try:
        import db  # your project-local module
        for fn_name in ("get_connection", "get_db", "connect"):
            if hasattr(db, fn_name):
                return getattr(db, fn_name)()
    except Exception:
        pass

    dsn = os.getenv("DATABASE_URL") or os.getenv("PG_DSN")
    if dsn:
        return psycopg.connect(dsn)

    params = {
        "host": os.getenv("PGHOST"),
        "port": os.getenv("PGPORT"),
        "user": os.getenv("PGUSER"),
        "password": os.getenv("PGPASSWORD"),
        "dbname": os.getenv("PGDATABASE"),
    }
    if params["host"]:
        return psycopg.connect(**params)

    raise RuntimeError(
        "No DB connection available. Provide a `db` module or set DATABASE_URL / PG* env vars."
    )


def _ensure_tsdb_player_column(conn) -> None:
    """
    Ensure players.tsdb_player_id BIGINT + unique index exist.
    """
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE players ADD COLUMN IF NOT EXISTS tsdb_player_id BIGINT")
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS players_tsdb_player_id_uk "
            "ON players(tsdb_player_id)"
        )
    conn.commit()


def _upsert_player(conn, p: Dict[str, Any]) -> str:
    """
    Upsert a player into `players`.

    Returns:
      'updated_by_tsdb_id' | 'matched_by_name' | 'inserted'
    """
    tsdb_player_id = p.get("idPlayer")
    full_name = _clean(p.get("strPlayer")) or ""
    nationality = _clean(p.get("strNationality"))
    dob = _clean(p.get("dateBorn"))
    first_name, last_name = _split_name(full_name)

    with conn.cursor() as cur:
        # 1) Update by tsdb_player_id
        cur.execute(
            """
            UPDATE players
               SET full_name = %s,
                   first_name = COALESCE(%s, first_name),
                   last_name = COALESCE(%s, last_name),
                   date_of_birth = COALESCE(%s, date_of_birth),
                   nationality = COALESCE(%s, nationality)
             WHERE tsdb_player_id = %s
            """,
            (full_name, first_name, last_name, dob, nationality, tsdb_player_id),
        )
        if getattr(cur, "rowcount", 0) > 0:
            return "updated_by_tsdb_id"

        # 2) Attach tsdb id to an existing player with same full_name
        cur.execute(
            """
            UPDATE players
               SET tsdb_player_id = %s,
                   first_name = COALESCE(%s, first_name),
                   last_name = COALESCE(%s, last_name),
                   date_of_birth = COALESCE(%s, date_of_birth),
                   nationality = COALESCE(%s, nationality)
             WHERE LOWER(full_name) = LOWER(%s) AND tsdb_player_id IS NULL
            """,
            (tsdb_player_id, first_name, last_name, dob, nationality, full_name),
        )
        if getattr(cur, "rowcount", 0) > 0:
            return "matched_by_name"

        # 3) Insert new player row
        cur.execute(
            """
            INSERT INTO players (full_name, first_name, last_name, date_of_birth, nationality, tsdb_player_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (full_name, first_name, last_name, dob, nationality, tsdb_player_id),
        )
        return "inserted"


def _write_csv(players: List[Dict[str, Any]], team_id: str, team_name: Optional[str]) -> str:
    out_dir = os.path.join(os.getcwd(), "data")
    os.makedirs(out_dir, exist_ok=True)
    safe_name = ""
    if team_name:
        safe_name = re.sub(r"[^A-Za-z0-9]+", "_", team_name).strip("_")
    fname = f"team_{team_id}_players.csv" if not safe_name else f"team_{team_id}_{safe_name}_players.csv"
    out_path = os.path.join(out_dir, fname)

    cols = [
        "idPlayer",
        "strPlayer",
        "strPosition",
        "strNationality",
        "dateBorn",
        "strSport",
    ]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        for p in players:
            writer.writerow({c: p.get(c) for c in cols})

    return out_path


# --- Main --------------------------------------------------------------------
def main() -> None:
    import argparse

    _load_dotenv_if_available()

    parser = argparse.ArgumentParser(
        description="Fetch squad list for a team from TheSportsDB and upsert into players table."
    )
    parser.add_argument(
        "--team-id",
        required=True,
        help="TheSportsDB team ID (idTeam) for the squad to fetch.",
    )
    parser.add_argument(
        "--team-name",
        default=None,
        help="Optional human-readable team name for logging/CSV filename.",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("THESPORTSDB_API_KEY", "1"),
        help="TheSportsDB API key (default: 1 = public test key).",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.0,
        help="Optional sleep after API call (helps if rate limiting).",
    )
    parser.add_argument(
        "--write-csv",
        action="store_true",
        help="Write CSV snapshot to ./data/team_{team_id}_players.csv",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose logging.",
    )
    args = parser.parse_args()

    team_id = str(args.team_id)
    api_key = args.api_key
    team_name = args.team_name
    sleep_seconds = max(args.sleep_seconds, 0.0)

    if args.verbose:
        print(f"[INFO] Using TheSportsDB key={api_key!r}")
        print(f"[INFO] Team idTeam={team_id}  team_name={team_name!r}")

    # 1) Fetch players from TheSportsDB
    players = _fetch_team_players(api_key, team_id, verbose=args.verbose)
    if not players:
        raise SystemExit(f"No players returned for team_id={team_id}. Aborting.")

    if args.verbose:
        names = [p.get("strPlayer") for p in players]
        print(f"[INFO] Retrieved {len(players)} players: {', '.join(filter(None, names))}")

    if sleep_seconds > 0:
        time.sleep(sleep_seconds)

    # 2) Upsert into DB
    conn = _get_db_connection()
    try:
        _ensure_tsdb_player_column(conn)
        inserted = updated_by_id = matched_by_name = 0

        for p in players:
            outcome = _upsert_player(conn, p)
            if outcome == "inserted":
                inserted += 1
            elif outcome == "updated_by_tsdb_id":
                updated_by_id += 1
            elif outcome == "matched_by_name":
                matched_by_name += 1

        conn.commit()
        print(
            "[OK] Player upsert complete -> "
            f"inserted={inserted}, updated_by_tsdb_id={updated_by_id}, matched_by_name={matched_by_name}"
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass

    # 3) Optional CSV snapshot
    if args.write_csv:
        out_path = _write_csv(players, team_id, team_name)
        print(f"[OK] Wrote CSV snapshot: {out_path}")


if __name__ == "__main__":
    main()
