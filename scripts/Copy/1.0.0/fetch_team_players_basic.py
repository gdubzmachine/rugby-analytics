#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
fetch_team_players_basic.py
---------------------------

Given a TheSportsDB team ID, fetch that team's squad list and upsert
basic player info into your `players` table.

Step 1 in your player pipeline:
- Just get: player ID, name, nationality, date of birth, position string.
- Later you can add a separate script to enrich per-player details.

What it does
------------
1. Calls v1 /lookup_all_players.php?id={team_id}.
2. Filters to rugby players (strSport starts with 'Rugby' / 'rugby').
3. Ensures players.tsdb_player_id exists (BIGINT / TEXT-like).
4. Upserts players into your DB:
   - update by tsdb_player_id if it exists
   - else attach tsdb_player_id to an existing row if full_name matches
   - else insert a new row
5. Optionally writes a CSV snapshot to ./data.

This version is **standalone**:
- No dependency on scr/ingest/tsdb_client
- Uses requests directly
"""

import os
import re
import sys
import csv
from typing import Any, Dict, List, Optional, Tuple

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
    print("Missing dependency: psycopg2-binary (pip install psycopg2-binary)", file=sys.stderr)
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
        # dotenv is optional
        pass


def _get_tsdb_api_key() -> str:
    key = os.getenv("THESPORTSDB_API_KEY")
    if not key:
        raise RuntimeError(
            "THESPORTSDB_API_KEY not set. Add it to your .env or environment."
        )
    return key


def _tsdb_base(api_key: str) -> str:
    return f"https://www.thesportsdb.com/api/v1/json/{api_key}"


# --- Small helpers -----------------------------------------------------------
def _clean(x: Optional[str]) -> Optional[str]:
    if x is None:
        return None
    x = x.strip()
    return x or None


def _split_name(full_name: str) -> Tuple[Optional[str], Optional[str]]:
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


# --- DB helpers --------------------------------------------------------------
def _get_conn():
    """
    Get a psycopg2 connection, preferring db.connection.get_db_connection().
    """
    if get_db_connection is not None:
        return get_db_connection()  # type: ignore

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "No db.connection.get_db_connection and no DATABASE_URL set. "
            "Either define db/connection.py with get_db_connection(), or set DATABASE_URL."
        )
    return psycopg2.connect(dsn)


def _ensure_tsdb_player_column(cur) -> None:
    """
    Ensure players.tsdb_player_id exists and is indexed.
    """
    cur.execute(
        """
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name   = 'players'
           AND column_name  = 'tsdb_player_id'
        LIMIT 1
        """
    )
    if cur.fetchone():
        return

    # Add the column
    cur.execute("ALTER TABLE players ADD COLUMN IF NOT EXISTS tsdb_player_id BIGINT")

    # Add a unique index if not present
    cur.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                  FROM pg_indexes
                 WHERE schemaname = 'public'
                   AND tablename  = 'players'
                   AND indexname  = 'players_tsdb_player_id_uk'
            ) THEN
                CREATE UNIQUE INDEX players_tsdb_player_id_uk
                    ON players(tsdb_player_id);
            END IF;
        END;
        $$;
        """
    )


def _upsert_player(cur, p: Dict[str, Any], verbose: bool = False) -> str:
    """
    Upsert a player into `players`.

    Strategy:
      1) UPDATE by tsdb_player_id if present.
      2) Else UPDATE (attach tsdb id) where LOWER(full_name) matches and tsdb_player_id IS NULL.
      3) Else INSERT a new row.

    Returns:
      'updated_by_tsdb_id' | 'matched_by_name' | 'inserted'
    """
    tsdb_player_id = _clean(p.get("idPlayer"))
    if not tsdb_player_id:
        if verbose:
            print("[SKIP] Player missing idPlayer", file=sys.stderr)
        return "skipped"

    full_name = _clean(p.get("strPlayer")) or ""
    nationality = _clean(p.get("strNationality"))
    dob = _clean(p.get("dateBorn"))
    first_name, last_name = _split_name(full_name)

    # 1) By tsdb_player_id
    cur.execute(
        """
        UPDATE players
           SET full_name   = %s,
               first_name  = COALESCE(%s, first_name),
               last_name   = COALESCE(%s, last_name),
               date_of_birth = COALESCE(%s, date_of_birth),
               nationality = COALESCE(%s, nationality),
               updated_at  = NOW()
         WHERE tsdb_player_id = %s
        """,
        (full_name, first_name, last_name, dob, nationality, tsdb_player_id),
    )
    if cur.rowcount > 0:
        if verbose:
            print(f"  [DB] updated existing player by tsdb_player_id={tsdb_player_id}")
        return "updated_by_tsdb_id"

    # 2) Attach tsdb id to existing player by name
    cur.execute(
        """
        UPDATE players
           SET tsdb_player_id = %s,
               first_name     = COALESCE(%s, first_name),
               last_name      = COALESCE(%s, last_name),
               date_of_birth  = COALESCE(%s, date_of_birth),
               nationality    = COALESCE(%s, nationality),
               updated_at     = NOW()
         WHERE LOWER(full_name) = LOWER(%s)
           AND tsdb_player_id IS NULL
        """,
        (tsdb_player_id, first_name, last_name, dob, nationality, full_name),
    )
    if cur.rowcount > 0:
        if verbose:
            print(f"  [DB] matched existing player by name='{full_name}', set tsdb_player_id={tsdb_player_id}")
        return "matched_by_name"

    # 3) Insert new
    if verbose:
        print(f"  [DB] insert new player '{full_name}' (tsdb_player_id={tsdb_player_id})")
    cur.execute(
        """
        INSERT INTO players (
            full_name,
            first_name,
            last_name,
            date_of_birth,
            nationality,
            tsdb_player_id,
            created_at,
            updated_at
        ) VALUES (
            %s, %s, %s, %s,
            %s,
            %s,
            NOW(),
            NOW()
        )
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

    if safe_name:
        fname = f"team_{team_id}_{safe_name}_players.csv"
    else:
        fname = f"team_{team_id}_players.csv"

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
            row = {c: p.get(c) for c in cols}
            writer.writerow(row)

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
        default=os.getenv("THESPORTSDB_API_KEY"),
        help="TheSportsDB API key (default: THESPORTSDB_API_KEY env).",
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
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose logging.",
    )
    args = parser.parse_args()

    team_id = str(args.team_id)
    api_key = args.api_key or _get_tsdb_api_key()
    team_name = args.team_name
    sleep_seconds = max(args.sleep_seconds, 0.0)
    verbose = args.verbose

    if verbose:
        print(f"[INFO] Using TheSportsDB key='{api_key}'")
        print(f"[INFO] Team idTeam={team_id}  team_name={team_name!r}")

    # 1) Fetch players from TheSportsDB
    url = f"{_tsdb_base(api_key)}/lookup_all_players.php"
    resp = requests.get(url, params={"id": team_id}, timeout=45)
    resp.raise_for_status()
    data = resp.json() or {}
    players = data.get("player") or []

    # Filter to rugby
    rugby_players: List[Dict[str, Any]] = []
    for p in players:
        sport = (p.get("strSport") or "").lower()
        if sport and not sport.startswith("rugby"):
            continue
        rugby_players.append(p)

    if not rugby_players:
        raise SystemExit(f"No rugby players returned for team_id={team_id}. Aborting.")

    if verbose:
        names = [p.get("strPlayer") for p in rugby_players]
        print(f"[INFO] Retrieved {len(rugby_players)} rugby players: {', '.join(filter(None, names))}")

    # 2) Upsert into DB
    conn = _get_conn()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=DictCursor)

    try:
        _ensure_tsdb_player_column(cur)
        inserted = updated_by_id = matched_by_name = 0

        for p in rugby_players:
            outcome = _upsert_player(cur, p, verbose=verbose)
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
    except Exception as exc:
        conn.rollback()
        print(f"[ERROR] DB upsert failed; rolled back: {exc}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()

    # 3) Optional CSV snapshot
    if args.write_csv:
        out_path = _write_csv(rugby_players, team_id, team_name)
        print(f"[OK] Wrote CSV snapshot: {out_path}")


if __name__ == "__main__":
    main()
