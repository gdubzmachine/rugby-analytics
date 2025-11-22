#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ingest_league_players.py
------------------------

Generic player ingest script for rugby leagues.

For a given TheSportsDB league and season, this script:

- Resolves the league + season in your Postgres DB (using tsdb_league_id).
- Finds all teams for that league+season from league_team_seasons / teams.
- For each team, calls TheSportsDB lookup_all_players.php via tsdb_client.lookup_team_players.
- Upserts players into the `players` table in a *schema-aware* way:
    - It introspects which columns actually exist and only updates/inserts those.
    - It ensures players.tsdb_player_id exists (creating it if needed, with a unique index).
    - It safely handles dodgy date strings (e.g. "0000-00-00") → NULL.
    - It optionally uses positions/preferred_position_id if your schema supports it.
- Upserts rows into player_teams with a NOT NULL season_id, if player_teams table exists.

Usage examples (from project root C:\\rugby-analytics):

    # Using TSDB numeric league id directly (e.g. 4446 for URC)
    python -m scripts.ingest_league_players --tsdb-league 4446 --write-csv -v

    # Using a league code defined in scr/ingest/league_catalog.py (e.g. 'urc')
    python -m scripts.ingest_league_players --league-code urc --write-csv -v

    # Explicit season label instead of "current" TSDB season:
    python -m scripts.ingest_league_players --league-code urc --season-label 2025-2026 -v

Run this AFTER:
    - ingest_rugby_leagues_catalog.py
    - ingest_rugby_seasons.py
    - ingest_league_teams_from_events.py  (so teams + league_team_seasons are populated)
"""

from __future__ import annotations

import os
import sys
import csv
import re
import datetime as _dt
from typing import List, Dict, Any, Optional, Tuple, Set

# ---------------------------------------------------------------------------
# Make sure scr/ is importable
# ---------------------------------------------------------------------------

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# TheSportsDB client (shared)
try:
    from scr.ingest.tsdb_client import (
        lookup_team_players,
        get_current_season_label,
    )
except Exception as exc:  # pragma: no cover
    print(f"[ERROR] Failed to import scr.ingest.tsdb_client: {exc}", file=sys.stderr)
    sys.exit(1)

# Optional league catalog (for --league-code)
try:
    from scr.ingest.league_catalog import RUGBY_LEAGUES  # type: ignore
except Exception:
    RUGBY_LEAGUES = []  # type: ignore

# DB
try:
    import psycopg2
    from psycopg2.extras import DictCursor
except Exception as exc:  # pragma: no cover
    print("Missing psycopg2. Install: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)

# Optional db helper
try:
    from db.connection import get_db_connection  # type: ignore
except Exception:
    get_db_connection = None  # type: ignore


# ---------------------------------------------------------------------------
# DB connection helper
# ---------------------------------------------------------------------------

def _get_conn():
    """
    Prefer your db.connection.get_db_connection(), otherwise use DATABASE_URL.
    """
    if get_db_connection is not None:
        return get_db_connection()

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "DATABASE_URL is not set and db.connection.get_db_connection() "
            "is not available."
        )
    return psycopg2.connect(dsn)


# ---------------------------------------------------------------------------
# League / season resolution
# ---------------------------------------------------------------------------

def _resolve_tsdb_league_id(
    tsdb_league_arg: Optional[str],
    league_code: Optional[str],
) -> str:
    """
    Resolve TSDB league id from either --tsdb-league or --league-code.
    """
    if tsdb_league_arg:
        return str(tsdb_league_arg)

    if league_code:
        code = league_code.lower()
        for lg in RUGBY_LEAGUES or []:
            if str(lg.get("code", "")).lower() == code:
                return str(lg["tsdb_league_id"])
        raise SystemExit(
            f"Unknown league code={league_code!r}. "
            "Make sure it's defined in scr/ingest/league_catalog.RUGBY_LEAGUES."
        )

    raise SystemExit("You must provide either --tsdb-league or --league-code.")


def _resolve_league_and_season(
    cur,
    tsdb_league_id: str,
    explicit_season_label: Optional[str] = None,
    verbose: bool = False,
) -> Tuple[int, int, str]:
    """
    Resolve:
      - league_id in DB for given tsdb_league_id
      - season_id in DB for current (or explicit) TSDB season label

    Returns: (league_id, season_id, season_label_db)
    """

    # 1) league_id
    cur.execute(
        """
        SELECT league_id, name
        FROM leagues
        WHERE tsdb_league_id = %s
        """,
        (tsdb_league_id,),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(
            f"No league found in DB with tsdb_league_id={tsdb_league_id}. "
            "You need to insert leagues & seasons first."
        )
    league_id = row["league_id"]
    league_name = row["name"]

    if verbose:
        print(f"[INFO] DB league_id={league_id} for TSDB league {tsdb_league_id} ({league_name})")

    # 2) season label (TSDB)
    if explicit_season_label:
        season_label = explicit_season_label.strip()
        if verbose:
            print(f"[INFO] Using explicit season label: {season_label!r}")
    else:
        season_label = get_current_season_label(tsdb_league_id, verbose=verbose)
        if not season_label:
            raise RuntimeError(
                f"Could not determine current TSDB season for league {tsdb_league_id}"
            )
        if verbose:
            print(f"[INFO] Current TSDB season label from TSDB: {season_label!r}")

    # 3) find matching season by tsdb_season_key / label
    cur.execute(
        """
        SELECT season_id, year, label
        FROM seasons
        WHERE league_id = %s
          AND (tsdb_season_key = %s OR label = %s)
        ORDER BY tsdb_season_key IS NULL, year DESC
        LIMIT 1
        """,
        (league_id, season_label, season_label),
    )
    srow = cur.fetchone()

    if not srow:
        # Fallback: latest by year
        if verbose:
            print(
                "[WARN] No season row with tsdb_season_key/label = "
                f"{season_label!r}; falling back to latest by year"
            )
        cur.execute(
            """
            SELECT season_id, year, label
            FROM seasons
            WHERE league_id = %s
            ORDER BY year DESC
            LIMIT 1
            """,
            (league_id,),
        )
        srow = cur.fetchone()
        if not srow:
            raise RuntimeError(
                f"No seasons found in DB for league_id={league_id}. "
                "Ingest matches/seasons first."
            )

    season_id = srow["season_id"]
    db_season_label = srow["label"]

    if verbose:
        print(
            f"[INFO] Using season_id={season_id} (DB label={db_season_label!r}) "
            f"for league_id={league_id}"
        )

    return league_id, season_id, db_season_label


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _split_name(name: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not name:
        return None, None
    parts = name.split()
    if len(parts) == 1:
        return parts[0], None
    first_name = " ".join(parts[:-1])
    last_name = parts[-1]
    return first_name, last_name


def _clean_date(value: Optional[str]) -> Optional[_dt.date]:
    """
    Make TSDB dateBorn safe for a DATE column.

    - Returns None if empty or obviously invalid (e.g. '0000-00-00').
    - Otherwise tries to parse with fromisoformat; on failure → None.
    """
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    # Common bogus values from some APIs
    if s.startswith("0000") or "0000-00-00" in s:
        return None
    # Keep only first 10 chars (YYYY-MM-DD) if there's a time part
    s = s[:10]
    try:
        return _dt.date.fromisoformat(s)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Position resolution (borrowed from ingest_urc_players)
# ---------------------------------------------------------------------------

_POSITIONS_HAS_TSDB_COLUMN: Optional[bool] = None


def _positions_has_tsdb_column(cur) -> bool:
    """
    Check once whether positions.tsdb_position_text exists.
    Avoids executing invalid SQL that would abort the transaction.
    """
    global _POSITIONS_HAS_TSDB_COLUMN
    if _POSITIONS_HAS_TSDB_COLUMN is not None:
        return _POSITIONS_HAS_TSDB_COLUMN

    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'positions'
          AND column_name = 'tsdb_position_text'
        LIMIT 1
        """
    )
    _POSITIONS_HAS_TSDB_COLUMN = cur.fetchone() is not None
    return _POSITIONS_HAS_TSDB_COLUMN


def _resolve_position_id(cur, pos_text: Optional[str]) -> Optional[int]:
    """
    Map TSDB free-text position string -> positions.position_id.

    Strategy:
      1. If positions.tsdb_position_text exists, try exact match on that.
      2. Fallback: match on positions.name (case-insensitive).
      3. If nothing found, return None.
    """
    if not pos_text:
        return None

    txt = pos_text.strip()
    if not txt:
        return None

    # 1) Try tsdb_position_text only if the column exists
    if _positions_has_tsdb_column(cur):
        cur.execute(
            """
            SELECT position_id
            FROM positions
            WHERE LOWER(tsdb_position_text) = LOWER(%s)
            LIMIT 1
            """,
            (txt,),
        )
        row = cur.fetchone()
        if row:
            return row["position_id"]

    # 2) Fallback: match on positions.name
    cur.execute(
        """
        SELECT position_id
        FROM positions
        WHERE LOWER(name) = LOWER(%s)
        LIMIT 1
        """,
        (txt,),
    )
    row = cur.fetchone()
    if row:
        return row["position_id"]

    # No match
    return None


# ---------------------------------------------------------------------------
# Players schema introspection & tsdb_player_id column
# ---------------------------------------------------------------------------

_PLAYER_COLUMNS: Optional[Set[str]] = None


def _get_player_columns(cur) -> Set[str]:
    """
    Return the set of column names on the players table.
    """
    global _PLAYER_COLUMNS
    if _PLAYER_COLUMNS is not None:
        return _PLAYER_COLUMNS

    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'players'
        """
    )
    _PLAYER_COLUMNS = {row[0] for row in cur.fetchall()}
    return _PLAYER_COLUMNS


def _ensure_tsdb_player_column(cur, verbose: bool = False) -> None:
    """
    Ensure players.tsdb_player_id exists and has a unique index,
    like in fetch_team_players_basic.py.
    """
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'players'
          AND column_name = 'tsdb_player_id'
        LIMIT 1
        """
    )
    if cur.fetchone():
        return

    if verbose:
        print("[INFO] Adding players.tsdb_player_id BIGINT column + unique index")

    cur.execute("ALTER TABLE players ADD COLUMN IF NOT EXISTS tsdb_player_id BIGINT;")
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


# ---------------------------------------------------------------------------
# Upsert helpers
# ---------------------------------------------------------------------------

def _upsert_player(
    cur,
    pdata: Dict[str, Any],
    verbose: bool = False,
) -> int:
    """
    Insert/update players table via tsdb_player_id, but:

    - Introspects which columns exist on players.
    - Only touches those columns.
    - Safely handles invalid dates.

    Returns: player_id (or 0 if we skip).
    """
    cols = _get_player_columns(cur)

    tsdb_player_id = pdata.get("tsdb_player_id")
    if not tsdb_player_id:
        if verbose:
            print("[SKIP] Player missing tsdb_player_id", file=sys.stderr)
        return 0

    full_name = pdata.get("full_name") or ""
    first_name = pdata.get("first_name")
    last_name = pdata.get("last_name")
    nationality = pdata.get("nationality")
    raw_dob = pdata.get("date_of_birth")
    dob = _clean_date(raw_dob) if "date_of_birth" in cols else None

    pos_text = pdata.get("tsdb_position_text")
    position_id = None
    if "preferred_position_id" in cols:
        position_id = _resolve_position_id(cur, pos_text)

    # 1) Find existing by tsdb_player_id
    cur.execute(
        "SELECT player_id FROM players WHERE tsdb_player_id = %s",
        (tsdb_player_id,),
    )
    row = cur.fetchone()

    # ---------------------------
    # UPDATE path
    # ---------------------------
    if row:
        player_id = row[0] if not isinstance(row, dict) else row["player_id"]
        if verbose:
            print(f"  [UPDATE] player_id={player_id}: {full_name}")

        set_fragments = []
        params: List[Any] = []

        # mandatory-ish
        if "full_name" in cols:
            set_fragments.append("full_name = %s")
            params.append(full_name)

        if "first_name" in cols:
            set_fragments.append("first_name = %s")
            params.append(first_name)

        if "last_name" in cols:
            set_fragments.append("last_name = %s")
            params.append(last_name)

        if "nationality" in cols:
            set_fragments.append("nationality = %s")
            params.append(nationality)

        if "date_of_birth" in cols:
            set_fragments.append("date_of_birth = %s")
            params.append(dob)

        if "preferred_position_id" in cols:
            set_fragments.append("preferred_position_id = %s")
            params.append(position_id)

        if "tsdb_position_text" in cols:
            set_fragments.append("tsdb_position_text = %s")
            params.append(pos_text)

        if "tsdb_player_id" in cols:
            set_fragments.append("tsdb_player_id = %s")
            params.append(tsdb_player_id)

        if "updated_at" in cols:
            set_fragments.append("updated_at = NOW()")

        if not set_fragments:
            # nothing to update
            return player_id

        sql = f"UPDATE players SET {', '.join(set_fragments)} WHERE player_id = %s"
        params.append(player_id)
        cur.execute(sql, tuple(params))
        return player_id

    # ---------------------------
    # INSERT path
    # ---------------------------
    if verbose:
        print(f"  [INSERT] {full_name} (tsdb_player_id={tsdb_player_id})")

    insert_cols: List[str] = []
    values: List[str] = []
    params2: List[Any] = []

    if "full_name" in cols:
        insert_cols.append("full_name")
        values.append("%s")
        params2.append(full_name)

    if "first_name" in cols:
        insert_cols.append("first_name")
        values.append("%s")
        params2.append(first_name)

    if "last_name" in cols:
        insert_cols.append("last_name")
        values.append("%s")
        params2.append(last_name)

    if "nationality" in cols:
        insert_cols.append("nationality")
        values.append("%s")
        params2.append(nationality)

    if "date_of_birth" in cols:
        insert_cols.append("date_of_birth")
        values.append("%s")
        params2.append(dob)

    if "tsdb_player_id" in cols:
        insert_cols.append("tsdb_player_id")
        values.append("%s")
        params2.append(tsdb_player_id)

    if "preferred_position_id" in cols:
        insert_cols.append("preferred_position_id")
        values.append("%s")
        params2.append(position_id)

    if "tsdb_position_text" in cols:
        insert_cols.append("tsdb_position_text")
        values.append("%s")
        params2.append(pos_text)

    if "created_at" in cols:
        insert_cols.append("created_at")
        values.append("NOW()")

    if "updated_at" in cols:
        insert_cols.append("updated_at")
        values.append("NOW()")

    if not insert_cols:
        raise RuntimeError("No insertable columns detected for players table.")

    sql = f"""
        INSERT INTO players ({', '.join(insert_cols)})
        VALUES ({', '.join(values)})
        RETURNING player_id
    """
    cur.execute(sql, tuple(params2))
    row = cur.fetchone()
    player_id = row[0] if not isinstance(row, dict) else row["player_id"]
    return player_id


def _upsert_player_team(
    cur,
    player_id: int,
    team_id: int,
    season_id: int,
) -> None:
    """
    Insert/Update into player_teams, if the table exists.
    """
    if not player_id:
        return

    # Check if player_teams table exists
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'player_teams'
        LIMIT 1
        """
    )
    if not cur.fetchone():
        # Table not present; silently skip
        return

    cur.execute(
        """
        INSERT INTO player_teams (
            player_id,
            team_id,
            season_id,
            created_at,
            updated_at
        ) VALUES (
            %s, %s, %s, NOW(), NOW()
        )
        ON CONFLICT (player_id, team_id, season_id)
        DO UPDATE SET updated_at = EXCLUDED.updated_at
        """,
        (player_id, team_id, season_id),
    )


def _write_csv(
    players: List[Dict[str, Any]],
    tsdb_league_id: str,
    season_label_db: str,
) -> str:
    """
    Write a CSV snapshot of players for debugging / inspection.

    Output: ./data/players_league_<tsdb_league_id>_<season>.csv
    """
    out_dir = os.path.join(os.getcwd(), "data")
    os.makedirs(out_dir, exist_ok=True)

    safe_season = re.sub(r"[^A-Za-z0-9]+", "_", season_label_db).strip("_") or "unknown"
    fname = f"players_league_{tsdb_league_id}_{safe_season}.csv"
    path = os.path.join(out_dir, fname)

    cols = [
        "tsdb_player_id",
        "full_name",
        "first_name",
        "last_name",
        "nationality",
        "date_of_birth",
        "tsdb_position_text",
        "team_name",
    ]

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for p in players:
            w.writerow(p)

    return path


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Ingest players for a rugby league (generic, TSDB-based)."
    )
    parser.add_argument(
        "--tsdb-league",
        help="TSDB league id (e.g. 4446 for URC). "
             "If omitted, you must provide --league-code.",
    )
    parser.add_argument(
        "--league-code",
        help="Optional league code defined in scr/ingest/league_catalog.RUGBY_LEAGUES "
             "(e.g. 'urc', 'six_nations').",
    )
    parser.add_argument(
        "--season-label",
        default=None,
        help="Optional TSDB season label (e.g. '2025-2026'). "
             "If omitted, use TSDB current season.",
    )
    parser.add_argument(
        "--write-csv",
        action="store_true",
        help="Write a CSV snapshot to ./data.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose logging.",
    )

    args = parser.parse_args()
    tsdb_league_id = _resolve_tsdb_league_id(args.tsdb_league, args.league_code)
    verbose = args.verbose

    conn = _get_conn()
    cur = conn.cursor(cursor_factory=DictCursor)

    try:
        # Ensure players.tsdb_player_id exists, and cache players columns
        _ensure_tsdb_player_column(cur, verbose=verbose)
        _get_player_columns(cur)  # warm the cache

        # 1) Resolve league + season
        print("[INFO] Resolving league and season from DB…")
        league_id_db, season_id_db, season_label_db = _resolve_league_and_season(
            cur,
            tsdb_league_id,
            explicit_season_label=args.season_label,
            verbose=verbose,
        )
        print(
            f"[INFO] Using league_id={league_id_db}, season_id={season_id_db} "
            f"(label={season_label_db!r}) for player_teams"
        )

        # 2) Get teams for this league + season
        print("[INFO] Fetching teams for this league/season from DB…")
        cur.execute(
            """
            SELECT t.team_id, t.tsdb_team_id, t.name
            FROM teams t
            JOIN league_team_seasons lts
              ON lts.team_id = t.team_id
            WHERE lts.league_id = %s
              AND lts.season_id = %s
              AND t.tsdb_team_id IS NOT NULL
            ORDER BY t.name
            """,
            (league_id_db, season_id_db),
        )
        teams = cur.fetchall()
        if not teams:
            print(
                "[WARN] No teams found for this league/season. "
                "Did you run ingest_league_teams_from_events.py first?",
                file=sys.stderr,
            )
            return

        print(f"[INFO] Found {len(teams)} teams for league_id={league_id_db} / season_id={season_id_db}")

        all_players_csv_data: List[Dict[str, Any]] = []
        processed_count = 0

        # 3) Per-team squad ingest
        for trow in teams:
            team_id_db = trow["team_id"]
            tsdb_team_id = trow["tsdb_team_id"]
            team_name = trow["name"]

            print(f"\n[TEAM] {team_name} (tsdb_team_id={tsdb_team_id})")

            squad = lookup_team_players(str(tsdb_team_id), rugby_only=True, verbose=verbose)
            print(f"[INFO] Found {len(squad)} players for {team_name}")

            for p in squad:
                pid = p.get("idPlayer")
                if not pid:
                    continue

                full_name = p.get("strPlayer") or ""
                first_name, last_name = _split_name(full_name)

                pdata: Dict[str, Any] = {
                    "tsdb_player_id": pid,
                    "full_name": full_name,
                    "first_name": first_name,
                    "last_name": last_name,
                    "nationality": p.get("strNationality"),
                    "date_of_birth": p.get("dateBorn"),
                    "tsdb_position_text": p.get("strPosition"),
                    "team_name": team_name,
                }

                # Upsert player + relation
                player_id = _upsert_player(cur, pdata, verbose=verbose)
                _upsert_player_team(cur, player_id, team_id_db, season_id_db)

                # For CSV snapshot
                all_players_csv_data.append(pdata)
                processed_count += 1

        conn.commit()
        print(f"\n[DONE] League player ingest complete. Processed {processed_count} players.")

        if args.write_csv:
            path = _write_csv(all_players_csv_data, tsdb_league_id, season_label_db)
            print(f"[OK] Wrote CSV: {path}")

    except Exception as exc:
        conn.rollback()
        print(f"[ERROR] Ingestion failed, rolled back transaction: {exc}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
