#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ingest_urc_players.py
---------------------

Fetch all players for all URC teams and ingest them into Postgres.

Design:
- ONE TSDB call per team: lookup_all_players.php (via lookup_team_players)
- No per-player lookupplayer.php → avoids 429 rate limits.
- Populate:
    players.tsdb_player_id
    players.full_name
    players.first_name
    players.last_name
    players.nationality
    players.date_of_birth
    players.tsdb_position_text
    players.preferred_position_id (best-effort, via positions table)
- Populate player_teams with a real, NOT NULL season_id.

Usage:
    python .\scripts\ingest_urc_players.py --write-csv -v
"""

import os
import sys
import csv
from typing import List, Dict, Any, Optional, Tuple

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
except Exception as exc:
    print(f"[ERROR] Failed to import scr.ingest.tsdb_client: {exc}", file=sys.stderr)
    sys.exit(1)

# DB
try:
    import psycopg2
    from psycopg2.extras import DictCursor
except Exception:
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
# League + season resolution
# ---------------------------------------------------------------------------

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

    Returns: (league_id, season_id, season_label)
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
            print(f"[INFO] Current TSDB season label: {season_label!r}")

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
    return parts[0], " ".join(parts[1:])


def _write_csv(players: List[Dict[str, Any]], season_label: str) -> str:
    os.makedirs("data", exist_ok=True)
    safe_season = season_label.replace("/", "-")
    path = os.path.join("data", f"urc_players_{safe_season}.csv")

    cols = [
        "tsdb_player_id",
        "full_name",
        "first_name",
        "last_name",
        "tsdb_position_text",
        "preferred_position_id",
        "nationality",
        "date_of_birth",
        "team_name",
    ]

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for p in players:
            w.writerow(p)

    return path


# ---------------------------------------------------------------------------
# Position resolution (with column existence check)
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
# Upsert helpers
# ---------------------------------------------------------------------------

def _upsert_player(
    cur,
    pdata: Dict[str, Any],
    verbose: bool = False,
) -> int:
    """
    Insert/update players table via tsdb_player_id.
    Returns player_id.
    """
    pos_text = pdata.get("tsdb_position_text")
    position_id = _resolve_position_id(cur, pos_text)

    cur.execute(
        "SELECT player_id FROM players WHERE tsdb_player_id = %s",
        (pdata["tsdb_player_id"],),
    )
    row = cur.fetchone()

    if row:
        player_id = row["player_id"]
        if verbose:
            print(f"  [UPDATE] player_id={player_id} : {pdata['full_name']}")
        cur.execute(
            """
            UPDATE players SET
              full_name = %s,
              first_name = %s,
              last_name = %s,
              nationality = %s,
              date_of_birth = %s,
              preferred_position_id = %s,
              tsdb_player_id = %s,
              tsdb_position_text = %s,
              updated_at = NOW()
            WHERE player_id = %s
            """,
            (
                pdata["full_name"],
                pdata["first_name"],
                pdata["last_name"],
                pdata["nationality"],
                pdata["date_of_birth"],
                position_id,
                pdata["tsdb_player_id"],
                pos_text,
                player_id,
            ),
        )
        return player_id

    if verbose:
        print(f"  [INSERT] {pdata['full_name']}")

    cur.execute(
        """
        INSERT INTO players (
            full_name,
            first_name,
            last_name,
            nationality,
            date_of_birth,
            tsdb_player_id,
            preferred_position_id,
            tsdb_position_text,
            created_at,
            updated_at
        ) VALUES (
            %s,%s,%s,%s,%s,
            %s,
            %s,
            %s,
            NOW(), NOW()
        )
        RETURNING player_id
        """,
        (
            pdata["full_name"],
            pdata["first_name"],
            pdata["last_name"],
            pdata["nationality"],
            pdata["date_of_birth"],
            pdata["tsdb_player_id"],
            position_id,
            pos_text,
        ),
    )
    return cur.fetchone()[0]


def _upsert_player_team(
    cur,
    player_id: int,
    team_id: int,
    season_id: int,
) -> None:
    """
    Insert/Update into player_teams.
    season_id is NOT NULL per your schema.
    """
    cur.execute(
        """
        INSERT INTO player_teams (
            player_id,
            team_id,
            season_id,
            created_at,
            updated_at
        )
        VALUES (%s,%s,%s,NOW(),NOW())
        ON CONFLICT (player_id, team_id, season_id)
        DO UPDATE SET updated_at = EXCLUDED.updated_at
        """,
        (player_id, team_id, season_id),
    )


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Ingest URC players into DB")
    parser.add_argument(
        "--league-id",
        default="4446",
        help="TSDB league id (default: 4446 for URC)",
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
        help="Write CSV snapshot to ./data",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose logging",
    )

    args = parser.parse_args()
    tsdb_league_id = str(args.league_id)
    verbose = args.verbose

    conn = _get_conn()
    cur = conn.cursor(cursor_factory=DictCursor)

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

    # 2) Get URC teams (we assume URC teams have tsdb_team_id set)
    print("[INFO] Fetching URC team list from DB…")
    cur.execute(
        """
        SELECT team_id, tsdb_team_id, name
        FROM teams
        WHERE tsdb_team_id IS NOT NULL
        ORDER BY name
        """
    )
    teams = cur.fetchall()
    print(f"[INFO] Found {len(teams)} URC teams in DB")

    all_players_csv_data: List[Dict[str, Any]] = []
    processed_count = 0

    try:
        for trow in teams:
            team_id_db = trow["team_id"]
            tsdb_team_id = trow["tsdb_team_id"]
            team_name = trow["name"]

            print(f"\n[TEAM] {team_name} (tsdb_team_id={tsdb_team_id})")

            # 3) TSDB: squad list (ONE call per team)
            squad = lookup_team_players(str(tsdb_team_id), rugby_only=True, verbose=verbose)
            print(f"[INFO] Found {len(squad)} players for {team_name}")

            for p in squad:
                pid = p.get("idPlayer")
                if not pid:
                    continue

                full_name = p.get("strPlayer")
                first_name, last_name = _split_name(full_name)

                pdata: Dict[str, Any] = {
                    "tsdb_player_id": pid,
                    "full_name": full_name,
                    "first_name": first_name,
                    "last_name": last_name,
                    "tsdb_position_text": p.get("strPosition"),
                    "nationality": p.get("strNationality"),
                    "date_of_birth": p.get("dateBorn"),
                    "team_name": team_name,
                }

                # 4) Upsert player row (including preferred_position_id + tsdb_position_text)
                player_id = _upsert_player(cur, pdata, verbose=verbose)

                # 5) Upsert relationship with this URC season
                _upsert_player_team(cur, player_id, team_id_db, season_id_db)

                # For CSV
                all_players_csv_data.append(
                    {
                        **pdata,
                        "preferred_position_id": None,  # informational; DB has the real mapping
                    }
                )
                processed_count += 1

        conn.commit()
        print(f"\n[DONE] URC player ingest complete. Processed {processed_count} players.")

        if args.write_csv:
            path = _write_csv(all_players_csv_data, season_label_db)
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
