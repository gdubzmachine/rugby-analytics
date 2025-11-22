#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
debug_db_snapshot.py
--------------------

Quick helper to inspect the Render Postgres DB without psql.

It will:
  - Connect via DATABASE_URL (or db.connection.get_db_connection).
  - List public tables.
  - Print row counts for key tables.
  - Show a sample of team_season_stats for the URC league (tsdb_league_id=4446).
"""

import os
import sys
from typing import List, Dict, Any

# ---------------------------------------------------------------------------
# Make project root importable
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def _load_dotenv_if_available() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# DB connection helper
# ---------------------------------------------------------------------------
try:
    from db.connection import get_db_connection  # type: ignore
except Exception:
    get_db_connection = None  # type: ignore

try:
    import psycopg2
    from psycopg2.extras import DictCursor
except ImportError:
    print("Missing dependency: psycopg2-binary (pip install psycopg2-binary)", file=sys.stderr)
    sys.exit(1)


def _get_conn():
    """
    Get DB connection, preferring db.connection.get_db_connection().
    """
    if get_db_connection is not None:
        return get_db_connection()  # type: ignore

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "DATABASE_URL not set and db.connection.get_db_connection() missing. "
            "Set DATABASE_URL in .env or implement db/connection.get_db_connection()."
        )
    return psycopg2.connect(dsn)


def main() -> None:
    _load_dotenv_if_available()

    conn = _get_conn()
    cur = conn.cursor(cursor_factory=DictCursor)

    try:
        print("=== TABLES (public schema) ===")
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
            """
        )
        for row in cur.fetchall():
            print(" -", row["table_name"])
        print()

        def count_table(name: str) -> None:
            cur.execute(f"SELECT COUNT(*) AS c FROM {name}")
            c = cur.fetchone()["c"]
            print(f"{name:24s}: {c}")

        print("=== ROW COUNTS ===")
        for t in [
            "sports",
            "leagues",
            "seasons",
            "teams",
            "venues",
            "matches",
            "league_team_seasons",
            "team_season_stats",
        ]:
            try:
                count_table(t)
            except Exception as e:
                print(f"{t:24s}: ERROR ({e})")
        print()

        # Find URC league_id
        print("=== LEAGUES (tsdb_league_id = 4446) ===")
        cur.execute(
            """
            SELECT league_id, tsdb_league_id, name
            FROM leagues
            WHERE tsdb_league_id = 4446
            """
        )
        leagues = cur.fetchall()
        if not leagues:
            print("No league with tsdb_league_id=4446 found.")
            return

        for row in leagues:
            print(
                f"league_id={row['league_id']} tsdb_league_id={row['tsdb_league_id']} name={row['name']}"
            )

        league_id = leagues[0]["league_id"]
        print(f"\nUsing league_id={league_id} for URC\n")

        print("=== Sample seasons for URC ===")
        cur.execute(
            """
            SELECT season_id, label, year
            FROM seasons
            WHERE league_id = %s
            ORDER BY year
            """,
            (league_id,),
        )
        for row in cur.fetchall():
            print(
                f"season_id={row['season_id']} label={row['label']} year={row['year']}"
            )
        print()

        print("=== Sample team_season_stats for URC (top 10 by competition_points) ===")
        cur.execute(
            """
            SELECT
                tss.league_id,
                tss.season_id,
                tss.team_id,
                tss.games_played,
                tss.wins,
                tss.draws,
                tss.losses,
                tss.points_for,
                tss.points_against,
                tss.points_diff,
                tss.competition_points,
                tm.name AS team_name
            FROM team_season_stats tss
            JOIN teams tm ON tm.team_id = tss.team_id
            WHERE tss.league_id = %s
            ORDER BY tss.season_id DESC, tss.competition_points DESC
            LIMIT 10
            """,
            (league_id,),
        )
        rows = cur.fetchall()
        if not rows:
            print("No rows in team_season_stats for this league.")
        else:
            for r in rows:
                print(
                    f"season_id={r['season_id']} team_id={r['team_id']} "
                    f"{r['team_name']}: GP={r['games_played']} W={r['wins']} D={r['draws']} "
                    f"L={r['losses']} PF={r['points_for']} PA={r['points_against']} "
                    f"PD={r['points_diff']} Pts={r['competition_points']}"
                )

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
