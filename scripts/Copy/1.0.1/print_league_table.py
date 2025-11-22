#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
print_league_table.py
---------------------

Print a season league table from team_season_stats.

Uses:

  - team_season_stats (from compute_team_season_stats.py)
  - teams
  - leagues (filter by tsdb_league_id)
  - seasons (filter by season label)

Outputs a text table with P, W, D, L, PF, PA, PD, Pts.


Usage examples (from C:\\rugby-analytics):

  # URC (TSDB league 4446), season 2023-2024
  python -m scripts.print_league_table --tsdb-league 4446 --season-label 2023-2024 -v

  # URC latest season we have (by seasons.year)
  python -m scripts.print_league_table --tsdb-league 4446 --latest-season -v
"""

import os
import sys
from typing import Any, Dict, List, Optional

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

try:
    import psycopg2
    from psycopg2.extras import DictCursor
except ImportError:
    print("Missing dependency: psycopg2-binary (pip install psycopg2-binary)", file=sys.stderr)
    sys.exit(1)

# Optional helper
try:
    from db.connection import get_db_connection  # type: ignore
except Exception:
    get_db_connection = None  # type: ignore


def _load_dotenv_if_available() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        pass


def _get_conn():
    if get_db_connection is not None:
        return get_db_connection()  # type: ignore

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "DATABASE_URL not set and db.connection.get_db_connection() missing. "
            "Set DATABASE_URL in .env or create db/connection.get_db_connection()."
        )
    return psycopg2.connect(dsn)


def _resolve_season_label(
    cur,
    tsdb_league_id: str,
    season_label: Optional[str],
    latest_season: bool,
    verbose: bool = False,
) -> str:
    """
    If season_label is provided, just return it.
    If latest_season=True, look up the season with max year for this league.
    """
    if season_label:
        return season_label

    if not latest_season:
        raise ValueError("You must provide --season-label or use --latest-season")

    # Find max season year for this TSDB league
    cur.execute(
        """
        SELECT s.label, s.year
        FROM seasons s
        JOIN leagues l ON l.league_id = s.league_id
        WHERE l.tsdb_league_id = %s
        ORDER BY s.year DESC NULLS LAST
        LIMIT 1
        """,
        (tsdb_league_id,),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"No seasons found in DB for tsdb_league_id={tsdb_league_id}")
    if verbose:
        print(f"[INFO] Using latest season label='{row['label']}' (year={row['year']})")
    return row["label"]


def _load_table(
    cur,
    tsdb_league_id: str,
    season_label: str,
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    """
    Load league table rows for a given TSDB league + season label.
    """
    sql = """
        SELECT
            l.name AS league_name,
            l.tsdb_league_id,
            s.label AS season_label,
            t.team_id,
            t.name AS team_name,
            stats.games_played,
            stats.wins,
            stats.draws,
            stats.losses,
            stats.points_for,
            stats.points_against,
            stats.points_diff,
            stats.competition_points
        FROM team_season_stats stats
        JOIN teams t
          ON t.team_id = stats.team_id
        JOIN leagues l
          ON l.league_id = stats.league_id
        JOIN seasons s
          ON s.season_id = stats.season_id
        WHERE l.tsdb_league_id = %s
          AND s.label = %s
        ORDER BY
          stats.competition_points DESC,
          stats.points_diff DESC,
          stats.points_for DESC,
          t.name ASC
    """
    cur.execute(sql, (tsdb_league_id, season_label))
    rows = cur.fetchall()

    if verbose:
        print(f"[INFO] Loaded {len(rows)} team rows for league {tsdb_league_id}, season '{season_label}'")

    table: List[Dict[str, Any]] = []
    for r in rows:
        table.append(dict(r))
    return table


def _print_table(table: List[Dict[str, Any]]) -> None:
    """
    Print a simple text league table.
    """
    if not table:
        print("No data to display.")
        return

    league_name = table[0]["league_name"]
    season_label = table[0]["season_label"]
    print(f"\n{league_name} - {season_label}")
    print("=" * (len(league_name) + len(season_label) + 3))

    headers = ["Pos", "Team", "P", "W", "D", "L", "PF", "PA", "PD", "Pts"]
    # column widths
    widths = [4, 25, 3, 3, 3, 3, 5, 5, 4, 5]

    def fmt_row(values):
        return (
            f"{str(values[0]).rjust(widths[0])} "
            f"{str(values[1])[:widths[1]].ljust(widths[1])} "
            f"{str(values[2]).rjust(widths[2])} "
            f"{str(values[3]).rjust(widths[3])} "
            f"{str(values[4]).rjust(widths[4])} "
            f"{str(values[5]).rjust(widths[5])} "
            f"{str(values[6]).rjust(widths[6])} "
            f"{str(values[7]).rjust(widths[7])} "
            f"{str(values[8]).rjust(widths[8])} "
            f"{str(values[9]).rjust(widths[9])}"
        )

    print(fmt_row(headers))
    print("-" * (sum(widths) + len(widths) - 1))

    for idx, row in enumerate(table, start=1):
        vals = [
            idx,
            row["team_name"],
            row["games_played"],
            row["wins"],
            row["draws"],
            row["losses"],
            row["points_for"],
            row["points_against"],
            row["points_diff"],
            row["competition_points"],
        ]
        print(fmt_row(vals))

    print()  # trailing newline


def main() -> None:
    import argparse

    _load_dotenv_if_available()

    parser = argparse.ArgumentParser(
        description="Print a season league table from team_season_stats."
    )
    parser.add_argument(
        "--tsdb-league",
        required=True,
        help="TSDB league id (e.g. 4446 for URC).",
    )
    parser.add_argument(
        "--season-label",
        help="Season label (e.g. '2023-2024'). If omitted, use latest season we have.",
    )
    parser.add_argument(
        "--latest-season",
        action="store_true",
        help="If set and --season-label is not given, use latest season (by seasons.year).",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose logging.",
    )

    args = parser.parse_args()
    verbose = args.verbose

    conn = _get_conn()
    cur = conn.cursor(cursor_factory=DictCursor)

    try:
        season_label = _resolve_season_label(
            cur,
            tsdb_league_id=args.tsdb_league,
            season_label=args.season_label,
            latest_season=args.latest_season,
            verbose=verbose,
        )
        table = _load_table(cur, args.tsdb_league, season_label, verbose=verbose)
        _print_table(table)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
