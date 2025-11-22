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

Outputs a text table with:

  - P, W, D, L, PF, PA, PD, Pts
  - And, if the columns exist in team_season_stats:
      * LB (losing_bonus_points)
      * TB (try_bonus_points)

So after adding bonus points in compute_team_season_stats.py, this will show:

  Pos Team    P W D L PF PA PD LB TB Pts


Usage examples (from C:\\rugby-analytics):

  # URC (TSDB league 4446), season 2023-2024
  python -m scripts.print_league_table --tsdb-league 4446 --season-label 2023-2024 -v

  # URC latest season we have (by seasons.year)
  python -m scripts.print_league_table --tsdb-league 4446 --latest-season -v
"""

import os
import sys
from typing import Any, Dict, List, Optional, Set

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


def _get_table_columns(cur, table_name: str) -> Set[str]:
    """
    Return the set of column names for a given table in the public schema.
    """
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
        """,
        (table_name,),
    )
    return {r[0] for r in cur.fetchall()}


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

    This is schema-aware: if team_season_stats has losing_bonus_points or
    try_bonus_points, we include them; otherwise we skip them.
    """
    stats_cols = _get_table_columns(cur, "team_season_stats")
    has_lb = "losing_bonus_points" in stats_cols
    has_tb = "try_bonus_points" in stats_cols

    # Base SELECT columns
    select_cols = [
        "l.name AS league_name",
        "l.tsdb_league_id",
        "s.label AS season_label",
        "t.team_id",
        "t.name AS team_name",
        "stats.games_played",
        "stats.wins",
        "stats.draws",
        "stats.losses",
        "stats.points_for",
        "stats.points_against",
        "stats.points_diff",
        "stats.competition_points",
    ]
    if has_lb:
        select_cols.append("stats.losing_bonus_points")
    if has_tb:
        select_cols.append("stats.try_bonus_points")

    select_sql = ",\n            ".join(select_cols)

    sql = f"""
        SELECT
            {select_sql}
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
        d = dict(r)
        # In case columns are missing, ensure keys exist with defaults
        if has_lb and "losing_bonus_points" not in d:
            d["losing_bonus_points"] = 0
        if has_tb and "try_bonus_points" not in d:
            d["try_bonus_points"] = 0
        table.append(d)

    return table


def _print_table(table: List[Dict[str, Any]]) -> None:
    """
    Print a simple text league table.

    If the rows contain losing_bonus_points / try_bonus_points, we include
    LB / TB columns.
    """
    if not table:
        print("No data to display.")
        return

    league_name = table[0]["league_name"]
    season_label = table[0]["season_label"]
    print(f"\n{league_name} - {season_label}")
    print("=" * (len(league_name) + len(season_label) + 3))

    # Check if bonus columns are present
    has_lb = "losing_bonus_points" in table[0]
    has_tb = "try_bonus_points" in table[0]

    if has_lb or has_tb:
        headers = ["Pos", "Team", "P", "W", "D", "L", "PF", "PA", "PD", "LB", "TB", "Pts"]
        widths = [4, 25, 3, 3, 3, 3, 5, 5, 4, 3, 3, 5]
    else:
        headers = ["Pos", "Team", "P", "W", "D", "L", "PF", "PA", "PD", "Pts"]
        widths = [4, 25, 3, 3, 3, 3, 5, 5, 4, 5]

    def fmt_row(values):
        # Join values according to widths
        parts = []
        for val, w in zip(values, widths):
            parts.append(str(val).rjust(w) if isinstance(val, int) or str(val).isdigit() else str(val)[:w].ljust(w))
        return " ".join(parts)

    print(fmt_row(headers))
    print("-" * (sum(widths) + len(widths) - 1))

    for idx, row in enumerate(table, start=1):
        base_vals = [
            idx,
            row["team_name"],
            row["games_played"],
            row["wins"],
            row["draws"],
            row["losses"],
            row["points_for"],
            row["points_against"],
            row["points_diff"],
        ]

        if has_lb or has_tb:
            lb_val = row.get("losing_bonus_points", 0) if has_lb else 0
            tb_val = row.get("try_bonus_points", 0) if has_tb else 0
            base_vals.extend([lb_val, tb_val, row["competition_points"]])
        else:
            base_vals.append(row["competition_points"])

        print(fmt_row(base_vals))

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
