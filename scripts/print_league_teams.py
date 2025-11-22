#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
print_league_teams.py
---------------------

List teams that appear in matches for a given TSDB league, optionally
restricted to a specific season label.

This is a helper so you can easily see team_id + team name for use with
other scripts such as:

  - print_team_fixtures_results.py
  - print_head_to_head.py

Usage examples (from C:\\rugby-analytics):

  # All teams that have ever appeared in URC matches (all seasons)
  python -m scripts.print_league_teams --tsdb-league 4446 -v

  # Teams in URC for a specific season
  python -m scripts.print_league_teams --tsdb-league 4446 --season-label 2023-2024 -v

  # Teams in URC latest season (by seasons.year)
  python -m scripts.print_league_teams --tsdb-league 4446 --latest-season -v
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
) -> Optional[str]:
    """
    If season_label is provided, return it.
    If latest_season=True, resolve to latest season by year.
    If both are False/None, return None (meaning "all seasons").
    """
    if season_label:
        return season_label

    if not latest_season:
        # No specific season -> all seasons
        return None

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
        raise RuntimeError(f"No seasons found for tsdb_league_id={tsdb_league_id}")
    if verbose:
        print(f"[INFO] Using latest season label='{row['label']}' (year={row['year']})")
    return row["label"]


def _load_league_teams(
    cur,
    tsdb_league_id: str,
    season_label: Optional[str],
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    """
    Find distinct teams that appear in matches for the given league,
    optionally filtered to a single season.
    """
    # We build the query around matches + seasons + leagues + teams
    # and then distinct on team_id & team name.
    cols_teams = _get_table_columns(cur, "teams")
    has_tsdb_team_id = "tsdb_team_id" in cols_teams

    base_sql = """
        SELECT DISTINCT
            t.team_id,
            t.name AS team_name,
            s.label AS season_label
    """
    if has_tsdb_team_id:
        base_sql += ", t.tsdb_team_id"

    base_sql += """
        FROM matches m
        JOIN leagues l
          ON l.league_id = m.league_id
        JOIN seasons s
          ON s.season_id = m.season_id
        JOIN teams t
          ON t.team_id = m.home_team_id
             OR t.team_id = m.away_team_id
        WHERE l.tsdb_league_id = %s
    """

    params: List[Any] = [tsdb_league_id]

    if season_label:
        base_sql += " AND s.label = %s"
        params.append(season_label)

    base_sql += " ORDER BY t.name ASC"

    cur.execute(base_sql, tuple(params))
    rows = cur.fetchall()

    if verbose:
        info = season_label if season_label else "ALL seasons"
        print(
            f"[INFO] Loaded {len(rows)} distinct teams for tsdb_league_id={tsdb_league_id}, {info}"
        )

    # Wrap as dict list
    teams: List[Dict[str, Any]] = []
    for r in rows:
        d: Dict[str, Any] = {
            "team_id": r["team_id"],
            "team_name": r["team_name"],
            "season_label": r["season_label"],
        }
        if has_tsdb_team_id:
            d["tsdb_team_id"] = r["tsdb_team_id"]
        teams.append(d)
    return teams


def _print_teams(
    teams: List[Dict[str, Any]],
    tsdb_league_id: str,
    season_label: Optional[str],
) -> None:
    if not teams:
        print("No teams found for that league/season.")
        return

    seasons = {t["season_label"] for t in teams}
    if season_label:
        season_info = season_label
    elif len(seasons) == 1:
        season_info = next(iter(seasons))
    else:
        season_info = f"{len(seasons)} seasons"

    title = f"Teams in league {tsdb_league_id} ({season_info})"
    print("\n" + title)
    print("=" * len(title))

    # Check if tsdb_team_id present
    has_tsdb_team_id = "tsdb_team_id" in teams[0]

    if has_tsdb_team_id:
        headers = ["team_id", "tsdb_team_id", "team_name", "season_label"]
        widths = [8, 12, 30, 12]
    else:
        headers = ["team_id", "team_name", "season_label"]
        widths = [8, 30, 12]

    def fmt_row(values: List[Any]) -> str:
        parts = []
        for val, w in zip(values, widths):
            parts.append(str(val)[:w].ljust(w))
        return " ".join(parts)

    print(fmt_row(headers))
    print("-" * (sum(widths) + len(widths) - 1))

    for t in teams:
        if has_tsdb_team_id:
            vals = [
                t["team_id"],
                t["tsdb_team_id"] if t["tsdb_team_id"] is not None else "",
                t["team_name"],
                t["season_label"],
            ]
        else:
            vals = [
                t["team_id"],
                t["team_name"],
                t["season_label"],
            ]
        print(fmt_row(vals))

    print()  # trailing newline


def main() -> None:
    import argparse

    _load_dotenv_if_available()

    parser = argparse.ArgumentParser(
        description="List teams that appear in matches for a given TSDB league (and optional season)."
    )
    parser.add_argument(
        "--tsdb-league",
        required=True,
        help="TSDB league id (e.g. 4446 for URC).",
    )
    parser.add_argument(
        "--season-label",
        help="Season label (e.g. '2023-2024').",
    )
    parser.add_argument(
        "--latest-season",
        action="store_true",
        help="If set and --season-label is not provided, use latest season (by seasons.year).",
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
        resolved_label = _resolve_season_label(
            cur,
            tsdb_league_id=args.tsdb_league,
            season_label=args.season_label,
            latest_season=args.latest_season,
            verbose=verbose,
        )
        teams = _load_league_teams(cur, args.tsdb_league, resolved_label, verbose=verbose)
        _print_teams(teams, args.tsdb_league, resolved_label)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
