#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
print_team_fixtures_results.py
------------------------------

Print all fixtures/results for a single team in a given league season.

Uses:

  - leagues (filter by tsdb_league_id)
  - seasons (filter by label or latest)
  - teams (select team by id or name)
  - matches (home/away, scores, kickoff_utc)

For each match, prints:

  [Date/time]  H/A  Opponent        Score   Result

Where "Result" is from the point of view of the selected team:
  - W = win
  - D = draw
  - L = loss
  - ? = no score yet (upcoming / unknown)

Usage examples (from C:\\rugby-analytics):

  # URC (4446), latest season, team by name
  python -m scripts.print_team_fixtures_results ^
    --tsdb-league 4446 ^
    --team-name "Bulls" ^
    --latest-season -v

  # URC (4446), specific season, team by id
  python -m scripts.print_team_fixtures_results ^
    --tsdb-league 4446 ^
    --season-label 2023-2024 ^
    --team-id 13 -v
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


# ---------------------------------------------------------------------------
# Season + team resolution
# ---------------------------------------------------------------------------

def _resolve_season_label(
    cur,
    tsdb_league_id: str,
    season_label: Optional[str],
    latest_season: bool,
    verbose: bool = False,
) -> str:
    """
    If season_label provided, just return it.
    If latest_season=True, pick the season with max year for this league.
    """
    if season_label:
        return season_label

    if not latest_season:
        raise ValueError("You must provide --season-label or use --latest-season")

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


def _resolve_team_id_by_name(
    cur,
    team_name: str,
    verbose: bool = False,
) -> int:
    """
    Find a team_id by fuzzy name (ILIKE).
    If multiple matches, picks the first ordered by name.
    """
    cur.execute(
        """
        SELECT team_id, name
        FROM teams
        WHERE name ILIKE %s
        ORDER BY name ASC
        """,
        (f"%{team_name}%",),
    )
    rows = cur.fetchall()
    if not rows:
        raise RuntimeError(f"No teams found matching name '{team_name}'")

    if verbose:
        if len(rows) > 1:
            print("[WARN] Multiple teams match that name, using the first:")
            for r in rows[:5]:
                print(f"       - team_id={r['team_id']} name='{r['name']}'")
        print(f"[INFO] Using team_id={rows[0]['team_id']} name='{rows[0]['name']}'")

    return int(rows[0]["team_id"])


# ---------------------------------------------------------------------------
# Load fixtures/results
# ---------------------------------------------------------------------------

def _load_team_matches(
    cur,
    tsdb_league_id: str,
    season_label: str,
    team_id: int,
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    """
    Load all matches for the given league+season where team_id is home or away.
    """
    sql = """
        SELECT
            m.match_id,
            m.kickoff_utc,
            m.home_team_id,
            m.away_team_id,
            m.home_score,
            m.away_score,
            th.name AS home_team_name,
            ta.name AS away_team_name,
            l.name  AS league_name,
            s.label AS season_label
        FROM matches m
        JOIN leagues l
          ON l.league_id = m.league_id
        JOIN seasons s
          ON s.season_id = m.season_id
        JOIN teams th
          ON th.team_id = m.home_team_id
        JOIN teams ta
          ON ta.team_id = m.away_team_id
        WHERE l.tsdb_league_id = %s
          AND s.label = %s
          AND (m.home_team_id = %s OR m.away_team_id = %s)
        ORDER BY m.kickoff_utc NULLS LAST, m.match_id ASC
    """
    cur.execute(sql, (tsdb_league_id, season_label, team_id, team_id))
    rows = cur.fetchall()

    if verbose:
        print(f"[INFO] Loaded {len(rows)} matches for team_id={team_id}, league={tsdb_league_id}, season='{season_label}'")

    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def _compute_result_for_team(
    row: Dict[str, Any],
    team_id: int,
) -> str:
    """
    From the perspective of team_id, return 'W', 'D', 'L' or '?'.
    """
    hs = row["home_score"]
    as_ = row["away_score"]

    if hs is None or as_ is None:
        return "?"

    hs = int(hs)
    as_ = int(as_)

    if row["home_team_id"] == team_id:
        if hs > as_:
            return "W"
        elif hs < as_:
            return "L"
        else:
            return "D"
    elif row["away_team_id"] == team_id:
        if as_ > hs:
            return "W"
        elif as_ < hs:
            return "L"
        else:
            return "D"
    else:
        return "?"


def _format_match_row(
    row: Dict[str, Any],
    team_id: int,
) -> Dict[str, Any]:
    """
    Build a simplified view of a match from the perspective of team_id.
    """
    is_home = (row["home_team_id"] == team_id)
    if is_home:
        side = "H"
        opponent = row["away_team_name"]
        scored = row["home_score"]
        conceded = row["away_score"]
    else:
        side = "A"
        opponent = row["home_team_name"]
        scored = row["away_score"]
        conceded = row["home_score"]

    result = _compute_result_for_team(row, team_id)
    dt = row["kickoff_utc"]
    dt_str = dt.isoformat(sep=" ", timespec="minutes") if dt is not None else "TBD"

    return {
        "date": dt_str,
        "side": side,
        "opponent": opponent,
        "scored": scored,
        "conceded": conceded,
        "result": result,
    }


def _print_team_fixtures(
    matches: List[Dict[str, Any]],
    team_id: int,
) -> None:
    if not matches:
        print("No matches found for that team/season.")
        return

    league_name = matches[0]["league_name"]
    season_label = matches[0]["season_label"]

    # Look up the team name from the first row that matches team_id
    team_name = None
    for r in matches:
        if r["home_team_id"] == team_id:
            team_name = r["home_team_name"]
            break
        if r["away_team_id"] == team_id:
            team_name = r["away_team_name"]
            break
    if team_name is None:
        team_name = f"team_id={team_id}"

    title = f"{league_name} - {season_label} - {team_name}"
    print("\n" + title)
    print("=" * len(title))

    headers = ["Date/Time (UTC)", "H/A", "Opponent", "Score", "Res"]
    widths = [20, 3, 25, 9, 3]

    def fmt_row(values):
        return (
            f"{str(values[0])[:widths[0]].ljust(widths[0])} "
            f"{str(values[1]).rjust(widths[1])} "
            f"{str(values[2])[:widths[2]].ljust(widths[2])} "
            f"{str(values[3]).rjust(widths[3])} "
            f"{str(values[4]).rjust(widths[4])}"
        )

    print(fmt_row(headers))
    print("-" * (sum(widths) + len(widths) - 1))

    for row in matches:
        m = _format_match_row(row, team_id)
        if m["scored"] is None or m["conceded"] is None:
            score_str = "-:-"
        else:
            score_str = f"{m['scored']}-{m['conceded']}"

        values = [
            m["date"],
            m["side"],
            m["opponent"],
            score_str,
            m["result"],
        ]
        print(fmt_row(values))

    print()  # trailing newline


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse

    _load_dotenv_if_available()

    parser = argparse.ArgumentParser(
        description="Print fixtures/results for a team in a given league season."
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
        "--team-id",
        type=int,
        help="Team id (from teams.team_id).",
    )
    parser.add_argument(
        "--team-name",
        help="Team name (or partial, case-insensitive). Used if --team-id is not provided.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose logging.",
    )

    args = parser.parse_args()
    verbose = args.verbose

    if args.team_id is None and not args.team_name:
        parser.error("You must provide either --team-id or --team-name")

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

        if args.team_id is not None:
            team_id = args.team_id
            if verbose:
                print(f"[INFO] Using team_id={team_id} (explicit)")
        else:
            team_id = _resolve_team_id_by_name(cur, args.team_name, verbose=verbose)

        matches = _load_team_matches(
            cur,
            tsdb_league_id=args.tsdb_league,
            season_label=season_label,
            team_id=team_id,
            verbose=verbose,
        )
        _print_team_fixtures(matches, team_id)

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
