#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
print_head_to_head.py
---------------------

Print head-to-head record between two teams in a given league.

Supports:

  - Filtering by TSDB league id (e.g. 4446 for URC)
  - Optional filtering by season label (e.g. '2023-2024')
    - If no season label is given, we show ALL seasons for that league.
  - Resolving teams by id or fuzzy name (ILIKE search), biased to teams
    that actually appear in that league's matches.

Output:

  - Summary:
        Team A vs Team B in <League> [all seasons or <season>]
        Games: X, Team A W-D-L, Team B W-D-L, Points for/against
  - Chronological list of matches:
        [Date/time]  Home vs Away   Score   Result (from Team A POV)
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
# Resolution helpers
# ---------------------------------------------------------------------------

def _resolve_team_id_by_name(
    cur,
    team_name: str,
    tsdb_league_id: Optional[str] = None,
    verbose: bool = False,
) -> int:
    """
    Resolve a team_id from a fuzzy name, preferring teams that actually
    appear in the given TSDB league (via matches).

    Strategy:
      1) If tsdb_league_id is provided:
         - Find distinct teams that have played in matches in that league.
         - Filter those by name ILIKE '%team_name%'.
         - If we get hits, use them.
      2) If none found (or no league passed):
         - Fall back to global teams.name ILIKE '%team_name%'.
    """

    candidates: List[Dict[str, Any]] = []

    if tsdb_league_id is not None:
        # League-specific candidates: teams that appear in matches in this league
        cur.execute(
            """
            SELECT DISTINCT t.team_id, t.name
            FROM teams t
            JOIN matches m
              ON m.home_team_id = t.team_id
              OR m.away_team_id = t.team_id
            JOIN leagues l
              ON l.league_id = m.league_id
            WHERE l.tsdb_league_id = %s
              AND t.name ILIKE %s
            ORDER BY t.name ASC
            """,
            (tsdb_league_id, f"%{team_name}%"),
        )
        rows = cur.fetchall()
        candidates = [dict(r) for r in rows]

        if verbose:
            print(
                f"[INFO] League-aware search for team '{team_name}' in tsdb_league_id={tsdb_league_id} "
                f"found {len(candidates)} candidate(s)"
            )

    if not candidates:
        # Fallback: global search across teams table
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
        candidates = [dict(r) for r in rows]

        if verbose:
            print(
                f"[INFO] Global search for team '{team_name}' found {len(candidates)} candidate(s)"
            )

    if not candidates:
        raise RuntimeError(f"No teams found matching name '{team_name}'")

    if verbose and len(candidates) > 1:
        print(f"[WARN] Multiple teams match '{team_name}', using the first:")
        for r in candidates[:5]:
            print(f"       - team_id={r['team_id']} name='{r['name']}'")

    chosen = candidates[0]
    if verbose:
        print(f"[INFO] Using team_id={chosen['team_id']} name='{chosen['name']}'")

    return int(chosen["team_id"])


def _resolve_league_name(cur, tsdb_league_id: str) -> str:
    cur.execute(
        """
        SELECT name
        FROM leagues
        WHERE tsdb_league_id = %s
        LIMIT 1
        """,
        (tsdb_league_id,),
    )
    row = cur.fetchone()
    return row["name"] if row else f"tsdb_league_id={tsdb_league_id}"


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _load_matches_between(
    cur,
    tsdb_league_id: str,
    team_a_id: int,
    team_b_id: int,
    season_label: Optional[str],
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    """
    Load all matches in the given TSDB league where team_a and team_b face each other.
    If season_label is None, includes all seasons; otherwise filters on that season.
    """
    base_sql = """
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
            s.label AS season_label,
            s.year  AS season_year
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
          AND (
               (m.home_team_id = %s AND m.away_team_id = %s)
            OR (m.home_team_id = %s AND m.away_team_id = %s)
          )
    """
    params: List[Any] = [tsdb_league_id, team_a_id, team_b_id, team_b_id, team_a_id]

    if season_label:
        base_sql += " AND s.label = %s"
        params.append(season_label)

    base_sql += " ORDER BY s.year ASC NULLS LAST, m.kickoff_utc NULLS LAST, m.match_id ASC"

    cur.execute(base_sql, tuple(params))
    rows = cur.fetchall()

    if verbose:
        label_info = season_label if season_label else "ALL seasons"
        print(
            f"[INFO] Loaded {len(rows)} matches between team_id={team_a_id} and team_id={team_b_id} "
            f"in league {tsdb_league_id}, {label_info}"
        )

    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def _compute_result_for_team(row: Dict[str, Any], team_id: int) -> str:
    """
    Return result from perspective of team_id: 'W', 'D', 'L', '?'
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


def _aggregate_h2h(
    matches: List[Dict[str, Any]],
    team_a_id: int,
    team_b_id: int,
) -> Dict[str, Any]:
    """
    Aggregate head-to-head stats for Team A and Team B.
    """
    agg = {
        "games": 0,
        "team_a": {"wins": 0, "draws": 0, "losses": 0, "pf": 0, "pa": 0},
        "team_b": {"wins": 0, "draws": 0, "losses": 0, "pf": 0, "pa": 0},
    }

    for row in matches:
        hs = row["home_score"]
        as_ = row["away_score"]

        # count game regardless of whether score is known
        agg["games"] += 1

        if hs is None or as_ is None:
            continue

        hs = int(hs)
        as_ = int(as_)

        # points for / against
        if row["home_team_id"] == team_a_id:
            # A home
            agg["team_a"]["pf"] += hs
            agg["team_a"]["pa"] += as_
            agg["team_b"]["pf"] += as_
            agg["team_b"]["pa"] += hs
        elif row["away_team_id"] == team_a_id:
            # A away
            agg["team_a"]["pf"] += as_
            agg["team_a"]["pa"] += hs
            agg["team_b"]["pf"] += hs
            agg["team_b"]["pa"] += as_
        elif row["home_team_id"] == team_b_id:
            # B home
            agg["team_b"]["pf"] += hs
            agg["team_b"]["pa"] += as_
            agg["team_a"]["pf"] += as_
            agg["team_a"]["pa"] += hs
        elif row["away_team_id"] == team_b_id:
            # B away
            agg["team_b"]["pf"] += as_
            agg["team_b"]["pa"] += hs
            agg["team_a"]["pf"] += hs
            agg["team_a"]["pa"] += as_

        # results
        res_a = _compute_result_for_team(row, team_a_id)
        if res_a == "W":
            agg["team_a"]["wins"] += 1
            agg["team_b"]["losses"] += 1
        elif res_a == "L":
            agg["team_a"]["losses"] += 1
            agg["team_b"]["wins"] += 1
        elif res_a == "D":
            agg["team_a"]["draws"] += 1
            agg["team_b"]["draws"] += 1

    return agg


# ---------------------------------------------------------------------------
# Printing
# ---------------------------------------------------------------------------

def _print_summary(
    matches: List[Dict[str, Any]],
    team_a_id: int,
    team_b_id: int,
    agg: Dict[str, Any],
) -> None:
    if not matches:
        print("No head-to-head matches found.")
        return

    league_name = matches[0]["league_name"]

    # Get team names
    team_a_name = None
    team_b_name = None
    seasons_set = set()

    for r in matches:
        seasons_set.add(r["season_label"])
        if r["home_team_id"] == team_a_id:
            team_a_name = r["home_team_name"]
        if r["away_team_id"] == team_a_id:
            team_a_name = r["away_team_name"]
        if r["home_team_id"] == team_b_id:
            team_b_name = r["home_team_name"]
        if r["away_team_id"] == team_b_id:
            team_b_name = r["away_team_name"]

    if team_a_name is None:
        team_a_name = f"team_id={team_a_id}"
    if team_b_name is None:
        team_b_name = f"team_id={team_b_id}"

    if len(seasons_set) == 1:
        season_info = next(iter(seasons_set))
    else:
        season_info = f"{len(seasons_set)} seasons"

    title = f"{league_name} - H2H: {team_a_name} vs {team_b_name} ({season_info})"
    print("\n" + title)
    print("=" * len(title))

    ga = agg["team_a"]
    gb = agg["team_b"]

    print(f"Games played: {agg['games']}")
    print(
        f"{team_a_name}: W{ga['wins']} D{ga['draws']} L{ga['losses']} "
        f"PF{ga['pf']} PA{ga['pa']} PD{ga['pf'] - ga['pa']}"
    )
    print(
        f"{team_b_name}: W{gb['wins']} D{gb['draws']} L{gb['losses']} "
        f"PF{gb['pf']} PA{gb['pa']} PD{gb['pf'] - gb['pa']}"
    )
    print()


def _print_match_list(
    matches: List[Dict[str, Any]],
    team_a_id: int,
    team_b_id: int,
) -> None:
    if not matches:
        return

    headers = ["Date/Time (UTC)", "Home", "Away", "Score", "Result (A)"]
    widths = [20, 20, 20, 9, 12]

    def fmt_row(values: List[Any]) -> str:
        return (
            f"{str(values[0])[:widths[0]].ljust(widths[0])} "
            f"{str(values[1])[:widths[1]].ljust(widths[1])} "
            f"{str(values[2])[:widths[2]].ljust(widths[2])} "
            f"{str(values[3]).rjust(widths[3])} "
            f"{str(values[4]).rjust(widths[4])}"
        )

    print("Match list (Result from Team A perspective):")
    print("-" * (sum(widths) + len(widths) - 1))
    print(fmt_row(headers))
    print("-" * (sum(widths) + len(widths) - 1))

    for r in matches:
        dt = r["kickoff_utc"]
        dt_str = dt.isoformat(sep=" ", timespec="minutes") if dt is not None else "TBD"

        hs = r["home_score"]
        as_ = r["away_score"]
        if hs is None or as_ is None:
            score_str = "-:-"
        else:
            score_str = f"{hs}-{as_}"

        res_a = _compute_result_for_team(r, team_a_id)
        if res_a == "?":
            res_label = "TBD"
        elif res_a == "D":
            res_label = "Draw"
        elif res_a == "W":
            res_label = "A win"
        else:
            res_label = "A loss"

        values = [
            dt_str,
            r["home_team_name"],
            r["away_team_name"],
            score_str,
            res_label,
        ]
        print(fmt_row(values))

    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse

    _load_dotenv_if_available()

    parser = argparse.ArgumentParser(
        description="Print head-to-head record between two teams in a given TSDB league."
    )
    parser.add_argument(
        "--tsdb-league",
        required=True,
        help="TSDB league id (e.g. 4446 for URC).",
    )
    parser.add_argument(
        "--season-label",
        help="Optional season label (e.g. '2023-2024'). If omitted, use ALL seasons.",
    )
    parser.add_argument(
        "--team-a-id",
        type=int,
        help="Team A id (teams.team_id). If not provided, use --team-a-name.",
    )
    parser.add_argument(
        "--team-b-id",
        type=int,
        help="Team B id (teams.team_id). If not provided, use --team-b-name.",
    )
    parser.add_argument(
        "--team-a-name",
        help="Team A name (or partial, case-insensitive).",
    )
    parser.add_argument(
        "--team-b-name",
        help="Team B name (or partial, case-insensitive).",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose logging.",
    )

    args = parser.parse_args()
    verbose = args.verbose

    if args.team_a_id is None and not args.team_a_name:
        parser.error("You must provide either --team-a-id or --team-a-name")
    if args.team_b_id is None and not args.team_b_name:
        parser.error("You must provide either --team-b-id or --team-b-name")

    conn = _get_conn()
    cur = conn.cursor(cursor_factory=DictCursor)

    try:
        # Resolve team ids if needed
        if args.team_a_id is not None:
            team_a_id = args.team_a_id
            if verbose:
                print(f"[INFO] Using team A id={team_a_id}")
        else:
            team_a_id = _resolve_team_id_by_name(
                cur,
                args.team_a_name,
                tsdb_league_id=args.tsdb_league,
                verbose=verbose,
            )

        if args.team_b_id is not None:
            team_b_id = args.team_b_id
            if verbose:
                print(f"[INFO] Using team B id={team_b_id}")
        else:
            team_b_id = _resolve_team_id_by_name(
                cur,
                args.team_b_name,
                tsdb_league_id=args.tsdb_league,
                verbose=verbose,
            )

        # Load matches
        matches = _load_matches_between(
            cur,
            tsdb_league_id=args.tsdb_league,
            team_a_id=team_a_id,
            team_b_id=team_b_id,
            season_label=args.season_label,
            verbose=verbose,
        )

        agg = _aggregate_h2h(matches, team_a_id, team_b_id)
        _print_summary(matches, team_a_id, team_b_id, agg)
        _print_match_list(matches, team_a_id, team_b_id)

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
