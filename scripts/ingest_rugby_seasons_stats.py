#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ingest_rugby_seasons_stats.py
-----------------------------

Compute per-team, per-season stats from the matches table and store them in
team_season_stats.

Usage examples (from C:\\rugby-analytics):

    # Recompute stats for URC (TSDB league id 4446)
    python -m scripts.ingest_rugby_seasons_stats --tsdb-league 4446 -v

If --tsdb-league is omitted, all leagues with a non-null tsdb_league_id
will be processed.
"""

import os
import sys
from typing import Dict, Any, List, Optional

# ---------------------------------------------------------------------------
# Make project root importable
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ---------------------------------------------------------------------------
# Optional dotenv
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def _load_leagues(cur, tsdb_league: Optional[int]) -> List[Dict[str, Any]]:
    """
    Load leagues to process, optionally filtered by tsdb_league_id.
    """
    if tsdb_league is not None:
        cur.execute(
            """
            SELECT league_id, tsdb_league_id, name
            FROM leagues
            WHERE tsdb_league_id = %s
            ORDER BY league_id
            """,
            (tsdb_league,),
        )
    else:
        cur.execute(
            """
            SELECT league_id, tsdb_league_id, name
            FROM leagues
            WHERE tsdb_league_id IS NOT NULL
            ORDER BY league_id
            """
        )
    rows = cur.fetchall()
    return [dict(r) for r in rows]


def _load_seasons_for_league(cur, league_id: int) -> List[Dict[str, Any]]:
    """
    Load seasons for a league.
    """
    cur.execute(
        """
        SELECT season_id, label, year
        FROM seasons
        WHERE league_id = %s
        ORDER BY year
        """,
        (league_id,),
    )
    rows = cur.fetchall()
    return [dict(r) for r in rows]


def _load_matches_for_season(cur, league_id: int, season_id: int) -> List[Dict[str, Any]]:
    """
    Load completed matches (with scores) for a given league+season.
    """
    cur.execute(
        """
        SELECT
            match_id,
            home_team_id,
            away_team_id,
            home_score,
            away_score
        FROM matches
        WHERE league_id = %s
          AND season_id = %s
          AND home_score IS NOT NULL
          AND away_score IS NOT NULL
        """,
        (league_id, season_id),
    )
    rows = cur.fetchall()
    return [dict(r) for r in rows]


def _compute_season_stats(matches: List[Dict[str, Any]]) -> Dict[int, Dict[str, int]]:
    """
    From a list of matches, compute per-team season stats.

    We use a standard rugby points system:
      - Win: 4 points
      - Draw: 2 points
      - Loss: 0 points
      - Losing bonus: +1 point if losing margin <= 7
      - Try bonus: currently always 0 (we don't have try counts in matches)

    We aggregate:
      games_played, wins, draws, losses,
      points_for, points_against, points_diff,
      competition_points, losing_bonus_points, try_bonus_points.
    """
    stats: Dict[int, Dict[str, int]] = {}

    def ensure_team(tid: int) -> Dict[str, int]:
        if tid not in stats:
            stats[tid] = {
                "games_played": 0,
                "wins": 0,
                "draws": 0,
                "losses": 0,
                "points_for": 0,
                "points_against": 0,
                "competition_points": 0,
                "losing_bonus_points": 0,
                "try_bonus_points": 0,
            }
        return stats[tid]

    for m in matches:
        ht = int(m["home_team_id"])
        at = int(m["away_team_id"])
        hs = int(m["home_score"])
        as_ = int(m["away_score"])

        home = ensure_team(ht)
        away = ensure_team(at)

        # update basic stats
        home["games_played"] += 1
        away["games_played"] += 1

        home["points_for"] += hs
        home["points_against"] += as_
        away["points_for"] += as_
        away["points_against"] += hs

        margin = hs - as_

        if margin > 0:
            # home win
            home["wins"] += 1
            home["competition_points"] += 4

            away["losses"] += 1
            # losing bonus if margin <= 7
            if abs(margin) <= 7:
                away["competition_points"] += 1
                away["losing_bonus_points"] += 1
        elif margin < 0:
            # away win
            away["wins"] += 1
            away["competition_points"] += 4

            home["losses"] += 1
            if abs(margin) <= 7:
                home["competition_points"] += 1
                home["losing_bonus_points"] += 1
        else:
            # draw
            home["draws"] += 1
            away["draws"] += 1
            home["competition_points"] += 2
            away["competition_points"] += 2

        # try_bonus_points stays 0 (we don't track tries per match yet)

    # compute points_diff
    for tid, s in stats.items():
        s["points_diff"] = s["points_for"] - s["points_against"]

    return stats


def _upsert_team_season_stats(
    cur,
    league_id: int,
    season_id: int,
    stats: Dict[int, Dict[str, int]],
    verbose: bool = False,
) -> None:
    """
    Upsert rows into team_season_stats for one league+season.
    """
    for team_id, s in stats.items():
        if verbose:
            print(
                f"  [UPSERT] league_id={league_id}, season_id={season_id}, team_id={team_id} "
                f"GP={s['games_played']} W={s['wins']} D={s['draws']} L={s['losses']} "
                f"PF={s['points_for']} PA={s['points_against']} "
                f"CP={s['competition_points']}"
            )
        cur.execute(
            """
            INSERT INTO team_season_stats (
                league_id,
                season_id,
                team_id,
                games_played,
                wins,
                draws,
                losses,
                points_for,
                points_against,
                points_diff,
                competition_points,
                losing_bonus_points,
                try_bonus_points,
                created_at,
                updated_at
            )
            VALUES (
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                NOW(), NOW()
            )
            ON CONFLICT (league_id, season_id, team_id) DO UPDATE
            SET
                games_played        = EXCLUDED.games_played,
                wins                = EXCLUDED.wins,
                draws               = EXCLUDED.draws,
                losses              = EXCLUDED.losses,
                points_for          = EXCLUDED.points_for,
                points_against      = EXCLUDED.points_against,
                points_diff         = EXCLUDED.points_diff,
                competition_points  = EXCLUDED.competition_points,
                losing_bonus_points = EXCLUDED.losing_bonus_points,
                try_bonus_points    = EXCLUDED.try_bonus_points,
                updated_at          = NOW()
            """,
            (
                league_id,
                season_id,
                team_id,
                s["games_played"],
                s["wins"],
                s["draws"],
                s["losses"],
                s["points_for"],
                s["points_against"],
                s["points_diff"],
                s["competition_points"],
                s["losing_bonus_points"],
                s["try_bonus_points"],
            ),
        )


def main() -> None:
    import argparse

    _load_dotenv_if_available()

    parser = argparse.ArgumentParser(
        description="Compute per-team, per-season stats into team_season_stats."
    )
    parser.add_argument(
        "--tsdb-league",
        type=int,
        help="Optional TSDB league id filter (e.g. 4446 for URC).",
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
        leagues = _load_leagues(cur, args.tsdb_league)
        if not leagues:
            print("[WARN] No leagues found with the given filter.")
            return

        if verbose:
            print(f"[INFO] Found {len(leagues)} league(s) to process.")

        for l in leagues:
            league_id = l["league_id"]
            tsdb_lid = l["tsdb_league_id"]
            name = l["name"]

            if verbose:
                print(f"\n=== LEAGUE {league_id} (TSDB={tsdb_lid}) {name} ===")

            seasons = _load_seasons_for_league(cur, league_id)
            if verbose:
                print(f"[INFO] Found {len(seasons)} season(s) for this league.")

            for s in seasons:
                season_id = s["season_id"]
                label = s["label"]
                year = s["year"]
                if verbose:
                    print(f"[SEASON] season_id={season_id}, label={label}, year={year}")

                matches = _load_matches_for_season(cur, league_id, season_id)
                if verbose:
                    print(f"  [INFO] {len(matches)} completed matches found")

                if not matches:
                    continue

                stats = _compute_season_stats(matches)
                _upsert_team_season_stats(cur, league_id, season_id, stats, verbose=verbose)

        conn.commit()
        if verbose:
            print("[OK] team_season_stats updated.")
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Ingestion failed, rolled back transaction: {e}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
