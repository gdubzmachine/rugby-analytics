#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
compute_team_season_stats.py
----------------------------

Aggregate season-level stats for each team from the matches table.

For each (league_id, season_id, team_id) we compute:

- games_played
- wins, draws, losses
- points_for, points_against, points_diff
- competition_points (simple 4/2/0 – no bonus points yet)

We then upsert into a team_season_stats table. The script is schema-aware:

- If team_season_stats does NOT exist, it will create:

    team_season_stats (
        stat_id            BIGSERIAL PRIMARY KEY,
        league_id          BIGINT NOT NULL,
        season_id          BIGINT NOT NULL,
        team_id            BIGINT NOT NULL,
        games_played       INTEGER NOT NULL DEFAULT 0,
        wins               INTEGER NOT NULL DEFAULT 0,
        draws              INTEGER NOT NULL DEFAULT 0,
        losses             INTEGER NOT NULL DEFAULT 0,
        points_for         INTEGER NOT NULL DEFAULT 0,
        points_against     INTEGER NOT NULL DEFAULT 0,
        points_diff        INTEGER NOT NULL DEFAULT 0,
        competition_points INTEGER NOT NULL DEFAULT 0,
        created_at         TIMESTAMPTZ,
        updated_at         TIMESTAMPTZ,
        UNIQUE (league_id, season_id, team_id)
    );

- If it DOES exist, we introspect its columns and only read/write the ones
  that are present.

Assumptions about the matches table:

- matches.home_team_id / matches.away_team_id (BIGINT, not null)
- matches.home_score / matches.away_score (INTEGER, or NULL if not played)
- matches.league_id, matches.season_id (BIGINT)
- We treat any match with BOTH scores non-null as "completed" and include it.

Usage examples (from C:\\rugby-analytics):

    # All leagues & seasons
    python -m scripts.compute_team_season_stats -v

    # Only URC (TSDB league 4446)
    python -m scripts.compute_team_season_stats --only-tsdb-league 4446 -v

    # Only specific season label in URC
    python -m scripts.compute_team_season_stats ^
        --only-tsdb-league 4446 ^
        --season-label 2023-2024 -v
"""

import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Tuple, Optional, Any, List, Set

# ---------------------------------------------------------------------------
# Ensure project root is importable
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# DB imports
try:
    import psycopg2
    from psycopg2.extras import DictCursor
except ImportError:
    print(
        "Missing dependency: psycopg2-binary (pip install psycopg2-binary)",
        file=sys.stderr,
    )
    sys.exit(1)

# Optional db helper
try:
    from db.connection import get_db_connection  # type: ignore
except Exception:
    get_db_connection = None  # type: ignore


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

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
            "Set DATABASE_URL in .env or implement db/connection.get_db_connection()."
        )
    return psycopg2.connect(dsn)


def _table_exists(cur, table_name: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = %s
        LIMIT 1
        """,
        (table_name,),
    )
    return cur.fetchone() is not None


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


def _ensure_team_season_stats_table(cur, verbose: bool = False) -> None:
    """
    Create team_season_stats if it does not exist.
    """
    if _table_exists(cur, "team_season_stats"):
        if verbose:
            print("[INFO] team_season_stats table already exists")
        return

    if verbose:
        print("[INFO] Creating team_season_stats table")

    cur.execute(
        """
        CREATE TABLE team_season_stats (
            stat_id            BIGSERIAL PRIMARY KEY,
            league_id          BIGINT NOT NULL,
            season_id          BIGINT NOT NULL,
            team_id            BIGINT NOT NULL,
            games_played       INTEGER NOT NULL DEFAULT 0,
            wins               INTEGER NOT NULL DEFAULT 0,
            draws              INTEGER NOT NULL DEFAULT 0,
            losses             INTEGER NOT NULL DEFAULT 0,
            points_for         INTEGER NOT NULL DEFAULT 0,
            points_against     INTEGER NOT NULL DEFAULT 0,
            points_diff        INTEGER NOT NULL DEFAULT 0,
            competition_points INTEGER NOT NULL DEFAULT 0,
            created_at         TIMESTAMPTZ DEFAULT NOW(),
            updated_at         TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (league_id, season_id, team_id)
        );
        """
    )


# ---------------------------------------------------------------------------
# Aggregation logic
# ---------------------------------------------------------------------------

@dataclass
class TeamSeasonAgg:
    league_id: int
    season_id: int
    team_id: int
    games_played: int = 0
    wins: int = 0
    draws: int = 0
    losses: int = 0
    points_for: int = 0
    points_against: int = 0
    competition_points: int = 0

    @property
    def points_diff(self) -> int:
        return (self.points_for or 0) - (self.points_against or 0)


def _load_matches(
    cur,
    only_tsdb_league: Optional[str],
    season_label: Optional[str],
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    """
    Load completed matches and their scores.

    We consider a match "completed" if:
      - home_team_id, away_team_id are not null,
      - home_score and away_score are not null.
    """

    sql = """
        SELECT
            m.match_id,
            m.league_id,
            m.season_id,
            m.home_team_id,
            m.away_team_id,
            m.home_score,
            m.away_score,
            s.label AS season_label,
            l.tsdb_league_id,
            l.name AS league_name
        FROM matches m
        JOIN leagues l ON l.league_id = m.league_id
        JOIN seasons s ON s.season_id = m.season_id
        WHERE m.home_team_id IS NOT NULL
          AND m.away_team_id IS NOT NULL
          AND m.home_score IS NOT NULL
          AND m.away_score IS NOT NULL
    """
    params: List[Any] = []

    if only_tsdb_league:
        sql += " AND l.tsdb_league_id = %s"
        params.append(only_tsdb_league)

    if season_label:
        sql += " AND s.label = %s"
        params.append(season_label)

    sql += " ORDER BY l.tsdb_league_id::TEXT, s.year ASC, m.match_id ASC"

    cur.execute(sql, tuple(params))
    rows = cur.fetchall()

    if verbose:
        print(f"[INFO] Loaded {len(rows)} completed matches for aggregation")

    matches: List[Dict[str, Any]] = []
    for r in rows:
        matches.append(
            {
                "match_id": r["match_id"],
                "league_id": r["league_id"],
                "season_id": r["season_id"],
                "home_team_id": r["home_team_id"],
                "away_team_id": r["away_team_id"],
                "home_score": r["home_score"],
                "away_score": r["away_score"],
                "season_label": r["season_label"],
                "tsdb_league_id": r["tsdb_league_id"],
                "league_name": r["league_name"],
            }
        )
    return matches


def _aggregate_team_season_stats(
    matches: List[Dict[str, Any]],
    verbose: bool = False,
) -> Dict[Tuple[int, int, int], TeamSeasonAgg]:
    """
    Aggregate per (league_id, season_id, team_id).
    """
    agg: Dict[Tuple[int, int, int], TeamSeasonAgg] = {}

    for m in matches:
        league_id = int(m["league_id"])
        season_id = int(m["season_id"])
        home_team_id = int(m["home_team_id"])
        away_team_id = int(m["away_team_id"])
        hs = int(m["home_score"])
        as_ = int(m["away_score"])

        key_home = (league_id, season_id, home_team_id)
        key_away = (league_id, season_id, away_team_id)

        if key_home not in agg:
            agg[key_home] = TeamSeasonAgg(league_id, season_id, home_team_id)
        if key_away not in agg:
            agg[key_away] = TeamSeasonAgg(league_id, season_id, away_team_id)

        home = agg[key_home]
        away = agg[key_away]

        # every completed match increments games_played
        home.games_played += 1
        away.games_played += 1

        home.points_for += hs
        home.points_against += as_
        away.points_for += as_
        away.points_against += hs

        if hs > as_:
            home.wins += 1
            away.losses += 1
            home.competition_points += 4  # simple 4 points for a win
        elif hs < as_:
            away.wins += 1
            home.losses += 1
            away.competition_points += 4
        else:
            home.draws += 1
            away.draws += 1
            home.competition_points += 2  # simple 2 for a draw
            away.competition_points += 2

    if verbose:
        print(f"[INFO] Aggregated stats for {len(agg)} team-season rows")

    return agg


def _upsert_team_season_stats(
    cur,
    agg: Dict[Tuple[int, int, int], TeamSeasonAgg],
    verbose: bool = False,
) -> None:
    """
    Upsert into team_season_stats in a schema-aware way.
    """
    cols = _get_table_columns(cur, "team_season_stats")

    for key, stat in agg.items():
        if verbose:
            lg, se, tm = key
            print(
                f"  [UPSERT] league_id={lg} season_id={se} team_id={tm} "
                f"GP={stat.games_played} W={stat.wins} D={stat.draws} L={stat.losses} "
                f"PF={stat.points_for} PA={stat.points_against} "
                f"PD={stat.points_diff} Pts={stat.competition_points}"
            )

        # Build upsert dynamically
        insert_cols: List[str] = ["league_id", "season_id", "team_id"]
        insert_vals: List[str] = ["%s", "%s", "%s"]
        params: List[Any] = [stat.league_id, stat.season_id, stat.team_id]

        # Only include columns that exist
        def add(name: str, value: Any):
            if name in cols:
                insert_cols.append(name)
                insert_vals.append("%s")
                params.append(value)

        add("games_played", stat.games_played)
        add("wins", stat.wins)
        add("draws", stat.draws)
        add("losses", stat.losses)
        add("points_for", stat.points_for)
        add("points_against", stat.points_against)
        add("points_diff", stat.points_diff)
        add("competition_points", stat.competition_points)

        # created_at / updated_at are handled as NOW() in SQL if present
        if "created_at" in cols:
            insert_cols.append("created_at")
            insert_vals.append("NOW()")
        if "updated_at" in cols:
            insert_cols.append("updated_at")
            insert_vals.append("NOW()")

        insert_cols_sql = ", ".join(insert_cols)
        insert_vals_sql = ", ".join(insert_vals)

        # Build SET part for ON CONFLICT (excluding PK/unique fields)
        set_fragments: List[str] = []
        update_params: List[Any] = []

        def add_update(name: str, value: Any):
            if name in cols:
                set_fragments.append(f"{name} = EXCLUDED.{name}")
                # nothing to add to params; we use EXCLUDED

        add_update("games_played", stat.games_played)
        add_update("wins", stat.wins)
        add_update("draws", stat.draws)
        add_update("losses", stat.losses)
        add_update("points_for", stat.points_for)
        add_update("points_against", stat.points_against)
        add_update("points_diff", stat.points_diff)
        add_update("competition_points", stat.competition_points)
        if "updated_at" in cols:
            set_fragments.append("updated_at = NOW()")

        set_sql = ", ".join(set_fragments) if set_fragments else ""

        sql = f"""
            INSERT INTO team_season_stats ({insert_cols_sql})
            VALUES ({insert_vals_sql})
            ON CONFLICT (league_id, season_id, team_id)
        """
        if set_sql:
            sql += f" DO UPDATE SET {set_sql}"
        else:
            sql += " DO NOTHING"

        cur.execute(sql, tuple(params))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse

    _load_dotenv_if_available()

    parser = argparse.ArgumentParser(
        description="Compute season-level stats for each team from matches."
    )
    parser.add_argument(
        "--only-tsdb-league",
        help="Optional TSDB league id filter (e.g. 4446 for URC).",
    )
    parser.add_argument(
        "--season-label",
        help="Optional season label filter (e.g. '2023-2024').",
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
        _ensure_team_season_stats_table(cur, verbose=verbose)
        conn.commit()

        matches = _load_matches(
            cur,
            only_tsdb_league=args.only_tsdb_league,
            season_label=args.season_label,
            verbose=verbose,
        )
        agg = _aggregate_team_season_stats(matches, verbose=verbose)
        _upsert_team_season_stats(cur, agg, verbose=verbose)

        conn.commit()

        if verbose:
            print("[DONE] team season stats computed and upserted.")

    except Exception as exc:
        conn.rollback()
        print(f"[ERROR] Failed to compute team season stats: {exc}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
