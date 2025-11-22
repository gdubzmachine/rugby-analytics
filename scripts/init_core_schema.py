#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
init_core_schema.py
-------------------

Create core tables needed for rugby ingest on a fresh Postgres DB:

- sports
- leagues
- seasons
- teams
- venues
- matches
- league_team_seasons
- team_season_stats

It uses DATABASE_URL from .env (or db.connection.get_db_connection if present).
"""

import os
import sys

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
            "Set DATABASE_URL in .env or create db/connection.py with get_db_connection()."
        )
    return psycopg2.connect(dsn)


# ---------------------------------------------------------------------------
# DDL for core tables
# ---------------------------------------------------------------------------

SPORTS_DDL = """
CREATE TABLE IF NOT EXISTS sports (
    sport_id   SERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    code       TEXT UNIQUE
);
"""

LEAGUES_DDL = """
CREATE TABLE IF NOT EXISTS leagues (
    league_id      BIGSERIAL PRIMARY KEY,
    name           TEXT NOT NULL,
    short_name     TEXT,
    slug           TEXT,
    country_code   CHAR(2),
    sport_id       INTEGER REFERENCES sports(sport_id),
    tsdb_league_id INTEGER UNIQUE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

SEASONS_DDL = """
CREATE TABLE IF NOT EXISTS seasons (
    season_id       BIGSERIAL PRIMARY KEY,
    league_id       BIGINT REFERENCES leagues(league_id) ON DELETE CASCADE,
    year            INTEGER,
    label           TEXT,
    start_date      DATE,
    end_date        DATE,
    tsdb_season_key TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (league_id, year)
);
"""

TEAMS_DDL = """
CREATE TABLE IF NOT EXISTS teams (
    team_id       BIGSERIAL PRIMARY KEY,
    name          TEXT NOT NULL,
    short_name    TEXT,
    slug          TEXT,
    country       TEXT,
    sport         TEXT,
    tsdb_team_id  TEXT UNIQUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

VENUES_DDL = """
CREATE TABLE IF NOT EXISTS venues (
    venue_id      BIGSERIAL PRIMARY KEY,
    name          TEXT,
    city          TEXT,
    country       TEXT,
    latitude      DOUBLE PRECISION,
    longitude     DOUBLE PRECISION,
    tsdb_venue_id TEXT UNIQUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

MATCHES_DDL = """
CREATE TABLE IF NOT EXISTS matches (
    match_id       BIGSERIAL PRIMARY KEY,
    league_id      BIGINT REFERENCES leagues(league_id) ON DELETE CASCADE,
    season_id      BIGINT REFERENCES seasons(season_id) ON DELETE CASCADE,
    venue_id       BIGINT REFERENCES venues(venue_id),
    home_team_id   BIGINT REFERENCES teams(team_id),
    away_team_id   BIGINT REFERENCES teams(team_id),
    status         TEXT,
    kickoff_utc    TIMESTAMPTZ,
    home_score     INTEGER,
    away_score     INTEGER,
    attendance     INTEGER,
    tsdb_event_id  TEXT UNIQUE,
    source         TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

LEAGUE_TEAM_SEASONS_DDL = """
CREATE TABLE IF NOT EXISTS league_team_seasons (
    league_id   BIGINT REFERENCES leagues(league_id) ON DELETE CASCADE,
    season_id   BIGINT REFERENCES seasons(season_id) ON DELETE CASCADE,
    team_id     BIGINT REFERENCES teams(team_id) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (league_id, season_id, team_id)
);
"""

TEAM_SEASON_STATS_DDL = """
CREATE TABLE IF NOT EXISTS team_season_stats (
    league_id           BIGINT REFERENCES leagues(league_id) ON DELETE CASCADE,
    season_id           BIGINT REFERENCES seasons(season_id) ON DELETE CASCADE,
    team_id             BIGINT REFERENCES teams(team_id) ON DELETE CASCADE,
    games_played        INTEGER NOT NULL DEFAULT 0,
    wins                INTEGER NOT NULL DEFAULT 0,
    draws               INTEGER NOT NULL DEFAULT 0,
    losses              INTEGER NOT NULL DEFAULT 0,
    points_for          INTEGER NOT NULL DEFAULT 0,
    points_against      INTEGER NOT NULL DEFAULT 0,
    points_diff         INTEGER NOT NULL DEFAULT 0,
    competition_points  INTEGER NOT NULL DEFAULT 0,
    losing_bonus_points INTEGER NOT NULL DEFAULT 0,
    try_bonus_points    INTEGER NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (league_id, season_id, team_id)
);
"""

INSERT_RUGBY_UNION = """
INSERT INTO sports (name, code)
VALUES ('Rugby', 'rugby_union')
ON CONFLICT (code) DO NOTHING;
"""


def init_core_schema(verbose: bool = False) -> None:
    conn = _get_conn()
    conn.autocommit = False
    cur = conn.cursor()
    try:
        if verbose:
            print("[INFO] Creating sports table (if not exists)…")
        cur.execute(SPORTS_DDL)

        if verbose:
            print("[INFO] Creating leagues table (if not exists)…")
        cur.execute(LEAGUES_DDL)

        if verbose:
            print("[INFO] Creating seasons table (if not exists)…")
        cur.execute(SEASONS_DDL)

        if verbose:
            print("[INFO] Creating teams table (if not exists)…")
        cur.execute(TEAMS_DDL)

        if verbose:
            print("[INFO] Creating venues table (if not exists)…")
        cur.execute(VENUES_DDL)

        if verbose:
            print("[INFO] Creating matches table (if not exists)…")
        cur.execute(MATCHES_DDL)

        if verbose:
            print("[INFO] Creating league_team_seasons table (if not exists)…")
        cur.execute(LEAGUE_TEAM_SEASONS_DDL)

        if verbose:
            print("[INFO] Creating team_season_stats table (if not exists)…")
        cur.execute(TEAM_SEASON_STATS_DDL)

        if verbose:
            print("[INFO] Ensuring rugby_union sport row exists in sports…")
        cur.execute(INSERT_RUGBY_UNION)

        conn.commit()
        if verbose:
            print("[OK] Core schema initialised.")
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Failed to init core schema: {e}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()


def main() -> None:
    import argparse

    _load_dotenv_if_available()

    parser = argparse.ArgumentParser(
        description="Initialise core rugby schema tables."
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose logging.",
    )
    args = parser.parse_args()
    init_core_schema(verbose=args.verbose)


if __name__ == "__main__":
    main()
