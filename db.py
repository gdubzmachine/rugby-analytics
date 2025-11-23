#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
db.py

Database connection + common query helpers and league/season lookup
functions for the rugby analytics API.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load .env for local development. On Render this is a no-op.
load_dotenv()


def get_conn():
    """
    Open a Postgres connection using the DATABASE_URL environment variable.
    """
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "DATABASE_URL is not set. Configure it in your .env for local dev, "
            "or as a Render env var in production."
        )
    return psycopg2.connect(dsn, cursor_factory=RealDictCursor)


def fetch_one(query: str, params: Tuple[Any, ...]) -> Optional[Dict[str, Any]]:
    """
    Run a query that returns a single row (or None).
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            row = cur.fetchone()
    return row


def fetch_all(query: str, params: Tuple[Any, ...]) -> List[Dict[str, Any]]:
    """
    Run a query that returns multiple rows.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
    return list(rows)


def resolve_league_by_tsdb(tsdb_league_id: int) -> Optional[Dict[str, Any]]:
    """
    Resolve an internal league row by tsdb_league_id.
    """
    return fetch_one(
        """
        SELECT id, name, tsdb_league_id
        FROM leagues
        WHERE tsdb_league_id = %s
        """,
        (tsdb_league_id,),
    )


def resolve_latest_season_for_league(league_id: int) -> Optional[Dict[str, Any]]:
    """
    Return the most recent season row for a given league.
    """
    return fetch_one(
        """
        SELECT id, label, year, start_date, end_date
        FROM seasons
        WHERE league_id = %s
        ORDER BY start_date DESC NULLS LAST, year DESC
        LIMIT 1
        """,
        (league_id,),
    )


def resolve_season_for_league_and_label(
    league_id: int, season_label: str
) -> Optional[Dict[str, Any]]:
    """
    Resolve a season row by league_id and label (e.g. '2023-2024' or '2024').
    """
    return fetch_one(
        """
        SELECT id, label, year, start_date, end_date
        FROM seasons
        WHERE league_id = %s
          AND label = %s
        LIMIT 1
        """,
        (league_id, season_label),
    )
