#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
api/main.py
-----------

Minimal FastAPI app for rugby analytics:

- /health
- /standings/{tsdb_league_id}

It expects the following tables (which we already created on Render):

- sports
- leagues (with tsdb_league_id)
- seasons  (with league_id, year, label)
- teams
- matches
- team_season_stats

It uses DATABASE_URL from the environment (.env locally, Render env vars in prod).
"""

import os
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from dotenv import load_dotenv

import psycopg2
from psycopg2.extras import RealDictCursor

# ---------------------------------------------------------------------------
# Environment / DB helpers
# ---------------------------------------------------------------------------

load_dotenv()  # locally; no-op on Render

DATABASE_URL = os.getenv("DATABASE_URL")


def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")
    # RealDictCursor returns rows as dicts, which is easier to turn into JSON
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class StandingRow(BaseModel):
    position: int
    team_id: int
    team_name: str
    season_label: str
    games_played: int
    wins: int
    draws: int
    losses: int
    points_for: int
    points_against: int
    points_diff: int
    competition_points: int
    losing_bonus_points: int
    try_bonus_points: int


class StandingsResponse(BaseModel):
    tsdb_league_id: int
    league_name: str
    season_label: str
    rows: List[StandingRow]


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Rugby Analytics API",
    version="0.1.0",
    description="Minimal API exposing rugby league standings from Postgres.",
)


@app.get("/health")
def health():
    """
    Simple health check. Also verifies DB connectivity.
    """
    try:
        conn = get_conn()
        conn.close()
        return {"status": "ok"}
    except Exception as e:
        # In prod you probably wouldn't expose the error string,
        # but it's useful while we're debugging.
        return {"status": "error", "detail": str(e)}


# ---------------------------------------------------------------------------
# Helper functions for /standings
# ---------------------------------------------------------------------------

def _resolve_league(cur, tsdb_league_id: int) -> dict:
    cur.execute(
        """
        SELECT league_id, tsdb_league_id, name
        FROM leagues
        WHERE tsdb_league_id = %s
        """,
        (tsdb_league_id,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"No league found with tsdb_league_id={tsdb_league_id}",
        )
    return row


def _resolve_season_label(cur, league_id: int, season_label: Optional[str], latest: bool) -> str:
    """
    If season_label is provided, validate it exists for this league.
    If latest is True (or no season_label is provided), pick the max year.
    """
    if season_label:
        cur.execute(
            """
            SELECT label
            FROM seasons
            WHERE league_id = %s
              AND label = %s
            """,
            (league_id, season_label),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(
                status_code=404,
                detail=f"Season '{season_label}' not found for league_id={league_id}",
            )
        return row["label"]

    # No season_label provided -> pick latest by year
    cur.execute(
        """
        SELECT label
        FROM seasons
        WHERE league_id = %s
        ORDER BY year DESC
        LIMIT 1
        """,
        (league_id,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"No seasons found for league_id={league_id}",
        )
    return row["label"]


# ---------------------------------------------------------------------------
# /standings endpoint
# ---------------------------------------------------------------------------

@app.get(
    "/standings/{tsdb_league_id}",
    response_model=StandingsResponse,
)
def get_standings(
    tsdb_league_id: int,
    latest: bool = Query(
        False,
        description="If true, use the latest season for this league.",
    ),
    season_label: Optional[str] = Query(
        None,
        description="Explicit season label, e.g. '2025-2026'. Overrides latest if provided.",
    ),
):
    """
    Get league table (standings) for a given TSDB league id and season.
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        # 1) Resolve league_id from tsdb_league_id
        league = _resolve_league(cur, tsdb_league_id)
        league_id = league["league_id"]
        league_name = league["name"]

        # 2) Resolve which season label we are using
        season_label_resolved = _resolve_season_label(
            cur,
            league_id=league_id,
            season_label=season_label,
            latest=latest or (season_label is None),
        )

        # 3) Load the standings rows from team_season_stats
        cur.execute(
            """
            SELECT
                s.label AS season_label,
                tss.team_id,
                t.name AS team_name,
                tss.games_played,
                tss.wins,
                tss.draws,
                tss.losses,
                tss.points_for,
                tss.points_against,
                tss.points_diff,
                tss.competition_points,
                tss.losing_bonus_points,
                tss.try_bonus_points
            FROM team_season_stats tss
            JOIN seasons s   ON s.season_id = tss.season_id
            JOIN teams   t   ON t.team_id   = tss.team_id
            WHERE tss.league_id = %s
              AND s.label = %s
            ORDER BY
                tss.competition_points DESC,
                tss.points_diff DESC,
                tss.points_for DESC,
                t.name ASC
            """,
            (league_id, season_label_resolved),
        )
        rows = cur.fetchall()

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No standings data for tsdb_league_id={tsdb_league_id}, season='{season_label_resolved}'",
            )

        standings: List[StandingRow] = []
        for idx, r in enumerate(rows, start=1):
            standings.append(
                StandingRow(
                    position=idx,
                    team_id=r["team_id"],
                    team_name=r["team_name"],
                    season_label=r["season_label"],
                    games_played=r["games_played"],
                    wins=r["wins"],
                    draws=r["draws"],
                    losses=r["losses"],
                    points_for=r["points_for"],
                    points_against=r["points_against"],
                    points_diff=r["points_diff"],
                    competition_points=r["competition_points"],
                    losing_bonus_points=r["losing_bonus_points"],
                    try_bonus_points=r["try_bonus_points"],
                )
            )

        return StandingsResponse(
            tsdb_league_id=tsdb_league_id,
            league_name=league_name,
            season_label=season_label_resolved,
            rows=standings,
        )

    finally:
        cur.close()
        conn.close()
