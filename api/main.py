#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
api/main.py
-----------

FastAPI app for rugby analytics:

- /health
- /standings/{tsdb_league_id}
- /headtohead/{tsdb_league_id}

It expects the following tables:

- sports
- leagues (with tsdb_league_id)
- seasons  (with league_id, year, label)
- teams
- matches
- team_season_stats

It uses DATABASE_URL from the environment (.env locally, Render env vars in prod),
falling back to a hard-coded Render URL for now.
"""

import os
from pathlib import Path
from typing import List, Optional

from datetime import datetime

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

from pydantic import BaseModel
from dotenv import load_dotenv

import psycopg2
from psycopg2.extras import RealDictCursor

# ---------------------------------------------------------------------------
# Environment / DB helpers
# ---------------------------------------------------------------------------

load_dotenv()  # locally; no-op on Render

# Fallback DB URL for Render if DATABASE_URL env var is missing.
DEFAULT_DB_URL = (
    "postgresql://rugby_analytics_user:"
    "a5tDWnLOBdGEqSQGEcEjfiXaSbIlFksT"
    "@dpg-d4grdqili9vc73dqbtf0-a.oregon-postgres.render.com"
    "/rugby_analytics"
)

DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DB_URL)


def get_conn():
    """
    Always try to connect using DATABASE_URL if set,
    otherwise fall back to DEFAULT_DB_URL.
    """
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


class MatchSummary(BaseModel):
    match_id: int
    season_label: str
    kickoff_utc: Optional[datetime]
    home_team_id: int
    home_team_name: str
    away_team_id: int
    away_team_name: str
    home_score: Optional[int]
    away_score: Optional[int]
    winner: Optional[str]


class HeadToHeadResponse(BaseModel):
    tsdb_league_id: int
    league_name: str
    team_a_id: int
    team_a_name: str
    team_b_id: int
    team_b_name: str
    total_matches: int
    team_a_wins: int
    team_b_wins: int
    draws: int
    team_a_win_rate: float
    team_b_win_rate: float
    current_streak_type: Optional[str]
    current_streak_length: int
    last_n: List[MatchSummary]


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Rugby Analytics API",
    version="0.2.0",
    description="API exposing rugby standings and head-to-head stats.",
)

# Serve a simple UI from / (index.html in ./static)
BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/", response_class=HTMLResponse)
def index():
    index_path = STATIC_DIR / "index.html"
    if not index_path.exists():
        return HTMLResponse(
            "<h1>Rugby Analytics</h1><p>Static UI not found yet. "
            "Create static/index.html on the server.</p>",
            status_code=200,
        )
    return index_path.read_text(encoding="utf-8")


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
# Helper functions
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


def _resolve_season_label(cur, league_id: int, season_label: Optional[str]) -> str:
    """
    If season_label is provided, validate it exists for this league.
    If it is None, pick the latest season by year.
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


def _resolve_team_in_league(cur, league_id: int, name_query: str) -> dict:
    """
    Find a team in this league by fuzzy name match.
    We restrict to teams that appear in team_season_stats for this league.
    """
    cur.execute(
        """
        SELECT DISTINCT t.team_id, t.name
        FROM teams t
        JOIN team_season_stats tss
          ON tss.team_id = t.team_id
        WHERE tss.league_id = %s
          AND t.name ILIKE %s
        ORDER BY t.name
        """,
        (league_id, f"%{name_query}%"),
    )
    rows = cur.fetchall()
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No team found in league_id={league_id} matching '{name_query}'",
        )
    if len(rows) > 1:
        # For now we just pick the first; UI can be improved later
        # to handle multiple matches.
        pass
    return rows[0]


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
            season_label=season_label if season_label else None,
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


# ---------------------------------------------------------------------------
# /headtohead endpoint
# ---------------------------------------------------------------------------

@app.get(
    "/headtohead/{tsdb_league_id}",
    response_model=HeadToHeadResponse,
)
def get_headtohead(
    tsdb_league_id: int,
    team_a: str = Query(..., description="Name (or part) of Team A"),
    team_b: str = Query(..., description="Name (or part) of Team B"),
    limit: int = Query(10, ge=1, le=100, description="Number of recent matches to consider"),
):
    """
    Head-to-head stats between two teams in a given league:

    - Last N matches (up to `limit`)
    - Total wins/draws
    - Win rates
    - Current streak in the matchup
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        # Resolve league
        league = _resolve_league(cur, tsdb_league_id)
        league_id = league["league_id"]
        league_name = league["name"]

        # Resolve teams (fuzzy match within this league)
        team_a_row = _resolve_team_in_league(cur, league_id, team_a)
        team_b_row = _resolve_team_in_league(cur, league_id, team_b)

        team_a_id = team_a_row["team_id"]
        team_a_name = team_a_row["name"]
        team_b_id = team_b_row["team_id"]
        team_b_name = team_b_row["name"]

        # Load last N matches between these two teams in this league
        cur.execute(
            """
            SELECT
                m.match_id,
                s.label AS season_label,
                m.kickoff_utc,
                ht.team_id AS home_team_id,
                ht.name    AS home_team_name,
                at.team_id AS away_team_id,
                at.name    AS away_team_name,
                m.home_score,
                m.away_score
            FROM matches m
            JOIN seasons s ON s.season_id = m.season_id
            JOIN teams ht ON ht.team_id = m.home_team_id
            JOIN teams at ON at.team_id = m.away_team_id
            WHERE m.league_id = %s
              AND (
                    (m.home_team_id = %s AND m.away_team_id = %s) OR
                    (m.home_team_id = %s AND m.away_team_id = %s)
                  )
            ORDER BY m.kickoff_utc DESC
            LIMIT %s
            """,
            (league_id, team_a_id, team_b_id, team_b_id, team_a_id, limit),
        )
        rows = cur.fetchall()

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"No matches found between '{team_a_name}' and '{team_b_name}' "
                    f"in tsdb_league_id={tsdb_league_id}"
                ),
            )

        # Compute stats
        total_matches = 0
        team_a_wins = 0
        team_b_wins = 0
        draws = 0
        match_summaries: List[MatchSummary] = []

        # For current streak we process from most recent backward
        streak_type: Optional[str] = None  # "team_a_win", "team_b_win", "draw"
        streak_length = 0

        for idx, r in enumerate(rows):
            total_matches += 1
            hs = r["home_score"]
            as_ = r["away_score"]

            winner: Optional[str] = None
            result_flag: Optional[str] = None

            if hs is not None and as_ is not None:
                if hs > as_:
                    winner = "home"
                elif as_ > hs:
                    winner = "away"
                else:
                    winner = "draw"

                # Map to team_a / team_b result
                if winner == "draw":
                    draws += 1
                    result_flag = "draw"
                else:
                    home_is_a = (r["home_team_id"] == team_a_id)
                    away_is_a = (r["away_team_id"] == team_a_id)

                    if winner == "home":
                        if home_is_a:
                            team_a_wins += 1
                            result_flag = "team_a_win"
                        else:
                            team_b_wins += 1
                            result_flag = "team_b_win"
                    elif winner == "away":
                        if away_is_a:
                            team_a_wins += 1
                            result_flag = "team_a_win"
                        else:
                            team_b_wins += 1
                            result_flag = "team_b_win"

            # Build match summary
            kickoff = r["kickoff_utc"]
            if isinstance(kickoff, str):
                try:
                    kickoff = datetime.fromisoformat(kickoff)
                except Exception:
                    kickoff = None

            match_summaries.append(
                MatchSummary(
                    match_id=r["match_id"],
                    season_label=r["season_label"],
                    kickoff_utc=kickoff,
                    home_team_id=r["home_team_id"],
                    home_team_name=r["home_team_name"],
                    away_team_id=r["away_team_id"],
                    away_team_name=r["away_team_name"],
                    home_score=hs,
                    away_score=as_,
                    winner=winner,
                )
            )

            # Update current streak (first rows are most recent)
            if idx == 0:
                # Initialize streak with first result
                streak_type = result_flag
                streak_length = 1 if result_flag is not None else 0
            else:
                if result_flag is not None and result_flag == streak_type:
                    streak_length += 1
                else:
                    # streak broken
                    break

        # Compute win rates
        if total_matches > 0:
            team_a_win_rate = team_a_wins / total_matches
            team_b_win_rate = team_b_wins / total_matches
        else:
            team_a_win_rate = 0.0
            team_b_win_rate = 0.0

        return HeadToHeadResponse(
            tsdb_league_id=tsdb_league_id,
            league_name=league_name,
            team_a_id=team_a_id,
            team_a_name=team_a_name,
            team_b_id=team_b_id,
            team_b_name=team_b_name,
            total_matches=total_matches,
            team_a_wins=team_a_wins,
            team_b_wins=team_b_wins,
            draws=draws,
            team_a_win_rate=team_a_win_rate,
            team_b_win_rate=team_b_win_rate,
            current_streak_type=streak_type,
            current_streak_length=streak_length,
            last_n=match_summaries,
        )

    finally:
        cur.close()
        conn.close()
