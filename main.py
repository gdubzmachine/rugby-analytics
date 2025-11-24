#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
main.py

FastAPI app wiring for rugby analytics:

- /health
- /leagues
- /teams
- /standings/{tsdb_league_id}
- /headtohead/{tsdb_league_id}
- /         (built-in HTML UI)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import Response
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR

from db import (
    get_conn,
    fetch_all,
    fetch_one,
    resolve_league_by_tsdb,
    resolve_latest_season_for_league,
    resolve_season_for_league_and_label,
)
from models import (
    FixtureSummary,
    HeadToHeadResponse,
    LeagueInfo,
    MatchSummary,
    StandingRow,
    StandingsResponse,
    TeamInfo,
)
from h2h_helpers import (
    build_fixture_summary_row,
    build_match_summary_row,
    compute_head_to_head_stats,
    resolve_team_global,
    resolve_team_in_league,
    expand_team_ids_for_club,
)
from index_html import INDEX_HTML


# ---------------------------------------------------------------------------
# FastAPI app and CORS
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Rugby Analytics API",
    version="0.6.0",
    description="Simple rugby analytics API (URC + multi-league head-to-head).",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Healthcheck
# ---------------------------------------------------------------------------


@app.get("/health")
def health_check() -> Dict[str, Any]:
    """
    Simple health check endpoint.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return {"status": "ok"}
    except Exception as exc:  # pragma: no cover
        return JSONResponse(
            status_code=HTTP_500_INTERNAL_SERVER_ERROR,
            content={"status": "error", "detail": str(exc)},
        )


# ---------------------------------------------------------------------------
# Leagues / teams
# ---------------------------------------------------------------------------


@app.get("/leagues", response_model=List[LeagueInfo])
def list_leagues() -> List[LeagueInfo]:
    """
    Return all leagues that have tsdb_league_id (i.e., rugby leagues catalogued).
    """
    rows = fetch_all(
        """
        SELECT id, name, country, tsdb_league_id
        FROM leagues
        WHERE tsdb_league_id IS NOT NULL
        ORDER BY country NULLS LAST, name
        """,
        (),
    )
    return [LeagueInfo(**row) for row in rows]


@app.get("/teams", response_model=List[TeamInfo])
def list_teams(
    league_id: Optional[int] = Query(
        None,
        description="Filter to a specific league_id. If omitted, returns all teams.",
    )
) -> List[TeamInfo]:
    """
    Return teams, optionally filtered by league_id.

    If league_id is provided, we look at teams that have at least one season in that league.
    """
    if league_id is None:
        rows = fetch_all(
            """
            SELECT t.id,
                   t.name,
                   NULL::integer AS league_id,
                   NULL::text    AS league_name
            FROM teams t
            ORDER BY t.name
            """,
            (),
        )
    else:
        rows = fetch_all(
            """
            SELECT DISTINCT
                t.id,
                t.name,
                s.league_id,
                l.name AS league_name
            FROM teams t
            JOIN league_team_seasons lts
              ON lts.team_id = t.id
            JOIN seasons s
              ON s.id = lts.season_id
            JOIN leagues l
              ON l.id = s.league_id
            WHERE s.league_id = %s
            ORDER BY t.name
            """,
            (league_id,),
        )

    return [TeamInfo(**row) for row in rows]


# ---------------------------------------------------------------------------
# Standings
# ---------------------------------------------------------------------------


@app.get("/standings/{tsdb_league_id}", response_model=StandingsResponse)
def get_standings(
    tsdb_league_id: int,
    season_label: Optional[str] = Query(
        None,
        description="Season label (e.g. '2023-2024'). If omitted, use the latest season.",
    ),
) -> StandingsResponse:
    """
    Returns league table standings for a given tsdb_league_id and season.
    """
    league = resolve_league_by_tsdb(tsdb_league_id)
    if not league:
        raise HTTPException(status_code=404, detail="League not found")

    league_id = league["id"]

    if season_label:
        season = resolve_season_for_league_and_label(league_id, season_label)
        if not season:
            raise HTTPException(
                status_code=404,
                detail=f"Season '{season_label}' not found for league.",
            )
    else:
        season = resolve_latest_season_for_league(league_id)
        if not season:
            raise HTTPException(
                status_code=404,
                detail="No seasons found for this league.",
            )

    season_id = season["id"]

    rows = fetch_all(
        """
        SELECT
            t.id AS team_id,
            t.name AS team_name,
            s.played,
            s.wins,
            s.draws,
            s.losses,
            s.points_for,
            s.points_against,
            s.points_diff,
            s.tries_for,
            s.tries_against,
            s.league_points,
            s.bonus_points
        FROM team_season_stats s
        JOIN teams t
          ON t.id = s.team_id
        WHERE s.season_id = %s
        ORDER BY s.league_points DESC,
                 s.points_diff DESC,
                 t.name
        """,
        (season_id,),
    )

    standings: List[StandingRow] = []
    for idx, row in enumerate(rows, start=1):
        standings.append(
            StandingRow(
                position=idx,
                team_id=row["team_id"],
                team_name=row["team_name"],
                played=row["played"],
                wins=row["wins"],
                draws=row["draws"],
                losses=row["losses"],
                points_for=row["points_for"],
                points_against=row["points_against"],
                points_diff=row["points_diff"],
                tries_for=row["tries_for"],
                tries_against=row["tries_against"],
                league_points=row["league_points"],
                bonus_points=row["bonus_points"],
            )
        )

    return StandingsResponse(
        league_id=league_id,
        league_name=league["name"],
        tsdb_league_id=tsdb_league_id,
        season_id=season_id,
        season_label=season["label"],
        standings=standings,
    )


# ---------------------------------------------------------------------------
# Head-to-head
# ---------------------------------------------------------------------------


@app.get(
    "/headtohead/{tsdb_league_id}",
    response_model=HeadToHeadResponse,
)
def head_to_head(
    tsdb_league_id: int = Query(
        ...,
        description=(
            "External TSDB league id. Use 0 for 'all leagues' mode "
            "with club alias groups."
        ),
    ),
    team_a: str = Query(..., description="Team A name (fuzzy match)."),
    team_b: str = Query(..., description="Team B name (fuzzy match)."),
    limit: int = Query(
        10,
        ge=1,
        le=100,
        description="How many recent matches to include in the history.",
    ),
) -> HeadToHeadResponse:
    """
    Head-to-head stats between two teams.

    If tsdb_league_id == 0, we allow matches across all leagues and
    use alias groups to unify clubs across competitions.
    """
    league = None
    league_id = None
    league_name = None

    if tsdb_league_id != 0:
        league = resolve_league_by_tsdb(tsdb_league_id)
        if not league:
            raise HTTPException(status_code=404, detail="League not found")
        league_id = league["id"]
        league_name = league["name"]

    # Resolve teams
    if tsdb_league_id == 0:
        team_a_row = resolve_team_global(team_a)
        team_b_row = resolve_team_global(team_b)
    else:
        team_a_row = resolve_team_in_league(league_id, team_a)
        team_b_row = resolve_team_in_league(league_id, team_b)

    if not team_a_row:
        raise HTTPException(status_code=404, detail=f"Team A not found: {team_a}")
    if not team_b_row:
        raise HTTPException(status_code=404, detail=f"Team B not found: {team_b}")

    team_a_id = team_a_row["id"]
    team_b_id = team_b_row["id"]

    # In "all leagues" mode, expand each club to all alias team IDs
    if tsdb_league_id == 0:
        team_a_ids = expand_team_ids_for_club(team_a_id, team_a_row["name"])
        team_b_ids = expand_team_ids_for_club(team_b_id, team_b_row["name"])
    else:
        team_a_ids = [team_a_id]
        team_b_ids = [team_b_id]

    # ---- Played matches ----
    if tsdb_league_id == 0:
        # Any league, alias-aware
        rows = fetch_all(
            """
            SELECT
                m.id AS match_id,
                m.kickoff_utc,
                h.name AS home_team,
                a.name AS away_team,
                m.home_score,
                m.away_score,
                v.name AS venue,
                l.name AS league,
                s.label AS season
            FROM matches m
            JOIN teams h
              ON h.id = m.home_team_id
            JOIN teams a
              ON a.id = m.away_team_id
            LEFT JOIN venues v
              ON v.id = m.venue_id
            LEFT JOIN seasons s
              ON s.id = m.season_id
            LEFT JOIN leagues l
              ON l.id = m.league_id
            WHERE
              (
                (m.home_team_id = ANY(%s) AND m.away_team_id = ANY(%s)) OR
                (m.home_team_id = ANY(%s) AND m.away_team_id = ANY(%s))
              )
            ORDER BY m.kickoff_utc DESC
            LIMIT %s
            """,
            (team_a_ids, team_b_ids, team_b_ids, team_a_ids, limit),
        )
    else:
        # Single league, simple IDs + league filter
        rows = fetch_all(
            """
            SELECT
                m.id AS match_id,
                m.kickoff_utc,
                h.name AS home_team,
                a.name AS away_team,
                m.home_score,
                m.away_score,
                v.name AS venue,
                l.name AS league,
                s.label AS season
            FROM matches m
            JOIN teams h
              ON h.id = m.home_team_id
            JOIN teams a
              ON a.id = m.away_team_id
            LEFT JOIN venues v
              ON v.id = m.venue_id
            LEFT JOIN seasons s
              ON s.id = m.season_id
            LEFT JOIN leagues l
              ON l.id = m.league_id
            WHERE
              (
                (m.home_team_id = %s AND m.away_team_id = %s) OR
                (m.home_team_id = %s AND m.away_team_id = %s)
              )
              AND m.league_id = %s
            ORDER BY m.kickoff_utc DESC
            LIMIT %s
            """,
            (team_a_id, team_b_id, team_b_id, team_a_id, league_id, limit),
        )

    last_matches = [build_match_summary_row(r) for r in rows]

    # ---- Upcoming fixtures ----
    if tsdb_league_id == 0:
        upcoming_rows = fetch_all(
            """
            SELECT
                m.id AS match_id,
                m.kickoff_utc,
                h.name AS home_team,
                a.name AS away_team,
                v.name AS venue,
                l.name AS league,
                s.label AS season
            FROM matches m
            JOIN teams h
              ON h.id = m.home_team_id
            JOIN teams a
              ON a.id = m.away_team_id
            LEFT JOIN venues v
              ON v.id = m.venue_id
            LEFT JOIN seasons s
              ON s.id = m.season_id
            LEFT JOIN leagues l
              ON l.id = m.league_id
            WHERE
              (
                (m.home_team_id = ANY(%s) AND m.away_team_id = ANY(%s)) OR
                (m.home_team_id = ANY(%s) AND m.away_team_id = ANY(%s))
              )
              AND m.kickoff_utc >= NOW()
            ORDER BY m.kickoff_utc ASC
            """,
            (team_a_ids, team_b_ids, team_b_ids, team_a_ids),
        )
    else:
        upcoming_rows = fetch_all(
            """
            SELECT
                m.id AS match_id,
                m.kickoff_utc,
                h.name AS home_team,
                a.name AS away_team,
                v.name AS venue,
                l.name AS league,
                s.label AS season
            FROM matches m
            JOIN teams h
              ON h.id = m.home_team_id
            JOIN teams a
              ON a.id = m.away_team_id
            LEFT JOIN venues v
              ON v.id = m.venue_id
            LEFT JOIN seasons s
              ON s.id = m.season_id
            LEFT JOIN leagues l
              ON l.id = m.league_id
            WHERE
              (
                (m.home_team_id = %s AND m.away_team_id = %s) OR
                (m.home_team_id = %s AND m.away_team_id = %s)
              )
              AND m.league_id = %s
              AND m.kickoff_utc >= NOW()
            ORDER BY m.kickoff_utc ASC
            """,
            (team_a_id, team_b_id, team_b_id, team_a_id, league_id),
        )

    upcoming_fixtures = [build_fixture_summary_row(r) for r in upcoming_rows]

    stats = compute_head_to_head_stats(
        last_matches, team_a_row["name"], team_b_row["name"]
    )

    return HeadToHeadResponse(
        league_id=league_id,
        league_name=league_name,
        tsdb_league_id=tsdb_league_id,
        team_a_id=team_a_id,
        team_b_id=team_b_id,
        team_a_name=team_a_row["name"],
        team_b_name=team_b_row["name"],
        total_matches=stats["total"],
        team_a_wins=stats["team_a_wins"],
        team_b_wins=stats["team_b_wins"],
        draws=stats["draws"],
        team_a_win_rate=stats["team_a_rate"],
        team_b_win_rate=stats["team_b_rate"],
        draws_rate=stats["draw_rate"],
        current_streak=stats["current_streak"],
        last_matches=last_matches,
        upcoming_fixtures=upcoming_fixtures,
    )


# ---------------------------------------------------------------------------
# Built-in HTML UI (index page)
# ---------------------------------------------------------------------------


@app.get("/", response_class=HTMLResponse)
def index() -> Response:
    """
    Serve the built-in head-to-head UI.
    """
    return HTMLResponse(content=INDEX_HTML)
