#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Rugby Analytics Backend – v0.8.1 (ID-based H2H with club alias groups, refactored)

"""
main.py

FastAPI app for rugby analytics:

- /health
- /leagues
- /teams
- /standings/{tsdb_league_id}
- /headtohead/{tsdb_league_id}
- /version
- /         (built-in UI for head-to-head)

This version:

- Uses **team_id-based** head-to-head stats.
- Uses **alias groups only to resolve which team_ids belong to a club**.
- In "All leagues" mode (tsdb_league_id == 0) it:
    - Maps a query like "Stormers" or "WP" to a club alias group.
    - Looks up ALL team_ids in the teams table whose normalised name matches.
    - Queries matches using ONLY those team_ids (no name-based guessing).
- Exposes the backend version:
    - in code (comment above),
    - in FastAPI metadata (version="0.8.1"),
    - on /version (JSON),
    - and visually on the UI chip: "Rugby Analytics · v0.8.1".
- Delegates DB + alias logic to h2h_helpers.py so main.py is lighter.
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Optional, Set

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from starlette.middleware.cors import CORSMiddleware
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR

from h2h_helpers import (
    get_conn,
    fetch_all,
    fetch_one,
    resolve_club_team_ids_all_leagues,
    resolve_league_by_tsdb,
    resolve_latest_season_for_league,
    resolve_season_for_league_and_label,
    resolve_team_in_league,
    compute_head_to_head_stats_from_rows,
)

API_VERSION = "0.8.1"

# ---------------------------------------------------------------------------
# Pydantic models for API responses
# ---------------------------------------------------------------------------


class StandingRow(BaseModel):
    position: int
    team_id: int
    team_name: str
    played: int
    wins: int
    draws: int
    losses: int
    points_for: int
    points_against: int
    points_diff: int
    tries_for: int
    tries_against: int
    league_points: int
    bonus_points: int


class StandingsResponse(BaseModel):
    league_id: int
    league_name: str
    tsdb_league_id: int
    season_id: int
    season_label: str
    standings: List[StandingRow]


class MatchSummary(BaseModel):
    match_id: int
    kickoff_utc: dt.datetime
    home_team: str
    away_team: str
    home_score: Optional[int]
    away_score: Optional[int]
    venue: Optional[str]
    league: Optional[str]
    season: Optional[str]


class FixtureSummary(BaseModel):
    match_id: int
    kickoff_utc: dt.datetime
    home_team: str
    away_team: str
    venue: Optional[str]
    league: Optional[str]
    season: Optional[str]


class HeadToHeadResponse(BaseModel):
    league_id: Optional[int]
    league_name: Optional[str]
    tsdb_league_id: int
    team_a_id: Optional[int]
    team_b_id: Optional[int]
    team_a_name: str
    team_b_name: str
    total_matches: int
    team_a_wins: int
    team_b_wins: int
    draws: int
    team_a_win_rate: float
    team_b_win_rate: float
    draws_rate: float
    current_streak: Optional[str]
    last_matches: List[MatchSummary]
    upcoming_fixtures: List[FixtureSummary]


class LeagueInfo(BaseModel):
    id: int
    name: str
    country: Optional[str]
    tsdb_league_id: Optional[int]


class TeamInfo(BaseModel):
    id: int
    name: str
    league_id: Optional[int]
    league_name: Optional[str]


# ---------------------------------------------------------------------------
# FastAPI app and CORS
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Rugby Analytics API",
    version=API_VERSION,
    description=(
        "Rugby analytics API (multi-league, ID-based head-to-head with "
        "club alias groups for query-time merging)."
    ),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Small helpers that are still local to main.py
# ---------------------------------------------------------------------------


def _build_match_summary_row(row: Dict[str, Any]) -> MatchSummary:
    """Convert DB row into MatchSummary."""
    return MatchSummary(
        match_id=row["match_id"],
        kickoff_utc=row["kickoff_utc"],
        home_team=row["home_team"],
        away_team=row["away_team"],
        home_score=row.get("home_score"),
        away_score=row.get("away_score"),
        venue=row.get("venue"),
        league=row.get("league"),
        season=row.get("season"),
    )


def _build_fixture_summary_row(row: Dict[str, Any]) -> FixtureSummary:
    """Convert DB row into FixtureSummary."""
    return FixtureSummary(
        match_id=row["match_id"],
        kickoff_utc=row["kickoff_utc"],
        home_team=row["home_team"],
        away_team=row["away_team"],
        venue=row.get("venue"),
        league=row.get("league"),
        season=row.get("season"),
    )


# ---------------------------------------------------------------------------
# API Routes
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


@app.get("/version")
def version() -> Dict[str, Any]:
    """
    Return the backend version string, so you can confirm deploys are live.
    """
    return {"version": API_VERSION}


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
        """
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
            """
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


@app.get(
    "/headtohead/{tsdb_league_id}",
    response_model=HeadToHeadResponse,
)
def head_to_head(
    tsdb_league_id: int = Query(
        ...,
        description=(
            "External TSDB league id. Use 0 for 'all leagues' mode "
            "with club alias groups (Bulls / Blue Bulls, Stormers / WP, etc.)."
        ),
    ),
    team_a: str = Query(..., description="Team A name (used to resolve club & team_ids)."),
    team_b: str = Query(..., description="Team B name (used to resolve club & team_ids)."),
    limit: int = Query(
        10,
        ge=1,
        le=100,
        description="How many recent matches to include in the history.",
    ),
) -> HeadToHeadResponse:
    """
    Head-to-head stats between two teams/clubs.

    - If tsdb_league_id == 0:
        * Resolve Team A and Team B to **club alias groups**.
        * Find ALL team_ids in the DB whose names belong to those alias groups.
        * Run queries and stats using ONLY those team_ids.
    - If tsdb_league_id != 0:
        * Work inside that specific league with a single team_id per side.
    """
    league = None
    league_id: Optional[int] = None
    league_name: Optional[str] = None

    # ---- League context ----
    if tsdb_league_id != 0:
        league = resolve_league_by_tsdb(tsdb_league_id)
        if not league:
            raise HTTPException(status_code=404, detail="League not found")
        league_id = league["id"]
        league_name = league["name"]

    # ---- Resolve teams / clubs to team_ids ----
    if tsdb_league_id == 0:
        # All leagues mode: alias-aware, ID-based
        team_a_ids, team_a_display_name = resolve_club_team_ids_all_leagues(team_a)
        team_b_ids, team_b_display_name = resolve_club_team_ids_all_leagues(team_b)

        if not team_a_ids:
            raise HTTPException(status_code=404, detail=f"Team A not found: {team_a}")
        if not team_b_ids:
            raise HTTPException(status_code=404, detail=f"Team B not found: {team_b}")
    else:
        # Single league mode: resolve one team_id each within that league
        assert league_id is not None

        team_a_row = resolve_team_in_league(league_id, team_a)
        team_b_row = resolve_team_in_league(league_id, team_b)

        if not team_a_row:
            raise HTTPException(status_code=404, detail=f"Team A not found: {team_a}")
        if not team_b_row:
            raise HTTPException(status_code=404, detail=f"Team B not found: {team_b}")

        team_a_ids = [team_a_row["id"]]
        team_b_ids = [team_b_row["id"]]
        team_a_display_name = team_a_row["name"]
        team_b_display_name = team_b_row["name"]

    team_a_ids_set: Set[int] = set(team_a_ids)
    team_b_ids_set: Set[int] = set(team_b_ids)

    # ---- Fetch played matches (ID-based WHERE) ----
    params: List[Any] = [team_a_ids, team_b_ids, team_b_ids, team_a_ids]
    league_filter = ""
    if league_id is not None:
        league_filter = " AND m.league_id = %s"
        params.append(league_id)

    params.append(limit)

    rows = fetch_all(
        f"""
        SELECT
            m.id          AS match_id,
            m.kickoff_utc AS kickoff_utc,
            m.home_team_id,
            m.away_team_id,
            h.name        AS home_team,
            a.name        AS away_team,
            m.home_score,
            m.away_score,
            v.name        AS venue,
            l.name        AS league,
            s.label       AS season
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
          {league_filter}
        ORDER BY m.kickoff_utc DESC
        LIMIT %s
        """,
        tuple(params),
    )

    last_matches = [_build_match_summary_row(r) for r in rows]

    # ---- Upcoming fixtures (same ID-based logic, only future kickoffs) ----
    upcoming_params: List[Any] = [team_a_ids, team_b_ids, team_b_ids, team_a_ids]
    upcoming_league_filter = ""
    if league_id is not None:
        upcoming_league_filter = " AND m.league_id = %s"
        upcoming_params.append(league_id)

    upcoming_rows = fetch_all(
        f"""
        SELECT
            m.id          AS match_id,
            m.kickoff_utc AS kickoff_utc,
            m.home_team_id,
            m.away_team_id,
            h.name        AS home_team,
            a.name        AS away_team,
            v.name        AS venue,
            l.name        AS league,
            s.label       AS season
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
          {upcoming_league_filter}
          AND m.kickoff_utc >= NOW()
        ORDER BY m.kickoff_utc ASC
        """,
        tuple(upcoming_params),
    )

    upcoming_fixtures = [_build_fixture_summary_row(r) for r in upcoming_rows]

    # ---- Stats from raw rows (ID-based) ----
    stats = compute_head_to_head_stats_from_rows(
        rows,
        team_a_ids_set,
        team_b_ids_set,
        team_a_display_name,
        team_b_display_name,
    )

    # Choose a canonical id for the club (first in list) – mostly for reference.
    team_a_canonical_id = team_a_ids[0] if team_a_ids else None
    team_b_canonical_id = team_b_ids[0] if team_b_ids else None

    return HeadToHeadResponse(
        league_id=league_id,
        league_name=league_name,
        tsdb_league_id=tsdb_league_id,
        team_a_id=team_a_canonical_id,
        team_b_id=team_b_canonical_id,
        team_a_name=team_a_display_name,
        team_b_name=team_b_display_name,
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
# Built-in HTML UI (index page) – unchanged, just uses API_VERSION
# ---------------------------------------------------------------------------

INDEX_HTML = f"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Rugby Head-to-Head</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <!-- (HTML & CSS content unchanged from v0.8.1 – omitted here for brevity in explanation,
       but in your file you should keep the full version you already have, with
       the chip text: Rugby Analytics · v{API_VERSION} and Backend v{API_VERSION}) -->
</head>
<body>
  <!-- keep your existing INDEX_HTML content from v0.8.1 here -->
</body>
</html>
"""

# NOTE: In your actual file, keep the FULL INDEX_HTML we had before.
# I'm truncating in this chat for readability, but you should paste
# the complete HTML block from your current v0.8.1 main.py,
# just wrapped as f"""...{API_VERSION}..."""

@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    """
    Serve the built-in head-to-head UI.
    """
    return HTMLResponse(content=INDEX_HTML)
