#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Rugby Analytics Backend – v0.8.0 (ID-based H2H with club alias groups)

"""
main.py

FastAPI app for rugby analytics:

- /health
- /leagues
- /teams
- /standings/{tsdb_league_id}
- /headtohead/{tsdb_league_id}
- /         (built-in UI for head-to-head)

This version:

- Uses **team_id-based** head-to-head stats again.
- Uses **alias groups only to resolve which team_ids belong to a club**.
- In "All leagues" mode (tsdb_league_id == 0) it:
    - Maps a query like "Stormers" or "WP" to a club alias group:
        {"stormers", "western province", "wp", "western stormers", "dhl stormers"}
    - Looks up ALL team_ids in the teams table whose normalised name
      equals any of those aliases.
    - Queries matches using ONLY those team_ids (no name-based guessing).
- Does NOT use substring logic on names to decide wins – only team_ids.
"""

from __future__ import annotations

import datetime as dt
import os
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from starlette.middleware.cors import CORSMiddleware
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Environment / DB helpers
# ---------------------------------------------------------------------------

load_dotenv()  # Locally: load .env. On Render this is a no-op.


def get_conn():
    """Open a Postgres connection using the DATABASE_URL environment variable."""
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "DATABASE_URL is not set. Configure it in your .env for local dev, "
            "or as a Render env var in production."
        )
    return psycopg2.connect(dsn, cursor_factory=RealDictCursor)


def fetch_one(query: str, params: Tuple[Any, ...]) -> Optional[Dict[str, Any]]:
    """Run a query that returns a single row (or None)."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            row = cur.fetchone()
    return row


def fetch_all(query: str, params: Tuple[Any, ...]) -> List[Dict[str, Any]]:
    """Run a query that returns multiple rows."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
    return list(rows)


# ---------------------------------------------------------------------------
# "Club" alias groups (Stormers + Western Province, etc.)
# Used ONLY to decide which team_ids to include in queries.
# Stats & results are purely team_id-based.
# ---------------------------------------------------------------------------

# Each set is a group of names that should be treated as the SAME club
# when tsdb_league_id == 0 (ALL leagues mode).
CLUB_ALIAS_GROUPS: List[Set[str]] = [
    # South African clubs – extended aliases as requested
    {"bulls", "blue bulls", "northern transvaal", "vodacom bulls", "pretoria bulls"},
    {"stormers", "western province", "wp", "western stormers", "dhl stormers"},
    {"sharks", "natal sharks", "natal", "sharks xv", "cell c sharks", "hollywoodbets sharks"},
    {"lions", "golden lions", "emirates lions", "mtn golden lions", "transvaal"},
    {"cheetahs", "free state cheetahs", "toyota cheetahs"},

    # Existing groups for other clubs / competitions (left as-is)
    {"munster"},
    {"leinster"},
    {"ulster"},
    {"connacht"},
    {"glasgow", "glasgow warriors"},
    {"edinburgh"},
    {"cardiff", "cardiff blues"},
    {"dragons", "newport gwent dragons"},
    {"scarlets", "llanelli scarlets"},
    {"ospreys"},
    {"benetton", "benetton treviso"},
    {"zebre", "zebre parma", "zebre rugby club"},
    {"waratahs", "nsw waratahs"},
    {"brumbies"},
    {"reds", "queensland reds"},
    {"rebels"},
    {"force", "western force"},
    {"blues", "auckland blues"},
    {"chiefs", "waikato chiefs"},
    {"crusaders"},
    {"highlanders"},
    {"hurricanes"},
    {"harlequins", "quins"},
    {"saracens"},
    {"exeter", "exeter chiefs"},
    {"leicester", "leicester tigers"},
    {"northampton", "northampton saints"},
    {"bath"},
    {"sale", "sale sharks"},
    {"gloucester"},
    {"bristol"},
    {"newcastle", "newcastle falcons"},
    {"wasps"},
    {"worcester"},
    {"bordeaux", "union bordeaux-bègles", "bordeaux-begles"},
    {"toulouse", "stade toulousain"},
    {"clermont", "clermont auvergne", "asm clermont"},
    {"racing 92", "racing metro"},
    {"toulon"},
    {"la rochelle"},
    {"lyon"},
    {"castres"},
    {"brive"},
    {"pau"},
    {"montpellier"},
    {"bayonne"},
    {"perpignan"},
    {"agen"},
    {"colomiers"},
    {"narbonne"},
    {"beziers"},
    {"dax"},
]


def normalise_name(name: str) -> str:
    """Lowercase and strip punctuation-ish noise for equality matching."""
    import re

    name = name.lower()
    name = re.sub(r"[^\w\s]", "", name)  # remove punctuation
    name = re.sub(r"\s+", " ", name).strip()
    return name


def find_alias_group(name: str) -> Optional[Set[str]]:
    """
    Return the alias group that contains `name` (by normalised equality), or None.

    This is used to decide *which club* a query like "Stormers" or "WP"
    refers to. It does NOT do substring checks; we want to be conservative.
    """
    norm = normalise_name(name)
    for group in CLUB_ALIAS_GROUPS:
        if norm in {normalise_name(x) for x in group}:
            return group
    return None


def resolve_club_team_ids_all_leagues(team_name: str) -> Tuple[List[int], str]:
    """
    For tsdb_league_id == 0:

    - If team_name belongs to an alias group, find ALL team_ids whose
      normalised name matches any alias in that group.
    - If nothing matches, fall back to a single global team lookup.
    - Returns (team_ids, representative_display_name).

    This is *club → team_ids* logic. Everything else stays team_id-based.
    """
    alias_group = find_alias_group(team_name)
    if alias_group:
        group_norms = {normalise_name(x) for x in alias_group}

        rows = fetch_all("SELECT id, name FROM teams", ())
        club_rows: List[Dict[str, Any]] = [
            r for r in rows if normalise_name(r["name"]) in group_norms
        ]

        if club_rows:
            ids = [r["id"] for r in club_rows]
            # Use the first DB name as "nice" display (e.g. 'Stormers' or 'DHL Stormers')
            rep_name = club_rows[0]["name"]
            return ids, rep_name

    # Fallback: no alias group or nothing matched in DB → just pick one team globally
    row = _resolve_team_global(team_name)
    if not row:
        return [], team_name
    return [row["id"]], row["name"]


# ---------------------------------------------------------------------------
# League / season / team resolution helpers
# ---------------------------------------------------------------------------


def resolve_league_by_tsdb(tsdb_league_id: int) -> Optional[Dict[str, Any]]:
    """Resolve an internal league row by tsdb_league_id."""
    row = fetch_one(
        """
        SELECT id, name, tsdb_league_id
        FROM leagues
        WHERE tsdb_league_id = %s
        """,
        (tsdb_league_id,),
    )
    return row


def resolve_latest_season_for_league(league_id: int) -> Optional[Dict[str, Any]]:
    """Return the most recent season row for a given league."""
    row = fetch_one(
        """
        SELECT id, label, year, start_date, end_date
        FROM seasons
        WHERE league_id = %s
        ORDER BY start_date DESC NULLS LAST, year DESC
        LIMIT 1
        """,
        (league_id,),
    )
    return row


def resolve_season_for_league_and_label(
    league_id: int, season_label: str
) -> Optional[Dict[str, Any]]:
    """
    Resolve a season row by league_id and label (e.g. '2023-2024' or '2024').
    """
    row = fetch_one(
        """
        SELECT id, label, year, start_date, end_date
        FROM seasons
        WHERE league_id = %s
          AND label = %s
        LIMIT 1
        """,
        (league_id, season_label),
    )
    return row


def _resolve_team_in_league(league_id: int, team_name: str) -> Optional[Dict[str, Any]]:
    """
    Resolve a team by name within a specific league:
    - tries exact match
    - then ILIKE %name%
    """
    row = fetch_one(
        """
        SELECT t.id, t.name
        FROM teams t
        JOIN league_team_seasons lts
          ON lts.team_id = t.id
        JOIN seasons s
          ON s.id = lts.season_id
        WHERE s.league_id = %s
          AND LOWER(t.name) = LOWER(%s)
        LIMIT 1
        """,
        (league_id, team_name),
    )
    if row:
        return row

    row = fetch_one(
        """
        SELECT t.id, t.name
        FROM teams t
        JOIN league_team_seasons lts
          ON lts.team_id = t.id
        JOIN seasons s
          ON s.id = lts.season_id
        JOIN leagues l
          ON l.id = s.league_id
        WHERE s.league_id = %s
          AND t.name ILIKE %s
        LIMIT 1
        """,
        (league_id, f"%{team_name}%"),
    )
    return row


def _resolve_team_global(team_name: str) -> Optional[Dict[str, Any]]:
    """
    Simpler global team resolve.

    We try:
    - direct LOWER(name) match
    - ILIKE %name%
    """
    row = fetch_one(
        """
        SELECT id, name
        FROM teams
        WHERE LOWER(name) = LOWER(%s)
        ORDER BY name
        LIMIT 1
        """,
        (team_name,),
    )
    if row:
        return row

    row = fetch_one(
        """
        SELECT id, name
        FROM teams
        WHERE name ILIKE %s
        ORDER BY name
        LIMIT 1
        """,
        (f"%{team_name}%",),
    )
    return row


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
    version="0.8.0",
    description=(
        "Rugby analytics API (multi-league, ID-based head-to-head with "
        "club alias groups for query-time merging)."
    ),
)

# Allow all origins for now (you can tighten this later).
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Helper functions for matches & stats (ID-based logic)
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


def _compute_head_to_head_stats_from_rows(
    rows: List[Dict[str, Any]],
    team_a_ids: Set[int],
    team_b_ids: Set[int],
    team_a_name: str,
    team_b_name: str,
) -> Dict[str, Any]:
    """
    Compute win/draw counts, win rates, and current streak from raw DB rows.

    IMPORTANT:
    - We only look at **team_ids** to decide which side is Team A / Team B.
    - No name-based substring magic here.
    """

    total = 0
    team_a_wins = 0
    team_b_wins = 0
    draws = 0

    for r in rows:
        home_id = r["home_team_id"]
        away_id = r["away_team_id"]
        home_score = r.get("home_score")
        away_score = r.get("away_score")

        if home_score is None or away_score is None:
            continue

        a_home = home_id in team_a_ids
        a_away = away_id in team_a_ids
        b_home = home_id in team_b_ids
        b_away = away_id in team_b_ids

        # Must involve both clubs somewhere
        if not ((a_home or a_away) and (b_home or b_away)):
            continue

        total += 1

        if home_score > away_score:
            winner = "home"
        elif home_score < away_score:
            winner = "away"
        else:
            winner = None

        if winner is None:
            draws += 1
        else:
            if winner == "home":
                if a_home:
                    team_a_wins += 1
                elif b_home:
                    team_b_wins += 1
            else:  # away
                if a_away:
                    team_a_wins += 1
                elif b_away:
                    team_b_wins += 1

    def _rate(x: int) -> float:
        return round(100.0 * x / total, 1) if total > 0 else 0.0

    team_a_rate = _rate(team_a_wins)
    team_b_rate = _rate(team_b_wins)
    draw_rate = _rate(draws)

    # Current streak = most recent match in rows that involves both clubs
    current_streak = None
    for r in rows:
        home_id = r["home_team_id"]
        away_id = r["away_team_id"]
        home_score = r.get("home_score")
        away_score = r.get("away_score")

        if home_score is None or away_score is None:
            continue

        a_home = home_id in team_a_ids
        a_away = away_id in team_a_ids
        b_home = home_id in team_b_ids
        b_away = away_id in team_b_ids

        if not ((a_home or a_away) and (b_home or b_away)):
            continue

        if home_score == away_score:
            current_streak = "Draw"
        else:
            if home_score > away_score:
                winner_is_home = True
            else:
                winner_is_home = False

            if winner_is_home:
                if a_home:
                    current_streak = f"{team_a_name} win"
                elif b_home:
                    current_streak = f"{team_b_name} win"
            else:
                if a_away:
                    current_streak = f"{team_a_name} win"
                elif b_away:
                    current_streak = f"{team_b_name} win"
        break

    return {
        "total": total,
        "team_a_wins": team_a_wins,
        "team_b_wins": team_b_wins,
        "draws": draws,
        "team_a_rate": team_a_rate,
        "team_b_rate": team_b_rate,
        "draw_rate": draw_rate,
        "current_streak": current_streak,
    }


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

    Behaviour:

    - If tsdb_league_id == 0:
        * Resolve Team A and Team B to **club alias groups** (Bulls / Stormers / etc.).
        * Find ALL team_ids in the DB whose names belong to those alias groups.
        * Run queries and stats using ONLY those team_ids (no name-based guesses).
        * This gives combined "club" stats across all competitions.

    - If tsdb_league_id != 0:
        * Work inside that specific league.
        * Resolve a single team_id for each side and use that.
        * Still purely team_id-based.
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

        team_a_row = _resolve_team_in_league(league_id, team_a)
        team_b_row = _resolve_team_in_league(league_id, team_b)

        if not team_a_row:
            raise HTTPException(status_code=404, detail=f"Team A not found: {team_a}")
        if not team_b_row:
            raise HTTPException(status_code=404, detail=f"Team B not found: {team_b}")

        team_a_ids = [team_a_row["id"]]
        team_b_ids = [team_b_row["id"]]
        team_a_display_name = team_a_row["name"]
        team_b_display_name = team_b_row["name"]

    team_a_ids_set = set(team_a_ids)
    team_b_ids_set = set(team_b_ids)

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
    stats = _compute_head_to_head_stats_from_rows(
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
# Built-in HTML UI (index page)
# ---------------------------------------------------------------------------

INDEX_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Rugby Head-to-Head</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root {
      color-scheme: dark;
      --bg: #0b1120;
      --bg-alt: #020617;
      --card: #020617;
      --accent: #38bdf8;
      --accent-soft: rgba(56, 189, 248, 0.15);
      --text: #e5e7eb;
      --text-muted: #9ca3af;
      --border-subtle: rgba(148, 163, 184, 0.35);
      --shadow-soft: 0 18px 40px rgba(15, 23, 42, 0.85);
      --radius-xl: 20px;
      --radius-2xl: 24px;
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "SF Pro Text", sans-serif;
      background: radial-gradient(circle at top, #1e293b 0, #020617 55%, black 100%);
      color: var(--text);
      min-height: 100vh;
      display: flex;
      justify-content: center;
      align-items: center;
      padding: 24px;
    }

    .app-shell {
      width: 100%;
      max-width: 1200px;
      background: radial-gradient(circle at top left, rgba(56, 189, 248, 0.08), transparent 55%),
                  radial-gradient(circle at bottom right, rgba(129, 140, 248, 0.08), transparent 55%),
                  linear-gradient(to bottom right, rgba(15,23,42,0.98), rgba(15,23,42,0.9));
      border-radius: 32px;
      border: 1px solid rgba(148, 163, 184, 0.35);
      box-shadow:
        0 25px 80px rgba(15,23,42,0.95),
        0 0 0 1px rgba(15,23,42,0.7);
      padding: 24px 24px 28px;
      position: relative;
      overflow: hidden;
    }

    .app-shell::before {
      content: "";
      position: absolute;
      inset: 0;
      background:
        radial-gradient(circle at top left, rgba(37,99,235,0.14), transparent 60%),
        radial-gradient(circle at bottom right, rgba(56,189,248,0.12), transparent 65%);
      opacity: 0.9;
      pointer-events: none;
    }

    .app-shell-inner {
      position: relative;
      z-index: 1;
    }

    header {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 16px;
      margin-bottom: 24px;
    }

    .brand {
      display: flex;
      align-items: center;
      gap: 14px;
    }

    .logo-pill {
      width: 40px;
      height: 40px;
      border-radius: 999px;
      background:
        conic-gradient(from 190deg, #38bdf8, #22c55e, #a855f7, #38bdf8);
      padding: 1.5px;
      box-shadow:
        0 0 0 1px rgba(15,23,42,0.9),
        0 0 22px rgba(56,189,248,0.65);
    }

    .logo-inner {
      width: 100%;
      height: 100%;
      border-radius: inherit;
      background: radial-gradient(circle at 20% 0%, rgba(248,250,252,0.16), transparent 45%),
                  radial-gradient(circle at 80% 120%, rgba(248,250,252,0.1), transparent 50%),
                  #020617;
      display: flex;
      align-items: center;
      justify-content: center;
    }

    .logo-mark {
      width: 20px;
      height: 20px;
      border-radius: 8px;
      border: 1px solid rgba(148, 163, 184, 0.75);
      display: grid;
      grid-template-columns: repeat(2, 1fr);
      gap: 1.5px;
      padding: 2px;
    }

    .logo-mark span {
      border-radius: 3px;
      background: linear-gradient(135deg, rgba(56,189,248,0.8), rgba(14,165,233,0.35));
      box-shadow: 0 0 10px rgba(56,189,248,0.7);
    }

    .logo-mark span:nth-child(2),
    .logo-mark span:nth-child(3) {
      background: linear-gradient(135deg, rgba(34,197,94,0.9), rgba(16,185,129,0.35));
    }

    .brand-text {
      display: flex;
      flex-direction: column;
      gap: 4px;
    }

    .brand-title {
      display: flex;
      align-items: baseline;
      gap: 7px;
    }

    .brand-title h1 {
      font-size: 22px;
      font-weight: 650;
      letter-spacing: 0.02em;
      margin: 0;
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }

    .brand-chip {
      font-size: 11px;
      padding: 4px 7px;
      border-radius: 999px;
      border: 1px solid rgba(148, 163, 184, 0.45);
      background: radial-gradient(circle at top, rgba(15,23,42,0.9), rgba(15,23,42,0.95));
      color: var(--text-muted);
    }

    .brand-subtitle {
      font-size: 12px;
      color: var(--text-muted);
      display: flex;
      gap: 10px;
      align-items: center;
    }

    .brand-subtitle span {
      display: inline-flex;
      align-items: center;
      gap: 4px;
    }

    .brand-subtitle svg {
      width: 13px;
      height: 13px;
      opacity: 0.85;
    }

    .meta {
      display: flex;
      flex-direction: column;
      align-items: flex-end;
      gap: 6px;
      font-size: 11px;
      color: var(--text-muted);
    }

    .meta-row {
      display: inline-flex;
      gap: 6px;
      align-items: center;
      padding: 4px 9px;
      border-radius: 999px;
      border: 1px solid rgba(148, 163, 184, 0.4);
      background: radial-gradient(circle at top, rgba(15,23,42,0.9), rgba(15,23,42,0.98));
      backdrop-filter: blur(12px);
    }

    .meta-dot {
      width: 7px;
      height: 7px;
      border-radius: 999px;
      background: #22c55e;
      box-shadow: 0 0 10px rgba(34,197,94,0.9);
    }

    .meta-pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 4px 8px;
      border-radius: 999px;
      background: radial-gradient(circle at top, rgba(15,23,42,0.95), rgba(15,23,42,1));
      border: 1px solid rgba(148, 163, 184, 0.5);
    }

    .meta-pill strong {
      font-weight: 600;
      color: #e5e7eb;
    }

    .meta-pill span {
      color: var(--text-muted);
    }

    main {
      display: grid;
      grid-template-columns: minmax(0, 1.1fr) minmax(0, 1.3fr);
      gap: 20px;
    }

    .panel {
      background: radial-gradient(circle at top, rgba(15,23,42,0.96), rgba(15,23,42,0.99));
      border-radius: var(--radius-2xl);
      border: 1px solid rgba(148, 163, 184, 0.4);
      box-shadow:
        0 18px 40px rgba(15, 23, 42, 0.95),
        inset 0 0 0 1px rgba(15,23,42,0.85);
      padding: 16px 16px 18px;
      position: relative;
      overflow: hidden;
    }

    .panel::before {
      content: "";
      position: absolute;
      inset: 0;
      background: radial-gradient(circle at top right, rgba(56,189,248,0.08), transparent 55%);
      opacity: 0.9;
      pointer-events: none;
    }

    .panel-inner {
      position: relative;
      z-index: 1;
    }

    .panel-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 12px;
      gap: 10px;
    }

    .panel-header-title {
      display: flex;
      flex-direction: column;
      gap: 2px;
    }

    .panel-header-title h2 {
      font-size: 14px;
      font-weight: 600;
      letter-spacing: 0.03em;
      text-transform: uppercase;
      margin: 0;
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .panel-header-title h2 span.pill {
      font-size: 10px;
      padding: 3px 7px;
      border-radius: 999px;
      border: 1px solid rgba(148, 163, 184, 0.6);
      background: rgba(15, 23, 42, 0.9);
      color: var(--text-muted);
    }

    .panel-header-title p {
      margin: 0;
      font-size: 11px;
      color: var(--text-muted);
    }

    .panel-header-meta {
      font-size: 11px;
      color: var(--text-muted);
      display: flex;
      flex-direction: column;
      align-items: flex-end;
      gap: 4px;
    }

    .panel-header-meta span {
      padding: 3px 8px;
      border-radius: 999px;
      border: 1px solid rgba(148, 163, 184, 0.45);
      background: rgba(15,23,42,0.9);
    }

    .form-grid {
      display: grid;
      grid-template-columns: minmax(0, 1.2fr) minmax(0, 1.2fr);
      gap: 10px;
      margin-bottom: 10px;
    }

    .form-group {
      display: flex;
      flex-direction: column;
      gap: 5px;
      font-size: 11px;
    }

    label {
      color: var(--text-muted);
      font-weight: 500;
      letter-spacing: 0.02em;
      text-transform: uppercase;
    }

    select,
    input[type="text"],
    input[type="number"] {
      width: 100%;
      padding: 7px 8px;
      border-radius: 10px;
      border: 1px solid rgba(148, 163, 184, 0.7);
      background: radial-gradient(circle at top, rgba(15,23,42,0.95), rgba(15,23,42,1));
      color: var(--text);
      font-size: 13px;
      outline: none;
      transition: border-color 0.15s ease, box-shadow 0.15s ease;
      box-shadow: inset 0 0 0 1px rgba(15,23,42,0.85);
    }

    select:focus,
    input[type="text"]:focus,
    input[type="number"]:focus {
      border-color: var(--accent);
      box-shadow:
        0 0 0 1px rgba(56,189,248,0.6),
        0 0 0 1.5px rgba(15,23,42,1);
    }

    input[type="number"]::-webkit-inner-spin-button,
    input[type="number"]::-webkit-outer-spin-button {
      -webkit-appearance: none;
      margin: 0;
    }

    .league-row {
      display: grid;
      grid-template-columns: minmax(0, 1.4fr) minmax(0, 1.3fr);
      gap: 10px;
    }

    .alias-note {
      font-size: 11px;
      color: var(--text-muted);
      margin-top: 4px;
    }

    .alias-note strong {
      color: #e5e7eb;
      font-weight: 500;
    }

    .actions {
      display: flex;
      justify-content: flex-end;
      gap: 8px;
      margin-top: 10px;
    }

    button {
      border-radius: 999px;
      border: 1px solid transparent;
      padding: 7px 12px;
      font-size: 12px;
      font-weight: 500;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      background: radial-gradient(circle at top left, #38bdf8, #0ea5e9);
      color: #0b1120;
      box-shadow:
        0 12px 30px rgba(56,189,248,0.65),
        0 0 0 1px rgba(15,23,42,0.8);
      transition:
        transform 0.08s ease-out,
        box-shadow 0.08s ease-out,
        filter 0.12s ease-out;
      white-space: nowrap;
    }

    button.secondary {
      background: rgba(15,23,42,0.95);
      color: var(--text-muted);
      border-color: rgba(148, 163, 184, 0.5);
      box-shadow:
        0 4px 16px rgba(15,23,42,0.9),
        inset 0 0 0 1px rgba(15,23,42,0.9);
    }

    button:hover {
      transform: translateY(-1px);
      filter: brightness(1.03);
      box-shadow:
        0 16px 35px rgba(56,189,248,0.75),
        0 0 0 1px rgba(15,23,42,0.9);
    }

    button.secondary:hover {
      box-shadow:
        0 10px 26px rgba(15,23,42,1),
        0 0 0 1px rgba(15,23,42,1);
      color: #e5e7eb;
    }

    button:active {
      transform: translateY(0);
      box-shadow:
        0 8px 20px rgba(56,189,248,0.55),
        0 0 0 1px rgba(15,23,42,0.9);
    }

    button.secondary:active {
      box-shadow:
        0 6px 16px rgba(15,23,42,1),
        0 0 0 1px rgba(15,23,42,1);
    }

    button svg {
      width: 14px;
      height: 14px;
    }

    .results-grid {
      display: grid;
      grid-template-rows: auto 1fr;
      gap: 12px;
      height: 100%;
    }

    .score-summary {
      display: grid;
      grid-template-columns: 1.1fr 1.5fr;
      gap: 12px;
      align-items: stretch;
    }

    .score-card {
      border-radius: 18px;
      border: 1px solid rgba(148, 163, 184, 0.4);
      background:
        radial-gradient(circle at top, rgba(15,23,42,0.96), rgba(15,23,42,1));
      padding: 10px 12px;
      box-shadow:
        0 14px 30px rgba(15,23,42,0.9),
        inset 0 0 0 1px rgba(15,23,42,0.9);
      display: flex;
      flex-direction: column;
      gap: 7px;
    }

    .score-header {
      display: flex;
      justify-content: space-between;
      font-size: 11px;
      color: var(--text-muted);
      align-items: center;
    }

    .score-teams {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 10px;
    }

    .team-block {
      display: flex;
      flex-direction: column;
      gap: 3px;
      flex: 1;
    }

    .team-name {
      font-weight: 600;
      font-size: 14px;
      letter-spacing: 0.02em;
    }

    .team-meta {
      font-size: 11px;
      color: var(--text-muted);
    }

    .score-badge {
      display: inline-flex;
      align-items: baseline;
      gap: 4px;
      padding: 4px 9px;
      border-radius: 999px;
      border: 1px solid rgba(148, 163, 184, 0.5);
      background: radial-gradient(circle at top, rgba(15,23,42,1), rgba(15,23,42,1));
      font-size: 13px;
    }

    .score-badge strong {
      font-size: 16px;
      font-weight: 650;
      color: #e5e7eb;
    }

    .score-badge span {
      color: var(--text-muted);
      font-size: 11px;
    }

    .streak-pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 3px 8px;
      border-radius: 999px;
      border: 1px solid rgba(52,211,153,0.5);
      background: radial-gradient(circle at top, rgba(22,163,74,0.4), rgba(5,46,22,0.95));
      font-size: 11px;
      color: #bbf7d0;
      box-shadow: 0 0 18px rgba(34,197,94,0.45);
    }

    .streak-pill svg {
      width: 13px;
      height: 13px;
    }

    .stats-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 6px;
      margin-top: 4px;
    }

    .stat-item {
      border-radius: 12px;
      border: 1px solid rgba(148, 163, 184, 0.4);
      padding: 6px 8px;
      font-size: 11px;
      background: radial-gradient(circle at top, rgba(15,23,42,0.97), rgba(15,23,42,1));
      display: flex;
      flex-direction: column;
      gap: 3px;
    }

    .stat-item span.label {
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
      font-size: 10px;
    }

    .stat-item span.value {
      font-weight: 600;
    }

    .history-card {
      border-radius: 18px;
      border: 1px solid rgba(148, 163, 184, 0.4);
      padding: 10px 12px;
      background:
        radial-gradient(circle at top, rgba(15,23,42,0.97), rgba(15,23,42,1));
      box-shadow:
        0 14px 32px rgba(15,23,42,0.95),
        inset 0 0 0 1px rgba(15,23,42,0.9);
      display: flex;
      flex-direction: column;
      gap: 8px;
    }

    .history-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      font-size: 11px;
      color: var(--text-muted);
    }

    .history-header strong {
      color: #e5e7eb;
      font-size: 12px;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }

    .history-list {
      border-radius: 12px;
      border: 1px solid rgba(148, 163, 184, 0.4);
      max-height: 210px;
      overflow: hidden auto;
      background: rgba(15,23,42,0.95);
    }

    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 11px;
    }

    thead {
      position: sticky;
      top: 0;
      background: linear-gradient(to bottom, #020617, #020617);
      box-shadow: 0 1px 0 rgba(51, 65, 85, 0.9);
      z-index: 1;
    }

    th, td {
      padding: 6px 8px;
      text-align: left;
      border-bottom: 1px solid rgba(30, 41, 59, 0.9);
      white-space: nowrap;
    }

    th {
      color: var(--text-muted);
      font-weight: 500;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-size: 10px;
    }

    tbody tr:last-child td {
      border-bottom: none;
    }

    tbody tr:nth-child(even) {
      background-color: rgba(15,23,42, 0.9);
    }

    tbody tr:hover {
      background-color: rgba(30,64,175,0.4);
    }

    .td-footnote {
      font-size: 10px;
      color: var(--text-muted);
      white-space: normal;
      line-height: 1.4;
    }

    .upcoming-card {
      border-radius: 18px;
      border: 1px solid rgba(148, 163, 184, 0.4);
      padding: 10px 12px;
      background:
        radial-gradient(circle at top, rgba(15,23,42,0.96), rgba(15,23,42,1));
      box-shadow:
        0 14px 32px rgba(15,23,42,0.95),
        inset 0 0 0 1px rgba(15,23,42,0.9);
      display: flex;
      flex-direction: column;
      gap: 8px;
    }

    .upcoming-list {
      border-radius: 12px;
      border: 1px solid rgba(148, 163, 184, 0.4);
      background: rgba(15,23,42,0.96);
      max-height: 150px;
      overflow: hidden auto;
    }

    .upcoming-item {
      padding: 6px 8px;
      border-bottom: 1px solid rgba(30, 41, 59, 0.85);
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 6px;
      font-size: 11px;
    }

    .upcoming-item:last-child {
      border-bottom: none;
    }

    .upcoming-item-main {
      display: flex;
      flex-direction: column;
      gap: 3px;
    }

    .upcoming-item-main strong {
      font-size: 11px;
    }

    .upcoming-meta {
      font-size: 10px;
      color: var(--text-muted);
    }

    .upcoming-kickoff {
      font-size: 11px;
      color: var(--text-muted);
      display: flex;
      flex-direction: column;
      align-items: flex-end;
      gap: 2px;
    }

    .tag-chip {
      display: inline-flex;
      align-items: center;
      gap: 5px;
      padding: 3px 7px;
      border-radius: 999px;
      border: 1px solid rgba(148, 163, 184, 0.4);
      background: rgba(15,23,42,0.95);
      font-size: 10px;
      color: var(--text-muted);
      white-space: nowrap;
    }

    .tag-dot {
      width: 6px;
      height: 6px;
      border-radius: 999px;
      background: #38bdf8;
      box-shadow: 0 0 8px rgba(56,189,248,0.8);
    }

    .error-banner {
      margin-top: 8px;
      border-radius: 12px;
      border: 1px solid rgba(248, 113, 113, 0.6);
      background: rgba(127,29,29,0.88);
      padding: 6px 8px;
      font-size: 11px;
      color: #fee2e2;
      display: none;
      align-items: center;
      gap: 6px;
    }

    .error-banner svg {
      width: 14px;
      height: 14px;
    }

    .loading-indicator {
      display: none;
      font-size: 11px;
      color: var(--text-muted);
      align-items: center;
      gap: 6px;
      margin-top: 6px;
    }

    .loading-indicator svg {
      width: 13px;
      height: 13px;
      animation: spin 1s linear infinite;
    }

    @keyframes spin {
      to {
        transform: rotate(360deg);
      }
    }

    @media (max-width: 900px) {
      main {
        grid-template-columns: minmax(0, 1fr);
      }

      .app-shell {
        padding: 18px;
      }

      .panel {
        padding: 14px 12px 16px;
      }

      .score-summary {
        grid-template-columns: minmax(0, 1fr);
      }
    }

    @media (max-width: 600px) {
      header {
        flex-direction: column;
      }

      .meta {
        align-items: flex-start;
      }

      .form-grid {
        grid-template-columns: minmax(0, 1fr);
      }

      .league-row {
        grid-template-columns: minmax(0, 1fr);
      }
    }
  </style>
</head>
<body>
  <div class="app-shell">
    <div class="app-shell-inner">
      <header>
        <div class="brand">
          <div class="logo-pill">
            <div class="logo-inner">
              <div class="logo-mark">
                <span></span><span></span><span></span><span></span>
              </div>
            </div>
          </div>
          <div class="brand-text">
            <div class="brand-title">
              <h1>Rugby Head-to-Head</h1>
              <span class="brand-chip">Rugby Analytics · Beta</span>
            </div>
            <div class="brand-subtitle">
              <span>
                <!-- ball icon -->
                <svg viewBox="0 0 24 24" aria-hidden="true">
                  <path fill="currentColor" d="M19.5 4.5C17.5 2.5 14.6 2 12 2 9.4 2 6.5 2.5 4.5 4.5 2.5 6.5 2 9.4 2 12s.5 5.5 2.5 7.5S9.4 22 12 22s5.5-.5 7.5-2.5S22 14.6 22 12s-.5-5.5-2.5-7.5Zm-1.94 1.56c.42.42.76.88 1.04 1.36-1.16.23-2.6.4-4.32.44-.1-.96-.25-1.84-.44-2.64 1.37.02 2.67.24 3.72.84ZM9.72 5.22c.63-.16 1.3-.27 2.02-.32.21.84.37 1.78.47 2.8-1.3.02-2.46-.04-3.5-.16.25-.97.58-1.82 1.01-2.32ZM6.44 6.44C7 5.88 7.72 5.5 8.5 5.24c-.35.7-.64 1.52-.86 2.44-.92-.12-1.74-.3-2.44-.54.26-.78.64-1.5 1.24-2.1ZM5.22 9.72c.5.19 1.09.35 1.76.48-.11.92-.16 1.98-.14 3.16-1.02-.1-1.96-.26-2.8-.47.05-.72.16-1.39.32-2.02.13-.5.29-.96.46-1.42Zm.76 5.36c.7.24 1.52.42 2.44.54.22.92.5 1.74.86 2.44-.78-.26-1.5-.64-2.06-1.2-.6-.6-.98-1.32-1.24-2.1Zm3.74 3.68c.47-.5.85-1.35 1.1-2.32 1.04-.12 2.2-.18 3.5-.16-.1 1.02-.26 1.96-.47 2.8-.72-.05-1.39-.16-2.02-.32-.46-.12-.9-.27-1.33-.46-.46-.17-.92-.33-1.28-.54Zm5.82-.4c.19-.8.34-1.68.44-2.64 1.72.04 3.16.21 4.32.44-.28.48-.62.94-1.04 1.36-1.05.6-2.35.82-3.72.84Zm.48-4.8c-1.3-.02-2.46.04-3.5.16-.12-1.3-.18-2.46-.16-3.5 1.18-.02 2.24.03 3.16.14.13.67.29 1.26.48 1.76-.19.46-.35.96-.48 1.44Z" />
                </svg>
                All major club & test competitions
              </span>
              <span>
                <!-- database icon -->
                <svg viewBox="0 0 24 24" aria-hidden="true">
                  <path fill="currentColor" d="M12 2C7.58 2 4 3.79 4 6v12c0 2.21 3.58 4 8 4s8-1.79 8-4V6c0-2.21-3.58-4-8-4Zm0 2c3.31 0 6 .9 6 2s-2.69 2-6 2-6-.9-6-2 2.69-2 6-2Zm0 14c-3.31 0-6-.9-6-2v-2.09C7.09 16.56 9.42 17 12 17s4.91-.44 6-1.09V16c0 1.1-2.69 2-6 2Zm0-4c-3.31 0-6-.9-6-2v-2.09C7.09 12.56 9.42 13 12 13s4.91-.44 6-1.09V12c0 1.1-2.69 2-6 2Z" />
                </svg>
                Powered by Postgres & TheSportsDB
              </span>
            </div>
          </div>
        </div>
        <div class="meta">
          <div class="meta-row">
            <span class="meta-dot"></span>
            Live database connected
          </div>
          <div class="meta-pill">
            <strong>Multi-league</strong>
            <span>URC · Premiership · Top 14 · Tests</span>
          </div>
        </div>
      </header>

      <main>
        <section class="panel">
          <div class="panel-inner">
            <div class="panel-header">
              <div class="panel-header-title">
                <h2>
                  Matchup Explorer
                  <span class="pill">Cross-competition mode</span>
                </h2>
                <p>Pick a league or go global, then compare any two teams by name.</p>
              </div>
              <div class="panel-header-meta">
                <span>Tip: Use “All leagues” to merge Stormers / WP, Bulls / Blue Bulls, etc.</span>
              </div>
            </div>

            <div class="form-grid">
              <div class="form-group">
                <label for="league">League</label>
                <div class="league-row">
                  <select id="league">
                    <option value="0">All leagues (alias-aware)</option>
                  </select>
                  <select id="team-league">
                    <option value="">Filter teams by league (optional)</option>
                  </select>
                </div>
                <div class="alias-note">
                  <strong>All leagues mode:</strong> combines club aliases across competitions,
                  e.g. Stormers + Western Province, Bulls + Blue Bulls, etc.
                </div>
              </div>
              <div class="form-group">
                <label for="limit">Recent games</label>
                <input type="number" id="limit" min="1" max="100" value="10" />
              </div>
            </div>

            <div class="form-grid">
              <div class="form-group">
                <label for="team-a">Team A</label>
                <input type="text" id="team-a" placeholder="e.g. Stormers" />
              </div>
              <div class="form-group">
                <label for="team-b">Team B</label>
                <input type="text" id="team-b" placeholder="e.g. Bulls" />
              </div>
            </div>

            <div class="actions">
              <button class="secondary" type="button" id="swap-teams">
                <!-- swap icon -->
                <svg viewBox="0 0 24 24" aria-hidden="true">
                  <path fill="currentColor" d="M7 7h10v2H7v3L3 8l4-4v3zm10 10H7v-2h10v-3l4 4-4 4v-3z" />
                </svg>
                Swap
              </button>
              <button type="button" id="compare-btn">
                <!-- spark icon -->
                <svg viewBox="0 0 24 24" aria-hidden="true">
                  <path fill="currentColor" d="M12 2 9.5 8.5 3 11l6.5 2.5L12 20l2.5-6.5L21 11l-6.5-2.5L12 2z" />
                </svg>
                Compare Teams
              </button>
            </div>

            <div class="loading-indicator" id="loading-indicator">
              <svg viewBox="0 0 24 24" aria-hidden="true">
                <path fill="currentColor" d="M12 2a10 10 0 0 0-9.95 9h2.02A8 8 0 1 1 12 20v2A10 10 0 0 0 12 2z" />
              </svg>
              Running head-to-head query...
            </div>

            <div class="error-banner" id="error-banner">
              <svg viewBox="0 0 24 24" aria-hidden="true">
                <path fill="currentColor" d="M12 2 1 21h22L12 2zm0 4.77L19.07 19H4.93L12 6.77zM11 10v5h2v-5h-2zm0 6v2h2v-2z" />
              </svg>
              <span id="error-text"></span>
            </div>
          </div>
        </section>

        <section class="panel">
          <div class="panel-inner">
            <div class="results-grid">
              <div class="score-summary">
                <div class="score-card">
                  <div class="score-header">
                    <span id="summary-league">Awaiting selection</span>
                    <span class="tag-chip">
                      <span class="tag-dot"></span>
                      Head-to-head summary
                    </span>
                  </div>
                  <div class="score-teams">
                    <div class="team-block">
                      <div class="team-name" id="team-a-name-display">Team A</div>
                      <div class="team-meta">
                        Wins: <span id="team-a-wins">0</span> ·
                        Win rate: <span id="team-a-rate">0%</span>
                      </div>
                    </div>
                    <div class="team-block" style="text-align: right;">
                      <div class="team-name" id="team-b-name-display">Team B</div>
                      <div class="team-meta">
                        Wins: <span id="team-b-wins">0</span> ·
                        Win rate: <span id="team-b-rate">0%</span>
                      </div>
                    </div>
                  </div>
                  <div class="stats-grid">
                    <div class="stat-item">
                      <span class="label">Total meetings</span>
                      <span class="value" id="total-matches">0</span>
                    </div>
                    <div class="stat-item">
                      <span class="label">Draws</span>
                      <span class="value">
                        <span id="draws-count">0</span>
                        &nbsp;(<span id="draws-rate">0%</span>)
                      </span>
                    </div>
                    <div class="stat-item">
                      <span class="label">Current streak</span>
                      <span class="value" id="current-streak">–</span>
                    </div>
                  </div>
                  <div style="margin-top: 6px;">
                    <span class="streak-pill" id="streak-pill">
                      <!-- arrow icon -->
                      <svg viewBox="0 0 24 24" aria-hidden="true">
                        <path fill="currentColor" d="M5 17.59 6.41 19 17 8.41V15h2V5h-10v2h6.59L5 17.59z" />
                      </svg>
                      Awaiting comparison
                    </span>
                  </div>
                </div>

                <div class="history-card">
                  <div class="history-header">
                    <strong>Recent results</strong>
                    <span>Sorted by latest kickoff</span>
                  </div>
                  <div class="history-list">
                    <table>
                      <thead>
                        <tr>
                          <th>Date</th>
                          <th>Home</th>
                          <th>Score</th>
                          <th>Away</th>
                          <th>Comp</th>
                          <th>Season</th>
                        </tr>
                      </thead>
                      <tbody id="history-body">
                        <tr>
                          <td colspan="6" class="td-footnote">
                            Use the form to run a comparison. Results will appear here with
                            competition, season and venue.
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>

              <div class="upcoming-card">
                <div class="history-header">
                  <strong>Upcoming fixtures</strong>
                  <span>Only matches with a future kickoff</span>
                </div>
                <div class="upcoming-list" id="upcoming-list">
                  <div class="upcoming-item">
                    <div class="upcoming-item-main">
                      <strong>No fixtures yet</strong>
                      <div class="upcoming-meta">
                        When teams have future matches, they will show here with kickoff time,
                        venue and competition.
                      </div>
                    </div>
                    <div class="upcoming-kickoff">
                      <span>—</span>
                      <span class="tag-chip">
                        <span class="tag-dot"></span>
                        Live from DB
                      </span>
                    </div>
                  </div>
                </div>
              </div>

            </div>
          </div>
        </section>
      </main>
    </div>
  </div>

  <script>
    const leagueSelect = document.getElementById("league");
    const teamLeagueSelect = document.getElementById("team-league");
    const teamAInput = document.getElementById("team-a");
    const teamBInput = document.getElementById("team-b");
    const limitInput = document.getElementById("limit");
    const compareBtn = document.getElementById("compare-btn");
    const swapBtn = document.getElementById("swap-teams");
    const loadingIndicator = document.getElementById("loading-indicator");
    const errorBanner = document.getElementById("error-banner");
    const errorText = document.getElementById("error-text");

    const summaryLeague = document.getElementById("summary-league");
    const teamANameDisplay = document.getElementById("team-a-name-display");
    const teamBNameDisplay = document.getElementById("team-b-name-display");
    const teamAWins = document.getElementById("team-a-wins");
    const teamBWins = document.getElementById("team-b-wins");
    const teamARate = document.getElementById("team-a-rate");
    const teamBRate = document.getElementById("team-b-rate");
    const drawsCount = document.getElementById("draws-count");
    const drawsRate = document.getElementById("draws-rate");
    const totalMatches = document.getElementById("total-matches");
    const currentStreak = document.getElementById("current-streak");
    const streakPill = document.getElementById("streak-pill");
    const historyBody = document.getElementById("history-body");
    const upcomingList = document.getElementById("upcoming-list");

    async function fetchJSON(url) {
      const res = await fetch(url);
      if (!res.ok) {
        let detail = "Unknown error";
        try {
          const data = await res.json();
          detail = data.detail || JSON.stringify(data);
        } catch (e) {}
        throw new Error(detail);
      }
      return res.json();
    }

    function setLoading(isLoading) {
      loadingIndicator.style.display = isLoading ? "inline-flex" : "none";
      compareBtn.disabled = isLoading;
    }

    function setError(message) {
      if (!message) {
        errorBanner.style.display = "none";
        errorText.textContent = "";
        return;
      }
      errorBanner.style.display = "inline-flex";
      errorText.textContent = message;
    }

    function formatDate(iso) {
      try {
        const d = new Date(iso);
        return d.toLocaleString(undefined, {
          year: "numeric",
          month: "short",
          day: "numeric",
          hour: "2-digit",
          minute: "2-digit",
        });
      } catch {
        return iso;
      }
    }

    function renderHistory(matches) {
      historyBody.innerHTML = "";
      if (!matches.length) {
        const tr = document.createElement("tr");
        const td = document.createElement("td");
        td.colSpan = 6;
        td.className = "td-footnote";
        td.textContent =
          "No historical results found. Try broadening to 'All leagues' or adjusting team names.";
        tr.appendChild(td);
        historyBody.appendChild(tr);
        return;
      }

      for (const m of matches) {
        const tr = document.createElement("tr");
        const score =
          m.home_score == null || m.away_score == null
            ? "TBD"
            : `${m.home_score}–${m.away_score}`;
        const comp = m.league || "—";
        const season = m.season || "—";

        tr.innerHTML = `
          <td>${formatDate(m.kickoff_utc)}</td>
          <td>${m.home_team}</td>
          <td>${score}</td>
          <td>${m.away_team}</td>
          <td>${comp}</td>
          <td>${season}</td>
        `;
        historyBody.appendChild(tr);
      }
    }

    function renderUpcoming(fixtures) {
      upcomingList.innerHTML = "";
      if (!fixtures.length) {
        const wrapper = document.createElement("div");
        wrapper.className = "upcoming-item";
        wrapper.innerHTML = `
          <div class="upcoming-item-main">
            <strong>No fixtures yet</strong>
            <div class="upcoming-meta">
              When teams have future matches, they will show here with kickoff time,
              venue and competition.
            </div>
          </div>
          <div class="upcoming-kickoff">
            <span>—</span>
            <span class="tag-chip">
              <span class="tag-dot"></span>
              Live from DB
            </span>
          </div>
        `;
        upcomingList.appendChild(wrapper);
        return;
      }

      for (const f of fixtures) {
        const item = document.createElement("div");
        item.className = "upcoming-item";
        const venue = f.venue || "Venue TBC";
        const comp = f.league || "—";
        const season = f.season || "—";

        item.innerHTML = `
          <div class="upcoming-item-main">
            <strong>${f.home_team} vs ${f.away_team}</strong>
            <div class="upcoming-meta">
              ${venue} · ${comp} · ${season}
            </div>
          </div>
          <div class="upcoming-kickoff">
            <span>${formatDate(f.kickoff_utc)}</span>
            <span class="tag-chip">
              <span class="tag-dot"></span>
              Scheduled fixture
            </span>
          </div>
        `;
        upcomingList.appendChild(item);
      }
    }

    async function loadLeagues() {
      try {
        const leagues = await fetchJSON("/leagues");
        for (const lg of leagues) {
          const opt = document.createElement("option");
          opt.value = lg.tsdb_league_id ?? "";
          opt.textContent = lg.country ? `${lg.name} (${lg.country})` : lg.name;
          leagueSelect.appendChild(opt);

          const opt2 = document.createElement("option");
          opt2.value = lg.id;
          opt2.textContent = lg.country ? `${lg.name} (${lg.country})` : lg.name;
          teamLeagueSelect.appendChild(opt2);
        }
      } catch (err) {
        console.error("Failed to load leagues:", err);
        setError("Could not load leagues from the API.");
      }
    }

    async function onCompare() {
      setError("");
      setLoading(true);

      const tsdbLeagueId = leagueSelect.value || "0";
      const teamA = teamAInput.value.trim();
      const teamB = teamBInput.value.trim();
      const limit = limitInput.value || "10";

      if (!teamA || !teamB) {
        setError("Please enter both Team A and Team B.");
        setLoading(false);
        return;
      }

      const qs = new URLSearchParams({
        team_a: teamA,
        team_b: teamB,
        limit: String(limit),
      });

      try {
        const url = `/headtohead/${encodeURIComponent(tsdbLeagueId)}?${qs.toString()}`;
        const data = await fetchJSON(url);

        summaryLeague.textContent =
          data.league_name || "All leagues (alias-aware mode)";
        teamANameDisplay.textContent = data.team_a_name;
        teamBNameDisplay.textContent = data.team_b_name;

        teamAWins.textContent = String(data.team_a_wins);
        teamBWins.textContent = String(data.team_b_wins);
        teamARate.textContent = `${data.team_a_win_rate}%`;
        teamBRate.textContent = `${data.team_b_win_rate}%`;
        drawsCount.textContent = String(data.draws);
        drawsRate.textContent = `${data.draws_rate}%`;
        totalMatches.textContent = String(data.total_matches);

        currentStreak.textContent = data.current_streak || "No recent result";

        if (!data.total_matches) {
          streakPill.innerHTML = `
            <svg viewBox="0 0 24 24" aria-hidden="true">
              <path fill="currentColor" d="M12 2 9.5 8.5 3 11l6.5 2.5L12 20l2.5-6.5L21 11l-6.5-2.5L12 2z" />
            </svg>
            No completed matches in sample
          `;
        } else {
          streakPill.innerHTML = `
            <svg viewBox="0 0 24 24" aria-hidden="true">
              <path fill="currentColor" d="M5 17.59 6.41 19 17 8.41V15h2V5h-10v2h6.59L5 17.59z" />
            </svg>
            Current streak: ${data.current_streak || "No recent result"}
          `;
        }

        renderHistory(data.last_matches || []);
        renderUpcoming(data.upcoming_fixtures || []);
      } catch (err) {
        console.error("Compare error:", err);
        setError(err.message || "Error running comparison.");
      } finally {
        setLoading(false);
      }
    }

    function onSwapTeams() {
      const a = teamAInput.value;
      teamAInput.value = teamBInput.value;
      teamBInput.value = a;
    }

    compareBtn.addEventListener("click", onCompare);
    swapBtn.addEventListener("click", onSwapTeams);

    document.addEventListener("keydown", (ev) => {
      if (ev.key === "Enter") {
        const active = document.activeElement;
        if (
          active === teamAInput ||
          active === teamBInput ||
          active === limitInput
        ) {
          onCompare();
        }
      }
    });

    loadLeagues();
  </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    """
    Serve the built-in head-to-head UI.
    """
    return HTMLResponse(content=INDEX_HTML)
