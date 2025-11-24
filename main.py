#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

- Goes back to **team_id-based** head-to-head stats.
- Uses **alias groups only to decide which team_ids to include**.
- In "All leagues" mode (tsdb_league_id == 0) it:
    - Maps a query like "Stormers" or "WP" to an alias group.
    - Finds all team_ids in the DB whose normalised name matches any alias
      in that group (e.g. Stormers / Western Province / WP / DHL Stormers, etc,
      depending on how they are stored).
    - Runs **pure team_id-based queries + stats** across all those IDs.
- Does NOT use substring logic on names to decide who won – only IDs.

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
    """Lowercase and strip punctuation-ish noise for fuzzy / equality matching."""
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
    version="0.7.0",
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
# KEEP YOUR EXISTING "Built-in HTML UI (index page)" SECTION BELOW THIS LINE.
# That means:
#   - Your existing INDEX_HTML = """...""" block
#   - The @app.get("/") route that returns HTMLResponse(INDEX_HTML)
# ---------------------------------------------------------------------------

