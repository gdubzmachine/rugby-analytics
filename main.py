#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Rugby Analytics Backend – v0.8.2 (ID-based H2H, alias-aware, no 404 on "no matches")

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

Key behaviour in this version:

- Uses **team_id-based** head-to-head stats.
- Uses **club alias groups only to decide which team_ids belong to a club**.
- In "All leagues" mode (tsdb_league_id == 0):
    - Team names like "Stormers" / "WP" / "DHL Stormers" / "Western Province" are treated as one club.
    - Team names like "Bulls" / "Blue Bulls" / "Vodacom Bulls" / "Northern Transvaal" are treated as one club.
    - It collects ALL matching team_ids and uses those in the WHERE clause.
- It **does not raise** an HTTPException when there are no matches.
  It just returns `total_matches = 0`.
- It only 404s when:
    - the league is unknown, or
    - Team A / Team B cannot be resolved to any team_ids.
"""

from __future__ import annotations

import datetime as dt
import os
from typing import Any, Dict, List, Optional, Set, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from starlette.middleware.cors import CORSMiddleware
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR

API_VERSION = "0.8.2"

# ---------------------------------------------------------------------------
# Env + DB helpers
# ---------------------------------------------------------------------------

load_dotenv()  # local dev .env; no-op on Render


def get_conn():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "DATABASE_URL is not set. Configure it in .env (local) or as Render env var."
        )
    return psycopg2.connect(dsn, cursor_factory=RealDictCursor)


def fetch_one(query: str, params: Tuple[Any, ...] = ()) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            row = cur.fetchone()
    return row


def fetch_all(query: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
    return list(rows)


# ---------------------------------------------------------------------------
# Name normalisation + alias groups
# ---------------------------------------------------------------------------

def normalise_name(name: str) -> str:
    """
    Normalise team names by:
    - lowercasing
    - removing common sponsor prefixes (DHL, Vodacom, Cell C, Emirates, MTN, Toyota, Hollywoodbets, "The")
    - stripping punctuation
    - collapsing whitespace

    So:
      "DHL Stormers"        -> "stormers"
      "Vodacom Bulls"       -> "bulls"
      "Hollywoodbets Sharks"-> "sharks"
      "Toyota Cheetahs"     -> "cheetahs"
      "The Sharks"          -> "sharks"
    """
    import re

    name = name.lower()

    sponsor_patterns = [
        r"\bdhl\b",
        r"\bvodacom\b",
        r"\bcell c\b",
        r"\bhollywoodbets\b",
        r"\bemirates\b",
        r"\bmtn\b",
        r"\btoyota\b",
        r"\bthe\b",
    ]
    for sp in sponsor_patterns:
        name = re.sub(sp, "", name)

    name = re.sub(r"[^\w\s]", "", name)  # remove punctuation
    name = re.sub(r"\s+", " ", name).strip()
    return name


# Each set = one "club" for ALL LEAGUES MODE (tsdb_league_id == 0)
CLUB_ALIAS_GROUPS: List[Set[str]] = [
    # South African clubs – extended aliases you gave
    {"bulls", "blue bulls", "northern transvaal", "vodacom bulls", "pretoria bulls"},
    {"stormers", "western province", "wp", "western stormers", "dhl stormers"},
    {"sharks", "natal sharks", "natal", "sharks xv", "cell c sharks", "hollywoodbets sharks"},
    {"lions", "golden lions", "emirates lions", "mtn golden lions", "transvaal"},
    {"cheetahs", "free state cheetahs", "toyota cheetahs"},

    # Other clubs (left as-is)
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


def find_alias_group(name: str) -> Optional[Set[str]]:
    norm = normalise_name(name)
    for group in CLUB_ALIAS_GROUPS:
        norm_group = {normalise_name(x) for x in group}
        if norm in norm_group:
            return group
    return None


# ---------------------------------------------------------------------------
# League / season / team resolution
# ---------------------------------------------------------------------------

def resolve_league_by_tsdb(tsdb_league_id: int) -> Optional[Dict[str, Any]]:
    return fetch_one(
        """
        SELECT id, name, tsdb_league_id
        FROM leagues
        WHERE tsdb_league_id = %s
        """,
        (tsdb_league_id,),
    )


def resolve_latest_season_for_league(league_id: int) -> Optional[Dict[str, Any]]:
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
    league_id: int,
    season_label: str,
) -> Optional[Dict[str, Any]]:
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


def resolve_team_in_league(league_id: int, team_name: str) -> Optional[Dict[str, Any]]:
    # exact LOWER(name)
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

    # fallback: ILIKE %name%
    row = fetch_one(
        """
        SELECT t.id, t.name
        FROM teams t
        JOIN league_team_seasons lts
          ON lts.team_id = t.id
        JOIN seasons s
          ON s.id = lts.season_id
        WHERE s.league_id = %s
          AND t.name ILIKE %s
        LIMIT 1
        """,
        (league_id, f"%{team_name}%",),
    )
    return row


def resolve_team_global(team_name: str) -> Optional[Dict[str, Any]]:
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


def resolve_club_team_ids_all_leagues(team_name: str) -> Tuple[List[int], str]:
    """
    For tsdb_league_id == 0:

    - If team_name belongs to an alias group, find ALL team_ids whose
      normalised names match any alias in that group.
    - If nothing matches, fall back to a single global team lookup.

    Returns: (team_ids, representative_display_name).
    """
    alias_group = find_alias_group(team_name)
    if alias_group:
        group_norms = {normalise_name(x) for x in alias_group}
        rows = fetch_all("SELECT id, name FROM teams")
        club_rows = [r for r in rows if normalise_name(r["name"]) in group_norms]

        if club_rows:
            ids = [r["id"] for r in club_rows]
            rep_name = club_rows[0]["name"]
            return ids, rep_name

    row = resolve_team_global(team_name)
    if not row:
        return [], team_name
    return [row["id"]], row["name"]


# ---------------------------------------------------------------------------
# Stats computation
# ---------------------------------------------------------------------------

def compute_head_to_head_stats_from_rows(
    rows: List[Dict[str, Any]],
    team_a_ids: Set[int],
    team_b_ids: Set[int],
    team_a_name: str,
    team_b_name: str,
) -> Dict[str, Any]:
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
            else:
                if a_away:
                    team_a_wins += 1
                elif b_away:
                    team_b_wins += 1

    def _rate(x: int) -> float:
        return round(100.0 * x / total, 1) if total > 0 else 0.0

    team_a_rate = _rate(team_a_wins)
    team_b_rate = _rate(team_b_wins)
    draw_rate = _rate(draws)

    current_streak: Optional[str] = None
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
            winner_is_home = home_score > away_score
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
# Pydantic models
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
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Rugby Analytics API",
    version=API_VERSION,
    description="Rugby analytics API – ID-based H2H with alias-aware all-leagues mode.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Small mapping helpers
# ---------------------------------------------------------------------------

def _build_match_summary_row(row: Dict[str, Any]) -> MatchSummary:
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
# Routes
# ---------------------------------------------------------------------------

@app.get("/health")
def health_check() -> Dict[str, Any]:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return {"status": "ok"}
    except Exception as exc:
        return JSONResponse(
            status_code=HTTP_500_INTERNAL_SERVER_ERROR,
            content={"status": "error", "detail": str(exc)},
        )


@app.get("/version")
def version() -> Dict[str, Any]:
    return {"version": API_VERSION}


@app.get("/leagues", response_model=List[LeagueInfo])
def list_leagues() -> List[LeagueInfo]:
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
    team_a: str = Query(..., description="Team A name (alias-aware)."),
    team_b: str = Query(..., description="Team B name (alias-aware)."),
    limit: int = Query(
        10,
        ge=1,
        le=100,
        description="How many recent matches to include in the history.",
    ),
) -> HeadToHeadResponse:
    """
    Head-to-head stats between two teams/clubs.

    IMPORTANT:
    - Only 404s when league or teams are missing.
    - If no matches exist after resolving IDs, returns total_matches=0.
      It does NOT raise an error like "No matches (played or upcoming)...".
    """
    league = None
    league_id: Optional[int] = None
    league_name: Optional[str] = None

    if tsdb_league_id != 0:
        league = resolve_league_by_tsdb(tsdb_league_id)
        if not league:
            raise HTTPException(status_code=404, detail="League not found")
        league_id = league["id"]
        league_name = league["name"]

    # Resolve clubs/teams to team_ids
    if tsdb_league_id == 0:
        team_a_ids, team_a_display_name = resolve_club_team_ids_all_leagues(team_a)
        team_b_ids, team_b_display_name = resolve_club_team_ids_all_leagues(team_b)

        if not team_a_ids:
            raise HTTPException(status_code=404, detail=f"Team A not found: {team_a}")
        if not team_b_ids:
            raise HTTPException(status_code=404, detail=f"Team B not found: {team_b}")
    else:
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

    # Played matches
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

    # Upcoming fixtures (future kickoffs)
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

    # Stats (NEVER raises on "no matches")
    stats = compute_head_to_head_stats_from_rows(
        rows,
        team_a_ids_set,
        team_b_ids_set,
        team_a_display_name,
        team_b_display_name,
    )

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
# Simple UI – version chip shows v{API_VERSION}
# ---------------------------------------------------------------------------

INDEX_HTML = f"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Rugby Head-to-Head</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    body {{
      margin: 0;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "SF Pro Text", sans-serif;
      background: radial-gradient(circle at top, #1e293b 0, #020617 55%, black 100%);
      color: #e5e7eb;
      min-height: 100vh;
      display: flex;
      justify-content: center;
      align-items: center;
      padding: 24px;
    }}
    .shell {{
      width: 100%;
      max-width: 1000px;
      background: rgba(15,23,42,0.96);
      border-radius: 24px;
      border: 1px solid rgba(148, 163, 184, 0.45);
      box-shadow: 0 25px 80px rgba(15,23,42,0.95);
      padding: 20px 20px 24px;
    }}
    header {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 16px;
      align-items: center;
    }}
    h1 {{
      margin: 0;
      font-size: 20px;
      font-weight: 650;
      letter-spacing: 0.03em;
      display: flex;
      align-items: center;
      gap: 8px;
    }}
    .chip {{
      font-size: 11px;
      padding: 3px 8px;
      border-radius: 999px;
      border: 1px solid rgba(148, 163, 184, 0.5);
      background: rgba(15,23,42,0.9);
      color: #9ca3af;
    }}
    main {{
      display: grid;
      grid-template-columns: minmax(0, 1.1fr) minmax(0, 1.3fr);
      gap: 16px;
    }}
    @media (max-width: 900px) {{
      main {{
        grid-template-columns: minmax(0, 1fr);
      }}
    }}
    .panel {{
      background: #020617;
      border-radius: 18px;
      border: 1px solid rgba(148,163,184,0.4);
      padding: 12px 14px 14px;
      box-shadow: 0 14px 30px rgba(15,23,42,0.8);
    }}
    label {{
      display: block;
      font-size: 11px;
      color: #9ca3af;
      margin-bottom: 4px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }}
    input, select {{
      width: 100%;
      padding: 6px 8px;
      font-size: 13px;
      border-radius: 10px;
      border: 1px solid rgba(148,163,184,0.6);
      background: #020617;
      color: #e5e7eb;
      outline: none;
    }}
    input:focus, select:focus {{
      border-color: #38bdf8;
      box-shadow: 0 0 0 1px rgba(56,189,248,0.6);
    }}
    .row {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
      margin-bottom: 10px;
    }}
    .actions {{
      display: flex;
      justify-content: flex-end;
      gap: 8px;
      margin-top: 4px;
      margin-bottom: 8px;
    }}
    button {{
      padding: 7px 12px;
      border-radius: 999px;
      border: 1px solid transparent;
      background: radial-gradient(circle at top left, #38bdf8, #0ea5e9);
      color: #0b1120;
      font-size: 12px;
      font-weight: 500;
      cursor: pointer;
      box-shadow: 0 12px 30px rgba(56,189,248,0.6);
    }}
    button.secondary {{
      background: #020617;
      color: #9ca3af;
      border-color: rgba(148,163,184,0.5);
      box-shadow: none;
    }}
    .error {{
      display: none;
      margin-top: 6px;
      padding: 6px 8px;
      border-radius: 10px;
      font-size: 11px;
      background: rgba(127,29,29,0.9);
      border: 1px solid rgba(248,113,113,0.7);
      color: #fee2e2;
    }}
    .summary {{
      font-size: 12px;
      margin-bottom: 6px;
    }}
    .summary strong {{
      font-weight: 600;
    }}
    .grid-small {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 6px;
      font-size: 11px;
    }}
    .stat {{
      padding: 6px 8px;
      border-radius: 10px;
      border: 1px solid rgba(148,163,184,0.4);
      background: #020617;
    }}
    .stat span.label {{
      display: block;
      font-size: 10px;
      color: #9ca3af;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 2px;
    }}
    .stat span.value {{
      font-weight: 600;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 11px;
    }}
    th, td {{
      padding: 5px 6px;
      border-bottom: 1px solid rgba(30,41,59,0.9);
      white-space: nowrap;
    }}
    th {{
      color: #9ca3af;
      text-transform: uppercase;
      font-size: 10px;
      letter-spacing: 0.08em;
    }}
    tbody tr:nth-child(even) {{
      background: rgba(15,23,42,0.95);
    }}
    tbody tr:hover {{
      background: rgba(30,64,175,0.5);
    }}
    .footnote {{
      font-size: 10px;
      color: #9ca3af;
      white-space: normal;
    }}
  </style>
</head>
<body>
  <div class="shell">
    <header>
      <div>
        <h1>
          Rugby Head-to-Head
          <span class="chip">Rugby Analytics · v{API_VERSION}</span>
        </h1>
        <div style="font-size:11px;color:#9ca3af;margin-top:4px;">
          ID-based, alias-aware, multi-league head-to-head across all ingested data.
        </div>
      </div>
      <div class="chip">
        Backend v{API_VERSION}
      </div>
    </header>

    <main>
      <section class="panel">
        <div style="font-size:12px;margin-bottom:8px;color:#9ca3af;">
          Choose a league (or all leagues) and two teams. In all-leagues mode,
          aliases like Stormers/WP and Bulls/Blue Bulls are merged.
        </div>
        <div class="row">
          <div>
            <label for="league">League</label>
            <select id="league">
              <option value="0">All leagues (alias-aware)</option>
            </select>
          </div>
          <div>
            <label for="limit">Recent games</label>
            <input type="number" id="limit" min="1" max="100" value="10" />
          </div>
        </div>
        <div class="row">
          <div>
            <label for="team-a">Team A</label>
            <input id="team-a" type="text" placeholder="e.g. Stormers" />
          </div>
          <div>
            <label for="team-b">Team B</label>
            <input id="team-b" type="text" placeholder="e.g. Blue Bulls" />
          </div>
        </div>
        <div class="actions">
          <button class="secondary" type="button" id="swap-btn">Swap</button>
          <button type="button" id="compare-btn">Compare</button>
        </div>
        <div id="error" class="error"></div>
      </section>

      <section class="panel">
        <div class="summary">
          <strong id="summary-league">Awaiting selection</strong>
        </div>
        <div class="summary">
          <strong id="summary-teams">Team A vs Team B</strong>
        </div>
        <div class="grid-small" style="margin-bottom:8px;">
          <div class="stat">
            <span class="label">Total meetings</span>
            <span class="value" id="total-matches">0</span>
          </div>
          <div class="stat">
            <span class="label">Team A wins</span>
            <span class="value"><span id="team-a-wins">0</span> (<span id="team-a-rate">0%</span>)</span>
          </div>
          <div class="stat">
            <span class="label">Team B wins</span>
            <span class="value"><span id="team-b-wins">0</span> (<span id="team-b-rate">0%</span>)</span>
          </div>
        </div>
        <div class="grid-small" style="margin-bottom:10px;">
          <div class="stat">
            <span class="label">Draws</span>
            <span class="value"><span id="draws-count">0</span> (<span id="draws-rate">0%</span>)</span>
          </div>
          <div class="stat">
            <span class="label">Current streak</span>
            <span class="value" id="current-streak">–</span>
          </div>
          <div class="stat">
            <span class="label">Sample</span>
            <span class="value" id="sample-note">No games yet</span>
          </div>
        </div>
        <div style="font-size:11px;margin-bottom:4px;color:#9ca3af;">Recent results</div>
        <div style="max-height:180px;overflow:auto;border-radius:10px;border:1px solid rgba(30,41,59,0.9);">
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
                <td colspan="6" class="footnote">
                  Results will appear here after you run a comparison.
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>
    </main>
  </div>

  <script>
    const leagueSelect = document.getElementById("league");
    const limitInput = document.getElementById("limit");
    const teamAInput = document.getElementById("team-a");
    const teamBInput = document.getElementById("team-b");
    const compareBtn = document.getElementById("compare-btn");
    const swapBtn = document.getElementById("swap-btn");
    const errorBox = document.getElementById("error");

    const summaryLeague = document.getElementById("summary-league");
    const summaryTeams = document.getElementById("summary-teams");
    const totalMatches = document.getElementById("total-matches");
    const teamAWins = document.getElementById("team-a-wins");
    const teamBWins = document.getElementById("team-b-wins");
    const teamARate = document.getElementById("team-a-rate");
    const teamBRate = document.getElementById("team-b-rate");
    const drawsCount = document.getElementById("draws-count");
    const drawsRate = document.getElementById("draws-rate");
    const currentStreak = document.getElementById("current-streak");
    const sampleNote = document.getElementById("sample-note");
    const historyBody = document.getElementById("history-body");

    function setError(msg) {{
      if (!msg) {{
        errorBox.style.display = "none";
        errorBox.textContent = "";
      }} else {{
        errorBox.style.display = "block";
        errorBox.textContent = msg;
      }}
    }}

    async function fetchJSON(url) {{
      const res = await fetch(url);
      let data;
      try {{
        data = await res.json();
      }} catch (e) {{
        throw new Error("Bad JSON from server");
      }}
      if (!res.ok) {{
        throw new Error(data.detail || JSON.stringify(data));
      }}
      return data;
    }}

    function formatDate(iso) {{
      try {{
        const d = new Date(iso);
        return d.toLocaleString(undefined, {{
          year: "numeric",
          month: "short",
          day: "numeric",
          hour: "2-digit",
          minute: "2-digit",
        }});
      }} catch {{
        return iso;
      }}
    }}

    function renderHistory(matches) {{
      historyBody.innerHTML = "";
      if (!matches || !matches.length) {{
        const tr = document.createElement("tr");
        const td = document.createElement("td");
        td.colSpan = 6;
        td.className = "footnote";
        td.textContent = "No historical results found in the sample.";
        tr.appendChild(td);
        historyBody.appendChild(tr);
        return;
      }}
      for (const m of matches) {{
        const tr = document.createElement("tr");
        const score = (m.home_score == null || m.away_score == null)
          ? "TBD"
          : `${{m.home_score}}–${{m.away_score}}`;
        const comp = m.league || "—";
        const season = m.season || "—";
        tr.innerHTML = `
          <td>${{formatDate(m.kickoff_utc)}}</td>
          <td>${{m.home_team}}</td>
          <td>${{score}}</td>
          <td>${{m.away_team}}</td>
          <td>${{comp}}</td>
          <td>${{season}}</td>
        `;
        historyBody.appendChild(tr);
      }}
    }}

    async function loadLeagues() {{
      try {{
        const leagues = await fetchJSON("/leagues");
        for (const lg of leagues) {{
          const opt = document.createElement("option");
          opt.value = lg.tsdb_league_id ?? "";
          opt.textContent = lg.country ? `${{lg.name}} (${{lg.country}})` : lg.name;
          leagueSelect.appendChild(opt);
        }}
      }} catch (err) {{
        console.error("Leagues load failed:", err);
        setError("Could not load leagues.");
      }}
    }}

    async function onCompare() {{
      setError("");
      const tsdbLeagueId = leagueSelect.value || "0";
      const teamA = teamAInput.value.trim();
      const teamB = teamBInput.value.trim();
      const limit = limitInput.value || "10";

      if (!teamA || !teamB) {{
        setError("Please enter both Team A and Team B.");
        return;
      }}

      const qs = new URLSearchParams({{
        team_a: teamA,
        team_b: teamB,
        limit: String(limit),
      }});

      try {{
        const url = `/headtohead/${{encodeURIComponent(tsdbLeagueId)}}?${{qs.toString()}}`;
        const data = await fetchJSON(url);

        summaryLeague.textContent = data.league_name || "All leagues (alias-aware)";
        summaryTeams.textContent = `${{data.team_a_name}} vs ${{data.team_b_name}}`;

        totalMatches.textContent = String(data.total_matches);
        teamAWins.textContent = String(data.team_a_wins);
        teamBWins.textContent = String(data.team_b_wins);
        teamARate.textContent = `${{data.team_a_win_rate}}%`;
        teamBRate.textContent = `${{data.team_b_win_rate}}%`;
        drawsCount.textContent = String(data.draws);
        drawsRate.textContent = `${{data.draws_rate}}%`;
        currentStreak.textContent = data.current_streak || "No recent result";

        if (!data.total_matches) {{
          sampleNote.textContent = "0 matches in this sample.";
        }} else {{
          sampleNote.textContent = "Based on completed matches returned.";
        }}

        renderHistory(data.last_matches || []);
      }} catch (err) {{
        console.error("Compare error:", err);
        setError(err.message || "Error running comparison.");
      }}
    }}

    function onSwap() {{
      const t = teamAInput.value;
      teamAInput.value = teamBInput.value;
      teamBInput.value = t;
    }}

    compareBtn.addEventListener("click", onCompare);
    swapBtn.addEventListener("click", onSwap);
    document.addEventListener("keydown", (e) => {{
      if (e.key === "Enter") {{
        const active = document.activeElement;
        if (active === teamAInput || active === teamBInput || active === limitInput) {{
          onCompare();
        }}
      }}
    }});

    loadLeagues();
  </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse(content=INDEX_HTML)
