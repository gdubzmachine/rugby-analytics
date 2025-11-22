#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
api/main.py
-----------

Small FastAPI app exposing your rugby analytics DB as a JSON API.

Endpoints:

  GET /health
      - Simple health check.

  GET /standings/{tsdb_league_id}
      - Query params:
          season_label: optional, e.g. 2023-2024
          latest: bool, default false – if true and no season_label given,
                  use latest season (by seasons.year)
      - Returns league table from team_season_stats.

  GET /teams/{tsdb_league_id}
      - Query params:
          season_label: optional (same semantics as print_league_teams.py)
          latest: bool, default false
      - Returns list of teams (team_id, name, tsdb_team_id, seasons present).

  GET /fixtures/{tsdb_league_id}/{team_id}
      - Query params:
          season_label: optional
          latest: bool, default false
      - Returns fixtures/results for that team.

  GET /form/{tsdb_league_id}/{team_id}
      - Query params:
          season_label: optional
          latest: bool, default false
          limit: int, default 5 (max 100)
      - Returns last N completed matches for that team + form summary.

  GET /league_form/{tsdb_league_id}
      - Query params:
          season_label: optional
          latest: bool, default false
          limit: int, default 5 (max 100)
      - Returns a "form table": last N completed games per team in league.

  GET /home_away/{tsdb_league_id}/{team_id}
      - Query params:
          season_label: optional
          latest: bool, default false
      - Returns home/away/combined splits for a team in a league+season.

  GET /h2h/{tsdb_league_id}/{team_a_id}/{team_b_id}
      - Query params:
          season_label: optional
          limit: optional int, how many *most recent* matches to include
                 (e.g. 1–50, default = all)
      - Returns head-to-head summary + match list, including:
          * totals (games, W/D/L, PF/PA)
          * averages (avg PF/PA + avg margin) over the filtered matches.

Assumes:
  - .env has DATABASE_URL
  - DB schema already set up by your ingest scripts
"""

import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

# Ensure project root is on path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# DB imports
import psycopg2
from psycopg2.extras import DictCursor

# Optional db helper
try:
    from db.connection import get_db_connection  # type: ignore
except Exception:
    get_db_connection = None  # type: ignore


# ---------------------------------------------------------------------------
# Env / DB helpers
# ---------------------------------------------------------------------------

def _load_dotenv_if_available() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        pass


_load_dotenv_if_available()


def _get_conn():
    """
    Get a DB connection, preferring db.connection.get_db_connection().
    """
    if get_db_connection is not None:
        return get_db_connection()  # type: ignore

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "DATABASE_URL not set and db.connection.get_db_connection() missing. "
            "Set DATABASE_URL in .env or implement db/connection.get_db_connection()."
        )
    return psycopg2.connect(dsn)


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


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class LeagueTableRow(BaseModel):
    team_id: int
    team_name: str
    games_played: int
    wins: int
    draws: int
    losses: int
    points_for: int
    points_against: int
    points_diff: int
    competition_points: int
    losing_bonus_points: Optional[int] = None
    try_bonus_points: Optional[int] = None


class LeagueTableResponse(BaseModel):
    league_name: str
    tsdb_league_id: str
    season_label: str
    rows: List[LeagueTableRow]


class TeamInfo(BaseModel):
    team_id: int
    team_name: str
    tsdb_team_id: Optional[str] = None
    season_label: str


class LeagueTeamsResponse(BaseModel):
    tsdb_league_id: str
    season_label: Optional[str]
    teams: List[TeamInfo]


class FixtureRow(BaseModel):
    match_id: int
    kickoff_utc: Optional[str]
    side: str             # 'H' or 'A'
    opponent: str
    scored: Optional[int]
    conceded: Optional[int]
    result: str           # 'W', 'D', 'L', '?'


class FixturesResponse(BaseModel):
    league_name: str
    tsdb_league_id: str
    season_label: str
    team_id: int
    team_name: str
    fixtures: List[FixtureRow]


class TeamFormMatch(BaseModel):
    match_id: int
    kickoff_utc: Optional[str]
    side: str             # 'H' or 'A'
    opponent: str
    scored: int
    conceded: int
    result: str           # 'W', 'D', 'L', '?'


class TeamFormSummary(BaseModel):
    games: int
    wins: int
    draws: int
    losses: int
    points_for: int
    points_against: int
    points_diff: int
    avg_points_for: Optional[float]
    avg_points_against: Optional[float]
    avg_margin: Optional[float]


class TeamFormResponse(BaseModel):
    league_name: str
    tsdb_league_id: str
    season_label: str
    team_id: int
    team_name: str
    limit: int
    matches: List[TeamFormMatch]
    summary: TeamFormSummary


class LeagueFormRow(BaseModel):
    team_id: int
    team_name: str
    games: int
    wins: int
    draws: int
    losses: int
    points_for: int
    points_against: int
    points_diff: int
    avg_points_for: Optional[float]
    avg_points_against: Optional[float]
    avg_margin: Optional[float]


class LeagueFormResponse(BaseModel):
    league_name: str
    tsdb_league_id: str
    season_label: str
    limit: int
    teams: List[LeagueFormRow]


class HomeAwayRecord(BaseModel):
    games: int
    wins: int
    draws: int
    losses: int
    points_for: int
    points_against: int
    points_diff: int
    avg_points_for: Optional[float]
    avg_points_against: Optional[float]
    avg_margin: Optional[float]


class HomeAwayResponse(BaseModel):
    league_name: str
    tsdb_league_id: str
    season_label: str
    team_id: int
    team_name: str
    home: HomeAwayRecord
    away: HomeAwayRecord
    combined: HomeAwayRecord


class H2HMatchRow(BaseModel):
    match_id: int
    kickoff_utc: Optional[str]
    home_team_name: str
    away_team_name: str
    home_score: Optional[int]
    away_score: Optional[int]
    result_for_team_a: str   # 'W', 'D', 'L', '?'


class H2HSummary(BaseModel):
    games: int                    # total matches returned (incl. without scores)
    completed_games: int          # matches with non-null scores
    team_a_wins: int
    team_a_draws: int
    team_a_losses: int
    team_a_pf: int
    team_a_pa: int
    team_a_avg_pf: Optional[float]
    team_a_avg_pa: Optional[float]
    team_a_avg_margin: Optional[float]
    team_b_wins: int
    team_b_draws: int
    team_b_losses: int
    team_b_pf: int
    team_b_pa: int
    team_b_avg_pf: Optional[float]
    team_b_avg_pa: Optional[float]
    team_b_avg_margin: Optional[float]


class H2HResponse(BaseModel):
    league_name: str
    tsdb_league_id: str
    season_labels: List[str]
    team_a_id: int
    team_a_name: str
    team_b_id: int
    team_b_name: str
    summary: H2HSummary
    matches: List[H2HMatchRow]


# ---------------------------------------------------------------------------
# Helpers for seasons, results, etc.
# ---------------------------------------------------------------------------

def _resolve_season_label(
    cur,
    tsdb_league_id: str,
    season_label: Optional[str],
    latest: bool,
) -> str:
    """
    If season_label is provided, return it.
    Else if latest=True, pick the season with max year.
    Else error.
    """
    if season_label:
        return season_label

    if not latest:
        raise HTTPException(
            status_code=400,
            detail="Must provide season_label or set latest=true",
        )

    cur.execute(
        """
        SELECT s.label, s.year
        FROM seasons s
        JOIN leagues l ON l.league_id = s.league_id
        WHERE l.tsdb_league_id = %s
        ORDER BY s.year DESC NULLS LAST
        LIMIT 1
        """,
        (tsdb_league_id,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"No seasons found for tsdb_league_id={tsdb_league_id}",
        )
    return row["label"]


def _compute_result_for_team(row: Dict[str, Any], team_id: int) -> str:
    """
    Return result from perspective of team_id: 'W', 'D', 'L', '?'
    """
    hs = row["home_score"]
    as_ = row["away_score"]

    if hs is None or as_ is None:
        return "?"

    hs = int(hs)
    as_ = int(as_)

    if row["home_team_id"] == team_id:
        if hs > as_:
            return "W"
        elif hs < as_:
            return "L"
        else:
            return "D"
    elif row["away_team_id"] == team_id:
        if as_ > hs:
            return "W"
        elif as_ < hs:
            return "L"
        else:
            return "D"
    else:
        return "?"


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="Rugby Analytics API")


@app.get("/health")
def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# /standings
# ---------------------------------------------------------------------------

@app.get(
    "/standings/{tsdb_league_id}",
    response_model=LeagueTableResponse,
)
def get_standings(
    tsdb_league_id: str,
    season_label: Optional[str] = Query(default=None),
    latest: bool = Query(default=False),
):
    conn = _get_conn()
    cur = conn.cursor(cursor_factory=DictCursor)
    try:
        resolved_season = _resolve_season_label(cur, tsdb_league_id, season_label, latest)
        stats_cols = _get_table_columns(cur, "team_season_stats")
        has_lb = "losing_bonus_points" in stats_cols
        has_tb = "try_bonus_points" in stats_cols

        select_cols = [
            "l.name AS league_name",
            "l.tsdb_league_id",
            "s.label AS season_label",
            "t.team_id",
            "t.name AS team_name",
            "stats.games_played",
            "stats.wins",
            "stats.draws",
            "stats.losses",
            "stats.points_for",
            "stats.points_against",
            "stats.points_diff",
            "stats.competition_points",
        ]
        if has_lb:
            select_cols.append("stats.losing_bonus_points")
        if has_tb:
            select_cols.append("stats.try_bonus_points")

        select_sql = ",\n            ".join(select_cols)

        sql = f"""
            SELECT
                {select_sql}
            FROM team_season_stats stats
            JOIN teams t
              ON t.team_id = stats.team_id
            JOIN leagues l
              ON l.league_id = stats.league_id
            JOIN seasons s
              ON s.season_id = stats.season_id
            WHERE l.tsdb_league_id = %s
              AND s.label = %s
            ORDER BY
              stats.competition_points DESC,
              stats.points_diff DESC,
              stats.points_for DESC,
              t.name ASC
        """

        cur.execute(sql, (tsdb_league_id, resolved_season))
        rows = cur.fetchall()

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No standings found for league={tsdb_league_id}, season={resolved_season}",
            )

        first = rows[0]
        league_name = first["league_name"]
        result_rows: List[LeagueTableRow] = []

        for r in rows:
            data = {
                "team_id": r["team_id"],
                "team_name": r["team_name"],
                "games_played": r["games_played"],
                "wins": r["wins"],
                "draws": r["draws"],
                "losses": r["losses"],
                "points_for": r["points_for"],
                "points_against": r["points_against"],
                "points_diff": r["points_diff"],
                "competition_points": r["competition_points"],
            }
            if has_lb:
                data["losing_bonus_points"] = r["losing_bonus_points"]
            if has_tb:
                data["try_bonus_points"] = r["try_bonus_points"]
            result_rows.append(LeagueTableRow(**data))

        return LeagueTableResponse(
            league_name=league_name,
            tsdb_league_id=str(tsdb_league_id),
            season_label=resolved_season,
            rows=result_rows,
        )
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# /teams
# ---------------------------------------------------------------------------

@app.get(
    "/teams/{tsdb_league_id}",
    response_model=LeagueTeamsResponse,
)
def get_league_teams(
    tsdb_league_id: str,
    season_label: Optional[str] = Query(default=None),
    latest: bool = Query(default=False),
):
    conn = _get_conn()
    cur = conn.cursor(cursor_factory=DictCursor)
    try:
        resolved_label = season_label
        if not resolved_label and latest:
            resolved_label = _resolve_season_label(cur, tsdb_league_id, None, True)

        cols_teams = _get_table_columns(cur, "teams")
        has_tsdb_team_id = "tsdb_team_id" in cols_teams

        base_sql = """
            SELECT DISTINCT
                t.team_id,
                t.name AS team_name,
                s.label AS season_label
        """
        if has_tsdb_team_id:
            base_sql += ", t.tsdb_team_id"

        base_sql += """
            FROM matches m
            JOIN leagues l
              ON l.league_id = m.league_id
            JOIN seasons s
              ON s.season_id = m.season_id
            JOIN teams t
              ON t.team_id = m.home_team_id
                 OR t.team_id = m.away_team_id
            WHERE l.tsdb_league_id = %s
        """

        params: List[Any] = [tsdb_league_id]

        if resolved_label:
            base_sql += " AND s.label = %s"
            params.append(resolved_label)

        base_sql += " ORDER BY t.name ASC"

        cur.execute(base_sql, tuple(params))
        rows = cur.fetchall()

        teams: List[TeamInfo] = []
        for r in rows:
            teams.append(
                TeamInfo(
                    team_id=r["team_id"],
                    team_name=r["team_name"],
                    tsdb_team_id=r.get("tsdb_team_id") if has_tsdb_team_id else None,
                    season_label=r["season_label"],
                )
            )

        return LeagueTeamsResponse(
            tsdb_league_id=str(tsdb_league_id),
            season_label=resolved_label,
            teams=teams,
        )
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# /fixtures
# ---------------------------------------------------------------------------

@app.get(
    "/fixtures/{tsdb_league_id}/{team_id}",
    response_model=FixturesResponse,
)
def get_team_fixtures(
    tsdb_league_id: str,
    team_id: int,
    season_label: Optional[str] = Query(default=None),
    latest: bool = Query(default=False),
):
    conn = _get_conn()
    cur = conn.cursor(cursor_factory=DictCursor)
    try:
        resolved_season = _resolve_season_label(cur, tsdb_league_id, season_label, latest)

        sql = """
            SELECT
                m.match_id,
                m.kickoff_utc,
                m.home_team_id,
                m.away_team_id,
                m.home_score,
                m.away_score,
                th.name AS home_team_name,
                ta.name AS away_team_name,
                l.name  AS league_name,
                l.tsdb_league_id,
                s.label AS season_label
            FROM matches m
            JOIN leagues l
              ON l.league_id = m.league_id
            JOIN seasons s
              ON s.season_id = m.season_id
            JOIN teams th
              ON th.team_id = m.home_team_id
            JOIN teams ta
              ON ta.team_id = m.away_team_id
            WHERE l.tsdb_league_id = %s
              AND s.label = %s
              AND (m.home_team_id = %s OR m.away_team_id = %s)
            ORDER BY m.kickoff_utc NULLS LAST, m.match_id ASC
        """
        cur.execute(sql, (tsdb_league_id, resolved_season, team_id, team_id))
        rows = cur.fetchall()
        matches = [dict(r) for r in rows]

        if not matches:
            raise HTTPException(
                status_code=404,
                detail=f"No matches found for team_id={team_id}, league={tsdb_league_id}, season={resolved_season}",
            )

        # Determine team name from first matching row
        team_name = None
        league_name = matches[0]["league_name"]
        for r in matches:
            if r["home_team_id"] == team_id:
                team_name = r["home_team_name"]
                break
            if r["away_team_id"] == team_id:
                team_name = r["away_team_name"]
                break
        if team_name is None:
            team_name = f"team_id={team_id}"

        fixtures: List[FixtureRow] = []
        for r in matches:
            is_home = (r["home_team_id"] == team_id)
            side = "H" if is_home else "A"
            opponent = r["away_team_name"] if is_home else r["home_team_name"]
            scored = r["home_score"] if is_home else r["away_score"]
            conceded = r["away_score"] if is_home else r["home_score"]
            result = _compute_result_for_team(r, team_id)

            dt = r["kickoff_utc"]
            dt_str = dt.isoformat() if dt is not None else None

            fixtures.append(
                FixtureRow(
                    match_id=r["match_id"],
                    kickoff_utc=dt_str,
                    side=side,
                    opponent=opponent,
                    scored=scored,
                    conceded=conceded,
                    result=result,
                )
            )

        return FixturesResponse(
            league_name=league_name,
            tsdb_league_id=str(tsdb_league_id),
            season_label=resolved_season,
            team_id=team_id,
            team_name=team_name,
            fixtures=fixtures,
        )
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# /form  (team last N games + summary)
# ---------------------------------------------------------------------------

@app.get(
    "/form/{tsdb_league_id}/{team_id}",
    response_model=TeamFormResponse,
)
def get_team_form(
    tsdb_league_id: str,
    team_id: int,
    season_label: Optional[str] = Query(default=None),
    latest: bool = Query(default=False),
    limit: int = Query(default=5, ge=1, le=100),
):
    """
    Team form: last N *completed* games for a team in a league + season.

    - If season_label is provided -> use that season.
    - Else if latest=True -> use latest season by year.
    - Matches are filtered to those with non-null scores (played games only).
    - Ordered most recent first, up to 'limit' matches.
    """
    conn = _get_conn()
    cur = conn.cursor(cursor_factory=DictCursor)
    try:
        resolved_season = _resolve_season_label(
            cur,
            tsdb_league_id=tsdb_league_id,
            season_label=season_label,
            latest=latest,
        )

        sql = """
            SELECT
                m.match_id,
                m.kickoff_utc,
                m.home_team_id,
                m.away_team_id,
                m.home_score,
                m.away_score,
                th.name AS home_team_name,
                ta.name AS away_team_name,
                l.name  AS league_name,
                l.tsdb_league_id,
                s.label AS season_label
            FROM matches m
            JOIN leagues l
              ON l.league_id = m.league_id
            JOIN seasons s
              ON s.season_id = m.season_id
            JOIN teams th
              ON th.team_id = m.home_team_id
            JOIN teams ta
              ON ta.team_id = m.away_team_id
            WHERE l.tsdb_league_id = %s
              AND s.label = %s
              AND (m.home_team_id = %s OR m.away_team_id = %s)
              AND m.home_score IS NOT NULL
              AND m.away_score IS NOT NULL
            ORDER BY m.kickoff_utc DESC NULLS LAST, m.match_id DESC
            LIMIT %s
        """
        cur.execute(sql, (tsdb_league_id, resolved_season, team_id, team_id, limit))
        rows = cur.fetchall()
        matches = [dict(r) for r in rows]

        if not matches:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"No completed matches found for team_id={team_id}, "
                    f"league={tsdb_league_id}, season={resolved_season}"
                ),
            )

        league_name = matches[0]["league_name"]

        # Determine team name from first row
        team_name = None
        for r in matches:
            if r["home_team_id"] == team_id:
                team_name = r["home_team_name"]
                break
            if r["away_team_id"] == team_id:
                team_name = r["away_team_name"]
                break
        if team_name is None:
            team_name = f"team_id={team_id}"

        # Build match list + aggregate summary
        form_matches: List[TeamFormMatch] = []
        games = 0
        wins = draws = losses = 0
        pf = pa = 0

        for r in matches:
            games += 1
            is_home = (r["home_team_id"] == team_id)
            side = "H" if is_home else "A"
            opponent = r["away_team_name"] if is_home else r["home_team_name"]
            scored = int(r["home_score"] if is_home else r["away_score"])
            conceded = int(r["away_score"] if is_home else r["home_score"])
            result = _compute_result_for_team(r, team_id)

            if result == "W":
                wins += 1
            elif result == "L":
                losses += 1
            elif result == "D":
                draws += 1

            pf += scored
            pa += conceded

            dt = r["kickoff_utc"]
            dt_str = dt.isoformat() if dt is not None else None

            form_matches.append(
                TeamFormMatch(
                    match_id=r["match_id"],
                    kickoff_utc=dt_str,
                    side=side,
                    opponent=opponent,
                    scored=scored,
                    conceded=conceded,
                    result=result,
                )
            )

        points_diff = pf - pa
        if games > 0:
            avg_pf = pf / games
            avg_pa = pa / games
            avg_margin = points_diff / games
        else:
            avg_pf = avg_pa = avg_margin = None

        summary = TeamFormSummary(
            games=games,
            wins=wins,
            draws=draws,
            losses=losses,
            points_for=pf,
            points_against=pa,
            points_diff=points_diff,
            avg_points_for=avg_pf,
            avg_points_against=avg_pa,
            avg_margin=avg_margin,
        )

        return TeamFormResponse(
            league_name=league_name,
            tsdb_league_id=str(tsdb_league_id),
            season_label=resolved_season,
            team_id=team_id,
            team_name=team_name,
            limit=limit,
            matches=form_matches,
            summary=summary,
        )
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# /league_form  (form table: last N games per team)
# ---------------------------------------------------------------------------

@app.get(
    "/league_form/{tsdb_league_id}",
    response_model=LeagueFormResponse,
)
def get_league_form(
    tsdb_league_id: str,
    season_label: Optional[str] = Query(default=None),
    latest: bool = Query(default=False),
    limit: int = Query(default=5, ge=1, le=100),
):
    """
    League form table: for each team in a league+season, consider its
    last N *completed* games (limit), and compute W/D/L + averages.

    - If season_label is provided -> use that season.
    - Else if latest=True -> use latest season by year.
    - Only matches with non-null scores are included.
    """
    conn = _get_conn()
    cur = conn.cursor(cursor_factory=DictCursor)
    try:
        resolved_season = _resolve_season_label(
            cur,
            tsdb_league_id=tsdb_league_id,
            season_label=season_label,
            latest=latest,
        )

        # Load all completed matches for this league+season
        sql = """
            SELECT
                m.match_id,
                m.kickoff_utc,
                m.home_team_id,
                m.away_team_id,
                m.home_score,
                m.away_score,
                th.name AS home_team_name,
                ta.name AS away_team_name,
                l.name  AS league_name,
                l.tsdb_league_id,
                s.label AS season_label
            FROM matches m
            JOIN leagues l
              ON l.league_id = m.league_id
            JOIN seasons s
              ON s.season_id = m.season_id
            JOIN teams th
              ON th.team_id = m.home_team_id
            JOIN teams ta
              ON ta.team_id = m.away_team_id
            WHERE l.tsdb_league_id = %s
              AND s.label = %s
              AND m.home_score IS NOT NULL
              AND m.away_score IS NOT NULL
        """
        cur.execute(sql, (tsdb_league_id, resolved_season))
        rows = cur.fetchall()
        matches = [dict(r) for r in rows]

        if not matches:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"No completed matches found for league={tsdb_league_id}, "
                    f"season={resolved_season}"
                ),
            )

        league_name = matches[0]["league_name"]

        # Group matches by team_id, tagging whether they were home or away
        from collections import defaultdict

        matches_by_team: Dict[int, List[Dict[str, Any]]] = defaultdict(list)

        for r in matches:
            # home side
            matches_by_team[r["home_team_id"]].append(
                {"row": r, "is_home": True}
            )
            # away side
            matches_by_team[r["away_team_id"]].append(
                {"row": r, "is_home": False}
            )

        teams_rows: List[LeagueFormRow] = []

        for team_id, t_matches in matches_by_team.items():
            # sort by kickoff descending (most recent first)
            def sort_key(entry: Dict[str, Any]):
                row = entry["row"]
                dt = row["kickoff_utc"]
                if dt is None:
                    # treat missing as very old
                    return (datetime.min, row["match_id"])
                return (dt, row["match_id"])

            t_matches_sorted = sorted(t_matches, key=sort_key, reverse=True)
            t_selected = t_matches_sorted[:limit]

            games = 0
            wins = draws = losses = 0
            pf = pa = 0
            team_name = None

            for entry in t_selected:
                row = entry["row"]
                is_home = entry["is_home"]
                games += 1

                if is_home:
                    name_here = row["home_team_name"]
                    scored = int(row["home_score"])
                    conceded = int(row["away_score"])
                else:
                    name_here = row["away_team_name"]
                    scored = int(row["away_score"])
                    conceded = int(row["home_score"])

                if team_name is None:
                    team_name = name_here

                # compute result from perspective of this team
                res = _compute_result_for_team(row, team_id)
                if res == "W":
                    wins += 1
                elif res == "L":
                    losses += 1
                elif res == "D":
                    draws += 1

                pf += scored
                pa += conceded

            if games == 0:
                continue

            points_diff = pf - pa
            avg_pf = pf / games
            avg_pa = pa / games
            avg_margin = points_diff / games

            if team_name is None:
                team_name = f"team_id={team_id}"

            teams_rows.append(
                LeagueFormRow(
                    team_id=team_id,
                    team_name=team_name,
                    games=games,
                    wins=wins,
                    draws=draws,
                    losses=losses,
                    points_for=pf,
                    points_against=pa,
                    points_diff=points_diff,
                    avg_points_for=avg_pf,
                    avg_points_against=avg_pa,
                    avg_margin=avg_margin,
                )
            )

        # Sort league form table: by wins, then points_diff, then points_for
        teams_rows_sorted = sorted(
            teams_rows,
            key=lambda r: (r.wins, r.points_diff, r.points_for),
            reverse=True,
        )

        return LeagueFormResponse(
            league_name=league_name,
            tsdb_league_id=str(tsdb_league_id),
            season_label=resolved_season,
            limit=limit,
            teams=teams_rows_sorted,
        )
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# /home_away  (home/away splits for a team)
# ---------------------------------------------------------------------------

@app.get(
    "/home_away/{tsdb_league_id}/{team_id}",
    response_model=HomeAwayResponse,
)
def get_home_away_split(
    tsdb_league_id: str,
    team_id: int,
    season_label: Optional[str] = Query(default=None),
    latest: bool = Query(default=False),
):
    """
    Home/away splits for a given team in a league+season.

    - If season_label is provided -> use that season.
    - Else if latest=True -> use latest season by year.
    - Only completed games (non-null scores) are considered.
    """
    conn = _get_conn()
    cur = conn.cursor(cursor_factory=DictCursor)
    try:
        resolved_season = _resolve_season_label(
            cur,
            tsdb_league_id=tsdb_league_id,
            season_label=season_label,
            latest=latest,
        )

        sql = """
            SELECT
                m.match_id,
                m.kickoff_utc,
                m.home_team_id,
                m.away_team_id,
                m.home_score,
                m.away_score,
                th.name AS home_team_name,
                ta.name AS away_team_name,
                l.name  AS league_name,
                l.tsdb_league_id,
                s.label AS season_label
            FROM matches m
            JOIN leagues l
              ON l.league_id = m.league_id
            JOIN seasons s
              ON s.season_id = m.season_id
            JOIN teams th
              ON th.team_id = m.home_team_id
            JOIN teams ta
              ON ta.team_id = m.away_team_id
            WHERE l.tsdb_league_id = %s
              AND s.label = %s
              AND (m.home_team_id = %s OR m.away_team_id = %s)
              AND m.home_score IS NOT NULL
              AND m.away_score IS NOT NULL
        """
        cur.execute(sql, (tsdb_league_id, resolved_season, team_id, team_id))
        rows = cur.fetchall()
        matches = [dict(r) for r in rows]

        if not matches:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"No completed matches found for team_id={team_id}, "
                    f"league={tsdb_league_id}, season={resolved_season}"
                ),
            )

        league_name = matches[0]["league_name"]

        # Determine team name from first row
        team_name = None
        for r in matches:
            if r["home_team_id"] == team_id:
                team_name = r["home_team_name"]
                break
            if r["away_team_id"] == team_id:
                team_name = r["away_team_name"]
                break
        if team_name is None:
            team_name = f"team_id={team_id}"

        # Aggregators for home, away, combined
        def _empty():
            return {
                "games": 0,
                "wins": 0,
                "draws": 0,
                "losses": 0,
                "pf": 0,
                "pa": 0,
            }

        home_stats = _empty()
        away_stats = _empty()
        combined_stats = _empty()

        def _update(stats: Dict[str, int], scored: int, conceded: int, result: str) -> None:
            stats["games"] += 1
            stats["pf"] += scored
            stats["pa"] += conceded
            if result == "W":
                stats["wins"] += 1
            elif result == "L":
                stats["losses"] += 1
            elif result == "D":
                stats["draws"] += 1

        for r in matches:
            is_home = (r["home_team_id"] == team_id)
            if is_home:
                scored = int(r["home_score"])
                conceded = int(r["away_score"])
            else:
                scored = int(r["away_score"])
                conceded = int(r["home_score"])

            result = _compute_result_for_team(r, team_id)

            # Update home/away
            if is_home:
                _update(home_stats, scored, conceded, result)
            else:
                _update(away_stats, scored, conceded, result)

            # Update combined
            _update(combined_stats, scored, conceded, result)

        def _build_record(stats: Dict[str, int]) -> HomeAwayRecord:
            games = stats["games"]
            pf = stats["pf"]
            pa = stats["pa"]
            diff = pf - pa
            if games > 0:
                avg_pf = pf / games
                avg_pa = pa / games
                avg_margin = diff / games
            else:
                avg_pf = avg_pa = avg_margin = None

            return HomeAwayRecord(
                games=games,
                wins=stats["wins"],
                draws=stats["draws"],
                losses=stats["losses"],
                points_for=pf,
                points_against=pa,
                points_diff=diff,
                avg_points_for=avg_pf,
                avg_points_against=avg_pa,
                avg_margin=avg_margin,
            )

        home_record = _build_record(home_stats)
        away_record = _build_record(away_stats)
        combined_record = _build_record(combined_stats)

        return HomeAwayResponse(
            league_name=league_name,
            tsdb_league_id=str(tsdb_league_id),
            season_label=resolved_season,
            team_id=team_id,
            team_name=team_name,
            home=home_record,
            away=away_record,
            combined=combined_record,
        )
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# /h2h  (supports limit + averages)
# ---------------------------------------------------------------------------

@app.get(
    "/h2h/{tsdb_league_id}/{team_a_id}/{team_b_id}",
    response_model=H2HResponse,
)
def get_h2h(
    tsdb_league_id: str,
    team_a_id: int,
    team_b_id: int,
    season_label: Optional[str] = Query(default=None),
    limit: Optional[int] = Query(default=None, ge=1, le=200),
):
    """
    Head-to-head between two teams in a league.

    - If season_label is provided -> restricted to that season.
    - If limit is provided -> only the most recent `limit` matches are used
      (ordered by season/year and kickoff date).
    - Summary + averages are computed over the filtered set.
    """
    conn = _get_conn()
    cur = conn.cursor(cursor_factory=DictCursor)
    try:
        base_sql = """
            SELECT
                m.match_id,
                m.kickoff_utc,
                m.home_team_id,
                m.away_team_id,
                m.home_score,
                m.away_score,
                th.name AS home_team_name,
                ta.name AS away_team_name,
                l.name  AS league_name,
                l.tsdb_league_id,
                s.label AS season_label,
                s.year  AS season_year
            FROM matches m
            JOIN leagues l
              ON l.league_id = m.league_id
            JOIN seasons s
              ON s.season_id = m.season_id
            JOIN teams th
              ON th.team_id = m.home_team_id
            JOIN teams ta
              ON ta.team_id = m.away_team_id
            WHERE l.tsdb_league_id = %s
              AND (
                   (m.home_team_id = %s AND m.away_team_id = %s)
                OR (m.home_team_id = %s AND m.away_team_id = %s)
              )
        """
        params: List[Any] = [tsdb_league_id, team_a_id, team_b_id, team_b_id, team_a_id]

        if season_label:
            base_sql += " AND s.label = %s"
            params.append(season_label)

        # Order oldest -> newest so we can slice last N for "recent"
        base_sql += " ORDER BY s.year ASC NULLS LAST, m.kickoff_utc NULLS LAST, m.match_id ASC"

        cur.execute(base_sql, tuple(params))
        rows = cur.fetchall()
        matches = [dict(r) for r in rows]

        if not matches:
            raise HTTPException(
                status_code=404,
                detail=f"No H2H matches found between team_id={team_a_id} and team_id={team_b_id} "
                       f"in league={tsdb_league_id}",
            )

        # If limit is set, keep only the most recent N matches
        if limit is not None and len(matches) > limit:
            matches = matches[-limit:]

        # aggregate
        ga = {"wins": 0, "draws": 0, "losses": 0, "pf": 0, "pa": 0}
        gb = {"wins": 0, "draws": 0, "losses": 0, "pf": 0, "pa": 0}
        games = 0               # all matches (even without scores)
        completed_games = 0     # matches where scores are present
        league_name = matches[0]["league_name"]
        seasons = set()

        for r in matches:
            games += 1
            seasons.add(r["season_label"])
            hs = r["home_score"]
            as_ = r["away_score"]

            if hs is not None and as_ is not None:
                completed_games += 1
                hs_i = int(hs)
                as_i = int(as_)
                # points
                if r["home_team_id"] == team_a_id:
                    ga["pf"] += hs_i
                    ga["pa"] += as_i
                    gb["pf"] += as_i
                    gb["pa"] += hs_i
                elif r["away_team_id"] == team_a_id:
                    ga["pf"] += as_i
                    ga["pa"] += hs_i
                    gb["pf"] += hs_i
                    gb["pa"] += as_i
                elif r["home_team_id"] == team_b_id:
                    gb["pf"] += hs_i
                    gb["pa"] += as_i
                    ga["pf"] += as_i
                    ga["pa"] += hs_i
                elif r["away_team_id"] == team_b_id:
                    gb["pf"] += as_i
                    gb["pa"] += hs_i
                    ga["pf"] += hs_i
                    ga["pa"] += as_i

                res_a = _compute_result_for_team(r, team_a_id)
                if res_a == "W":
                    ga["wins"] += 1
                    gb["losses"] += 1
                elif res_a == "L":
                    ga["losses"] += 1
                    gb["wins"] += 1
                elif res_a == "D":
                    ga["draws"] += 1
                    gb["draws"] += 1

        # names
        team_a_name = None
        team_b_name = None
        for r in matches:
            if r["home_team_id"] == team_a_id:
                team_a_name = r["home_team_name"]
            if r["away_team_id"] == team_a_id:
                team_a_name = r["away_team_name"]
            if r["home_team_id"] == team_b_id:
                team_b_name = r["home_team_name"]
            if r["away_team_id"] == team_b_id:
                team_b_name = r["away_team_name"]
        if team_a_name is None:
            team_a_name = f"team_id={team_a_id}"
        if team_b_name is None:
            team_b_name = f"team_id={team_b_id}"

        # match list (based on filtered matches)
        match_list: List[H2HMatchRow] = []
        for r in matches:
            dt = r["kickoff_utc"]
            dt_str = dt.isoformat() if dt is not None else None
            res_a = _compute_result_for_team(r, team_a_id)
            match_list.append(
                H2HMatchRow(
                    match_id=r["match_id"],
                    kickoff_utc=dt_str,
                    home_team_name=r["home_team_name"],
                    away_team_name=r["away_team_name"],
                    home_score=r["home_score"],
                    away_score=r["away_score"],
                    result_for_team_a=res_a,
                )
            )

        # averages over completed games only
        if completed_games > 0:
            team_a_avg_pf = ga["pf"] / completed_games
            team_a_avg_pa = ga["pa"] / completed_games
            team_a_avg_margin = (ga["pf"] - ga["pa"]) / completed_games

            team_b_avg_pf = gb["pf"] / completed_games
            team_b_avg_pa = gb["pa"] / completed_games
            team_b_avg_margin = (gb["pf"] - gb["pa"]) / completed_games
        else:
            team_a_avg_pf = team_a_avg_pa = team_a_avg_margin = None
            team_b_avg_pf = team_b_avg_pa = team_b_avg_margin = None

        summary = H2HSummary(
            games=games,
            completed_games=completed_games,
            team_a_wins=ga["wins"],
            team_a_draws=ga["draws"],
            team_a_losses=ga["losses"],
            team_a_pf=ga["pf"],
            team_a_pa=ga["pa"],
            team_a_avg_pf=team_a_avg_pf,
            team_a_avg_pa=team_a_avg_pa,
            team_a_avg_margin=team_a_avg_margin,
            team_b_wins=gb["wins"],
            team_b_draws=gb["draws"],
            team_b_losses=gb["losses"],
            team_b_pf=gb["pf"],
            team_b_pa=gb["pa"],
            team_b_avg_pf=team_b_avg_pf,
            team_b_avg_pa=team_b_avg_pa,
            team_b_avg_margin=team_b_avg_margin,
        )

        return H2HResponse(
            league_name=league_name,
            tsdb_league_id=str(tsdb_league_id),
            season_labels=sorted(seasons),
            team_a_id=team_a_id,
            team_a_name=team_a_name,
            team_b_id=team_b_id,
            team_b_name=team_b_name,
            summary=summary,
            matches=match_list,
        )
    finally:
        cur.close()
        conn.close()
