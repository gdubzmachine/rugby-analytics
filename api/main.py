import os
from datetime import date
from typing import List, Optional, Dict, Any

import psycopg2
from psycopg2.extras import RealDictCursor

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    # We don't crash import, but endpoints will raise a 500 with a clear message
    print("WARNING: DATABASE_URL is not set. API endpoints that hit the DB will fail.")

app = FastAPI(title="Rugby Analytics API", version="0.2.0")

# Allow frontends (localhost dev + Render site) – adjust as needed
origins = [
    "http://localhost",
    "http://localhost:5173",
    "http://localhost:3000",
    "*",  # you can tighten this later
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_conn():
    if not DATABASE_URL:
        raise HTTPException(status_code=500, detail="DATABASE_URL not set")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB connection failed: {e}")


# Groups of teams that should be treated as the *same* franchise
# Keyed by lower-case name for easy matching.
TEAM_ALIAS_MAP: Dict[str, List[str]] = {
    # Stormers franchise
    "stormers": ["Stormers", "Western Province"],
    "western province": ["Stormers", "Western Province"],
    # Bulls franchise
    "bulls": ["Bulls", "Blue Bulls"],
    "blue bulls": ["Bulls", "Blue Bulls"],
    # Add more mappings here as needed
}


def _get_team_group_ids_and_label(cur, team_id: int) -> Dict[str, Any]:
    """
    Given a concrete team_id, return:
      - all team_ids that belong to the same franchise group (alias group)
      - a nice display label like 'Stormers (Western Province)'.

    If there is no alias mapping, this just returns the single team_id and name.
    """
    cur.execute("SELECT team_id, name FROM teams WHERE team_id = %s", (team_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Team with id={team_id} not found")

    base_id = row["team_id"]
    base_name = row["name"]
    base_key = base_name.lower()

    alias_names = TEAM_ALIAS_MAP.get(base_key, [base_name])

    # Fetch all matching teams by name (case-insensitive)
    lower_aliases = [n.lower() for n in alias_names]
    cur.execute(
        """
        SELECT team_id, name
        FROM teams
        WHERE lower(name) = ANY(%s)
        ORDER BY name
        """,
        (lower_aliases,),
    )
    rows = cur.fetchall()

    if not rows:
        # Shouldn't happen, but fallback to just the base team
        return {
            "team_ids": [base_id],
            "display_name": base_name,
            "canonical_name": base_name,
        }

    team_ids = [r["team_id"] for r in rows]
    names = [r["name"] for r in rows]

    # Canonical name = base name
    canonical = base_name
    # Display label: "Stormers (Western Province)" if >1 distinct name
    unique_names = sorted(set(names))
    if len(unique_names) == 1:
        display = unique_names[0]
    else:
        primary = base_name
        others = [n for n in unique_names if n != primary]
        if not others:
            # if base_name not in unique_names for some reason, just take first as primary
            primary = unique_names[0]
            others = unique_names[1:]
        display = f"{primary} ({', '.join(others)})"

    return {
        "team_ids": team_ids,
        "display_name": display,
        "canonical_name": canonical,
    }


def _load_latest_season_label(cur, tsdb_league_id: int) -> str:
    cur.execute(
        """
        SELECT s.label
        FROM team_season_stats tss
        JOIN seasons s ON s.season_id = tss.season_id
        JOIN leagues l ON l.league_id = tss.league_id
        WHERE l.tsdb_league_id = %s
        ORDER BY s.year DESC
        LIMIT 1
        """,
        (tsdb_league_id,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="No seasons found for this league")
    return row["label"]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/leagues")
def list_leagues():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT league_id, tsdb_league_id, name, short_name, slug, country_code, "group"
                FROM leagues
                ORDER BY name
                """
            )
            rows = cur.fetchall()
            return {"leagues": rows}
    finally:
        conn.close()


@app.get("/teams")
def list_teams(
    tsdb_league_id: int = Query(0, description="Filter by TSDB league id; 0 = all leagues"),
    q: Optional[str] = Query(None, description="Optional name search"),
    limit: int = Query(50, ge=1, le=200),
):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            params: List[Any] = []
            where = []

            if tsdb_league_id:
                # limit to teams that have at least one season in this league
                where.append("l.tsdb_league_id = %s")
                params.append(tsdb_league_id)

            if q:
                where.append("LOWER(t.name) LIKE %s")
                params.append(f"%{q.lower()}%")

            where_sql = " AND ".join(where)
            if where_sql:
                where_sql = "WHERE " + where_sql

            sql = f"""
                SELECT DISTINCT
                    t.team_id,
                    t.name,
                    t.short_name,
                    t.abbreviation
                FROM teams t
                JOIN matches m
                  ON m.home_team_id = t.team_id OR m.away_team_id = t.team_id
                JOIN leagues l
                  ON l.league_id = m.league_id
                {where_sql}
                ORDER BY t.name
                LIMIT %s
            """
            params.append(limit)
            cur.execute(sql, params)
            rows = cur.fetchall()
            return {"teams": rows}
    finally:
        conn.close()


@app.get("/standings/{tsdb_league_id}")
def get_standings(
    tsdb_league_id: int,
    season_label: Optional[str] = Query(None),
    latest: bool = Query(False, description="If true, ignore season_label and use latest season"),
):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if latest or not season_label:
                season_label = _load_latest_season_label(cur, tsdb_league_id)

            cur.execute(
                """
                SELECT
                    t.team_id,
                    t.name AS team_name,
                    s.label AS season_label,
                    s.year,
                    tss.games_played,
                    tss.wins,
                    tss.draws,
                    tss.losses,
                    tss.points_for,
                    tss.points_against,
                    tss.points_diff,
                    tss.competition_points
                FROM team_season_stats tss
                JOIN teams t ON t.team_id = tss.team_id
                JOIN seasons s ON s.season_id = tss.season_id
                JOIN leagues l ON l.league_id = tss.league_id
                WHERE l.tsdb_league_id = %s
                  AND s.label = %s
                ORDER BY tss.competition_points DESC, tss.points_diff DESC, t.team_id
                """,
                (tsdb_league_id, season_label),
            )
            rows = cur.fetchall()
            return {
                "tsdb_league_id": tsdb_league_id,
                "season_label": season_label,
                "standings": rows,
            }
    finally:
        conn.close()


@app.get("/compare-teams")
def compare_teams(
    team_a_id: int = Query(..., description="DB team_id for Team A"),
    team_b_id: int = Query(..., description="DB team_id for Team B"),
    last_n: int = Query(10, ge=1, le=100, description="How many recent matches to consider"),
    tsdb_league_id: int = Query(0, description="Optional TSDB league filter; 0 = all leagues"),
):
    """
    Head-to-head comparison between two *franchises*.

    If a team has aliases (e.g. Stormers / Western Province, Bulls / Blue Bulls),
    we treat them as one combined group for the query and the stats. The display
    name becomes e.g. 'Stormers (Western Province)'.
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Resolve franchise groups for both sides
            group_a = _get_team_group_ids_and_label(cur, team_a_id)
            group_b = _get_team_group_ids_and_label(cur, team_b_id)

            team_a_ids: List[int] = group_a["team_ids"]
            team_b_ids: List[int] = group_b["team_ids"]

            today = date.today()

            params: List[Any] = [
                team_a_ids,
                team_b_ids,
                team_b_ids,
                team_a_ids,
                today,
            ]

            league_filter = ""
            if tsdb_league_id:
                league_filter = "AND l.tsdb_league_id = %s"
                params.append(tsdb_league_id)

            sql_matches = f"""
                SELECT
                    m.match_id,
                    m.kickoff_utc::date AS match_date,
                    s.label AS season_label,
                    l.tsdb_league_id,
                    l.name AS league_name,
                    th.name AS home_team,
                    ta.name AS away_team,
                    m.home_team_id,
                    m.away_team_id,
                    m.home_score,
                    m.away_score
                FROM matches m
                JOIN leagues l ON l.league_id = m.league_id
                JOIN seasons s ON s.season_id = m.season_id
                JOIN teams th ON th.team_id = m.home_team_id
                JOIN teams ta ON ta.team_id = m.away_team_id
                WHERE
                    (
                        (m.home_team_id = ANY(%s) AND m.away_team_id = ANY(%s))
                        OR
                        (m.home_team_id = ANY(%s) AND m.away_team_id = ANY(%s))
                    )
                  AND m.kickoff_utc::date <= %s -- exclude future fixtures from stats
                  {league_filter}
                ORDER BY m.kickoff_utc DESC
                LIMIT %s
            """
            params.append(last_n)

            cur.execute(sql_matches, params)
            all_matches = cur.fetchall()

            # Only count matches that actually have a final score
            played = [m for m in all_matches if m["home_score"] is not None and m["away_score"] is not None]

            total_played = len(played)
            wins_a = 0
            wins_b = 0
            draws = 0

            # Compute W/L/D and also current streak from most recent backwards
            current_streak_side = None  # "A" or "B"
            current_streak_len = 0

            for m in played:
                hs = m["home_score"]
                as_ = m["away_score"]
                home_id = m["home_team_id"]
                away_id = m["away_team_id"]

                winner = None
                if hs > as_:
                    if home_id in team_a_ids:
                        winner = "A"
                    elif home_id in team_b_ids:
                        winner = "B"
                elif as_ > hs:
                    if away_id in team_a_ids:
                        winner = "A"
                    elif away_id in team_b_ids:
                        winner = "B"

                if winner == "A":
                    wins_a += 1
                elif winner == "B":
                    wins_b += 1
                else:
                    draws += 1

            # Streak: re-iterate from most recent played match
            for m in played:
                hs = m["home_score"]
                as_ = m["away_score"]
                home_id = m["home_team_id"]
                away_id = m["away_team_id"]

                winner = None
                if hs > as_:
                    if home_id in team_a_ids:
                        winner = "A"
                    elif home_id in team_b_ids:
                        winner = "B"
                elif as_ > hs:
                    if away_id in team_a_ids:
                        winner = "A"
                    elif away_id in team_b_ids:
                        winner = "B"

                if winner is None:
                    break

                if current_streak_side is None:
                    current_streak_side = winner
                    current_streak_len = 1
                elif winner == current_streak_side:
                    current_streak_len += 1
                else:
                    break

            win_rate_a = (wins_a / total_played) * 100 if total_played else 0.0
            win_rate_b = (wins_b / total_played) * 100 if total_played else 0.0

            # Also fetch upcoming fixtures between these franchises, for UI to show separately
            params_upcoming: List[Any] = [
                team_a_ids,
                team_b_ids,
                team_b_ids,
                team_a_ids,
                today,
            ]
            if tsdb_league_id:
                params_upcoming.append(tsdb_league_id)
                league_filter_up = "AND l.tsdb_league_id = %s"
            else:
                league_filter_up = ""

            sql_upcoming = f"""
                SELECT
                    m.match_id,
                    m.kickoff_utc::date AS match_date,
                    s.label AS season_label,
                    l.tsdb_league_id,
                    l.name AS league_name,
                    th.name AS home_team,
                    ta.name AS away_team,
                    m.home_score,
                    m.away_score
                FROM matches m
                JOIN leagues l ON l.league_id = m.league_id
                JOIN seasons s ON s.season_id = m.season_id
                JOIN teams th ON th.team_id = m.home_team_id
                JOIN teams ta ON ta.team_id = m.away_team_id
                WHERE
                    (
                        (m.home_team_id = ANY(%s) AND m.away_team_id = ANY(%s))
                        OR
                        (m.home_team_id = ANY(%s) AND m.away_team_id = ANY(%s))
                    )
                  AND m.kickoff_utc::date > %s
                  {league_filter_up}
                ORDER BY m.kickoff_utc ASC
            """
            cur.execute(sql_upcoming, params_upcoming)
            upcoming = cur.fetchall()

            # Drop internal ids from the match list we return (keep it clean)
            def _strip_ids(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
                out: List[Dict[str, Any]] = []
                for r in rows:
                    r2 = dict(r)
                    r2.pop("home_team_id", None)
                    r2.pop("away_team_id", None)
                    out.append(r2)
                return out

            return {
                "team_a": {
                    "team_ids": team_a_ids,
                    "name": group_a["display_name"],  # e.g. "Stormers (Western Province)"
                    "canonical_name": group_a["canonical_name"],
                },
                "team_b": {
                    "team_ids": team_b_ids,
                    "name": group_b["display_name"],  # e.g. "Bulls (Blue Bulls)"
                    "canonical_name": group_b["canonical_name"],
                },
                "total_played": total_played,
                "wins_a": wins_a,
                "wins_b": wins_b,
                "draws": draws,
                "win_rate_a": win_rate_a,
                "win_rate_b": win_rate_b,
                "current_streak_side": current_streak_side,
                "current_streak_len": current_streak_len,
                "matches": _strip_ids(played),
                "upcoming": upcoming,
            }
    finally:
        conn.close()
