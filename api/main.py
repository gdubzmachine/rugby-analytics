#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
api/main.py
-----------

FastAPI app for rugby analytics:

- /health
- /leagues
- /teams
- /standings/{tsdb_league_id}
- /headtohead/{tsdb_league_id}
- /         (simple built-in UI for head-to-head)

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
from typing import List, Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query
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
# Built-in UI HTML at "/"
# ---------------------------------------------------------------------------

INDEX_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Rugby Analytics – Head to Head</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <style>
    * { box-sizing: border-box; }
    body {
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      margin: 0;
      padding: 0;
      background: #0b1020;
      color: #f5f5f5;
    }
    header {
      padding: 16px 24px;
      background: #111827;
      border-bottom: 1px solid #1f2933;
    }
    header h1 {
      margin: 0;
      font-size: 1.4rem;
    }
    header p {
      margin: 4px 0 0;
      font-size: 0.9rem;
      color: #9ca3af;
    }
    main {
      max-width: 1100px;
      margin: 24px auto 40px;
      padding: 0 16px;
    }
    .card {
      background: #111827;
      border-radius: 12px;
      padding: 16px 20px;
      margin-bottom: 20px;
      border: 1px solid #1f2937;
      box-shadow: 0 18px 45px rgba(0, 0, 0, 0.35);
    }
    .card h2 {
      margin-top: 0;
      font-size: 1.1rem;
      margin-bottom: 10px;
    }
    label {
      display: block;
      font-size: 0.85rem;
      margin-bottom: 4px;
      color: #d1d5db;
    }
    input, select, button {
      font-family: inherit;
      font-size: 0.95rem;
    }
    input, select {
      width: 100%;
      padding: 8px 10px;
      border-radius: 8px;
      border: 1px solid #374151;
      background: #020617;
      color: #f9fafb;
      outline: none;
    }
    input:focus, select:focus {
      border-color: #3b82f6;
    }
    .form-row {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 12px;
      margin-bottom: 12px;
    }
    button {
      margin-top: 8px;
      padding: 10px 16px;
      border-radius: 999px;
      border: none;
      background: #3b82f6;
      color: #fff;
      cursor: pointer;
      font-weight: 600;
    }
    button:hover {
      background: #2563eb;
    }
    button:disabled {
      background: #374151;
      cursor: wait;
    }
    .error {
      margin-top: 8px;
      font-size: 0.9rem;
      color: #fecaca;
    }
    .summary-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 16px;
      margin-top: 8px;
    }
    .summary-box {
      background: #020617;
      border-radius: 10px;
      padding: 10px 12px;
      border: 1px solid #1f2937;
    }
    .summary-box h3 {
      margin: 0 0 4px;
      font-size: 0.9rem;
      color: #9ca3af;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }
    .summary-box .value {
      font-size: 1.1rem;
      font-weight: 600;
    }
    .summary-box .sub {
      font-size: 0.85rem;
      color: #9ca3af;
      margin-top: 2px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 8px;
      font-size: 0.9rem;
    }
    th, td {
      padding: 6px 8px;
      border-bottom: 1px solid #1f2933;
      text-align: left;
    }
    th {
      font-weight: 500;
      color: #9ca3af;
      background: #020617;
      position: sticky;
      top: 0;
      z-index: 1;
    }
    tr:nth-child(even) td {
      background: #020617;
    }
    .pill {
      display: inline-block;
      padding: 2px 8px;
      border-radius: 999px;
      font-size: 0.8rem;
    }
    .pill-win {
      background: rgba(34, 197, 94, 0.15);
      color: #4ade80;
    }
    .pill-loss {
      background: rgba(248, 113, 113, 0.15);
      color: #fca5a5;
    }
    .pill-draw {
      background: rgba(251, 191, 36, 0.15);
      color: #facc15;
    }
    .small {
      font-size: 0.8rem;
      color: #9ca3af;
      margin-top: 4px;
    }
  </style>
</head>
<body>
  <header>
    <h1>Rugby Analytics – Head-to-Head</h1>
    <p>Compare two teams: head-to-head, upcoming fixtures, and overall form.</p>
  </header>
  <main>
    <section class="card">
      <h2>Compare Teams</h2>
      <form id="h2h-form">
        <div class="form-row">
          <div>
            <label for="league-select">League</label>
            <select id="league-select">
              <option value="">Loading leagues…</option>
            </select>
            <div class="small">
              Leagues are loaded from your rugby_analytics DB.
              Choose “All leagues” to combine all competitions.
            </div>
          </div>
          <div>
            <label for="team-a">Team A</label>
            <input id="team-a" type="text" placeholder="e.g. Bulls" list="team-a-options" />
            <datalist id="team-a-options"></datalist>
          </div>
          <div>
            <label for="team-b">Team B</label>
            <input id="team-b" type="text" placeholder="e.g. Leinster" list="team-b-options" />
            <datalist id="team-b-options"></datalist>
          </div>
          <div>
            <label for="limit">Last N matches (H2H & overall)</label>
            <select id="limit">
              <option value="5" selected>5</option>
              <option value="10">10</option>
              <option value="20">20</option>
              <option value="50">50</option>
            </select>
          </div>
        </div>
        <button type="submit" id="submit-btn">Compare</button>
        <div id="error" class="error" style="display:none;"></div>
      </form>
    </section>

    <section class="card" id="results-card" style="display:none;">
      <h2 id="results-title">Head-to-Head Results</h2>
      <div class="summary-grid">
        <div class="summary-box">
          <h3>Teams</h3>
          <div class="value" id="teams-label"></div>
          <div class="sub" id="league-label"></div>
        </div>
        <div class="summary-box">
          <h3>H2H Record</h3>
          <div class="value" id="overall-record"></div>
          <div class="sub" id="overall-extra"></div>
        </div>
        <div class="summary-box">
          <h3>H2H Win Rates</h3>
          <div class="value" id="win-rates"></div>
          <div class="sub" id="win-rates-extra"></div>
        </div>
        <div class="summary-box">
          <h3>H2H Streak</h3>
          <div class="value" id="streak"></div>
          <div class="sub" id="streak-extra"></div>
        </div>
      </div>

      <div class="summary-grid" style="margin-top:16px;">
        <div class="summary-box">
          <h3 id="overall-a-title">Overall – Team A</h3>
          <div class="value" id="overall-a-record"></div>
          <div class="sub" id="overall-a-extra"></div>
        </div>
        <div class="summary-box">
          <h3 id="overall-b-title">Overall – Team B</h3>
          <div class="value" id="overall-b-record"></div>
          <div class="sub" id="overall-b-extra"></div>
        </div>
      </div>

      <h3 style="margin-top:16px;">Last Head-to-Head Matches</h3>
      <div class="small" id="last-n-label"></div>
      <div style="max-height:320px; overflow-y:auto; margin-top:4px;">
        <table>
          <thead>
            <tr>
              <th>Date</th>
              <th>Season</th>
              <th>Match</th>
              <th>Score</th>
              <th>Result</th>
            </tr>
          </thead>
          <tbody id="matches-body"></tbody>
        </table>
      </div>

      <h3 style="margin-top:16px; display:none;" id="upcoming-title">Upcoming Fixtures</h3>
      <div class="small" id="upcoming-label" style="display:none;"></div>
      <div style="max-height:320px; overflow-y:auto; margin-top:4px; display:none;" id="upcoming-container">
        <table>
          <thead>
            <tr>
              <th>Date</th>
              <th>Season</th>
              <th>Match</th>
            </tr>
          </thead>
          <tbody id="upcoming-body"></tbody>
        </table>
      </div>
    </section>
  </main>

  <script>
    function formatDate(iso) {
      if (!iso) return "-";
      try {
        const d = new Date(iso);
        if (isNaN(d.getTime())) return "-";
        return d.toISOString().slice(0, 10);
      } catch {
        return "-";
      }
    }

    function percent(n) {
      return (n * 100).toFixed(1) + "%";
    }

    function winnerLabel(row, teamAName, teamBName) {
      if (row.home_score == null || row.away_score == null) return "No score";

      if (row.home_score > row.away_score) {
        if (row.home_team_name === teamAName) return teamAName + " win";
        if (row.home_team_name === teamBName) return teamBName + " win";
        return row.home_team_name + " win";
      } else if (row.away_score > row.home_score) {
        if (row.away_team_name === teamAName) return teamAName + " win";
        if (row.away_team_name === teamBName) return teamBName + " win";
        return row.away_team_name + " win";
      } else {
        return "Draw";
      }
    }

    function resultPillClass(row, teamAName, teamBName) {
      if (row.home_score == null || row.away_score == null) return "";
      if (row.home_score === row.away_score) return "pill pill-draw";

      const label = winnerLabel(row, teamAName, teamBName);
      if (label.startsWith(teamAName)) return "pill pill-win";
      if (label.startsWith(teamBName)) return "pill pill-loss";
      return "pill";
    }

    async function fetchJSON(url) {
      const res = await fetch(url);
      if (!res.ok) {
        let msg = "Request failed: " + res.status;
        try {
          const data = await res.json();
          if (data.detail) msg = data.detail;
        } catch {}
        throw new Error(msg);
      }
      return res.json();
    }

    function populateTeamsOptions(teams) {
      const listA = document.getElementById("team-a-options");
      const listB = document.getElementById("team-b-options");
      listA.innerHTML = "";
      listB.innerHTML = "";

      teams.forEach((t) => {
        const optA = document.createElement("option");
        optA.value = t.name;
        listA.appendChild(optA);

        const optB = document.createElement("option");
        optB.value = t.name;
        listB.appendChild(optB);
      });
    }

    async function loadTeamsForLeague(leagueId) {
      const listA = document.getElementById("team-a-options");
      const listB = document.getElementById("team-b-options");
      listA.innerHTML = "";
      listB.innerHTML = "";
      document.getElementById("team-a").value = "";
      document.getElementById("team-b").value = "";

      if (!leagueId && leagueId !== 0 && leagueId !== "0") {
        return;
      }

      try {
        const url =
          "/teams?limit=200" +
          "&tsdb_league_id=" +
          encodeURIComponent(leagueId);
        const teams = await fetchJSON(url);
        populateTeamsOptions(teams);
      } catch (err) {
        console.error(err);
      }
    }

    async function loadLeagues() {
      const select = document.getElementById("league-select");
      select.innerHTML = "";
      const placeholder = document.createElement("option");
      placeholder.value = "";
      placeholder.textContent = "Select league…";
      select.appendChild(placeholder);

      const allOption = document.createElement("option");
      allOption.value = "0";
      allOption.textContent = "All leagues";
      select.appendChild(allOption);

      try {
        const leagues = await fetchJSON("/leagues");

        leagues.forEach((l) => {
          const opt = document.createElement("option");
          opt.value = String(l.tsdb_league_id);
          opt.textContent = l.name + " (TSDB " + l.tsdb_league_id + ")";
          select.appendChild(opt);
        });

        const defaultLeagueId = "4446";
        const hasDefault =
          Array.from(select.options).some((o) => o.value === defaultLeagueId);
        if (hasDefault) {
          select.value = defaultLeagueId;
          loadTeamsForLeague(defaultLeagueId);
        }
      } catch (err) {
        console.error(err);
        placeholder.textContent = "Failed to load leagues";
      }

      select.addEventListener("change", (e) => {
        const val = e.target.value;
        if (!val) {
          document.getElementById("team-a-options").innerHTML = "";
          document.getElementById("team-b-options").innerHTML = "";
          document.getElementById("team-a").value = "";
          document.getElementById("team-b").value = "";
          return;
        }
        loadTeamsForLeague(val);
      });
    }

    document.getElementById("h2h-form").addEventListener("submit", async (e) => {
      e.preventDefault();

      const leagueId = document.getElementById("league-select").value.trim();
      const teamA = document.getElementById("team-a").value.trim();
      const teamB = document.getElementById("team-b").value.trim();
      const limit = document.getElementById("limit").value;

      const errorEl = document.getElementById("error");
      const btn = document.getElementById("submit-btn");
      const resultsCard = document.getElementById("results-card");
      const matchesBody = document.getElementById("matches-body");
      const upcomingTitle = document.getElementById("upcoming-title");
      const upcomingLabel = document.getElementById("upcoming-label");
      const upcomingContainer = document.getElementById("upcoming-container");
      const upcomingBody = document.getElementById("upcoming-body");

      errorEl.style.display = "none";
      errorEl.textContent = "";
      resultsCard.style.display = "none";
      matchesBody.innerHTML = "";
      upcomingBody.innerHTML = "";
      upcomingTitle.style.display = "none";
      upcomingLabel.style.display = "none";
      upcomingContainer.style.display = "none";

      if (!leagueId) {
        errorEl.textContent = "Please select a league (or 'All leagues').";
        errorEl.style.display = "block";
        return;
      }
      if (!teamA || !teamB) {
        errorEl.textContent = "Please enter both Team A and Team B.";
        errorEl.style.display = "block";
        return;
      }

      btn.disabled = true;

      try {
        const params = new URLSearchParams({
          team_a: teamA,
          team_b: teamB,
          limit: String(limit),
        });

        const url =
          "/headtohead/" +
          encodeURIComponent(leagueId) +
          "?" +
          params.toString();
        const data = await fetchJSON(url);

        document.getElementById("results-title").textContent =
          "Head-to-head: " + data.team_a_name + " vs " + data.team_b_name;
        document.getElementById("teams-label").textContent =
          data.team_a_name + " vs " + data.team_b_name;
        document.getElementById("league-label").textContent =
          data.league_name +
          " (TSDB " +
          data.tsdb_league_id +
          ")";

        document.getElementById("overall-record").textContent =
          data.team_a_wins + " – " + data.team_b_wins + " (W–L)";
        document.getElementById("overall-extra").textContent =
          data.draws +
          " draw(s) across " +
          data.total_matches +
          " played head-to-head matches";

        document.getElementById("win-rates").textContent =
          percent(data.team_a_win_rate) +
          " vs " +
          percent(data.team_b_win_rate);
        document.getElementById("win-rates-extra").textContent =
          data.team_a_name + " vs " + data.team_b_name + " (H2H only)";

        let streakText = "No streak data";
        if (data.current_streak_type === "team_a_win") {
          streakText =
            data.team_a_name +
            " – " +
            data.current_streak_length +
            " H2H win(s) in a row";
        } else if (data.current_streak_type === "team_b_win") {
          streakText =
            data.team_b_name +
            " – " +
            data.current_streak_length +
            " H2H win(s) in a row";
        } else if (data.current_streak_type === "draw") {
          streakText =
            data.current_streak_length + " H2H draw(s) in a row";
        }
        document.getElementById("streak").textContent = streakText;
        document.getElementById("streak-extra").textContent =
          "Based on most recent played head-to-head matches";

        // Overall summaries
        const oa = data.team_a_overall;
        const ob = data.team_b_overall;

        document.getElementById("overall-a-title").textContent =
          "Overall – " + oa.team_name;
        document.getElementById("overall-b-title").textContent =
          "Overall – " + ob.team_name;

        document.getElementById("overall-a-record").textContent =
          oa.wins + "-" + oa.draws + "-" + oa.losses +
          " (W-D-L)";
        document.getElementById("overall-b-record").textContent =
          ob.wins + "-" + ob.draws + "-" + ob.losses +
          " (W-D-L)";

        document.getElementById("overall-a-extra").textContent =
          "Last " + oa.total_played + " played: " +
          percent(oa.win_rate) +
          " win rate, win streak " +
          oa.current_win_streak;
        document.getElementById("overall-b-extra").textContent =
          "Last " + ob.total_played + " played: " +
          percent(ob.win_rate) +
          " win rate, win streak " +
          ob.current_win_streak;

        // H2H last N table
        document.getElementById("last-n-label").textContent =
          "Showing up to " +
          data.last_n.length +
          " most recent played head-to-head matches between these teams.";

        for (const row of data.last_n) {
          const tr = document.createElement("tr");

          const tdDate = document.createElement("td");
          tdDate.textContent = formatDate(row.kickoff_utc);
          tr.appendChild(tdDate);

          const tdSeason = document.createElement("td");
          tdSeason.textContent = row.season_label || "-";
          tr.appendChild(tdSeason);

          const tdMatch = document.createElement("td");
          tdMatch.textContent =
            row.home_team_name + " vs " + row.away_team_name;
          tr.appendChild(tdMatch);

          const tdScore = document.createElement("td");
          if (row.home_score == null || row.away_score == null) {
            tdScore.textContent = "-";
          } else {
            tdScore.textContent =
              row.home_score + " – " + row.away_score;
          }
          tr.appendChild(tdScore);

          const tdResult = document.createElement("td");
          const pill = document.createElement("span");
          pill.className = resultPillClass(
            row,
            data.team_a_name,
            data.team_b_name
          );
          pill.textContent = winnerLabel(
            row,
            data.team_a_name,
            data.team_b_name
          );
          tdResult.appendChild(pill);
          tr.appendChild(tdResult);

          matchesBody.appendChild(tr);
        }

        // Upcoming fixtures
        if (data.upcoming && data.upcoming.length > 0) {
          upcomingTitle.style.display = "block";
          upcomingLabel.style.display = "block";
          upcomingContainer.style.display = "block";
          upcomingLabel.textContent =
            "Upcoming fixtures between these teams (not included in H2H or overall streak/win rates).";

          for (const row of data.upcoming) {
            const tr = document.createElement("tr");

            const tdDate = document.createElement("td");
            tdDate.textContent = formatDate(row.kickoff_utc);
            tr.appendChild(tdDate);

            const tdSeason = document.createElement("td");
            tdSeason.textContent = row.season_label || "-";
            tr.appendChild(tdSeason);

            const tdMatch = document.createElement("td");
            tdMatch.textContent =
              row.home_team_name + " vs " + row.away_team_name;
            tr.appendChild(tdMatch);

            upcomingBody.appendChild(tr);
          }
        }

        resultsCard.style.display = "block";
      } catch (err) {
        console.error(err);
        errorEl.textContent = err.message || "Something went wrong.";
        errorEl.style.display = "block";
      } finally {
        btn.disabled = false;
      }
    });

    loadLeagues();
  </script>
</body>
</html>
"""


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


class FixtureSummary(BaseModel):
    match_id: int
    season_label: str
    kickoff_utc: Optional[datetime]
    home_team_id: int
    home_team_name: str
    away_team_id: int
    away_team_name: str


class TeamOverallSummary(BaseModel):
    team_id: int
    team_name: str
    total_played: int
    wins: int
    draws: int
    losses: int
    win_rate: float
    current_win_streak: int
    last_n_overall: List[MatchSummary]


class HeadToHeadResponse(BaseModel):
    tsdb_league_id: int
    league_name: str
    team_a_id: int
    team_a_name: str
    team_b_id: int
    team_b_name: str
    total_matches: int  # number of PLAYED H2H matches
    team_a_wins: int
    team_b_wins: int
    draws: int
    team_a_win_rate: float
    team_b_win_rate: float
    current_streak_type: Optional[str]
    current_streak_length: int
    last_n: List[MatchSummary]          # last N played H2H matches
    upcoming: List[FixtureSummary]      # future fixtures (not in streak/win rate)
    team_a_overall: TeamOverallSummary  # overall form across all leagues
    team_b_overall: TeamOverallSummary  # overall form across all leagues


class LeagueInfo(BaseModel):
    tsdb_league_id: int
    name: str


class TeamInfo(BaseModel):
    team_id: int
    name: str


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Rugby Analytics API",
    version="0.6.0",
    description="API exposing rugby standings and head-to-head stats, plus a simple UI.",
)


@app.get("/", response_class=HTMLResponse)
def index():
    return HTMLResponse(INDEX_HTML)


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
    return rows[0]


def _resolve_team_global(cur, name_query: str) -> dict:
    """
    Find a team by fuzzy name across ALL leagues.
    """
    cur.execute(
        """
        SELECT t.team_id, t.name
        FROM teams t
        WHERE t.name ILIKE %s
        ORDER BY t.name
        """,
        (f"%{name_query}%",),
    )
    rows = cur.fetchall()
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No team found matching '{name_query}' in any league",
        )
    return rows[0]


def _compute_overall_for_team(cur, team_id: int, team_name: str, limit: int) -> TeamOverallSummary:
    """
    Compute overall form for a team across ALL leagues:

    - last `limit` played matches (scores not null)
    - wins/draws/losses over that sample
    - win_rate = wins / total_played
    - current_win_streak = consecutive wins from most recent match backwards
    """
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
        WHERE (m.home_team_id = %s OR m.away_team_id = %s)
          AND m.home_score IS NOT NULL
          AND m.away_score IS NOT NULL
        ORDER BY m.kickoff_utc DESC
        LIMIT %s
        """,
        (team_id, team_id, limit),
    )
    rows = cur.fetchall()

    last_matches: List[MatchSummary] = []
    wins = draws = losses = 0
    results: List[str] = []  # "win"/"draw"/"loss" from perspective of this team

    for r in rows:
        hs = r["home_score"]
        as_ = r["away_score"]
        home_is_team = (r["home_team_id"] == team_id)

        if hs > as_:
            if home_is_team:
                result = "win"
            else:
                result = "loss"
        elif as_ > hs:
            if not home_is_team:
                result = "win"
            else:
                result = "loss"
        else:
            result = "draw"

        if result == "win":
            wins += 1
        elif result == "draw":
            draws += 1
        else:
            losses += 1

        results.append(result)

        kickoff = r["kickoff_utc"]
        if isinstance(kickoff, str):
            try:
                kickoff = datetime.fromisoformat(kickoff)
            except Exception:
                kickoff = None

        # winner field kept as "home"/"away"/"draw" for consistency
        if hs > as_:
            winner = "home"
        elif as_ > hs:
            winner = "away"
        else:
            winner = "draw"

        last_matches.append(
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

    total_played = len(rows)
    win_rate = (wins / total_played) if total_played > 0 else 0.0

    streak = 0
    for res in results:
        if res == "win":
            streak += 1
        else:
            break

    return TeamOverallSummary(
        team_id=team_id,
        team_name=team_name,
        total_played=total_played,
        wins=wins,
        draws=draws,
        losses=losses,
        win_rate=win_rate,
        current_win_streak=streak,
        last_n_overall=last_matches,
    )


# ---------------------------------------------------------------------------
# Leagues & teams endpoints for the UI
# ---------------------------------------------------------------------------

@app.get("/leagues", response_model=List[LeagueInfo])
def list_leagues():
    """
    List all leagues in the DB (tsdb_league_id + name).
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT tsdb_league_id, name
            FROM leagues
            ORDER BY name
            """
        )
        rows = cur.fetchall()
        return [
            LeagueInfo(tsdb_league_id=r["tsdb_league_id"], name=r["name"])
            for r in rows
        ]
    finally:
        cur.close()
        conn.close()


@app.get("/teams", response_model=List[TeamInfo])
def list_teams(
    tsdb_league_id: Optional[int] = Query(
        None,
        description="Optional TSDB league id to filter teams. Use 0 for 'all leagues'.",
    ),
    q: Optional[str] = Query(
        None,
        description="Optional name filter (ILIKE).",
    ),
    limit: int = Query(
        50,
        ge=1,
        le=500,
        description="Max number of teams to return.",
    ),
):
    """
    List teams, optionally filtered by a TSDB league and/or name query.
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        params = []
        if tsdb_league_id is not None and tsdb_league_id != 0:
            league = _resolve_league(cur, tsdb_league_id)
            league_id = league["league_id"]

            sql = """
                SELECT DISTINCT t.team_id, t.name
                FROM teams t
                JOIN team_season_stats tss
                  ON tss.team_id = t.team_id
                WHERE tss.league_id = %s
            """
            params.append(league_id)

            if q:
                sql += " AND t.name ILIKE %s"
                params.append(f"%{q}%")

            sql += " ORDER BY t.name LIMIT %s"
            params.append(limit)
        else:
            sql = """
                SELECT DISTINCT t.team_id, t.name
                FROM teams t
                WHERE 1=1
            """
            if q:
                sql += " AND t.name ILIKE %s"
                params.append(f"%{q}%")
            sql += " ORDER BY t.name LIMIT %s"
            params.append(limit)

        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        return [TeamInfo(team_id=r["team_id"], name=r["name"]) for r in rows]

    finally:
        cur.close()
        conn.close()


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
        league = _resolve_league(cur, tsdb_league_id)
        league_id = league["league_id"]
        league_name = league["name"]

        season_label_resolved = _resolve_season_label(
            cur,
            league_id=league_id,
            season_label=season_label if season_label else None,
        )

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
    limit: int = Query(
        10,
        ge=1,
        le=100,
        description="Number of recent matches to consider (H2H and overall).",
    ),
):
    """
    Head-to-head stats between two teams:

    - If tsdb_league_id != 0: restrict H2H to that league.
    - If tsdb_league_id == 0: use ALL leagues in the DB.

    H2H stats (wins, win rates, streak) are based ONLY on played matches
    where home_score AND away_score are NOT NULL.

    Upcoming fixtures (missing scores, kickoff in the future) are returned
    in a separate 'upcoming' list and DO NOT affect percentages or streaks.

    Overall form for each team (team_a_overall / team_b_overall) is based on
    their last `limit` played matches across ALL leagues.
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        now_utc = datetime.utcnow()

        if tsdb_league_id == 0:
            league_name = "All leagues"
            league_id = None

            team_a_row = _resolve_team_global(cur, team_a)
            team_b_row = _resolve_team_global(cur, team_b)

            team_a_id = team_a_row["team_id"]
            team_a_name = team_a_row["name"]
            team_b_id = team_b_row["team_id"]
            team_b_name = team_b_row["name"]

            base_where = """
                (
                    (m.home_team_id = %s AND m.away_team_id = %s) OR
                    (m.home_team_id = %s AND m.away_team_id = %s)
                )
            """
            base_params = (team_a_id, team_b_id, team_b_id, team_a_id)

            sql_played = f"""
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
                WHERE {base_where}
                  AND m.home_score IS NOT NULL
                  AND m.away_score IS NOT NULL
                ORDER BY m.kickoff_utc DESC
                LIMIT %s
            """
            params_played = base_params + (limit,)

            sql_upcoming = f"""
                SELECT
                    m.match_id,
                    s.label AS season_label,
                    m.kickoff_utc,
                    ht.team_id AS home_team_id,
                    ht.name    AS home_team_name,
                    at.team_id AS away_team_id,
                    at.name    AS away_team_name
                FROM matches m
                JOIN seasons s ON s.season_id = m.season_id
                JOIN teams ht ON ht.team_id = m.home_team_id
                JOIN teams at ON at.team_id = m.away_team_id
                WHERE {base_where}
                  AND (m.home_score IS NULL OR m.away_score IS NULL)
                  AND m.kickoff_utc IS NOT NULL
                  AND m.kickoff_utc >= %s
                ORDER BY m.kickoff_utc ASC
            """
            params_upcoming = base_params + (now_utc,)
        else:
            league = _resolve_league(cur, tsdb_league_id)
            league_id = league["league_id"]
            league_name = league["name"]

            team_a_row = _resolve_team_in_league(cur, league_id, team_a)
            team_b_row = _resolve_team_in_league(cur, league_id, team_b)

            team_a_id = team_a_row["team_id"]
            team_a_name = team_a_row["name"]
            team_b_id = team_b_row["team_id"]
            team_b_name = team_b_row["name"]

            base_where = """
                m.league_id = %s
                AND (
                    (m.home_team_id = %s AND m.away_team_id = %s) OR
                    (m.home_team_id = %s AND m.away_team_id = %s)
                )
            """
            base_params = (league_id, team_a_id, team_b_id, team_b_id, team_a_id)

            sql_played = f"""
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
                WHERE {base_where}
                  AND m.home_score IS NOT NULL
                  AND m.away_score IS NOT NULL
                ORDER BY m.kickoff_utc DESC
                LIMIT %s
            """
            params_played = base_params + (limit,)

            sql_upcoming = f"""
                SELECT
                    m.match_id,
                    s.label AS season_label,
                    m.kickoff_utc,
                    ht.team_id AS home_team_id,
                    ht.name    AS home_team_name,
                    at.team_id AS away_team_id,
                    at.name    AS away_team_name
                FROM matches m
                JOIN seasons s ON s.season_id = m.season_id
                JOIN teams ht ON ht.team_id = m.home_team_id
                JOIN teams at ON at.team_id = m.away_team_id
                WHERE {base_where}
                  AND (m.home_score IS NULL OR m.away_score IS NULL)
                  AND m.kickoff_utc IS NOT NULL
                  AND m.kickoff_utc >= %s
                ORDER BY m.kickoff_utc ASC
            """
            params_upcoming = base_params + (now_utc,)

        # Played H2H matches
        cur.execute(sql_played, params_played)
        played_rows = cur.fetchall()

        # Upcoming fixtures
        cur.execute(sql_upcoming, params_upcoming)
        upcoming_rows = cur.fetchall()

        # It is okay if there are only upcoming fixtures (no played yet),
        # but then H2H stats will all be zero.
        if not played_rows and not upcoming_rows:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"No matches (played or upcoming) found between '{team_a_name}' and "
                    f"'{team_b_name}' in tsdb_league_id={tsdb_league_id}"
                ),
            )

        total_matches = 0
        team_a_wins = 0
        team_b_wins = 0
        draws = 0
        match_summaries: List[MatchSummary] = []

        streak_type: Optional[str] = None  # "team_a_win", "team_b_win", "draw"
        streak_length = 0

        for idx, r in enumerate(played_rows):
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

            if idx == 0:
                streak_type = result_flag
                streak_length = 1 if result_flag is not None else 0
            else:
                if result_flag is not None and result_flag == streak_type:
                    streak_length += 1
                else:
                    break

        if total_matches > 0:
            team_a_win_rate = team_a_wins / total_matches
            team_b_win_rate = team_b_wins / total_matches
        else:
            team_a_win_rate = 0.0
            team_b_win_rate = 0.0

        # Upcoming fixtures (H2H)
        upcoming_list: List[FixtureSummary] = []
        for r in upcoming_rows:
            kickoff = r["kickoff_utc"]
            if isinstance(kickoff, str):
                try:
                    kickoff = datetime.fromisoformat(kickoff)
                except Exception:
                    kickoff = None

            upcoming_list.append(
                FixtureSummary(
                    match_id=r["match_id"],
                    season_label=r["season_label"],
                    kickoff_utc=kickoff,
                    home_team_id=r["home_team_id"],
                    home_team_name=r["home_team_name"],
                    away_team_id=r["away_team_id"],
                    away_team_name=r["away_team_name"],
                )
            )

        # Overall form for each team (across ALL leagues)
        team_a_overall = _compute_overall_for_team(cur, team_a_id, team_a_name, limit)
        team_b_overall = _compute_overall_for_team(cur, team_b_id, team_b_name, limit)

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
            upcoming=upcoming_list,
            team_a_overall=team_a_overall,
            team_b_overall=team_b_overall,
        )

    finally:
        cur.close()
        conn.close()
