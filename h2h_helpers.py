#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
h2h_helpers.py

Helpers for head-to-head logic:

- club alias groups
- name normalisation / fuzzy matching
- global and league-scoped team resolution
- match summary builders
- head-to-head stats computation
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional

from db import fetch_one
from models import FixtureSummary, MatchSummary


# ---------------------------------------------------------------------------
# "Club" alias groups (Stormers + Western Province, etc.)
# ---------------------------------------------------------------------------

# Each set is a group of names that should be treated as the SAME club
# when tsdb_league_id == 0 (ALL leagues mode).
CLUB_ALIAS_GROUPS = [
    {"stormers", "western province"},
    {"bulls", "blue bulls"},
    {"sharks", "natal sharks"},
    {"lions", "golden lions"},
    {"cheetahs", "free state cheetahs"},
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
    """
    Lowercase and strip punctuation-ish noise for fuzzy matching.
    """
    name = name.lower()
    name = re.sub(r"[^\w\s]", "", name)  # remove punctuation
    name = re.sub(r"\s+", " ", name).strip()
    return name


def find_alias_group(name: str) -> Optional[Iterable[str]]:
    """
    Return the alias group that contains `name`, or None.
    """
    norm = normalise_name(name)
    for group in CLUB_ALIAS_GROUPS:
        if norm in {normalise_name(x) for x in group}:
            return group
    return None


def resolve_team_in_league(league_id: int, team_name: str) -> Optional[Dict[str, Any]]:
    """
    Resolve a team by name within a specific league:

    1. Try exact LOWER(name) match
    2. Then try ILIKE %name%
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
        WHERE s.league_id = %s
          AND t.name ILIKE %s
        LIMIT 1
        """,
        (league_id, f"%{team_name}%"),
    )
    return row


def resolve_team_global(team_name: str) -> Optional[Dict[str, Any]]:
    """
    Global team resolve for tsdb_league_id == 0 mode.

    We try:
    - alias group match on normalised names
    - direct LOWER(name) match
    - ILIKE %name%
    """
    alias_group = find_alias_group(team_name)

    if alias_group:
        placeholders = ", ".join(["LOWER(%s)"] * len(alias_group))
        params = tuple(alias_group)
        query = f"""
            SELECT id, name
            FROM teams
            WHERE LOWER(name) IN ({placeholders})
            ORDER BY name
            LIMIT 1
        """
        row = fetch_one(query, params)
        if row:
            return row

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


def build_match_summary_row(row: Dict[str, Any]) -> MatchSummary:
    """
    Convert DB row into MatchSummary.
    """
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


def build_fixture_summary_row(row: Dict[str, Any]) -> FixtureSummary:
    """
    Convert DB row into FixtureSummary.
    """
    return FixtureSummary(
        match_id=row["match_id"],
        kickoff_utc=row["kickoff_utc"],
        home_team=row["home_team"],
        away_team=row["away_team"],
        venue=row.get("venue"),
        league=row.get("league"),
        season=row.get("season"),
    )


def compute_head_to_head_stats(
    matches: List[MatchSummary], team_a_name: str, team_b_name: str
) -> Dict[str, Any]:
    """
    Compute win/draw counts, win rates, and current streak.
    """
    team_a_norm = normalise_name(team_a_name)
    team_b_norm = normalise_name(team_b_name)

    total = 0
    team_a_wins = 0
    team_b_wins = 0
    draws = 0

    for m in matches:
        if m.home_score is None or m.away_score is None:
            continue
        total += 1

        home_norm = normalise_name(m.home_team)
        away_norm = normalise_name(m.away_team)

        a_is_home = home_norm == team_a_norm
        a_is_away = away_norm == team_a_norm
        b_is_home = home_norm == team_b_norm
        b_is_away = away_norm == team_b_norm

        if not ((a_is_home or a_is_away) and (b_is_home or b_is_away)):
            continue

        if m.home_score > m.away_score:
            winner = home_norm
        elif m.home_score < m.away_score:
            winner = away_norm
        else:
            winner = None

        if winner is None:
            draws += 1
        elif winner == team_a_norm:
            team_a_wins += 1
        elif winner == team_b_norm:
            team_b_wins += 1

    def _rate(x: int) -> float:
        return round(100.0 * x / total, 1) if total > 0 else 0.0

    team_a_rate = _rate(team_a_wins)
    team_b_rate = _rate(team_b_wins)
    draw_rate = _rate(draws)

    current_streak = None
    for m in matches:
        if m.home_score is None or m.away_score is None:
            continue

        home_norm = normalise_name(m.home_team)
        away_norm = normalise_name(m.away_team)
        a_involved = team_a_norm in {home_norm, away_norm}
        b_involved = team_b_norm in {home_norm, away_norm}
        if not (a_involved and b_involved):
            continue

        if m.home_score == m.away_score:
            current_streak = "Draw"
        else:
            winner = home_norm if m.home_score > m.away_score else away_norm
            if winner == team_a_norm:
                current_streak = f"{team_a_name} win"
            elif winner == team_b_norm:
                current_streak = f"{team_b_name} win"
            else:
                current_streak = "Other"
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
