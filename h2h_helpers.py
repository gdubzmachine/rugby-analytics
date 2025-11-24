#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
h2h_helpers.py

Shared helpers for Rugby Analytics:

- DB connection & simple query helpers
- club alias groups (Bulls / Blue Bulls, Stormers / WP, etc.)
- name normalisation / alias-group resolution
- league & season resolution
- team resolution (global + per-league)
- core head-to-head stats computation (ID-based)
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Set, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load env for local dev (.env); no-op on Render
load_dotenv()


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def get_conn():
    """
    Open a Postgres connection using the DATABASE_URL environment variable.
    """
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "DATABASE_URL is not set. Configure it in your .env for local dev, "
            "or as a Render env var in production."
        )
    return psycopg2.connect(dsn, cursor_factory=RealDictCursor)


def fetch_one(query: str, params: Tuple[Any, ...] = ()) -> Optional[Dict[str, Any]]:
    """Run a query that returns a single row (or None)."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            row = cur.fetchone()
    return row


def fetch_all(query: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    """Run a query that returns multiple rows."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
    return list(rows)


# ---------------------------------------------------------------------------
# Name normalisation
# ---------------------------------------------------------------------------


def normalise_name(name: str) -> str:
    """
    Normalise team names by:
    - making lowercase
    - removing common sponsor prefixes (DHL, Vodacom, Cell C, Emirates, MTN, Toyota, Hollywoodbets, etc.)
    - stripping punctuation
    - collapsing whitespace

    This is critical so that DB names like:
      "DHL Stormers" -> "stormers"
      "Vodacom Bulls" -> "bulls"
      "Hollywoodbets Sharks" -> "sharks"
      "Toyota Cheetahs" -> "cheetahs"
    match the alias groups.
    """
    import re

    name = name.lower()

    # remove sponsor / branding prefixes and common noise words
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

    # remove punctuation/noise characters
    name = re.sub(r"[^\w\s]", "", name)

    # collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()
    return name


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

    # Existing groups for other clubs / competitions
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
    """
    Return the alias group that contains `name` (by normalised equality), or None.

    This is used to decide *which club* a query like "Stormers" or "Blue Bulls"
    refers to. It does NOT do substring checks; we want to be conservative and
    only match exactly (after normalisation).
    """
    norm = normalise_name(name)
    for group in CLUB_ALIAS_GROUPS:
        norm_group = {normalise_name(x) for x in group}
        if norm in norm_group:
            return group
    return None


# ---------------------------------------------------------------------------
# League / season / team resolution helpers
# ---------------------------------------------------------------------------


def resolve_league_by_tsdb(tsdb_league_id: int) -> Optional[Dict[str, Any]]:
    """
    Resolve an internal league row by tsdb_league_id.
    """
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
    """
    Return the most recent season row for a given league.
    """
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
    league_id: int,
    season_label: str,
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


def resolve_team_in_league(league_id: int, team_name: str) -> Optional[Dict[str, Any]]:
    """
    Resolve a team by name within a specific league:

    - tries exact LOWER(name) match
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
        (league_id, f"%{team_name}%",),
    )
    return row


def resolve_team_global(team_name: str) -> Optional[Dict[str, Any]]:
    """
    Global team resolve, used as a fallback when no alias group matches.

    We try:
    - direct LOWER(name) match
    - then ILIKE %name%
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


def resolve_club_team_ids_all_leagues(team_name: str) -> Tuple[List[int], str]:
    """
    For tsdb_league_id == 0 (ALL leagues mode):

    - If team_name belongs to an alias group, find ALL team_ids whose
      normalised name matches any alias in that group.
    - If nothing matches, fall back to a single global team lookup.
    - Returns: (team_ids, representative_display_name).

    This is *club → team_ids* logic. Everything else stays team_id-based.
    """
    alias_group = find_alias_group(team_name)
    if alias_group:
        group_norms = {normalise_name(x) for x in alias_group}

        # Pull all teams and filter in Python to respect our normalisation rules
        rows = fetch_all("SELECT id, name FROM teams")
        club_rows: List[Dict[str, Any]] = [
            r for r in rows if normalise_name(r["name"]) in group_norms
        ]

        if club_rows:
            ids = [r["id"] for r in club_rows]
            # Use the first DB name as "nice" display (e.g. 'Stormers' or 'DHL Stormers')
            rep_name = club_rows[0]["name"]
            return ids, rep_name

    # Fallback: no alias group or nothing matched in DB → just pick one team globally
    row = resolve_team_global(team_name)
    if not row:
        return [], team_name
    return [row["id"]], row["name"]


# ---------------------------------------------------------------------------
# Core head-to-head stats (ID-based)
# ---------------------------------------------------------------------------


def compute_head_to_head_stats_from_rows(
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
    - No name-based substring matching.
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

        # Skip fixtures with no scores yet
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
