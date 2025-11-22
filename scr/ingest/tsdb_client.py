#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
scr.ingest.tsdb_client
----------------------

Shared TheSportsDB V1 client for rugby-analytics.

All HTTP access to TheSportsDB should go through this module.

Exposed functions:

    get_league_meta(league_id: str, verbose: bool=False) -> dict
    get_current_season_label(league_id: str, verbose: bool=False) -> Optional[str]
    get_events_for_season_rugby(league_id: str, season: str, verbose: bool=False) -> List[dict]

    lookup_team_players(team_id: str, rugby_only: bool=True, verbose: bool=False) -> List[dict]
    lookup_player(player_id: str, verbose: bool=False) -> Optional[dict]

    get_team_details(team_id: str, verbose: bool=False) -> Optional[dict]
        # <-- new: used by ingest_urc_teams.py

API key:
    Read from env / .env: THESPORTSDB_API_KEY
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

import requests

# ---------------------------------------------------------------------------
# Load .env (optional)
# ---------------------------------------------------------------------------

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    # dotenv is optional
    pass

# ---------------------------------------------------------------------------
# API key / base URL
# ---------------------------------------------------------------------------

THESPORTSDB_API_KEY: str = os.getenv("THESPORTSDB_API_KEY", "1")


def _base_url() -> str:
    """
    Construct the v1 base URL using the current env API key.
    """
    key = os.getenv("THESPORTSDB_API_KEY", THESPORTSDB_API_KEY) or "1"
    return f"https://www.thesportsdb.com/api/v1/json/{key}"


# ---------------------------------------------------------------------------
# HTTP session + backoff
# ---------------------------------------------------------------------------

_session: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        s = requests.Session()
        s.headers.update({"User-Agent": "rugby-analytics/tsdb-client"})
        _session = s
    return _session


def _get_json_with_backoff(
    endpoint: str,
    params: Dict[str, Any],
    max_retries: int = 4,
    verbose: bool = False,
) -> Dict[str, Any]:
    """
    GET {base}/{endpoint}?{params} with simple exponential backoff on
    429 or 5xx responses.

    Returns decoded JSON (or {}).
    Raises requests.HTTPError for final non-2xx response.
    """
    url = f"{_base_url().rstrip('/')}/{endpoint.lstrip('/')}"
    sess = _get_session()
    delay = 0.8

    for attempt in range(1, max_retries + 1):
        resp = sess.get(url, params=params, timeout=45)
        status = resp.status_code

        if status in (429, 500, 502, 503, 504) and attempt < max_retries:
            if verbose:
                print(
                    f"[TSDB] HTTP {status} on {endpoint} {params} "
                    f"(attempt {attempt}/{max_retries}); sleeping {delay:.1f}s…"
                )
            time.sleep(delay)
            delay *= 1.8
            continue

        resp.raise_for_status()
        try:
            return resp.json() or {}
        except Exception:
            return {}

    # Last response has already been raise_for_status()'d above if error.
    return {}


# ---------------------------------------------------------------------------
# League helpers
# ---------------------------------------------------------------------------

def get_league_meta(league_id: str, verbose: bool = False) -> Dict[str, Any]:
    """
    Wrap v1 /lookupleague.php?id={league_id}
    """
    data = _get_json_with_backoff("lookupleague.php", {"id": league_id}, verbose=verbose)
    leagues = data.get("leagues") or []
    league = leagues[0] if leagues else {}
    if verbose and league:
        print(f"[TSDB] League {league_id}: {league.get('strLeague')}")
    return league


def get_current_season_label(league_id: str, verbose: bool = False) -> Optional[str]:
    """
    Convenience wrapper: league -> strCurrentSeason.
    """
    league = get_league_meta(league_id, verbose=verbose)
    label = (league.get("strCurrentSeason") or "").strip()
    if verbose:
        print(f"[TSDB] Current season for league {league_id}: {label}")
    return label or None


def get_events_for_season_rugby(
    league_id: str,
    season: str,
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    """
    Wrap v1 /eventsseason.php?id={league_id}&s={season}, filtered to rugby events.
    """
    data = _get_json_with_backoff(
        "eventsseason.php",
        {"id": league_id, "s": season},
        verbose=verbose,
    )
    events = data.get("events") or []
    rugby: List[Dict[str, Any]] = []
    for e in events:
        sport = (e.get("strSport") or "").lower()
        if sport.startswith("rugby"):
            rugby.append(e)
    if verbose:
        print(
            f"[TSDB] eventsseason id={league_id} season={season} "
            f"-> {len(rugby)} rugby events"
        )
    return rugby


# ---------------------------------------------------------------------------
# Player / team helpers
# ---------------------------------------------------------------------------

def lookup_team_players(
    team_id: str,
    rugby_only: bool = True,
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    """
    Wrap v1 /lookup_all_players.php?id={team_id}.
    If rugby_only=True, filter to strSport starting with 'rugby'.
    """
    data = _get_json_with_backoff(
        "lookup_all_players.php",
        {"id": team_id},
        verbose=verbose,
    )
    players = data.get("player") or []
    if rugby_only:
        filtered: List[Dict[str, Any]] = []
        for p in players:
            sport = (p.get("strSport") or "").lower()
            if sport and not sport.startswith("rugby"):
                continue
            filtered.append(p)
        if verbose:
            print(
                f"[TSDB] lookup_all_players id={team_id}: "
                f"{len(players)} total, {len(filtered)} rugby"
            )
        return filtered
    if verbose:
        print(f"[TSDB] lookup_all_players id={team_id}: {len(players)} players")
    return players


def lookup_player(
    player_id: str,
    verbose: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Wrap v1 /lookupplayer.php?id={player_id}.
    """
    data = _get_json_with_backoff(
        "lookupplayer.php",
        {"id": player_id},
        verbose=verbose,
    )
    players = data.get("players") or data.get("player") or []
    player = players[0] if players else None
    if verbose:
        if player:
            print(f"[TSDB] lookupplayer id={player_id}: {player.get('strPlayer')}")
        else:
            print(f"[TSDB] lookupplayer id={player_id}: NOT FOUND")
    return player


def get_team_details(
    team_id: str,
    verbose: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Wrap v1 /lookupteam.php?id={team_id}.

    Returns the first team dict, or None if not found.
    """
    data = _get_json_with_backoff(
        "lookupteam.php",
        {"id": team_id},
        verbose=verbose,
    )
    teams = data.get("teams") or data.get("team") or []
    team = teams[0] if teams else None
    if verbose:
        if team:
            print(f"[TSDB] lookupteam id={team_id}: {team.get('strTeam')}")
        else:
            print(f"[TSDB] lookupteam id={team_id}: NOT FOUND")
    return team
