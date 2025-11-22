#!/usr/bin/env python3
"""
Scan key ESPN rugby union leagues and see which hidden API endpoints
are available for each league (summary, boxscore, play-by-play, etc.).

This version:

- Uses a curated list of CONFIRMED league IDs from the public ESPN
  rugby scoreboards (not the broken dropdown IDs).
- For each league, looks backwards in time on the scoreboard to find
  *any* event.
- Once an event is found, probes multiple JSON endpoints and prints a
  capability table with a simple "richness" rating.

Usage examples (from repo root):

    # default: scan all known leagues, ~5.5 years back, step 7 days
    python .\\scripts\\espn_rugby_league_scanner.py

    # go lighter (2 years, step 7 days)
    python .\\scripts\\espn_rugby_league_scanner.py --days-back 730 --step-days 7

    # debug fewer leagues
    python .\\scripts\\espn_rugby_league_scanner.py --max-leagues 3

"""

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests


# -----------------------------------------------------------------------------
# CONFIG: curated rugby union leagues with confirmed league_id values
# -----------------------------------------------------------------------------
# These IDs come directly from ESPN rugby scoreboard URLs, e.g.:
#   https://www.espn.com/rugby/scoreboard/_/league/180659  -> Six Nations
#   https://www.espn.com/rugby/scoreboard/_/league/244293  -> The Rugby Championship
#   https://www.espn.com/rugby/scoreboard/_/league/164205  -> Rugby World Cup
#   https://www.espn.com/rugby/scoreboard/_/league/289234  -> International Test Match
# etc.
#
# If you discover new leagues, just add them here.
RUGBY_LEAGUES: List[Dict[str, str]] = [
    # Test & tournament level
    {"id": "289234", "name": "International Test Match"},
    {"id": "180659", "name": "Six Nations"},
    {"id": "244293", "name": "The Rugby Championship"},
    {"id": "164205", "name": "Rugby World Cup"},
    {"id": "289237", "name": "Women’s Rugby World Cup"},
    {"id": "268565", "name": "British & Irish Lions Tour 2025"},

    # Club / regional competitions
    {"id": "242041", "name": "Super Rugby Pacific"},
    {"id": "267979", "name": "Premiership Rugby"},
    {"id": "289279", "name": "URBA Top 12"},
    {"id": "171198", "name": "Americas Rugby Championship"},
]

USER_AGENT = os.getenv("ESPN_USER_AGENT", "RugbyAnalytics/1.0 (league-scanner)")
REGION = os.getenv("ESPN_REGION", "us")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})


# -----------------------------------------------------------------------------
# HTTP helpers
# -----------------------------------------------------------------------------

def _json_headers() -> Dict[str, str]:
    return {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.espn.com",
        "Referer": "https://www.espn.com/",
    }


def http_get_json(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 20,
) -> Tuple[int, Optional[Any]]:
    """
    Safe GET that returns (status_code, json_or_None).
    Does *not* raise on non-2xx. If non-JSON, returns (status, None).
    """
    try:
        r = SESSION.get(url, params=params, headers=_json_headers(), timeout=timeout)
    except Exception as e:
        print(f"  HTTP ERROR for {url}: {e}")
        return 0, None

    status = r.status_code
    ctype = (r.headers.get("Content-Type") or "").lower()
    if "json" not in ctype:
        return status, None

    try:
        return status, r.json()
    except Exception:
        return status, None


# -----------------------------------------------------------------------------
# 1) Find a sample event for each league via scoreboard
# -----------------------------------------------------------------------------

def find_event_for_league(
    league_id: str,
    days_back: int,
    step_days: int,
) -> Optional[str]:
    """
    Scan backwards in time using the scoreboard endpoint to find ANY event
    for this league.

    - days_back: how far back to search (e.g. 730, 1500, 2000)
    - step_days: how many days between samples (e.g. 1, 3, 7)

    Returns the first event_id found as a string, or None if none found.
    """
    if step_days <= 0:
        step_days = 1

    today = datetime.now(timezone.utc).date()
    base_url = (
        f"https://site.web.api.espn.com/apis/site/v2/sports/rugby/"
        f"{league_id}/scoreboard"
    )

    print(f"    Scanning up to {days_back} days back, step={step_days} days...")

    for offset in range(0, days_back, step_days):
        day = today - timedelta(days=offset)
        datestr = day.strftime("%Y%m%d")
        params = {
            "dates": datestr,
            "lang": "en",
            "region": REGION,
            "contentorigin": "espn",
        }

        status, data = http_get_json(base_url, params=params)
        if status != 200 or not isinstance(data, dict):
            continue

        events = data.get("events") or []
        if not events:
            continue

        for ev in events:
            ev_id = ev.get("id")
            if ev_id:
                print(f"    ✅ found event {ev_id} on {datestr}")
                return str(ev_id)

    print(f"    ⚠️ no events found in last {days_back} days.")
    return None


# -----------------------------------------------------------------------------
# 2) Probe JSON endpoints for a single (league, event)
# -----------------------------------------------------------------------------

def probe_league_event(league_id: str, league_name: str, event_id: str) -> Dict[str, Any]:
    """
    For a given league_id + event_id, probe the main JSON endpoints and
    return a dict describing which ones are available plus a richness score.
    """
    result: Dict[str, Any] = {
        "league_id": league_id,
        "league_name": league_name,
        "event_id": event_id,
        # site APIs
        "site_summary": False,
        "site_boxscore": False,
        "site_playbyplay": False,
        "site_lineups": False,
        # core APIs
        "core_plays": False,
        "core_boxscore": False,
        "core_competitors": False,
        # overall metric
        "richness": 0,
    }

    base_site = f"https://site.web.api.espn.com/apis/site/v2/sports/rugby/{league_id}"
    common_params = {
        "event": event_id,
        "lang": "en",
        "region": REGION,
        "contentorigin": "espn",
    }

    # --- site summary
    status, data = http_get_json(base_site + "/summary", params=common_params)
    if status == 200 and isinstance(data, dict) and "header" in data:
        result["site_summary"] = True

    # site boxscore
    status, _ = http_get_json(base_site + "/boxscore", params=common_params)
    if status == 200:
        result["site_boxscore"] = True

    # site play-by-play
    status, _ = http_get_json(base_site + "/playbyplay", params=common_params)
    if status == 200:
        result["site_playbyplay"] = True

    # site lineups
    status, _ = http_get_json(base_site + "/lineups", params=common_params)
    if status == 200:
        result["site_lineups"] = True

    # --- core APIs
    base_core = (
        f"https://sports.core.api.espn.com/v2/sports/rugby/leagues/"
        f"{league_id}/events/{event_id}"
    )
    core_comp = f"{base_core}/competitions/{event_id}"

    # core competitors
    status, _ = http_get_json(f"{core_comp}/competitors")
    if status == 200:
        result["core_competitors"] = True

    # core plays
    status, _ = http_get_json(f"{core_comp}/plays")
    if status == 200:
        result["core_plays"] = True

    # core boxscore
    status, _ = http_get_json(f"{core_comp}/boxscore")
    if status == 200:
        result["core_boxscore"] = True

    # compute richness score
    richness = 0
    for key in [
        "site_summary",
        "site_boxscore",
        "site_playbyplay",
        "site_lineups",
        "core_plays",
        "core_boxscore",
        "core_competitors",
    ]:
        if result[key]:
            richness += 1
    result["richness"] = richness

    return result


def richness_to_stars(richness: int) -> str:
    """Map richness [0..7] to a star rating."""
    if richness <= 0:
        return "–"
    if richness == 1:
        return "⭐"
    if richness <= 3:
        return "⭐⭐"
    if richness <= 5:
        return "⭐⭐⭐"
    if richness == 6:
        return "⭐⭐⭐⭐"
    return "⭐⭐⭐⭐⭐"


def mark(flag: bool) -> str:
    return "✔" if flag else "✖"


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Scan ESPN rugby union leagues for available hidden API endpoints."
    )
    ap.add_argument(
        "--days-back",
        type=int,
        default=2000,
        help="How many days back to search for events (default 2000 ≈ 5.5 years).",
    )
    ap.add_argument(
        "--step-days",
        type=int,
        default=7,
        help="How many days between scoreboard samples (default 7 = weekly).",
    )
    ap.add_argument(
        "--max-leagues",
        type=int,
        default=0,
        help="Optional cap on number of leagues to scan (0 = all).",
    )
    args = ap.parse_args()

    leagues = list(RUGBY_LEAGUES)
    if args.max_leagues > 0:
        leagues = leagues[: args.max_leagues]

    print(f"Scanning {len(leagues)} curated rugby union leagues...")
    print(f"  days_back={args.days_back}, step_days={args.step_days}\n")

    results: List[Dict[str, Any]] = []

    for lg in leagues:
        lid = lg["id"]
        name = lg["name"]
        print(f"=== League {name} (id={lid}) ===")

        event_id = find_event_for_league(
            lid, days_back=args.days_back, step_days=args.step_days
        )
        if not event_id:
            print("    Skipping – no events found in this time window.\n")
            continue

        res = probe_league_event(lid, name, event_id)
        results.append(res)

        print(
            "    Probed endpoints:"
            f" summary={res['site_summary']},"
            f" boxscore={res['site_boxscore']},"
            f" pbp={res['site_playbyplay']},"
            f" lineups={res['site_lineups']},"
            f" core_plays={res['core_plays']},"
            f" core_box={res['core_boxscore']},"
            f" core_competitors={res['core_competitors']},"
            f" richness={res['richness']}\n"
        )

    if not results:
        print("No leagues with events found in the configured time window.")
        return

    # sort by richness desc
    results.sort(key=lambda r: r["richness"], reverse=True)

    print("\n===============================================================================")
    print("ESPN RUGBY UNION LEAGUE DATA CAPABILITY MAP")
    print("===============================================================================\n")

    header = (
        f"{'League Name':35} {'ID':8} "
        f"{'sum':3} {'box':3} {'pbp':3} {'lin':3} "
        f"{'plays':5} {'c_box':5} {'c_comp':6} {'richness':9}"
    )
    print(header)
    print("-" * len(header))

    for r in results:
        name = (r["league_name"] or "")[:33]
        lid = r["league_id"]
        stars = richness_to_stars(r["richness"])

        line = (
            f"{name:35} {lid:8} "
            f"{mark(r['site_summary']):3} "
            f"{mark(r['site_boxscore']):3} "
            f"{mark(r['site_playbyplay']):3} "
            f"{mark(r['site_lineups']):3} "
            f"{mark(r['core_plays']):5} "
            f"{mark(r['core_boxscore']):5} "
            f"{mark(r['core_competitors']):6} "
            f"{stars:9}"
        )
        print(line)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
