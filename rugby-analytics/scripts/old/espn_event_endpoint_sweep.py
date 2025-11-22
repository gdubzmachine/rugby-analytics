#!/usr/bin/env python3
"""
Brute-force sweep of common ESPN rugby endpoints for a single (league_id, event_id).

- Fetches summary first (which we already know works).
- Then tries a bunch of SITE endpoints (summary, boxscore, play-by-play, etc.).
- Then tries a bunch of CORE endpoints (boxscore, summary, plays, etc.).
- For each:
    * prints HTTP status + content-type
    * if JSON, prints top-level keys and some quick counts
    * optional: save JSON/text to disk for manual inspection

No DB, no project imports. Pure diagnostics.
"""

import os
import sys
import json
import argparse
from typing import Any, Dict, List, Tuple, Optional

import requests

USER_AGENT = os.getenv("ESPN_USER_AGENT", "RugbyAnalytics/1.0 (endpoint-sweep)")
REGION = os.getenv("ESPN_REGION", "us")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})


def hdr_json() -> Dict[str, str]:
    return {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.espn.com",
        "Referer": "https://www.espn.com/",
    }


def hdr_html() -> Dict[str, str]:
    return {
        "User-Agent": USER_AGENT,
        "Accept": "text/html, */*",
    }


def try_get(label: str, url: str, params: Optional[Dict[str, Any]] = None,
            prefer_json: bool = True, save_dir: Optional[str] = None) -> None:
    headers = hdr_json() if prefer_json else hdr_html()
    try:
        r = SESSION.get(url, params=params, headers=headers, timeout=30)
    except Exception as e:
        print(f"[{label}] ERROR: {e}")
        return

    full_url = r.url
    ctype = r.headers.get("Content-Type", "")
    print(f"[{label}] {r.status_code} {ctype}")
    print(f"          {full_url}")

    # Try JSON
    is_json = "json" in ctype.lower()
    data: Any = None
    if is_json:
        try:
            data = r.json()
        except Exception:
            is_json = False

    if is_json and isinstance(data, dict):
        keys = list(data.keys())
        print(f"          keys: {', '.join(keys[:8])}{'...' if len(keys) > 8 else ''}")
        # some heuristics
        if "header" in data:
            print("          has 'header'")
        if "boxscore" in data:
            bs = data["boxscore"] or {}
            print(f"          boxscore.teams={len(bs.get('teams') or [])}, boxscore.players={len(bs.get('players') or [])}")
        if "scoringPlays" in data and isinstance(data["scoringPlays"], list):
            print(f"          scoringPlays={len(data['scoringPlays'])}")
        if "plays" in data and isinstance(data["plays"], list):
            print(f"          plays={len(data['plays'])}")

        if save_dir:
            os.makedirs(save_dir, exist_ok=True)
            fname = label.replace(" ", "_").replace("/", "_") + ".json"
            out_path = os.path.join(save_dir, fname)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            print(f"          saved JSON -> {out_path}")
    else:
        # save text/html snippet if asked
        if save_dir and r.text:
            os.makedirs(save_dir, exist_ok=True)
            fname = label.replace(" ", "_").replace("/", "_") + ".txt"
            out_path = os.path.join(save_dir, fname)
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(r.text[:20000])
            print(f"          saved text snippet -> {out_path}")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Sweep common ESPN rugby endpoints for a single (league_id, event_id)."
    )
    ap.add_argument("--league-id", "-l", type=int, default=289234,
                    help="Competition/league id (default 289234 = internationals)")
    ap.add_argument("--event-id", "-e", required=True,
                    help="ESPN event id, e.g. 602480")
    ap.add_argument("--save-dir", "-o", help="Optional directory to save JSON/text responses")
    args = ap.parse_args()

    league_id = args.league_id
    event_id = args.event_id.strip()
    save_dir = args.save_dir

    # ---- SITE API patterns ----
    base_site = f"https://site.web.api.espn.com/apis/site/v2/sports/rugby/{league_id}"
    common_params = {"event": event_id, "lang": "en", "region": REGION, "contentorigin": "espn"}

    print("\n=== SITE API sweep ===\n")

    try_get("site summary",     base_site + "/summary",      params=common_params, save_dir=save_dir)
    try_get("site boxscore",    base_site + "/boxscore",     params=common_params, save_dir=save_dir)
    try_get("site playbyplay",  base_site + "/playbyplay",   params=common_params, save_dir=save_dir)
    try_get("site lineups",     base_site + "/lineups",      params=common_params, save_dir=save_dir)
    try_get("site gamecast",    base_site + "/gamecast",     params=common_params, save_dir=save_dir)
    try_get("site gamepackage", base_site + "/gamepackage",  params=common_params, save_dir=save_dir)

    # ---- CORE API patterns ----
    print("\n=== CORE API sweep ===\n")
    base_core = f"https://sports.core.api.espn.com/v2/sports/rugby/leagues/{league_id}/events/{event_id}"

    # Root event object
    try_get("core event root", base_core, prefer_json=True, save_dir=save_dir)

    # We *guess* competitions/{event_id} path (common pattern for other sports)
    core_comp = f"{base_core}/competitions/{event_id}"
    try_get("core competition", core_comp, prefer_json=True, save_dir=save_dir)

    # Then some relatives under competition
    try_get("core comp boxscore",   f"{core_comp}/boxscore",   prefer_json=True, save_dir=save_dir)
    try_get("core comp summary",    f"{core_comp}/summary",    prefer_json=True, save_dir=save_dir)
    try_get("core comp plays",      f"{core_comp}/plays",      prefer_json=True, save_dir=save_dir)
    try_get("core comp score",      f"{core_comp}/score",      prefer_json=True, save_dir=save_dir)
    try_get("core comp competitors",f"{core_comp}/competitors",prefer_json=True, save_dir=save_dir)
    try_get("core comp linescore",  f"{core_comp}/linescore",  prefer_json=True, save_dir=save_dir)
    try_get("core comp leaders",    f"{core_comp}/leaders",    prefer_json=True, save_dir=save_dir)

    # HTML “front-end” pages (for debugging / manual scraping if needed)
    print("\n=== HTML front pages (for reference) ===\n")
    html_base_match = f"https://www.espn.com/rugby/match/_/gameId/{event_id}/league/{league_id}"
    html_base_stats = f"https://www.espn.com/rugby/matchstats/_/gameId/{event_id}/league/{league_id}"
    html_base_lineups = f"https://www.espn.com/rugby/lineups/_/gameId/{event_id}/league/{league_id}"
    try_get("html match",    html_base_match,   prefer_json=False, save_dir=save_dir)
    try_get("html matchstats", html_base_stats, prefer_json=False, save_dir=save_dir)
    try_get("html lineups",  html_base_lineups, prefer_json=False, save_dir=save_dir)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
