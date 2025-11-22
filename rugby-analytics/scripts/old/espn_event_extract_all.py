#!/usr/bin/env python3
"""
Extract as much structured data as possible for a single ESPN rugby event
from the summary JSON alone (no DB).

What it does:
- Fetches summary JSON for league_id + event_id.
- Prints:
    * Match metadata (teams, scores, venue, officials).
    * Scoring plays.
    * Team stats (boxscore.teams).
    * Per-player stats (flattened from boxscore.players[*].statistics[*].athletes).
- Optionally writes:
    * match_team_stats.csv
    * match_player_stats.csv

Usage examples (from project root):
  python scripts/espn_event_extract_all.py --league-id 289234 --event-id 602480
  python scripts/espn_event_extract_all.py --league-id 289234 --event-id 602480 ^
      --team-csv .\\exports\\team_stats_602480.csv ^
      --player-csv .\\exports\\player_stats_602480.csv
"""

import os
import sys
import csv
import argparse
from typing import Any, Dict, List, Tuple, Optional

import requests

USER_AGENT = os.getenv("ESPN_USER_AGENT", "RugbyAnalytics/1.0 (event-extract-all)")
REGION = os.getenv("ESPN_REGION", "us")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})

# -------------------------------------------------------------------
# HTTP
# -------------------------------------------------------------------

def _json_headers() -> Dict[str, str]:
    return {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.espn.com",
        "Referer": "https://www.espn.com/",
    }


def fetch_summary(league_id: int, event_id: str) -> Dict[str, Any]:
    url = f"https://site.web.api.espn.com/apis/site/v2/sports/rugby/{league_id}/summary"
    params = {"event": event_id, "lang": "en", "region": REGION, "contentorigin": "espn"}
    r = SESSION.get(url, params=params, headers=_json_headers(), timeout=30)
    print(f"[summary] {r.status_code} -> {r.url}")
    r.raise_for_status()
    j = r.json()
    if not isinstance(j, dict) or "header" not in j:
        raise RuntimeError("Unexpected summary JSON (no 'header').")
    return j


# -------------------------------------------------------------------
# Printing helpers
# -------------------------------------------------------------------

def _team_line(c: Optional[Dict[str, Any]]) -> str:
    if not c:
        return "UNKNOWN"
    t = (c.get("team") or {})
    name = t.get("displayName") or t.get("name") or "UNKNOWN"
    abbr = t.get("abbreviation") or ""
    score = c.get("score")
    score_str = f"{score}" if score is not None else "-"
    return f"{name} ({abbr}) {score_str}"


def print_match_meta(summary: Dict[str, Any]) -> None:
    header = summary.get("header", {})
    comps = header.get("competitions") or []
    comp = comps[0] if comps else {}

    date_iso = comp.get("date")
    competitors = comp.get("competitors") or []
    home = next((c for c in competitors if c.get("homeAway") == "home"), None)
    away = next((c for c in competitors if c.get("homeAway") == "away"), None)

    # status
    status = (comp.get("status") or {}).get("type", {}).get("description") or "-"

    # venue / attendance
    game_info = summary.get("gameInfo") or {}
    venue = game_info.get("venue") or comp.get("venue") or {}
    venue_name = None
    city = None
    country = None
    if isinstance(venue, dict):
        venue_name = venue.get("fullName") or venue.get("name")
        addr = venue.get("address") or {}
        city = addr.get("city")
        country = addr.get("country")
    attendance = game_info.get("attendance")

    print("\n=== MATCH META ===")
    print(f"Status : {status}")
    print(f"Date   : {date_iso}")
    print(f"Home   : {_team_line(home)}")
    print(f"Away   : {_team_line(away)}")
    if venue_name:
        bits = [b for b in [city, country] if b]
        loc_str = ", ".join(bits) if bits else ""
        print(f"Venue  : {venue_name}{(' ('+loc_str+')') if loc_str else ''}")
    if attendance:
        print(f"Attnd. : {attendance}")

    # Officials if present
    officials = game_info.get("officials") or []
    if officials:
        print("Officials:")
        for o in officials:
            name = o.get("displayName") or o.get("name")
            role = o.get("position") or o.get("title")
            if isinstance(role, dict):
                role = role.get("displayName") or role.get("name")
            print(f"  - {name} ({role})")


def print_scoring_plays(summary: Dict[str, Any]) -> None:
    plays = summary.get("scoringPlays") or []
    print("\n=== SCORING PLAYS ===")
    if not plays:
        print("(none in feed)")
        return

    for p in plays:
        minute = p.get("clock")
        period = p.get("period")
        team_name = (p.get("team") or {}).get("displayName") or (p.get("team") or {}).get("abbreviation") or ""
        text = p.get("text") or (p.get("type") or {}).get("text") or (p.get("type") or {}).get("name") or ""
        atts = p.get("athletes") or []
        who = ", ".join(
            [
                a.get("athlete", {}).get("displayName")
                for a in atts
                if isinstance(a, dict) and a.get("athlete")
            ]
        )

        home_score = p.get("homeScore")
        away_score = p.get("awayScore")
        scoreline = f" [{home_score}-{away_score}]" if home_score is not None and away_score is not None else ""

        if minute:
            when = f"{minute}'"
        elif period:
            when = f"P{period}"
        else:
            when = ""

        who_str = f" — {who}" if who else ""
        team_str = f"{team_name}: " if team_name else ""
        print(f"- {when} {team_str}{text}{who_str}{scoreline}")


def print_team_stats(summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Print and return a list of team stat records:
      {team_id, team_name, category, stat_label, stat_value}
    """
    box = summary.get("boxscore") or {}
    teams = box.get("teams") or []
    records: List[Dict[str, Any]] = []

    print("\n=== TEAM STATS ===")
    if not teams:
        print("(none in feed)")
        return records

    for t in teams:
        team = t.get("team") or {}
        tname = team.get("displayName") or team.get("name") or "UNKNOWN"
        tid = team.get("id")
        print(f"\n{tname}")
        stats = t.get("statistics") or []
        for s in stats:
            # For rugby, there is usually label/displayName + value/displayValue
            label = s.get("label") or s.get("name")
            value = s.get("displayValue") or s.get("value")
            if label is None and value is None:
                continue
            print(f"  - {label}: {value}")
            records.append({
                "team_id": tid,
                "team_name": tname,
                "stat_label": label,
                "stat_value": value,
            })

    return records


# -------------------------------------------------------------------
# Player stats flattening
# -------------------------------------------------------------------

def flatten_player_stats(summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Flatten boxscore.players[*].statistics[*].athletes[*] into a list of
    per-player stat records.

    Raw shape (typical ESPN rugby):
      boxscore.players = [
        {
          "team": {...},
          "statistics": [
            {
              "name": "scoring",
              "keys": ["T","C","P","DG","PTS"],
              "athletes": [
                {"athlete": {...}, "stats": ["1","3","0","0","13"], ...},
                ...
              ]
            },
            {
              "name": "attacking",
              "keys": [...],
              "athletes": [...]
            },
            ...
          ]
        },
        {... second team ...}
      ]

    We return one row per player with columns:
      event_id, team_id, team_name, athlete_id, athlete_name, position, jersey,
      plus one column per (category_name + "_" + key).
    """
    box = summary.get("boxscore") or {}
    players_blocks = box.get("players") or []
    results: List[Dict[str, Any]] = []

    # Find event id from header
    header = summary.get("header", {})
    event_id = header.get("id") or (header.get("competitions") or [{}])[0].get("id") or ""

    # Build per-player aggregator keyed by (team_id, athlete_id)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for team_block in players_blocks:
        team = team_block.get("team") or {}
        team_id = str(team.get("id") or "")
        team_name = team.get("displayName") or team.get("name") or team_id

        stats_cats = team_block.get("statistics") or []
        for cat in stats_cats:
            cat_name = cat.get("name") or cat.get("shortDisplayName") or cat.get("displayName") or "stat"
            keys = cat.get("keys") or []
            athletes = cat.get("athletes") or []
            for a in athletes:
                ath = a.get("athlete") or {}
                athlete_id = str(ath.get("id") or "")
                athlete_name = ath.get("displayName") or ath.get("fullName") or ath.get("shortName") or athlete_id
                pos_obj = a.get("position") or {}
                position = pos_obj.get("abbreviation") or pos_obj.get("name") or pos_obj.get("displayName") or ""
                jersey = a.get("jersey") or a.get("uniform") or a.get("uniformNumber") or ""

                key = (team_id, athlete_id)
                if key not in agg:
                    agg[key] = {
                        "event_id": event_id,
                        "team_id": team_id,
                        "team_name": team_name,
                        "athlete_id": athlete_id,
                        "athlete_name": athlete_name,
                        "position": position,
                        "jersey": jersey,
                    }

                stats_values = a.get("stats") or []
                for idx, k in enumerate(keys):
                    if idx >= len(stats_values):
                        continue
                    val = stats_values[idx]
                    col = f"{cat_name}_{k}"
                    agg[key][col] = val

    # Convert dict to list
    results = list(agg.values())
    return results


def print_player_stats_summary(rows: List[Dict[str, Any]], max_per_team: int = 5) -> None:
    """
    Print a small sample of player stats per team to the terminal so you can
    see what you're getting.
    """
    print("\n=== PLAYER STATS (sample) ===")
    if not rows:
        print("(none in feed)")
        return

    # Group by team
    by_team: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        by_team.setdefault(r["team_name"], []).append(r)

    for team_name, plist in by_team.items():
        print(f"\n{team_name}")
        print("-" * 60)
        for r in plist[:max_per_team]:
            base = f"{r.get('jersey',''):>2} {r.get('position',''):>3} {r['athlete_name']}"
            # show a couple of stat columns
            extra_cols = [k for k in r.keys() if k not in (
                "event_id","team_id","team_name","athlete_id","athlete_name","position","jersey"
            )]
            extra_cols.sort()
            preview = ", ".join([f"{k}={r[k]}" for k in extra_cols[:4]])
            print(f"  - {base} :: {preview}")


# -------------------------------------------------------------------
# CSV writers
# -------------------------------------------------------------------

def write_team_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        print(f"No team stats to write to {path}")
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fieldnames = ["team_id", "team_name", "stat_label", "stat_value"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"Team stats CSV written: {path}")


def write_player_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        print(f"No player stats to write to {path}")
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)

    # Gather all columns
    all_fields = set()
    for r in rows:
        all_fields.update(r.keys())
    fieldnames = sorted(all_fields)

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"Player stats CSV written: {path}")


# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Extract and print ESPN rugby event data (summary/boxscore/player stats) for one event (no DB)."
    )
    ap.add_argument("--league-id", "-l", type=int, default=289234,
                    help="Competition/league id (default 289234 = internationals)")
    ap.add_argument("--event-id", "-e", required=True,
                    help="ESPN event id, e.g. 602480")
    ap.add_argument("--team-csv", help="Optional path to write team stats CSV")
    ap.add_argument("--player-csv", help="Optional path to write player stats CSV")
    args = ap.parse_args()

    league_id = args.league_id
    event_id = args.event_id.strip()

    summary = fetch_summary(league_id, event_id)

    # 1) meta
    print_match_meta(summary)

    # 2) scoring
    print_scoring_plays(summary)

    # 3) team stats (and optional CSV)
    team_rows = print_team_stats(summary)

    # 4) player stats flatten
    player_rows = flatten_player_stats(summary)
    print_player_stats_summary(player_rows)

    if args.team_csv:
        write_team_csv(args.team_csv, team_rows)
    if args.player_csv:
        write_player_csv(args.player_csv, player_rows)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
