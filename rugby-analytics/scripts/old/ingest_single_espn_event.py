#!/usr/bin/env python3
import os
import sys
import re
import html as ihtml
import argparse
from typing import Dict, Any, List, Tuple, Optional

import requests

USER_AGENT = os.getenv("ESPN_USER_AGENT", "RugbyAnalytics/1.0 (espn_lineups_html)")
REGION = os.getenv("ESPN_REGION", "us")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})

def _hdrs_json() -> Dict[str, str]:
    return {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.espn.com",
        "Referer": "https://www.espn.com/",
    }

def _hdrs_html() -> Dict[str, str]:
    return {"User-Agent": USER_AGENT, "Accept": "text/html, */*"}

def fetch_summary(league_id: int, event_id: str) -> Dict[str, Any]:
    url = f"https://site.web.api.espn.com/apis/site/v2/sports/rugby/{league_id}/summary"
    params = {"event": event_id, "lang": "en", "region": REGION, "contentorigin": "espn"}
    r = SESSION.get(url, params=params, headers=_hdrs_json(), timeout=30)
    print(f"[summary] {r.status_code} -> {r.url}")
    r.raise_for_status()
    j = r.json()
    if not isinstance(j, dict) or "header" not in j:
        raise RuntimeError("Unexpected summary JSON")
    return j

def team_maps_from_summary(summary: Dict[str, Any]) -> Tuple[Dict[str, str], List[str]]:
    """
    Returns:
      abbr->name map (e.g. 'SCO'->'Scotland', 'ARG'->'Argentina')
      home-away order as a list of abbrs [home_abbr, away_abbr]
    """
    header = summary.get("header", {})
    comps = (header.get("competitions") or [{}])
    comp = comps[0] if comps else {}
    competitors = comp.get("competitors") or []

    abbr_to_name: Dict[str, str] = {}
    order: List[str] = []

    for ha in ("home", "away"):
        c = next((x for x in competitors if x.get("homeAway") == ha), None)
        if not c:
            continue
        t = (c.get("team") or {})
        abbr = t.get("abbreviation") or ""
        name = t.get("displayName") or t.get("name") or abbr
        if abbr:
            abbr_to_name[abbr] = name
            order.append(abbr)
    return abbr_to_name, order

def fetch_lineups_text(league_id: int, event_id: str) -> str:
    """
    Fetch the public lineups HTML and return plain text (tags stripped).
    """
    url = f"https://www.espn.com/rugby/lineups/_/gameId/{event_id}/league/{league_id}"
    r = SESSION.get(url, headers=_hdrs_html(), timeout=30)
    print(f"[lineups_html] {r.status_code} -> {r.url}")
    r.raise_for_status()

    html = r.text
    # Remove script/style to reduce noise
    html = re.sub(r"(?is)<script.*?>.*?</script>", "", html)
    html = re.sub(r"(?is)<style.*?>.*?</style>", "", html)
    # Insert newlines at logical points
    html = re.sub(r"(?i)</(li|p|div|br|tr|h\d|section)>", "\n", html)
    # Strip remaining tags
    text = re.sub(r"<[^>]+>", "", html)
    # Unescape entities and normalize whitespace/glyphs
    text = ihtml.unescape(text)
    text = text.replace("\uE000", "").replace("\uE0DF", "").replace("\uE0A0", "")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\r?\n", "\n", text)
    return text

def parse_lineups_from_text(text: str) -> Dict[str, Dict[str, List[Tuple[str, str, str]]]]:
    """
    Parse the plain text into:
      { 'SCO': { 'starters': [(no,name,pos), ...], 'replacements': [...] },
        'ARG': { ... } }
    We locate blocks headed by "<ABBR> No.Name" and split each by "Replacements".
    """
    result: Dict[str, Dict[str, List[Tuple[str, str, str]]]] = {}

    # Find headers like "SCO No.Name", "ARG No.Name"
    heads = list(re.finditer(r"\b([A-Z]{2,4})\s+No\.Name\b", text))
    for idx, m in enumerate(heads):
        abbr = m.group(1)
        start = m.end()
        end = heads[idx + 1].start() if idx + 1 < len(heads) else len(text)
        chunk = text[start:end]

        # starters up to "Replacements"; rest is bench
        parts = re.split(r"(?i)\bReplacements\b", chunk, maxsplit=1)
        starters_txt = parts[0]
        bench_txt = parts[1] if len(parts) > 1 else ""

        line_re = re.compile(r"^\s*(\d{1,2})\s+([A-Za-zÀ-ÖØ-öø-ÿ'’\-\.\s]+?),\s*([A-Z0-9/]+)\s*$", re.M)
        starters = [(n, nm.strip(), pos) for (n, nm, pos) in line_re.findall(starters_txt)]
        bench = [(n, nm.strip(), pos) for (n, nm, pos) in line_re.findall(bench_txt)]

        if starters or bench:
            result[abbr] = {"starters": starters, "replacements": bench}

    return result

def print_lineups_for_event(league_id: int, event_id: str) -> None:
    summary = fetch_summary(league_id, event_id)
    abbr_to_name, order = team_maps_from_summary(summary)
    text = fetch_lineups_text(league_id, event_id)
    parsed = parse_lineups_from_text(text)

    print("\n" + "=" * 80)
    print(f"Lineups — event {event_id} (league {league_id})")
    print("=" * 80)

    if not parsed:
        print("(No lineup rows found on ESPN page. For some fixtures ESPN doesn’t publish lineups.)")
        return

    # Print in Home/Away order where possible
    keys = order + [k for k in parsed.keys() if k not in order]
    printed = set()
    for abbr in keys:
        if abbr in printed or abbr not in parsed:
            continue
        team_name = abbr_to_name.get(abbr, abbr)
        blk = parsed[abbr]
        print(f"\n{team_name} ({abbr})")
        print("-" * 80)
        if blk["starters"]:
            print("Starters (XV):")
            for (no, nm, pos) in blk["starters"]:
                print(f"  - {no:>2}  {nm}  ({pos})")
        else:
            print("Starters: (none)")
        if blk["replacements"]:
            print("\nReplacements:")
            for (no, nm, pos) in blk["replacements"]:
                print(f"  - {no:>2}  {nm}  ({pos})")
        else:
            print("\nReplacements: (none)")
        printed.add(abbr)

def main():
    ap = argparse.ArgumentParser(description="Print rugby lineups (Starting XV + Replacements) from ESPN HTML (no DB)")
    ap.add_argument("--league-id", "-l", type=int, default=289234, help="Competition id (default: 289234 Internationals)")
    ap.add_argument("--event-id", "-e", required=True, help="ESPN event id (e.g., 602480)")
    args = ap.parse_args()
    try:
        print_lineups_for_event(args.league_id, args.event_id)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
