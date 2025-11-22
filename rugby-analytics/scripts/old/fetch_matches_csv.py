#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""
fetch_matches_csv.py
--------------------

Pull season fixtures (past + upcoming) for a rugby league from TheSportsDB
and write clean CSV snapshots to ./data.

Designed for:
- League: United Rugby Championship (URC), id=4446 (default, override with --league-id)
- Premium V1 key stored in .env as THESPORTSDB_API_KEY=752396

Modes:
- Single season:
    * default: auto-detect current season using league.strCurrentSeason
    * or: --season "2024-2025"

- Multi-season:
    * --last-n-seasons N      → fetch the N most recent seasons from the seasons list
    * --all-seasons           → fetch ALL seasons from the seasons list
    * --from-current-back N   → ignore the seasons list and fetch N seasons
                                going backwards from league.strCurrentSeason
                                (e.g. 5 → 2025-2026, 2024-2025, 2023-2024, ...)

This script DOES NOT touch your DB. It only writes CSVs into ./data.

Usage examples
--------------
# Current URC season only (via strCurrentSeason)
python .\scripts\fetch_matches_csv.py --write-csv -v

# Explicit season
python .\scripts\fetch_matches_csv.py --season "2024-2025" --write-csv -v

# Last 3 seasons based on seasons list
python .\scripts\fetch_matches_csv.py --last-n-seasons 3 --write-csv -v

# ALL seasons from seasons list
python .\scripts\fetch_matches_csv.py --all-seasons --write-csv -v

# MODERN: current season + previous 4 (5 total), ignoring seasons list
python .\scripts\fetch_matches_csv.py --from-current-back 5 --write-csv -v

Notes
-----
- We normalize match status to your enum: 'scheduled', 'in_progress', 'final', 'postponed', 'cancelled'
- kickoff_utc is derived from strTimestamp when present; otherwise from dateEvent+strTime (best effort).
"""

import csv
import os
import sys
import time
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

# --- HTTP --------------------------------------------------------------------
try:
    import requests
except ImportError:
    print("Missing dependency: requests (pip install requests)", file=sys.stderr)
    sys.exit(1)


# --- Env helpers -------------------------------------------------------------
def _load_dotenv_if_available() -> None:
    """Load .env if python-dotenv is installed."""
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        # optional dependency; ignore if missing
        pass


def _tsdb_base(api_key: str) -> str:
    """Base URL for V1 API using the given key."""
    return f"https://www.thesportsdb.com/api/v1/json/{api_key}"


def _session_with_retries() -> requests.Session:
    """Create a requests.Session; we manage retries manually."""
    s = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=0)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _get_with_backoff(
    session: requests.Session,
    url: str,
    params: Dict[str, Any],
    max_retries: int = 4,
    verbose: bool = False,
) -> requests.Response:
    """GET with simple exponential backoff on 429."""
    delay = 0.8
    for attempt in range(1, max_retries + 1):
        resp = session.get(url, params=params, timeout=45)
        if resp.status_code == 429:
            if verbose:
                print(
                    f"[WARN] 429 Too Many Requests (attempt {attempt}/{max_retries}); "
                    f"sleeping {delay:.1f}s…",
                    file=sys.stderr,
                )
            time.sleep(delay)
            delay *= 1.75
            continue
        resp.raise_for_status()
        return resp
    # last try, raise if still failing
    resp.raise_for_status()
    return resp


# --- Small utils -------------------------------------------------------------
def _slugify(name: str) -> str:
    """Make a filesystem-safe slug from a league name."""
    out: List[str] = []
    prev_dash = False
    for ch in name.lower():
        if ch.isalnum():
            out.append(ch)
            prev_dash = False
        else:
            if not prev_dash:
                out.append("-")
                prev_dash = True
    slug = "".join(out).strip("-")
    return slug or "league"


def _parse_ts(ts: Optional[str]) -> Optional[datetime]:
    """
    Parse TheSportsDB strTimestamp, e.g.
      '2025-11-12 19:35:00+00:00' or '2025-11-12 19:35:00'
    Return an aware datetime in UTC if possible.
    """
    if not ts:
        return None
    s = ts.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        try:
            # 'YYYY-MM-DD HH:MM:SS'
            dt = datetime.strptime(s.split("+")[0], "%Y-%m-%d %H:%M:%S")
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None


def _combine_date_time(date_str: Optional[str], time_str: Optional[str]) -> Optional[datetime]:
    """
    Combine dateEvent + strTime (no offset typically); assume UTC.
    """
    if not date_str:
        return None
    ds = date_str.strip()
    ts = (time_str or "00:00:00").strip()
    try:
        dt = datetime.strptime(f"{ds} {ts}", "%Y-%m-%d %H:%M:%S")
    except ValueError:
        # Sometimes strTime is '20:35:00+00:00' or '20:35'
        try:
            dt = datetime.strptime(f"{ds} {ts[:5]}", "%Y-%m-%d %H:%M")
        except Exception:
            return None
    return dt.replace(tzinfo=timezone.utc)


def _map_status(raw_status: Optional[str]) -> str:
    """
    Map TheSportsDB status/progress codes to your enum:
      'scheduled', 'in_progress', 'final', 'postponed', 'cancelled'
    """
    if not raw_status:
        return "scheduled"
    s = raw_status.strip().upper()
    if s in {"NS", "TBD", "PST"}:
        return "scheduled"
    if any(code in s for code in ("1H", "HT", "2H", "ET", "BT", "PT", "LIVE", "INPLAY")):
        return "in_progress"
    if s in {"FT", "AET", "AW", "FINISHED", "COMPLETE", "COMPLETED"}:
        return "final"
    if s in {"POST", "PPD"}:
        return "postponed"
    if s in {"CANC", "ABD", "INTR", "SUSP"}:
        return "cancelled"
    # Default conservative fallback
    return "scheduled"


def _parse_season_year(str_season: str) -> int:
    """
    Extract base year from TSDB season label, e.g.
      '2011-2012' -> 2011
      '2012'      -> 2012
    Return 0 if no year can be parsed.
    """
    if not str_season:
        return 0
    s = str_season.strip()
    try:
        return int(s[:4])
    except Exception:
        return 0


def _previous_season_label(label: str) -> str:
    """
    Given a season label like '2025-2026', '2024/2025' or '2013',
    return the previous season label:
      - '2025-2026' -> '2024-2025'
      - '2013'      -> '2012'
    """
    s = (label or "").strip()
    if len(s) >= 9 and s[4] in "-/":
        # Treat anything like YYYY?YYYY as a range; regenerate with '-'
        try:
            start_year = int(s[:4])
            prev_start = start_year - 1
            prev_end = prev_start + 1
            return f"{prev_start}-{prev_end}"
        except Exception:
            pass
    # Fallback: treat first 4 chars as a year
    try:
        year = int(s[:4])
        return str(year - 1)
    except Exception:
        # give up, return original
        return s


# --- TheSportsDB calls -------------------------------------------------------
def _lookup_league(
    session: requests.Session,
    api_key: str,
    league_id: str,
    verbose: bool = False,
) -> Dict[str, Any]:
    """V1 lookupleague.php"""
    url = f"{_tsdb_base(api_key)}/lookupleague.php"
    resp = _get_with_backoff(session, url, {"id": league_id}, verbose=verbose)
    data = resp.json() or {}
    leagues = data.get("leagues") or []
    return leagues[0] if leagues else {}


def _search_all_seasons(
    session: requests.Session,
    api_key: str,
    league_id: str,
    verbose: bool = False,
) -> List[str]:
    """V1 search_all_seasons.php (often incomplete for modern URC)."""
    url = f"{_tsdb_base(api_key)}/search_all_seasons.php"
    resp = _get_with_backoff(session, url, {"id": league_id}, verbose=verbose)
    data = resp.json() or {}
    seasons = data.get("seasons") or []
    out: List[str] = []
    for s in seasons:
        lab = (s.get("strSeason") or "").strip()
        if lab:
            out.append(lab)
    return out


def _events_for_season(
    session: requests.Session,
    api_key: str,
    league_id: str,
    season: str,
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    """V1 eventsseason.php for the given league+season."""
    url = f"{_tsdb_base(api_key)}/eventsseason.php"
    resp = _get_with_backoff(session, url, {"id": league_id, "s": season}, verbose=verbose)
    data = resp.json() or {}
    events = data.get("events") or []
    # Filter to rugby sport only (safety)
    rugby: List[Dict[str, Any]] = []
    for e in events:
        sport = (e.get("strSport") or "").lower()
        if sport.startswith("rugby"):
            rugby.append(e)
    return rugby


# --- CSV writer --------------------------------------------------------------
def _ensure_data_dir() -> str:
    out = os.path.join(os.getcwd(), "data")
    os.makedirs(out, exist_ok=True)
    return out


def _write_matches_csv(
    events: List[Dict[str, Any]],
    league_slug: str,
    season: str,
) -> str:
    out_dir = _ensure_data_dir()
    fname = f"matches_{league_slug}_{season}.csv".replace("/", "-")
    path = os.path.join(out_dir, fname)

    cols = [
        # identity
        "idEvent",
        "strSport",
        "idLeague",
        "strLeague",
        "strSeason",

        # timing
        "dateEvent",
        "strTime",
        "strTimestamp",
        "kickoff_utc",

        # status
        "status_raw",
        "status",

        # teams
        "idHomeTeam",
        "strHomeTeam",
        "idAwayTeam",
        "strAwayTeam",

        # venue + attendance
        "idVenue",
        "strVenue",
        "intAttendance",

        # scores
        "intHomeScore",
        "intAwayScore",

        # misc
        "intRound",
        "strFilename",     # TSDB sometimes exposes a unique slug
        "data_source",
    ]

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for e in events:
            # kickoff
            kickoff = _parse_ts(e.get("strTimestamp")) or _combine_date_time(
                e.get("dateEvent"),
                e.get("strTime"),
            )
            kickoff_utc = kickoff.astimezone(timezone.utc).isoformat() if kickoff else None

            raw_status = (e.get("strStatus") or e.get("strProgress") or "").strip()
            status = _map_status(raw_status)

            row = {
                "idEvent": e.get("idEvent"),
                "strSport": e.get("strSport"),
                "idLeague": e.get("idLeague"),
                "strLeague": e.get("strLeague"),
                "strSeason": e.get("strSeason"),

                "dateEvent": e.get("dateEvent"),
                "strTime": e.get("strTime"),
                "strTimestamp": e.get("strTimestamp"),
                "kickoff_utc": kickoff_utc,

                "status_raw": raw_status,
                "status": status,

                "idHomeTeam": e.get("idHomeTeam"),
                "strHomeTeam": e.get("strHomeTeam"),
                "idAwayTeam": e.get("idAwayTeam"),
                "strAwayTeam": e.get("strAwayTeam"),

                "idVenue": e.get("idVenue"),
                "strVenue": e.get("strVenue"),
                "intAttendance": e.get("intAttendance"),

                "intHomeScore": e.get("intHomeScore"),
                "intAwayScore": e.get("intAwayScore"),

                "intRound": e.get("intRound"),
                "strFilename": e.get("strFilename"),
                "data_source": "thesportsdb",
            }
            w.writerow(row)

    return path


# --- Main --------------------------------------------------------------------
def main() -> None:
    import argparse

    _load_dotenv_if_available()

    parser = argparse.ArgumentParser(
        description="Fetch rugby league season fixtures (past + upcoming) to CSV."
    )
    parser.add_argument(
        "--league-id",
        default="4446",
        help="TheSportsDB idLeague (default: URC = 4446)",
    )
    parser.add_argument(
        "--season",
        default=None,
        help="Explicit season label (e.g., '2024-2025'). "
             "Ignored if --last-n-seasons > 0 or --all-seasons or --from-current-back > 0.",
    )
    parser.add_argument(
        "--last-n-seasons",
        type=int,
        default=0,
        help="If > 0, fetch the N most recent seasons from the seasons list "
             "(each season to its own CSV). Ignored if --all-seasons or --from-current-back > 0.",
    )
    parser.add_argument(
        "--all-seasons",
        action="store_true",
        help="Fetch ALL seasons available for the league (from seasons list). "
             "Overrides --season and --last-n-seasons (ignored if --from-current-back > 0).",
    )
    parser.add_argument(
        "--from-current-back",
        type=int,
        default=0,
        help="If > 0, ignore seasons list and fetch this many seasons going backwards "
             "from league.strCurrentSeason. Example: 5 → current + previous 4 seasons.",
    )
    parser.add_argument(
        "--from-date",
        dest="from_date",
        default=None,
        help="Optional YYYY-MM-DD filter (inclusive) AFTER fetch.",
    )
    parser.add_argument(
        "--to-date",
        dest="to_date",
        default=None,
        help="Optional YYYY-MM-DD filter (inclusive) AFTER fetch.",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("THESPORTSDB_API_KEY", "752396"),
        help="TheSportsDB V1 API key. Default comes from THESPORTSDB_API_KEY in .env, "
             "fallback '752396' (your premium key).",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.5,
        help="Sleep after API calls (default: 0.5)",
    )
    parser.add_argument(
        "--write-csv",
        dest="write_csv",
        action="store_true",
        help="Write CSV snapshot(s) to ./data",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose logging.",
    )
    args = parser.parse_args()

    api_key = args.api_key
    league_id = str(args.league_id)
    explicit_season = args.season
    last_n_seasons = max(args.last_n_seasons, 0)
    all_seasons_flag = args.all_seasons
    from_current_back = max(args.from_current_back, 0)
    from_date = args.from_date
    to_date = args.to_date
    sleep_seconds = max(args.sleep_seconds, 0.0)
    verbose = args.verbose

    # Debug: confirm which key is actually being used
    if verbose:
        shown = api_key if len(api_key) <= 4 else api_key[:2] + "***" + api_key[-2:]
        print(f"[INFO] Using TheSportsDB V1 key: '{shown}'")

    sess = _session_with_retries()

    # 1) Resolve league metadata
    league_meta = _lookup_league(sess, api_key, league_id, verbose=verbose)
    league_name = (league_meta.get("strLeague") or "League").strip() if league_meta else f"league-{league_id}"
    league_slug = _slugify(league_name)
    current_season_label = (league_meta.get("strCurrentSeason") or "").strip()

    if verbose:
        print(f"[INFO] League: {league_name} (id={league_id})")
        print(f"[INFO] strCurrentSeason from league metadata: {current_season_label!r}")

    # 2) Get seasons list for the league (old data / debugging)
    seasons_from_list = _search_all_seasons(sess, api_key, league_id, verbose=verbose)
    if verbose:
        print(f"[INFO] Seasons from search_all_seasons for league {league_id}: {seasons_from_list}")

    if not seasons_from_list and not from_current_back:
        # Guard: for URC we actually have a list; if not AND not using synthetic mode, bail.
        raise SystemExit(f"No seasons found for league id={league_id} via search_all_seasons.")

    # Sort seasons by parsed base year, then label
    all_seasons_sorted = sorted(
        seasons_from_list,
        key=lambda s: (_parse_season_year(s), s),
    )

    seasons_to_fetch: List[str] = []

    # Priority 1: from-current-back (modern synthetic seasons)
    if from_current_back > 0:
        if not current_season_label:
            raise SystemExit(
                "from-current-back requested but league.strCurrentSeason is empty. "
                "You may need to specify --season explicitly."
            )
        label = current_season_label
        for _ in range(from_current_back):
            seasons_to_fetch.append(label)
            label = _previous_season_label(label)
        if verbose:
            print(f"[INFO] Using synthetic seasons from current back: {seasons_to_fetch}")

    # Priority 2: all-seasons uses the list from API
    elif all_seasons_flag:
        seasons_to_fetch = all_seasons_sorted
        if verbose:
            print(f"[INFO] Fetching ALL seasons from seasons list: {seasons_to_fetch}")

    # Priority 3: last-n-seasons based on seasons list
    elif last_n_seasons > 0:
        if last_n_seasons >= len(all_seasons_sorted):
            seasons_to_fetch = all_seasons_sorted
        else:
            seasons_to_fetch = all_seasons_sorted[-last_n_seasons:]
        if verbose:
            print(f"[INFO] Fetching last {last_n_seasons} seasons from seasons list: {seasons_to_fetch}")

    # Priority 4: single-season mode
    else:
        if explicit_season:
            seasons_to_fetch = [explicit_season]
        else:
            # try current_season_label first
            if current_season_label:
                seasons_to_fetch = [current_season_label]
                if verbose:
                    print(f"[INFO] Using league.strCurrentSeason: {current_season_label}")
            else:
                latest = all_seasons_sorted[-1]
                seasons_to_fetch = [latest]
                if verbose:
                    print(f"[INFO] Fallback to latest season from list: {latest}")

    # 3) Helper for date filter
    def _in_window(ev: Dict[str, Any]) -> bool:
        d = ev.get("dateEvent")
        if not d:
            return True
        if from_date and d < from_date:
            return False
        if to_date and d > to_date:
            return False
        return True

    # 4) Fetch events for each season and write CSV
    any_written = False
    for idx, season in enumerate(seasons_to_fetch, start=1):
        if verbose:
            print(f"[INFO] ({idx}/{len(seasons_to_fetch)}) Fetching events for season={season!r}")

        events = _events_for_season(sess, api_key, league_id, season, verbose=verbose)
        if verbose:
            print(f"[INFO]  -> retrieved {len(events)} rugby events for season={season}")

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

        if from_date or to_date:
            before = len(events)
            events = [e for e in events if _in_window(e)]
            if verbose:
                print(
                    f"[INFO]  -> date filter from={from_date} to={to_date} "
                    f"→ {len(events)}/{before} events for season={season}"
                )

        if not events:
            if verbose:
                print(f"[WARN]  -> no events to write for season={season} after filtering")
            continue

        if args.write_csv:
            out_path = _write_matches_csv(events, league_slug, season)
            any_written = True
            print(f"[OK] Wrote CSV for season={season}: {out_path}")
        else:
            # If not writing CSV, show a small preview
            preview = min(5, len(events))
            print(f"[INFO] Preview for season={season} ({preview} events):")
            for e in events[:preview]:
                print(
                    f"  {e.get('dateEvent')} {e.get('strTime')}  "
                    f"{e.get('strHomeTeam')} vs {e.get('strAwayTeam')}  "
                    f"status={e.get('strStatus') or e.get('strProgress')}"
                )

    if args.write_csv and not any_written:
        raise SystemExit("No events were written for any selected season.")


if __name__ == "__main__":
    main()
