#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""
ingest_urc_matches_to_db.py
---------------------------

Fetch multiple seasons of URC (or any rugby league) matches from TheSportsDB
and ingest into your Postgres `matches` table.

Key points
==========
- Uses V1 API with your premium key from .env:
    THESPORTSDB_API_KEY=752396

- Resolves seasons by:
    --from-current-back N  (default: 10)
    → current season from league.strCurrentSeason
      then previous seasons by subtracting 1 year, e.g.
      2025-2026, 2024-2025, 2023-2024, ...

- For each event (match) it:
    * Maps TSDB league + season to DB league_id + season_id via:
        leagues.tsdb_league_id
        seasons.tsdb_season_key
    * Maps TSDB team IDs to DB teams via:
        teams.tsdb_team_id
    * Maps TSDB venue ID to DB venues via:
        venues.tsdb_venue_id  (OPTIONAL – if your schema doesn't have it, we skip venue mapping)
    * Inserts/updates `matches` using a natural key:
        (league_id, season_id, home_team_id, away_team_id, kickoff_utc)

    * Fills:
        matches.tsdb_event_id  ← TSDB idEvent
        matches.round_label    ← strRound or intRound
        matches.stage          ← strStage (when present)

DB connection
-------------
- Preferred: use your existing helper in rugby-analytics/db:
    from db.connection import get_db_connection
    conn = get_db_connection()

- Fallback: use DATABASE_URL env var:
    export DATABASE_URL=postgres://user:pass@host:port/dbname

Usage
-----
# Ingest last 10 seasons (default) for URC (4446)
python scripts/ingest_urc_matches_to_db.py -v

# Ingest last 5 seasons
python scripts/ingest_urc_matches_to_db.py --from-current-back 5 -v

# Ingest last 10 seasons for another league
python scripts/ingest_urc_matches_to_db.py --league-id 4550 --from-current-back 10 -v

# Also write CSVs as snapshots to ./data
python scripts/ingest_urc_matches_to_db.py --from-current-back 10 --write-csv -v
"""

import os
import sys
import time
import csv
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# --- HTTP --------------------------------------------------------------------
try:
    import requests
except ImportError:
    print("Missing dependency: requests (pip install requests)", file=sys.stderr)
    sys.exit(1)

# --- DB ----------------------------------------------------------------------
try:
    import psycopg2
    from psycopg2.extras import DictCursor
except ImportError:
    print("Missing dependency: psycopg2 (pip install psycopg2)", file=sys.stderr)
    sys.exit(1)

# Try to use your existing db connection helper, if present
try:
    from db.connection import get_db_connection  # type: ignore
except Exception:
    get_db_connection = None  # type: ignore


# --- Env helpers -------------------------------------------------------------
def _load_dotenv_if_available() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        pass


def _tsdb_base(api_key: str) -> str:
    return f"https://www.thesportsdb.com/api/v1/json/{api_key}"


def _session_with_retries() -> requests.Session:
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
    """
    GET with simple exponential backoff.

    Retries on:
      - 429 Too Many Requests
      - 500 / 502 / 503 / 504 server-side errors
    """
    delay = 0.8
    retry_statuses = {429, 500, 502, 503, 504}

    for attempt in range(1, max_retries + 1):
        resp = session.get(url, params=params, timeout=45)

        if resp.status_code in retry_statuses:
            if attempt == max_retries:
                # Last attempt, break and let raise_for_status below handle it
                break

            if verbose:
                print(
                    f"[WARN] HTTP {resp.status_code} on {url} "
                    f"(attempt {attempt}/{max_retries}); sleeping {delay:.1f}s…",
                    file=sys.stderr,
                )
            time.sleep(delay)
            delay *= 1.75
            continue

        # Success or non-retryable error
        resp.raise_for_status()
        return resp

    # If we get here, we exhausted retries or had a non-retryable status
    resp.raise_for_status()
    return resp


# --- Small utils -------------------------------------------------------------
def _slugify(name: str) -> str:
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
            dt = datetime.strptime(s.split("+")[0], "%Y-%m-%d %H:%M:%S")
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None


def _combine_date_time(date_str: Optional[str], time_str: Optional[str]) -> Optional[datetime]:
    if not date_str:
        return None
    ds = date_str.strip()
    ts = (time_str or "00:00:00").strip()
    try:
        dt = datetime.strptime(f"{ds} {ts}", "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            dt = datetime.strptime(f"{ds} {ts[:5]}", "%Y-%m-%d %H:%M")
        except Exception:
            return None
    return dt.replace(tzinfo=timezone.utc)


def _map_status(raw_status: Optional[str]) -> str:
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
    return "scheduled"


def _previous_season_label(label: str) -> str:
    s = (label or "").strip()
    if len(s) >= 9 and s[4] in "-/":
        try:
            start_year = int(s[:4])
            prev_start = start_year - 1
            prev_end = prev_start + 1
            return f"{prev_start}-{prev_end}"
        except Exception:
            pass
    try:
        year = int(s[:4])
        return str(year - 1)
    except Exception:
        return s


# --- TheSportsDB calls -------------------------------------------------------
def _lookup_league(
    session: requests.Session,
    api_key: str,
    league_id: str,
    verbose: bool = False,
) -> Dict[str, Any]:
    url = f"{_tsdb_base(api_key)}/lookupleague.php"
    resp = _get_with_backoff(session, url, {"id": league_id}, verbose=verbose)
    data = resp.json() or {}
    leagues = data.get("leagues") or []
    return leagues[0] if leagues else {}


def _events_for_season(
    session: requests.Session,
    api_key: str,
    league_id: str,
    season: str,
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    url = f"{_tsdb_base(api_key)}/eventsseason.php"
    resp = _get_with_backoff(session, url, {"id": league_id, "s": season}, verbose=verbose)
    data = resp.json() or {}
    events = data.get("events") or []
    rugby: List[Dict[str, Any]] = []
    for e in events:
        sport = (e.get("strSport") or "").lower()
        if sport.startswith("rugby"):
            rugby.append(e)
    return rugby


# --- CSV writer (optional) ---------------------------------------------------
def _ensure_data_dir() -> str:
    out = os.path.join(os.getcwd(), "data")
    os.makedirs(out, exist_ok=True)
    return out


def _write_matches_csv(events: List[Dict[str, Any]], league_slug: str, season: str) -> str:
    out_dir = _ensure_data_dir()
    fname = f"matches_{league_slug}_{season}.csv".replace("/", "-")
    path = os.path.join(out_dir, fname)

    cols = [
        "idEvent",
        "strSport",
        "idLeague",
        "strLeague",
        "strSeason",
        "dateEvent",
        "strTime",
        "strTimestamp",
        "kickoff_utc",
        "status_raw",
        "status",
        "idHomeTeam",
        "strHomeTeam",
        "idAwayTeam",
        "strAwayTeam",
        "idVenue",
        "strVenue",
        "intAttendance",
        "intHomeScore",
        "intAwayScore",
        "intRound",
        "strRound",
        "strStage",
        "strFilename",
        "data_source",
    ]

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for e in events:
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
                "strRound": e.get("strRound"),
                "strStage": e.get("strStage"),
                "strFilename": e.get("strFilename"),
                "data_source": "thesportsdb",
            }
            w.writerow(row)
    return path


# --- DB helpers --------------------------------------------------------------
def _get_conn():
    if get_db_connection is not None:
        return get_db_connection()  # type: ignore
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("No DATABASE_URL and no db.connection.get_db_connection().")
    return psycopg2.connect(dsn)


def _ensure_matches_tsdb_column(cur, verbose: bool = False) -> None:
    """
    Ensure matches.tsdb_event_id exists and is indexed uniquely.
    """
    cur.execute(
        """
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public'
          AND table_name='matches'
          AND column_name='tsdb_event_id'
        LIMIT 1
        """
    )
    if not cur.fetchone():
        if verbose:
            print("[INFO] Adding matches.tsdb_event_id (TEXT)")
        cur.execute("ALTER TABLE matches ADD COLUMN tsdb_event_id TEXT;")

    cur.execute(
        """
        SELECT 1
        FROM pg_indexes
        WHERE schemaname='public'
          AND tablename='matches'
          AND indexname='uniq_matches_tsdb_event_id'
        LIMIT 1
        """
    )
    if not cur.fetchone():
        if verbose:
            print("[INFO] Creating unique index on matches.tsdb_event_id")
        cur.execute(
            "CREATE UNIQUE INDEX uniq_matches_tsdb_event_id ON matches(tsdb_event_id);"
        )


def _lookup_league_id(cur, tsdb_league_id: str, cache: Dict[str, int]) -> int:
    if tsdb_league_id in cache:
        return cache[tsdb_league_id]
    cur.execute(
        "SELECT league_id FROM leagues WHERE tsdb_league_id = %s",
        (tsdb_league_id,),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"No league found with tsdb_league_id={tsdb_league_id}")
    league_id = row[0]
    cache[tsdb_league_id] = league_id
    return league_id


def _ensure_league_in_db(
    cur,
    tsdb_league_id: str,
    league_meta: Dict[str, Any],
    verbose: bool = False,
) -> None:
    name = (league_meta.get("strLeague") or "").strip()
    alt = (league_meta.get("strLeagueAlternate") or "").strip()
    country = (league_meta.get("strCountry") or "").strip() or None
    if not name:
        raise RuntimeError("Cannot ensure league: missing strLeague")

    short_name = alt or name
    slug = _slugify(name)

    if verbose:
        print(
            f"[INFO] _ensure_league_in_db: name='{name}', short_name='{short_name}', "
            f"country='{country}', tsdb_league_id={tsdb_league_id}"
        )

    cur.execute(
        """
        SELECT league_id
        FROM leagues
        WHERE LOWER(name) = LOWER(%s)
           OR LOWER(short_name) = LOWER(%s)
        LIMIT 1
        """,
        (name, short_name),
    )
    row = cur.fetchone()
    if row:
        league_id = row[0]
        if verbose:
            print(f"[INFO] Found existing league_id={league_id}; updating tsdb_league_id")
        cur.execute(
            "UPDATE leagues SET tsdb_league_id=%s, updated_at=NOW() WHERE league_id=%s",
            (tsdb_league_id, league_id),
        )
        return

    cur.execute(
        """
        SELECT sport_id
        FROM sports
        WHERE code='rugby_union' OR LOWER(name) LIKE 'rugby%%'
        ORDER BY code='rugby_union' DESC, name ASC
        LIMIT 1
        """
    )
    sport_row = cur.fetchone()
    if not sport_row:
        raise RuntimeError("No rugby sport row found in sports.")
    sport_id = sport_row[0]

    if verbose:
        print(f"[INFO] Inserting new league row for tsdb_league_id={tsdb_league_id}")

    cur.execute(
        """
        INSERT INTO leagues (
            name, short_name, slug,
            espn_league_id, country_code,
            sport_id, tsdb_league_id,
            created_at, updated_at
        ) VALUES (
            %s, %s, %s,
            NULL, %s,
            %s, %s,
            NOW(), NOW()
        )
        """,
        (name, short_name, slug, country, sport_id, tsdb_league_id),
    )


def _ensure_or_get_season_id(
    cur,
    league_id: int,
    tsdb_season_key: str,
    cache: Dict[Tuple[int, str], int],
    verbose: bool = False,
) -> int:
    key = (league_id, tsdb_season_key)
    if key in cache:
        return cache[key]

    cur.execute(
        "SELECT season_id FROM seasons WHERE league_id=%s AND tsdb_season_key=%s",
        (league_id, tsdb_season_key),
    )
    row = cur.fetchone()
    if row:
        season_id = row[0]
        cache[key] = season_id
        return season_id

    def _year(lbl: str) -> Optional[int]:
        if not lbl:
            return None
        try:
            return int(lbl[:4])
        except Exception:
            return None

    year = _year(tsdb_season_key)

    if verbose:
        print(f"[INFO] Ensuring season for league_id={league_id}, tsdb_season_key='{tsdb_season_key}', year={year}")

    cur.execute(
        """
        INSERT INTO seasons (
            league_id, year, label,
            start_date, end_date,
            tsdb_season_key,
            created_at, updated_at
        ) VALUES (
            %s, %s, %s,
            NULL, NULL,
            %s,
            NOW(), NOW()
        )
        ON CONFLICT (league_id, year)
        DO UPDATE SET
            label = EXCLUDED.label,
            tsdb_season_key = EXCLUDED.tsdb_season_key,
            updated_at = NOW()
        RETURNING season_id
        """,
        (league_id, year, tsdb_season_key, tsdb_season_key),
    )
    season_id = cur.fetchone()[0]
    cache[key] = season_id
    return season_id


_VENUES_HAS_TSDB_COLUMN: Optional[bool] = None

def _venues_has_tsdb_column(cur) -> bool:
    global _VENUES_HAS_TSDB_COLUMN
    if _VENUES_HAS_TSDB_COLUMN is not None:
        return _VENUES_HAS_TSDB_COLUMN
    cur.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public'
          AND table_name='venues'
          AND column_name='tsdb_venue_id'
        LIMIT 1
    """)
    _VENUES_HAS_TSDB_COLUMN = cur.fetchone() is not None
    return _VENUES_HAS_TSDB_COLUMN


def _lookup_team_id(cur, tsdb_team_id: str, cache: Dict[str, int]) -> Optional[int]:
    if not tsdb_team_id:
        return None
    if tsdb_team_id in cache:
        return cache[tsdb_team_id]
    cur.execute("SELECT team_id FROM teams WHERE tsdb_team_id=%s", (tsdb_team_id,))
    row = cur.fetchone()
    if not row:
        return None
    team_id = row[0]
    cache[tsdb_team_id] = team_id
    return team_id


def _lookup_venue_id(cur, tsdb_venue_id: Optional[str], cache: Dict[str, int]) -> Optional[int]:
    if not tsdb_venue_id:
        return None
    if tsdb_venue_id in cache:
        return cache[tsdb_venue_id]
    if not _venues_has_tsdb_column(cur):
        return None
    cur.execute("SELECT venue_id FROM venues WHERE tsdb_venue_id=%s", (tsdb_venue_id,))
    row = cur.fetchone()
    if not row:
        return None
    venue_id = row[0]
    cache[tsdb_venue_id] = venue_id
    return venue_id


def _upsert_match(
    cur,
    league_id: int,
    season_id: int,
    venue_id: Optional[int],
    home_team_id: int,
    away_team_id: int,
    kickoff_utc: Optional[datetime],
    status: str,
    home_score: Optional[int],
    away_score: Optional[int],
    attendance: Optional[int],
    tsdb_event_id: Optional[str],
    round_label: Optional[str],
    stage: Optional[str],
    verbose: bool = False,
) -> None:
    """
    Upsert based on natural key:
      (league_id, season_id, home_team_id, away_team_id, kickoff_utc)
    Also updates:
      tsdb_event_id, round_label, stage, venue_id, scores, status.
    """
    cur.execute(
        """
        SELECT match_id
        FROM matches
        WHERE league_id = %s
          AND season_id = %s
          AND home_team_id = %s
          AND away_team_id = %s
          AND ((kickoff_utc = %s) OR (kickoff_utc IS NULL AND %s IS NULL))
        """,
        (
            league_id,
            season_id,
            home_team_id,
            away_team_id,
            kickoff_utc,
            kickoff_utc,
        ),
    )
    row = cur.fetchone()
    if row:
        match_id = row[0]
        if verbose:
            print(f"  [DB] update match_id={match_id}")
        cur.execute(
            """
            UPDATE matches
               SET home_team_id = %s,
                   away_team_id = %s,
                   status = %s,
                   kickoff_utc = %s,
                   home_score = %s,
                   away_score = %s,
                   attendance = %s,
                   venue_id = %s,
                   tsdb_event_id = COALESCE(%s, tsdb_event_id),
                   round_label = %s,
                   stage = %s,
                   source = %s,
                   updated_at = NOW()
             WHERE match_id = %s
            """,
            (
                home_team_id,
                away_team_id,
                status,
                kickoff_utc,
                home_score,
                away_score,
                attendance,
                venue_id,
                tsdb_event_id,
                round_label,
                stage,
                "other",   # or 'thesportsdb' if you add it to the enum
                match_id,
            ),
        )
    else:
        if verbose:
            print("  [DB] insert new match")
        cur.execute(
            """
            INSERT INTO matches (
                league_id,
                season_id,
                venue_id,
                home_team_id,
                away_team_id,
                status,
                kickoff_utc,
                home_score,
                away_score,
                attendance,
                tsdb_event_id,
                round_label,
                stage,
                source,
                created_at,
                updated_at
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                NOW(), NOW()
            )
            """,
            (
                league_id,
                season_id,
                venue_id,
                home_team_id,
                away_team_id,
                status,
                kickoff_utc,
                home_score,
                away_score,
                attendance,
                tsdb_event_id,
                round_label,
                stage,
                "other",
            ),
        )


# --- Main --------------------------------------------------------------------
def main() -> None:
    import argparse

    _load_dotenv_if_available()

    parser = argparse.ArgumentParser(
        description="Ingest rugby league matches from TheSportsDB into Postgres."
    )
    parser.add_argument(
        "--league-id",
        default="4446",
        help="TheSportsDB idLeague (default: URC = 4446)",
    )
    parser.add_argument(
        "--from-current-back",
        type=int,
        default=10,
        help="How many seasons backwards from league.strCurrentSeason.",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("THESPORTSDB_API_KEY", "752396"),
        help="TheSportsDB V1 API key.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.5,
        help="Sleep between seasons.",
    )
    parser.add_argument(
        "--write-csv",
        action="store_true",
        help="Also write CSV snapshots to ./data.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose logging.",
    )
    args = parser.parse_args()

    api_key = args.api_key
    league_id_tsdb = str(args.league_id)
    from_current_back = max(args.from_current_back, 1)
    sleep_seconds = max(args.sleep_seconds, 0.0)
    verbose = args.verbose

    if verbose:
        shown = api_key if len(api_key) <= 4 else api_key[:2] + "***" + api_key[-2:]
        print(f"[INFO] Using TheSportsDB V1 key: '{shown}'")
        print(f"[INFO] Target TSDB league_id={league_id_tsdb}, seasons back={from_current_back}")

    sess = _session_with_retries()

    league_meta = _lookup_league(sess, api_key, league_id_tsdb, verbose=verbose)
    league_name = (league_meta.get("strLeague") or f"league-{league_id_tsdb}").strip()
    league_slug = _slugify(league_name)
    current_season_label = (league_meta.get("strCurrentSeason") or "").strip()

    if verbose:
        print(f"[INFO] League: {league_name} (TSDB id={league_id_tsdb})")
        print(f"[INFO] strCurrentSeason: {current_season_label!r}")

    if not current_season_label:
        raise SystemExit("League metadata does not include strCurrentSeason.")

    seasons_to_fetch: List[str] = []
    label = current_season_label
    for _ in range(from_current_back):
        seasons_to_fetch.append(label)
        label = _previous_season_label(label)

    if verbose:
        print(f"[INFO] Seasons to fetch (from current back): {seasons_to_fetch}")

    conn = _get_conn()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=DictCursor)

    league_id_cache: Dict[str, int] = {}
    season_id_cache: Dict[Tuple[int, str], int] = {}
    team_id_cache: Dict[str, int] = {}
    venue_id_cache: Dict[str, int] = {}

    try:
        _ensure_matches_tsdb_column(cur, verbose=verbose)

        try:
            league_id_db = _lookup_league_id(cur, league_id_tsdb, league_id_cache)
        except RuntimeError:
            if verbose:
                print("[INFO] No league row with this tsdb_league_id, creating/attaching...")
            _ensure_league_in_db(cur, league_id_tsdb, league_meta, verbose=verbose)
            conn.commit()
            league_id_db = _lookup_league_id(cur, league_id_tsdb, league_id_cache)

        total_inserted = 0
        total_updated = 0
        total_skipped = 0

        for idx, season in enumerate(seasons_to_fetch, start=1):
            if verbose:
                print(f"[INFO] ({idx}/{len(seasons_to_fetch)}) Fetching season='{season}'")

            events = _events_for_season(sess, api_key, league_id_tsdb, season, verbose=verbose)
            if verbose:
                print(f"[INFO]  -> retrieved {len(events)} rugby events for season={season}")

            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

            if not events:
                if verbose:
                    print(f"[WARN]  -> no events for season={season}, skipping")
                continue

            if args.write_csv:
                path = _write_matches_csv(events, league_slug, season)
                print(f"[OK] Wrote CSV for season={season}: {path}")

            season_id_db = _ensure_or_get_season_id(
                cur, league_id_db, season, season_id_cache, verbose=verbose
            )

            for e in events:
                tsdb_event_id = (e.get("idEvent") or "").strip() or None

                id_home_tsdb = (e.get("idHomeTeam") or "").strip()
                id_away_tsdb = (e.get("idAwayTeam") or "").strip()
                id_venue_tsdb = (e.get("idVenue") or "").strip() or None

                home_team_id = _lookup_team_id(cur, id_home_tsdb, team_id_cache)
                away_team_id = _lookup_team_id(cur, id_away_tsdb, team_id_cache)

                if home_team_id is None or away_team_id is None:
                    if verbose:
                        print(
                            f"  [SKIP] Missing team(s) for idEvent={tsdb_event_id}: "
                            f"home_tsdb={id_home_tsdb}, away_tsdb={id_away_tsdb}"
                        )
                    total_skipped += 1
                    continue

                venue_id = _lookup_venue_id(cur, id_venue_tsdb, venue_id_cache)

                kickoff = _parse_ts(e.get("strTimestamp")) or _combine_date_time(
                    e.get("dateEvent"),
                    e.get("strTime"),
                )
                raw_status = (e.get("strStatus") or e.get("strProgress") or "").strip()
                status = _map_status(raw_status)

                def _to_int(v):
                    try:
                        return int(v) if v not in (None, "", "null") else None
                    except Exception:
                        return None

                home_score = _to_int(e.get("intHomeScore"))
                away_score = _to_int(e.get("intAwayScore"))
                attendance = _to_int(e.get("intAttendance"))

                round_label = (e.get("strRound") or "").strip() or None
                if not round_label:
                    ir = e.get("intRound")
                    round_label = str(ir) if ir not in (None, "", "null") else None

                stage = (e.get("strStage") or "").strip() or None

                cur.execute(
                    """
                    SELECT match_id
                    FROM matches
                    WHERE league_id = %s
                      AND season_id = %s
                      AND home_team_id = %s
                      AND away_team_id = %s
                      AND ((kickoff_utc = %s) OR (kickoff_utc IS NULL AND %s IS NULL))
                    """,
                    (
                        league_id_db,
                        season_id_db,
                        home_team_id,
                        away_team_id,
                        kickoff,
                        kickoff,
                    ),
                )
                row = cur.fetchone()
                existed = row is not None

                _upsert_match(
                    cur,
                    league_id_db,
                    season_id_db,
                    venue_id,
                    home_team_id,
                    away_team_id,
                    kickoff,
                    status,
                    home_score,
                    away_score,
                    attendance,
                    tsdb_event_id,
                    round_label,
                    stage,
                    verbose=verbose,
                )

                if existed:
                    total_updated += 1
                else:
                    total_inserted += 1

            conn.commit()
            if verbose:
                print(f"[INFO]  -> committed season={season}")

        print(
            f"[DONE] Ingestion complete. Inserted={total_inserted}, "
            f"Updated={total_updated}, Skipped={total_skipped}"
        )

    except Exception as exc:
        conn.rollback()
        print(f"[ERROR] Ingestion failed, rolled back transaction: {exc}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
