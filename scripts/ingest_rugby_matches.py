#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""
ingest_rugby_matches.py
-----------------------

Ingest rugby matches from TheSportsDB for *all rugby leagues* in your DB that
have a tsdb_league_id, using the seasons already stored in `seasons`.

Assumed schema fragments (matches-related):

  leagues (
      league_id      BIGSERIAL PRIMARY KEY,
      name           TEXT,
      short_name     TEXT,
      slug           TEXT,
      country_code   CHAR(2),
      sport_id       BIGINT,
      tsdb_league_id TEXT UNIQUE,
      created_at     TIMESTAMPTZ,
      updated_at     TIMESTAMPTZ
  )

  seasons (
      season_id       BIGSERIAL PRIMARY KEY,
      league_id       BIGINT REFERENCES leagues(league_id) ON DELETE CASCADE,
      year            INTEGER,
      label           TEXT,
      start_date      DATE,
      end_date        DATE,
      tsdb_season_key TEXT,
      created_at      TIMESTAMPTZ,
      updated_at      TIMESTAMPTZ,
      UNIQUE (league_id, year)
  )

  teams (
      team_id       BIGSERIAL PRIMARY KEY,
      name          TEXT,
      short_name    TEXT,
      abbreviation  TEXT,
      country       TEXT,
      tsdb_team_id  TEXT UNIQUE,
      created_at    TIMESTAMPTZ,
      updated_at    TIMESTAMPTZ
  )

  venues (
      venue_id      BIGSERIAL PRIMARY KEY,
      name          TEXT,
      city          TEXT,
      country       TEXT,
      latitude      DOUBLE PRECISION,
      longitude     DOUBLE PRECISION,
      tsdb_venue_id TEXT UNIQUE,
      created_at    TIMESTAMPTZ,
      updated_at    TIMESTAMPTZ
  )

  matches (
      match_id       BIGSERIAL PRIMARY KEY,
      league_id      BIGINT REFERENCES leagues(league_id) ON DELETE CASCADE,
      season_id      BIGINT REFERENCES seasons(season_id) ON DELETE CASCADE,
      venue_id       BIGINT REFERENCES venues(venue_id),
      home_team_id   BIGINT REFERENCES teams(team_id),
      away_team_id   BIGINT REFERENCES teams(team_id),
      status         TEXT,        -- match_status enum
      kickoff_utc    TIMESTAMPTZ,
      home_score     INTEGER,
      away_score     INTEGER,
      attendance     INTEGER,
      tsdb_event_id  TEXT UNIQUE, -- TheSportsDB idEvent
      source         TEXT,        -- data_source enum; we use 'thesportsdb'
      created_at     TIMESTAMPTZ,
      updated_at     TIMESTAMPTZ
  )

This script:

- Uses your shared `scr.ingest.tsdb_client` module (no `src`).
- Reads all rugby leagues with a tsdb_league_id (or just one via --only-tsdb-league).
- For each league, reads seasons from the `seasons` table (optionally limiting via --limit-seasons-back).
- For each (league, season), fetches events via tsdb_client.get_events_for_season_rugby()
  with *additional* season-level 429 handling and sleeps.
- For each event:
    * Resolves home/away teams via teams.tsdb_team_id
    * Resolves venue via venues.tsdb_venue_id (if present)
    * Maps TSDB status to your match_status enum
    * Parses kickoff datetime in UTC
    * Upserts into `matches` using tsdb_event_id as the natural key.

Usage (from C:\rugby-analytics):

  # Ingest ALL rugby leagues & ALL seasons (can be a lot of API calls)
  python .\scripts\ingest_rugby_matches.py --write-csv -v

  # Ingest only URC (TSDB league 4446), all seasons in DB
  python .\scripts\ingest_rugby_matches.py --only-tsdb-league 4446 --write-csv -v

  # Ingest only last 5 seasons per league
  python .\scripts\ingest_rugby_matches.py --limit-seasons-back 5 --write-csv -v

  # Be gentle with TSDB (bigger sleeps & more retries for 429s)
  python .\scripts\ingest_rugby_matches.py ^
    --only-tsdb-league 4446 ^
    --limit-seasons-back 10 ^
    --season-max-retries 6 ^
    --season-base-sleep 8 ^
    --write-csv -v
"""

import os
import sys
import csv
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from requests.exceptions import HTTPError

# ---------------------------------------------------------------------------
# Ensure project ROOT on sys.path so scr.* and db.* are importable
# ---------------------------------------------------------------------------
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# ---------------------------------------------------------------------------
# TSDB client (YOUR actual module: scr.ingest.tsdb_client)
# ---------------------------------------------------------------------------
try:
    from scr.ingest.tsdb_client import (
        get_events_for_season_rugby,
    )
except Exception as exc:
    print(f"[IMPORT ERROR] Cannot import from scr.ingest.tsdb_client: {exc!r}", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# DB imports
# ---------------------------------------------------------------------------
try:
    import psycopg2
    from psycopg2.extras import DictCursor
except ImportError:
    print("Missing dependency: psycopg2-binary (pip install psycopg2-binary)", file=sys.stderr)
    sys.exit(1)

# Try to use your existing helper, if present
try:
    from db.connection import get_db_connection  # type: ignore
except Exception:
    get_db_connection = None  # type: ignore


# ---------------------------------------------------------------------------
# Small utils
# ---------------------------------------------------------------------------
def _load_dotenv_if_available() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        pass


def _slugify(name: str) -> str:
    out: List[str] = []
    prev_dash = False
    for ch in (name or "").lower():
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
    """
    Map TSDB rugby status codes to your match_status enum:
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
    return "scheduled"


def _to_int(v: Any) -> Optional[int]:
    try:
        if v in (None, "", "null"):
            return None
        return int(v)
    except Exception:
        return None


def _ensure_data_dir() -> str:
    out = os.path.join(os.getcwd(), "data")
    os.makedirs(out, exist_ok=True)
    return out


def _write_matches_csv(
    events: List[Dict[str, Any]],
    league_slug: str,
    season_label: str,
) -> str:
    out_dir = _ensure_data_dir()
    fname = f"matches_{league_slug}_{season_label}.csv".replace("/", "-")
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
                "strFilename": e.get("strFilename"),
                "data_source": "thesportsdb",
            }
            w.writerow(row)

    print(f"[OK] Wrote CSV for season={season_label}: {path}")
    return path


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def _get_conn():
    """
    Get a psycopg2 connection, preferring db.connection.get_db_connection().
    """
    if get_db_connection is not None:
        return get_db_connection()  # type: ignore

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "DATABASE_URL not set and db.connection.get_db_connection() missing. "
            "Set DATABASE_URL in .env or create db/connection.py with get_db_connection()."
        )
    return psycopg2.connect(dsn)


def _load_rugby_leagues(
    cur,
    only_tsdb_league: Optional[str] = None,
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    """
    Load rugby leagues that have a tsdb_league_id.
    """
    if only_tsdb_league:
        cur.execute(
            """
            SELECT l.league_id,
                   l.name,
                   l.tsdb_league_id,
                   s.name AS sport_name
            FROM leagues l
            JOIN sports s ON s.sport_id = l.sport_id
            WHERE l.tsdb_league_id = %s
              AND (LOWER(s.name) LIKE 'rugby%%' OR s.code = 'rugby_union')
            ORDER BY l.league_id
            """,
            (only_tsdb_league,),
        )
    else:
        cur.execute(
            """
            SELECT l.league_id,
                   l.name,
                   l.tsdb_league_id,
                   s.name AS sport_name
            FROM leagues l
            JOIN sports s ON s.sport_id = l.sport_id
            WHERE l.tsdb_league_id IS NOT NULL
              AND (LOWER(s.name) LIKE 'rugby%%' OR s.code = 'rugby_union')
            ORDER BY l.league_id
            """
        )
    rows = cur.fetchall()
    leagues: List[Dict[str, Any]] = []
    for r in rows:
        leagues.append(
            {
                "league_id": int(r["league_id"]),
                "name": r["name"],
                "tsdb_league_id": str(r["tsdb_league_id"]),
                "sport_name": r["sport_name"],
            }
        )
    if verbose:
        print(f"[INFO] Loaded {len(leagues)} rugby league(s) with tsdb_league_id")
    return leagues


def _load_seasons_for_league(
    cur,
    league_id: int,
    limit_seasons_back: Optional[int] = None,
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    """
    Load seasons for a league from the seasons table, sorted by year DESC if limiting,
    otherwise ASC.
    """
    if limit_seasons_back is not None:
        cur.execute(
            """
            SELECT season_id, year, label, tsdb_season_key
            FROM seasons
            WHERE league_id = %s
              AND tsdb_season_key IS NOT NULL
            ORDER BY year DESC
            LIMIT %s
            """,
            (league_id, limit_seasons_back),
        )
        rows = cur.fetchall()
        rows = sorted(rows, key=lambda r: (r["year"] or 0))
    else:
        cur.execute(
            """
            SELECT season_id, year, label, tsdb_season_key
            FROM seasons
            WHERE league_id = %s
              AND tsdb_season_key IS NOT NULL
            ORDER BY year ASC
            """,
            (league_id,),
        )
        rows = cur.fetchall()

    seasons: List[Dict[str, Any]] = []
    for r in rows:
        seasons.append(
            {
                "season_id": int(r["season_id"]),
                "year": r["year"],
                "label": r["label"],
                "tsdb_season_key": r["tsdb_season_key"],
            }
        )
    if verbose:
        print(f"[INFO]  -> {len(seasons)} seasons found in DB for league_id={league_id}")
    return seasons


def _lookup_team_id(
    cur,
    tsdb_team_id: str,
    cache: Dict[str, Optional[int]],
) -> Optional[int]:
    if not tsdb_team_id:
        return None
    if tsdb_team_id in cache:
        return cache[tsdb_team_id]
    cur.execute(
        "SELECT team_id FROM teams WHERE tsdb_team_id = %s",
        (tsdb_team_id,),
    )
    row = cur.fetchone()
    if not row:
        cache[tsdb_team_id] = None
        return None
    team_id = int(row[0])
    cache[tsdb_team_id] = team_id
    return team_id


def _lookup_venue_id(
    cur,
    tsdb_venue_id: Optional[str],
    cache: Dict[str, Optional[int]],
) -> Optional[int]:
    if not tsdb_venue_id:
        return None
    if tsdb_venue_id in cache:
        return cache[tsdb_venue_id]
    cur.execute(
        "SELECT venue_id FROM venues WHERE tsdb_venue_id = %s",
        (tsdb_venue_id,),
    )
    row = cur.fetchone()
    if not row:
        cache[tsdb_venue_id] = None
        return None
    vid = int(row[0])
    cache[tsdb_venue_id] = vid
    return vid


def _upsert_match_by_tsdb_id(
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
    tsdb_event_id: str,
    verbose: bool = False,
) -> Tuple[bool, bool]:
    """
    Upsert based on tsdb_event_id. Returns (inserted, updated).
    """
    cur.execute(
        "SELECT match_id FROM matches WHERE tsdb_event_id = %s",
        (tsdb_event_id,),
    )
    row = cur.fetchone()
    if row:
        match_id = int(row[0])
        if verbose:
            print(f"    [DB] update match_id={match_id} (tsdb_event_id={tsdb_event_id})")
        cur.execute(
            """
            UPDATE matches
               SET league_id    = %s,
                   season_id    = %s,
                   venue_id     = %s,
                   home_team_id = %s,
                   away_team_id = %s,
                   status       = %s,
                   kickoff_utc  = %s,
                   home_score   = %s,
                   away_score   = %s,
                   attendance   = %s,
                   source       = %s,
                   updated_at   = NOW()
             WHERE match_id = %s
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
                "thesportsdb",
                match_id,
            ),
        )
        return False, True

    if verbose:
        print(f"    [DB] insert new match (tsdb_event_id={tsdb_event_id})")
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
            source,
            created_at,
            updated_at
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s,
            %s,
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
            "thesportsdb",
        ),
    )
    return True, False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    import argparse

    _load_dotenv_if_available()

    parser = argparse.ArgumentParser(
        description="Ingest rugby matches from TheSportsDB into the matches table."
    )
    parser.add_argument(
        "--only-tsdb-league",
        help="If set, only process this TSDB league id (e.g. 4446 for URC).",
    )
    parser.add_argument(
        "--limit-seasons-back",
        type=int,
        help="If set, for each league only ingest the last N seasons (by year).",
    )
    parser.add_argument(
        "--season-max-retries",
        type=int,
        default=5,
        help="Max retries per season when TSDB returns 429 (default: 5).",
    )
    parser.add_argument(
        "--season-base-sleep",
        type=float,
        default=5.0,
        help="Base sleep in seconds for season-level 429 handling "
             "(exponential backoff: base, 2*base, 4*base, ...) default 5.0.",
    )
    parser.add_argument(
        "--write-csv",
        action="store_true",
        help="Also write CSV snapshots under ./data for each league+season.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose logging.",
    )
    args = parser.parse_args()

    verbose = args.verbose
    season_max_retries = max(args.season_max_retries, 1)
    season_base_sleep = max(args.season_base_sleep, 0.0)

    conn = _get_conn()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=DictCursor)

    try:
        leagues = _load_rugby_leagues(
            cur,
            only_tsdb_league=args.only_tsdb_league,
            verbose=verbose,
        )
        if not leagues:
            raise SystemExit("No rugby leagues with tsdb_league_id found. Did you ingest leagues correctly?")

        total_inserted = 0
        total_updated = 0
        total_skipped = 0

        for l_idx, lg in enumerate(leagues, start=1):
            league_id_db = lg["league_id"]
            league_name = lg["name"]
            tsdb_league_id = lg["tsdb_league_id"]

            league_slug = _slugify(league_name)

            if verbose:
                print(
                    f"\n[LEAGUE {l_idx}/{len(leagues)}] DB league_id={league_id_db}, "
                    f"TSDB id={tsdb_league_id}, name='{league_name}'"
                )

            seasons = _load_seasons_for_league(
                cur,
                league_id=league_id_db,
                limit_seasons_back=args.limit_seasons_back,
                verbose=verbose,
            )
            if not seasons:
                if verbose:
                    print(f"[WARN]  -> no seasons in DB for league_id={league_id_db}, skipping")
                continue

            # Cache for team/venue resolving per league
            team_cache: Dict[str, Optional[int]] = {}
            venue_cache: Dict[str, Optional[int]] = {}

            for s_idx, srow in enumerate(seasons, start=1):
                season_id_db = srow["season_id"]
                tsdb_season_key = srow["tsdb_season_key"]
                season_label = srow["label"] or tsdb_season_key

                if verbose:
                    print(
                        f"[SEASON {s_idx}/{len(seasons)}] league_id={league_id_db}, "
                        f"season_id={season_id_db}, tsdb_season_key='{tsdb_season_key}'"
                    )

                # 1) Fetch events from TSDB with season-level 429 handling
                events: List[Dict[str, Any]] = []
                last_exc: Optional[Exception] = None

                for attempt in range(1, season_max_retries + 1):
                    try:
                        events = get_events_for_season_rugby(
                            str(tsdb_league_id),
                            str(tsdb_season_key),
                            verbose=verbose,
                        )
                        last_exc = None
                        break
                    except HTTPError as exc:
                        last_exc = exc
                        status = getattr(exc.response, "status_code", None)
                        if status == 429 and attempt < season_max_retries:
                            sleep_s = season_base_sleep * (2 ** (attempt - 1))
                            print(
                                f"[TSDB] HTTP 429 on eventsseason id={tsdb_league_id} "
                                f"season={tsdb_season_key} (attempt {attempt}/{season_max_retries}); "
                                f"sleeping {sleep_s:.1f}s before retry…",
                                file=sys.stderr,
                            )
                            time.sleep(sleep_s)
                            continue
                        # Non-429 or final attempt -> re-raise
                        raise
                    except Exception as exc:
                        last_exc = exc
                        # For non-HTTP errors, we don't keep hammering
                        print(
                            f"[ERROR] Unexpected error fetching events for "
                            f"league={tsdb_league_id}, season={tsdb_season_key}: {exc}",
                            file=sys.stderr,
                        )
                        raise

                if last_exc is not None:
                    # We've exhausted retries
                    raise last_exc

                if verbose:
                    print(
                        f"[TSDB] eventsseason id={tsdb_league_id} "
                        f"season={tsdb_season_key} -> {len(events)} rugby events"
                    )

                if not events:
                    if verbose:
                        print(f"[WARN]  -> no rugby events returned for this season; skipping DB ingest")
                    continue

                # Optionally write CSV snapshot
                if args.write_csv:
                    _write_matches_csv(events, league_slug, tsdb_season_key)

                inserted = 0
                updated = 0
                skipped = 0

                # 2) Ingest each event
                for e in events:
                    tsdb_event_id = (e.get("idEvent") or "").strip()
                    if not tsdb_event_id:
                        if verbose:
                            print("    [SKIP] Event missing idEvent, skipping")
                        skipped += 1
                        continue

                    id_home_tsdb = (e.get("idHomeTeam") or "").strip()
                    id_away_tsdb = (e.get("idAwayTeam") or "").strip()
                    id_venue_tsdb = (e.get("idVenue") or "").strip() or None

                    home_team_id = _lookup_team_id(cur, id_home_tsdb, team_cache)
                    away_team_id = _lookup_team_id(cur, id_away_tsdb, team_cache)

                    if home_team_id is None or away_team_id is None:
                        if verbose:
                            print(
                                f"    [SKIP] Missing team(s) for idEvent={tsdb_event_id}: "
                                f"home_tsdb={id_home_tsdb}, away_tsdb={id_away_tsdb}"
                            )
                        skipped += 1
                        continue

                    venue_id = _lookup_venue_id(cur, id_venue_tsdb, venue_cache)

                    kickoff = _parse_ts(e.get("strTimestamp")) or _combine_date_time(
                        e.get("dateEvent"),
                        e.get("strTime"),
                    )
                    raw_status = (e.get("strStatus") or e.get("strProgress") or "").strip()
                    status = _map_status(raw_status)

                    home_score = _to_int(e.get("intHomeScore"))
                    away_score = _to_int(e.get("intAwayScore"))
                    attendance = _to_int(e.get("intAttendance"))

                    ins, upd = _upsert_match_by_tsdb_id(
                        cur,
                        league_id=league_id_db,
                        season_id=season_id_db,
                        venue_id=venue_id,
                        home_team_id=home_team_id,
                        away_team_id=away_team_id,
                        kickoff_utc=kickoff,
                        status=status,
                        home_score=home_score,
                        away_score=away_score,
                        attendance=attendance,
                        tsdb_event_id=tsdb_event_id,
                        verbose=verbose,
                    )
                    inserted += int(ins)
                    updated += int(upd)

                conn.commit()
                total_inserted += inserted
                total_updated += updated
                total_skipped += skipped

                if verbose:
                    print(
                        f"[INFO]  -> season={tsdb_season_key}: "
                        f"inserted={inserted}, updated={updated}, skipped={skipped}"
                    )

        print(
            f"[DONE] Matches ingest complete. "
            f"Inserted={total_inserted}, Updated={total_updated}, Skipped={total_skipped}"
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
