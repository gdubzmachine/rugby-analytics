#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""
ingest_urc_matches.py
---------------------

Ingest URC (or any TSDB rugby league) matches into Postgres.

- Uses scr.ingest.tsdb_client for TheSportsDB calls.
- Maps:
    leagues.tsdb_league_id   ← TSDB league id
    seasons.tsdb_season_key  ← TSDB strSeason
    teams.tsdb_team_id       ← TSDB idHomeTeam / idAwayTeam
    venues.tsdb_venue_id     ← TSDB idVenue
    matches.tsdb_event_id    ← TSDB idEvent

Usage (from project root C:\rugby-analytics):

  # Re-ingest last 10 seasons of URC
  python .\scripts\ingest_urc_matches.py --seasons-back 10 -v

  # Different league
  python .\scripts\ingest_urc_matches.py --league-id 4550 --seasons-back 5 -v
"""

import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Ensure project ROOT on sys.path so "scr" is importable
# ---------------------------------------------------------------------------
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# ---------------------------------------------------------------------------
# TSDB client (shared helper module: scr/ingest/tsdb_client.py)
# ---------------------------------------------------------------------------
try:
    from scr.ingest.tsdb_client import (
        THESPORTSDB_API_KEY,
        get_league_meta,
        get_current_season_label,
        get_events_for_season_rugby,
    )
except Exception as e:
    print(f"[ERROR] Failed to import scr.ingest.tsdb_client: {e}", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------
try:
    import psycopg2
    from psycopg2.extras import DictCursor
except ImportError:
    print("Missing dependency psycopg2-binary. Run: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)

# Try to use your existing connection helper if present
try:
    from db.connection import get_db_connection  # type: ignore
except Exception:
    get_db_connection = None  # type: ignore


def _load_dotenv_if_available() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        pass


def _get_conn():
    """
    Get a psycopg2 connection, using db.connection.get_db_connection()
    if available; otherwise use DATABASE_URL.
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


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
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


def _combine_date_time(date_str: Optional[str], time_str: Optional[str]) -> Optional[datetime]:
    if not date_str:
        return None
    ds = date_str.strip()
    ts = (time_str or "00:00:00").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(f"{ds} {ts}", fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


def _parse_kickoff(e: Dict[str, Any]) -> Optional[datetime]:
    # Prefer TSDB strTimestamp if present
    ts = e.get("strTimestamp")
    if ts:
        s = ts.strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            pass
    # Fallback: dateEvent + strTime
    return _combine_date_time(e.get("dateEvent"), e.get("strTime"))


def _parse_int(value: Any) -> Optional[int]:
    try:
        if value in (None, "", "null"):
            return None
        return int(value)
    except Exception:
        return None


def _parse_year_from_season(label: str) -> Optional[int]:
    if not label:
        return None
    s = label.strip()
    try:
        return int(s[:4])
    except Exception:
        return None


def _previous_season_label(label: str) -> str:
    s = (label or "").strip()
    if len(s) >= 9 and s[4] in "-/":
        try:
            start = int(s[:4])
            prev_start = start - 1
            prev_end = prev_start + 1
            return f"{prev_start}-{prev_end}"
        except Exception:
            pass
    try:
        year = int(s[:4])
        return str(year - 1)
    except Exception:
        return s


# ---------------------------------------------------------------------------
# DB lookups
# ---------------------------------------------------------------------------
def _get_league_id(cur, tsdb_league_id: str) -> int:
    cur.execute(
        "SELECT league_id FROM leagues WHERE tsdb_league_id = %s",
        (tsdb_league_id,),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"No league row with tsdb_league_id={tsdb_league_id}")
    return int(row[0])


def _get_or_create_season_id(cur, league_id: int, season_label: str) -> int:
    """
    Map TSDB strSeason -> seasons.season_id via tsdb_season_key.
    Creates a row if needed.
    """
    cur.execute(
        """
        SELECT season_id
        FROM seasons
        WHERE league_id = %s
          AND tsdb_season_key = %s
        """,
        (league_id, season_label),
    )
    row = cur.fetchone()
    if row:
        return int(row[0])

    year = _parse_year_from_season(season_label)
    if year is None:
        year = 0

    cur.execute(
        """
        INSERT INTO seasons (
            league_id,
            year,
            label,
            start_date,
            end_date,
            tsdb_season_key,
            created_at,
            updated_at
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
        (league_id, year, season_label, season_label),
    )
    return int(cur.fetchone()[0])


def _lookup_team_id(cur, tsdb_team_id: str, cache: Dict[str, Optional[int]]) -> Optional[int]:
    if not tsdb_team_id:
        return None
    if tsdb_team_id in cache:
        return cache[tsdb_team_id]
    cur.execute(
        "SELECT team_id FROM teams WHERE tsdb_team_id = %s",
        (tsdb_team_id,),
    )
    row = cur.fetchone()
    team_id = int(row[0]) if row else None
    cache[tsdb_team_id] = team_id
    return team_id


def _lookup_venue_id(cur, tsdb_venue_id: Optional[str], cache: Dict[str, Optional[int]]) -> Optional[int]:
    if not tsdb_venue_id:
        return None
    if tsdb_venue_id in cache:
        return cache[tsdb_venue_id]
    cur.execute(
        "SELECT venue_id FROM venues WHERE tsdb_venue_id = %s",
        (tsdb_venue_id,),
    )
    row = cur.fetchone()
    venue_id = int(row[0]) if row else None
    cache[tsdb_venue_id] = venue_id
    return venue_id


# ---------------------------------------------------------------------------
# Match upsert
# ---------------------------------------------------------------------------
def _upsert_match(
    cur,
    league_id: int,
    season_id: int,
    venue_id: Optional[int],
    home_team_id: int,
    away_team_id: int,
    e: Dict[str, Any],
    data_source: str = "other",
    verbose: bool = False,
) -> None:
    """
    Upsert a match using tsdb_event_id (idEvent) as the natural key.
    """
    tsdb_event_id = (e.get("idEvent") or "").strip()
    if not tsdb_event_id:
        if verbose:
            print("  [SKIP] Event without idEvent, skipping", file=sys.stderr)
        return

    kickoff = _parse_kickoff(e)
    status = _map_status((e.get("strStatus") or e.get("strProgress") or "").strip())
    home_score = _parse_int(e.get("intHomeScore"))
    away_score = _parse_int(e.get("intAwayScore"))
    attendance = _parse_int(e.get("intAttendance"))

    # Round / stage
    round_label = None
    if e.get("strRound"):
        round_label = str(e.get("strRound")).strip()
    elif e.get("intRound"):
        round_label = str(e.get("intRound")).strip()

    stage = (e.get("strStage") or "").strip() or None

    # Check if exists
    cur.execute(
        "SELECT match_id FROM matches WHERE tsdb_event_id = %s",
        (tsdb_event_id,),
    )
    row = cur.fetchone()

    if row:
        match_id = int(row[0])
        if verbose:
            print(f"  [DB] UPDATE match_id={match_id} (tsdb_event_id={tsdb_event_id})")
        cur.execute(
            """
            UPDATE matches
               SET league_id      = %s,
                   season_id      = %s,
                   venue_id       = %s,
                   home_team_id   = %s,
                   away_team_id   = %s,
                   status         = %s,
                   kickoff_utc    = %s,
                   home_score     = %s,
                   away_score     = %s,
                   attendance     = %s,
                   round_label    = %s,
                   stage          = %s,
                   source         = %s,
                   updated_at     = NOW()
             WHERE match_id = %s
            """,
            (
                league_id,
                season_id,
                venue_id,
                home_team_id,
                away_team_id,
                status,
                kickoff,
                home_score,
                away_score,
                attendance,
                round_label,
                stage,
                data_source,
                match_id,
            ),
        )
    else:
        if verbose:
            print(f"  [DB] INSERT tsdb_event_id={tsdb_event_id}")
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
                round_label,
                stage,
                source,
                tsdb_event_id,
                created_at,
                updated_at
            ) VALUES (
                %s, %s, %s,
                %s, %s,
                %s,
                %s,
                %s, %s,
                %s,
                %s,
                %s,
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
                kickoff,
                home_score,
                away_score,
                attendance,
                round_label,
                stage,
                data_source,
                tsdb_event_id,
            ),
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    import argparse

    _load_dotenv_if_available()

    parser = argparse.ArgumentParser(
        description="Ingest rugby matches from TheSportsDB into Postgres."
    )
    parser.add_argument(
        "--league-id",
        default="4446",
        help="TheSportsDB league id (URC = 4446 by default).",
    )
    parser.add_argument(
        "--seasons-back",
        type=int,
        default=10,
        help="Number of seasons back from current TSDB season label (default: 10).",
    )
    parser.add_argument(
        "--write-csv",
        action="store_true",
        help="(Currently unused; kept for backwards compatibility).",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose logging.",
    )
    args = parser.parse_args()

    league_id_tsdb = str(args.league_id)
    seasons_back = max(args.seasons_back, 1)
    verbose = args.verbose

    # Resolve league + current season via TSDB
    league_meta = get_league_meta(league_id_tsdb, verbose=verbose)
    league_name = (league_meta.get("strLeague") or f"league-{league_id_tsdb}").strip()
    current_season_label = get_current_season_label(league_id_tsdb, verbose=verbose)

    if verbose:
        print(f"[INFO] League: {league_name} (TSDB id={league_id_tsdb})")
        print(f"[INFO] Current TSDB season label: {current_season_label!r}")

    if not current_season_label:
        raise SystemExit("TSDB did not return strCurrentSeason for this league.")

    # Build seasons list: [current, previous, ...]
    seasons_to_fetch: List[str] = []
    lbl = current_season_label
    for _ in range(seasons_back):
        seasons_to_fetch.append(lbl)
        lbl = _previous_season_label(lbl)

    if verbose:
        print(f"[INFO] Seasons to ingest (current back {seasons_back}): {seasons_to_fetch}")

    # DB connection
    conn = _get_conn()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=DictCursor)

    try:
        league_id_db = _get_league_id(cur, league_id_tsdb)
    except Exception as e:
        conn.close()
        raise RuntimeError(
            f"No league row with tsdb_league_id={league_id_tsdb}. "
            f"Insert leagues first."
        ) from e

    team_cache: Dict[str, Optional[int]] = {}
    venue_cache: Dict[str, Optional[int]] = {}

    total_inserted = 0
    total_updated = 0
    total_skipped = 0

    try:
        for idx, season_label in enumerate(seasons_to_fetch, start=1):
            if verbose:
                print(f"[SEASON {idx}/{len(seasons_to_fetch)}] {season_label}")

            events = get_events_for_season_rugby(
                league_id_tsdb,
                season_label,
                verbose=verbose,
            )

            if verbose:
                print(f"[TSDB] eventsseason id={league_id_tsdb} season={season_label} -> {len(events)} rugby events")

            if not events:
                if verbose:
                    print(f"[WARN] No events for season={season_label}, skipping.")
                continue

            season_id_db = _get_or_create_season_id(cur, league_id_db, season_label)

            for e in events:
                home_tsdb = (e.get("idHomeTeam") or "").strip()
                away_tsdb = (e.get("idAwayTeam") or "").strip()
                venue_tsdb = (e.get("idVenue") or "").strip() or None

                home_team_id = _lookup_team_id(cur, home_tsdb, team_cache)
                away_team_id = _lookup_team_id(cur, away_tsdb, team_cache)

                if home_team_id is None or away_team_id is None:
                    if verbose:
                        print(
                            f"  [SKIP] Missing team(s) for event {e.get('idEvent')}: "
                            f"home_tsdb={home_tsdb}, away_tsdb={away_tsdb}"
                        )
                    total_skipped += 1
                    continue

                venue_id = _lookup_venue_id(cur, venue_tsdb, venue_cache)

                # For stats: does this match already exist?
                tsdb_event_id = (e.get("idEvent") or "").strip()
                cur.execute(
                    "SELECT match_id FROM matches WHERE tsdb_event_id = %s",
                    (tsdb_event_id,),
                )
                existed = cur.fetchone() is not None

                _upsert_match(
                    cur,
                    league_id_db,
                    season_id_db,
                    venue_id,
                    home_team_id,
                    away_team_id,
                    e,
                    data_source="other",  # using 'other' in your data_source enum for TSDB
                    verbose=verbose,
                )

                if existed:
                    total_updated += 1
                else:
                    total_inserted += 1

            conn.commit()
            if verbose:
                print(f"[INFO] Committed season={season_label}")

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
