#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""
ingest_urc_officials.py
-----------------------

Populate `officials` and `match_officials` from TheSportsDB for URC (or any
TSDB rugby league) using the `strReferee` field on events.

Assumptions:
- You already have:
    officials (
        official_id   BIGSERIAL PK,
        full_name     TEXT NOT NULL,
        country       TEXT,
        created_at    TIMESTAMPTZ,
        updated_at    TIMESTAMPTZ
    )

    match_officials (
        match_official_id  BIGSERIAL PK,
        match_id           BIGINT NOT NULL,
        official_id        BIGINT NOT NULL,
        role               TEXT NOT NULL,
        created_at         TIMESTAMPTZ,
        updated_at         TIMESTAMPTZ
    )

- Your `matches` table has:
    matches.tsdb_event_id TEXT
  and already contains URC matches for the seasons you care about.

- Your `leagues` table has:
    leagues.tsdb_league_id TEXT
  with a row for URC (4446).

DB connection:
- Uses db.connection.get_db_connection() if available
- Otherwise uses DATABASE_URL from .env

TSDB client:
- Uses scr.ingest.tsdb_client helpers you already have.

Usage (from C:\rugby-analytics):

  # Ingest refs for all seasons we have for URC (tsdb_league_id=4446)
  python .\scripts\ingest_urc_officials.py -v

  # Different league
  python .\scripts\ingest_urc_officials.py --league-id 4550 -v
"""

import os
import sys
from typing import Dict, Optional, Any, List

# ---------------------------------------------------------------------------
# Ensure project ROOT is on sys.path so "scr" is importable
# ---------------------------------------------------------------------------
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# ---------------------------------------------------------------------------
# TSDB client
# ---------------------------------------------------------------------------
try:
    from scr.ingest.tsdb_client import (
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

# Try to use your existing connection helper if available
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
# Helpers
# ---------------------------------------------------------------------------
def _get_league_id(cur, tsdb_league_id: str) -> int:
    cur.execute(
        "SELECT league_id FROM leagues WHERE tsdb_league_id = %s",
        (tsdb_league_id,),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(
            f"No league row with tsdb_league_id={tsdb_league_id}. "
            f"Ingest leagues/seasons first."
        )
    return int(row[0])


def _get_seasons_for_league(cur, league_id: int) -> List[str]:
    """
    Get the list of tsdb_season_key values in seasons for this league.
    We use these labels to call TSDB eventsseason.
    """
    cur.execute(
        """
        SELECT tsdb_season_key
        FROM seasons
        WHERE league_id = %s
          AND tsdb_season_key IS NOT NULL
        ORDER BY year
        """,
        (league_id,),
    )
    return [r[0] for r in cur.fetchall()]


def _build_match_map(cur, league_id: int) -> Dict[str, int]:
    """
    Build a map: tsdb_event_id -> match_id, for this league.
    Assumes matches.tsdb_event_id is populated by your matches ingest.
    """
    cur.execute(
        """
        SELECT tsdb_event_id, match_id
        FROM matches
        WHERE league_id = %s
          AND tsdb_event_id IS NOT NULL
        """,
        (league_id,),
    )
    mapping: Dict[str, int] = {}
    for row in cur.fetchall():
        tsdb_event_id, match_id = row
        if tsdb_event_id:
            mapping[str(tsdb_event_id)] = int(match_id)
    return mapping


def _get_or_create_official_id(cur, name: str, verbose: bool = False) -> int:
    """
    Upsert an official row by full_name.
    We don't have TSDB IDs for refs, so we treat full_name as the natural key.
    """
    name_clean = name.strip()
    if not name_clean:
        raise ValueError("Empty referee name")

    cur.execute(
        """
        SELECT official_id
        FROM officials
        WHERE LOWER(full_name) = LOWER(%s)
        """,
        (name_clean,),
    )
    row = cur.fetchone()
    if row:
        return int(row[0])

    if verbose:
        print(f"  [INSERT] official '{name_clean}'")

    cur.execute(
        """
        INSERT INTO officials (
            full_name,
            country,
            created_at,
            updated_at
        ) VALUES (
            %s,
            NULL,
            NOW(),
            NOW()
        )
        RETURNING official_id
        """,
        (name_clean,),
    )
    return int(cur.fetchone()[0])


def _ensure_match_official(
    cur,
    match_id: int,
    official_id: int,
    role: str = "Referee",
    verbose: bool = False,
) -> None:
    """
    Ensure there is a row in match_officials for (match_id, official_id, role).
    """
    cur.execute(
        """
        SELECT match_official_id
        FROM match_officials
        WHERE match_id = %s
          AND official_id = %s
          AND role = %s
        """,
        (match_id, official_id, role),
    )
    row = cur.fetchone()
    if row:
        if verbose:
            print(f"  [SKIP] match_officials already has match_id={match_id}, official_id={official_id}, role={role}")
        return

    if verbose:
        print(f"  [INSERT] match_officials: match_id={match_id}, official_id={official_id}, role={role}")

    cur.execute(
        """
        INSERT INTO match_officials (
            match_id,
            official_id,
            role,
            created_at,
            updated_at
        ) VALUES (
            %s, %s, %s,
            NOW(), NOW()
        )
        """,
        (match_id, official_id, role),
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    import argparse

    _load_dotenv_if_available()

    parser = argparse.ArgumentParser(
        description="Ingest referees from TSDB events into officials + match_officials."
    )
    parser.add_argument(
        "--league-id",
        default="4446",
        help="TheSportsDB league id (URC = 4446 by default).",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose logging.",
    )
    args = parser.parse_args()

    tsdb_league_id = str(args.league_id)
    verbose = args.verbose

    if verbose:
        print(f"[INFO] Using TSDB league_id={tsdb_league_id}")

    # Resolve league from TSDB just to confirm it exists (and log)
    league_meta = get_league_meta(tsdb_league_id, verbose=verbose)
    league_name = (league_meta.get("strLeague") or f"league-{tsdb_league_id}").strip()
    if verbose:
        print(f"[INFO] TheSportsDB league: {league_name}")

    # DB connection
    conn = _get_conn()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=DictCursor)

    try:
        league_id_db = _get_league_id(cur, tsdb_league_id)
        if verbose:
            print(f"[INFO] DB league_id={league_id_db} for tsdb_league_id={tsdb_league_id}")

        seasons = _get_seasons_for_league(cur, league_id_db)
        if not seasons:
            raise RuntimeError(
                f"No seasons found in DB for league_id={league_id_db}. "
                f"Ingest matches/seasons first."
            )

        if verbose:
            print(f"[INFO] Seasons in DB for this league: {seasons}")

        # Build map tsdb_event_id -> match_id (for all seasons)
        match_map = _build_match_map(cur, league_id_db)
        if verbose:
            print(f"[INFO] Found {len(match_map)} matches with tsdb_event_id for this league")

        total_events_seen = 0
        total_with_ref = 0
        total_inserted_officials = 0
        total_inserted_links = 0

        try:
            for s in seasons:
                if verbose:
                    print(f"[SEASON] TSDB season={s}")

                events = get_events_for_season_rugby(tsdb_league_id, s, verbose=verbose)
                if verbose:
                    print(f"[TSDB] eventsseason id={tsdb_league_id} season={s} -> {len(events)} rugby events")

                for e in events:
                    total_events_seen += 1
                    tsdb_event_id = (e.get("idEvent") or "").strip()
                    if not tsdb_event_id:
                        continue

                    ref_name = (e.get("strReferee") or "").strip()
                    if not ref_name:
                        continue

                    total_with_ref += 1

                    match_id = match_map.get(tsdb_event_id)
                    if match_id is None:
                        # We don't have this match in DB (maybe outdated season), skip
                        if verbose:
                            print(f"  [SKIP] No match row matching tsdb_event_id={tsdb_event_id}")
                        continue

                    # Upsert official
                    before_officials = _count_officials(cur) if verbose else None
                    official_id = _get_or_create_official_id(cur, ref_name, verbose=verbose)
                    after_officials = _count_officials(cur) if verbose else None
                    if verbose and before_officials is not None and after_officials is not None:
                        if after_officials > before_officials:
                            total_inserted_officials += 1

                    # Create match_official link
                    before_links = _count_links(cur) if verbose else None
                    _ensure_match_official(cur, match_id, official_id, role="Referee", verbose=verbose)
                    after_links = _count_links(cur) if verbose else None
                    if verbose and before_links is not None and after_links is not None:
                        if after_links > before_links:
                            total_inserted_links += 1

                conn.commit()
                if verbose:
                    print(f"[INFO] Committed season={s}")

        except Exception as exc:
            conn.rollback()
            print(f"[ERROR] Ingestion failed, rolled back transaction: {exc}", file=sys.stderr)
            raise

        print(
            f"[DONE] Officials ingest complete.\n"
            f"       Events seen: {total_events_seen}\n"
            f"       Events with referee: {total_with_ref}\n"
            f"       (Note: inserted_officials/links are approximate in verbose mode.)"
        )

    finally:
        cur.close()
        conn.close()


def _count_officials(cur) -> int:
    cur.execute("SELECT COUNT(*) FROM officials")
    return int(cur.fetchone()[0])


def _count_links(cur) -> int:
    cur.execute("SELECT COUNT(*) FROM match_officials")
    return int(cur.fetchone()[0])


if __name__ == "__main__":
    main()
