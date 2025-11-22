#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""
ingest_match_events_raw.py
--------------------------

Fetch *raw* per-match JSON from TheSportsDB for matches that already exist
in your `matches` table (via tsdb_event_id) and store them in a generic
`raw_tsdb_events` table as JSONB.

This is intentionally generic and "schema-free" so we don't have to know
exactly what TSDB returns for rugby events yet. Later we can build
match_events / match_stats tables by parsing this raw JSON.

Assumed existing schema (from ingest_rugby_matches.py):

  leagues (
      league_id      BIGSERIAL PRIMARY KEY,
      name           TEXT,
      tsdb_league_id TEXT UNIQUE,
      ...
  )

  seasons (
      season_id       BIGSERIAL PRIMARY KEY,
      league_id       BIGINT REFERENCES leagues(league_id),
      year            INTEGER,
      label           TEXT,
      tsdb_season_key TEXT,
      ...
  )

  matches (
      match_id       BIGSERIAL PRIMARY KEY,
      league_id      BIGINT REFERENCES leagues(league_id),
      season_id      BIGINT REFERENCES seasons(season_id),
      tsdb_event_id  TEXT UNIQUE,
      kickoff_utc    TIMESTAMPTZ,
      ...
  )

Target table used by this script:

  raw_tsdb_events (
      raw_id        BIGSERIAL PRIMARY KEY,
      tsdb_event_id TEXT NOT NULL UNIQUE,
      payload       JSONB NOT NULL,     -- full TSDB response (typically dict)
      source        TEXT NOT NULL DEFAULT 'thesportsdb',
      fetched_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
  )

If raw_tsdb_events already exists with a different shape, this script will:
  - Keep the existing table,
  - ADD columns tsdb_event_id, payload, source, fetched_at where missing,
  - Detect an existing NOT NULL raw_json column and populate it with payload too,
  - Ensure a unique index on tsdb_event_id for ON CONFLICT upserts.

Usage (from C:\rugby-analytics):

  python -m scripts.ingest_match_events_raw ^
    --only-tsdb-league 4446 ^
    --sleep-between 1.5 ^
    -v
"""

import os
import sys
import time
from typing import Any, Dict, List, Optional, Set

# ---------------------------------------------------------------------------
# Ensure project ROOT on sys.path so scr.* and db.* are importable
# ---------------------------------------------------------------------------
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# ---------------------------------------------------------------------------
# TSDB client
# ---------------------------------------------------------------------------
try:
    from scr.ingest import tsdb_client
except Exception as exc:
    print(
        f"[IMPORT ERROR] Cannot import scr.ingest.tsdb_client: {exc!r}",
        file=sys.stderr,
    )
    sys.exit(1)

# ---------------------------------------------------------------------------
# DB imports
# ---------------------------------------------------------------------------
try:
    import psycopg2
    from psycopg2.extras import DictCursor, Json
except ImportError:
    print(
        "Missing dependency: psycopg2-binary (pip install psycopg2-binary)",
        file=sys.stderr,
    )
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


def _ensure_raw_table(cur, verbose: bool = False) -> None:
    """
    Ensure raw_tsdb_events table exists AND has the columns we need.

    If the table already exists with a different shape, we:
      - ADD tsdb_event_id (TEXT) if missing,
      - ADD payload (JSONB) if missing,
      - ADD source (TEXT) if missing,
      - ADD fetched_at (TIMESTAMPTZ) if missing,
      - Ensure a unique index on tsdb_event_id for ON CONFLICT.

    NOTE: We do NOT touch an existing raw_json column or its NOT NULL
    constraint; instead _store_raw_event will populate raw_json with payload.
    """
    if verbose:
        print("[INFO] Ensuring raw_tsdb_events table exists and has required columns…")

    # Does the table exist?
    cur.execute("SELECT to_regclass('public.raw_tsdb_events');")
    row = cur.fetchone()
    table_exists = bool(row and row[0])

    if not table_exists:
        # Create from scratch with full schema
        cur.execute(
            """
            CREATE TABLE raw_tsdb_events (
                raw_id        BIGSERIAL PRIMARY KEY,
                tsdb_event_id TEXT NOT NULL UNIQUE,
                payload       JSONB NOT NULL,
                source        TEXT NOT NULL DEFAULT 'thesportsdb',
                fetched_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        return

    # Table exists: make sure required columns are present
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'raw_tsdb_events'
        """
    )
    existing_cols = {r[0] for r in cur.fetchall()}

    if "tsdb_event_id" not in existing_cols:
        if verbose:
            print("[INFO] Adding tsdb_event_id column to raw_tsdb_events")
        cur.execute("ALTER TABLE raw_tsdb_events ADD COLUMN IF NOT EXISTS tsdb_event_id TEXT;")

    if "payload" not in existing_cols:
        if verbose:
            print("[INFO] Adding payload column (JSONB) to raw_tsdb_events")
        cur.execute("ALTER TABLE raw_tsdb_events ADD COLUMN IF NOT EXISTS payload JSONB;")

    if "source" not in existing_cols:
        if verbose:
            print("[INFO] Adding source column to raw_tsdb_events")
        cur.execute(
            "ALTER TABLE raw_tsdb_events "
            "ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'thesportsdb';"
        )

    if "fetched_at" not in existing_cols:
        if verbose:
            print("[INFO] Adding fetched_at column to raw_tsdb_events")
        cur.execute(
            "ALTER TABLE raw_tsdb_events "
            "ADD COLUMN IF NOT EXISTS fetched_at TIMESTAMPTZ DEFAULT NOW();"
        )

    # Ensure unique index on tsdb_event_id for ON CONFLICT
    cur.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                  FROM pg_indexes
                 WHERE schemaname = 'public'
                   AND tablename  = 'raw_tsdb_events'
                   AND indexname  = 'raw_tsdb_events_tsdb_event_id_uk'
            ) THEN
                CREATE UNIQUE INDEX raw_tsdb_events_tsdb_event_id_uk
                    ON raw_tsdb_events(tsdb_event_id);
            END IF;
        END;
        $$;
        """
    )


def _load_already_fetched_ids(cur, verbose: bool = False) -> Set[str]:
    """
    Return a set of tsdb_event_id values already in raw_tsdb_events.
    """
    cur.execute("SELECT tsdb_event_id FROM raw_tsdb_events;")
    rows = cur.fetchall()
    existing: Set[str] = set()
    for r in rows:
        val = r[0]
        if val:
            existing.add(str(val))
    if verbose:
        print(f"[INFO] Found {len(existing)} events already stored in raw_tsdb_events")
    return existing


def _load_candidate_matches(
    cur,
    only_tsdb_league: Optional[str] = None,
    limit_seasons_back: Optional[int] = None,
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    """
    Load matches that have a tsdb_event_id, optionally restricted by TSDB league
    and limited to the last N seasons (by seasons.year) per league.
    """
    if only_tsdb_league:
        cur.execute(
            """
            SELECT
                m.match_id,
                m.tsdb_event_id,
                l.tsdb_league_id,
                l.name AS league_name,
                s.season_id,
                s.year,
                s.label AS season_label
            FROM matches m
            JOIN leagues l ON l.league_id = m.league_id
            JOIN seasons s ON s.season_id = m.season_id
            WHERE l.tsdb_league_id = %s
              AND m.tsdb_event_id IS NOT NULL
            ORDER BY s.year ASC, m.kickoff_utc NULLS LAST, m.match_id ASC;
            """,
            (only_tsdb_league,),
        )
    else:
        cur.execute(
            """
            SELECT
                m.match_id,
                m.tsdb_event_id,
                l.tsdb_league_id,
                l.name AS league_name,
                s.season_id,
                s.year,
                s.label AS season_label
            FROM matches m
            JOIN leagues l ON l.league_id = m.league_id
            JOIN seasons s ON s.season_id = m.season_id
            WHERE l.tsdb_league_id IS NOT NULL
              AND m.tsdb_event_id IS NOT NULL
            ORDER BY l.tsdb_league_id::TEXT, s.year ASC, m.kickoff_utc NULLS LAST, m.match_id ASC;
            """
        )

    rows = cur.fetchall()
    matches: List[Dict[str, Any]] = []
    for r in rows:
        matches.append(
            {
                "match_id": int(r["match_id"]),
                "tsdb_event_id": str(r["tsdb_event_id"]),
                "tsdb_league_id": str(r["tsdb_league_id"]),
                "league_name": r["league_name"],
                "season_id": int(r["season_id"]),
                "year": r["year"],
                "season_label": r["season_label"],
            }
        )

    if limit_seasons_back is not None and limit_seasons_back > 0:
        # Keep only last N seasons *per TSDB league*
        by_league: Dict[str, List[Dict[str, Any]]] = {}
        for m in matches:
            by_league.setdefault(m["tsdb_league_id"], []).append(m)

        filtered: List[Dict[str, Any]] = []
        for lg_id, lg_matches in by_league.items():
            # Unique seasons sorted by year
            seasons: Dict[int, List[Dict[str, Any]]] = {}
            for m in lg_matches:
                yr = m["year"] or 0
                seasons.setdefault(yr, []).append(m)
            sorted_years = sorted(seasons.keys())
            if len(sorted_years) > limit_seasons_back:
                sorted_years = sorted_years[-limit_seasons_back:]
            keep_years = set(sorted_years)
            for yr, group in seasons.items():
                if yr in keep_years:
                    filtered.extend(group)

        matches = filtered

    if verbose:
        lg_info: Dict[str, int] = {}
        for m in matches:
            lg = m["tsdb_league_id"]
            lg_info[lg] = lg_info.get(lg, 0) + 1
        print(
            f"[INFO] Candidate matches to fetch raw JSON for: {len(matches)} "
            f"across {len(lg_info)} TSDB league(s)"
        )
        for lg, cnt in sorted(lg_info.items()):
            print(f"       - TSDB league {lg}: {cnt} matches")

    return matches


def _fetch_event_json(
    tsdb_event_id: str,
    verbose: bool = False,
    max_retries: int = 4,
) -> Optional[Dict[str, Any]]:
    """
    Use tsdb_client._get_json_with_backoff to hit lookupevent.php?id={idEvent}.
    Returns the decoded JSON dict (or None on failure).
    """
    endpoint = "lookupevent.php"
    params = {"id": tsdb_event_id}

    try:
        data = tsdb_client._get_json_with_backoff(  # type: ignore[attr-defined]
            endpoint,
            params,
            max_retries=max_retries,
            verbose=verbose,
        )
    except Exception as exc:
        print(
            f"[ERROR] Failed to fetch lookupevent for idEvent={tsdb_event_id}: {exc}",
            file=sys.stderr,
        )
        return None

    if not data:
        if verbose:
            print(f"[WARN] Empty JSON for idEvent={tsdb_event_id}")
        return None

    return data


# cache raw_tsdb_events columns so we can detect raw_json, etc.
_RAW_EVENT_COLUMNS: Optional[Set[str]] = None


def _get_raw_event_columns(cur) -> Set[str]:
    global _RAW_EVENT_COLUMNS
    if _RAW_EVENT_COLUMNS is not None:
        return _RAW_EVENT_COLUMNS

    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'raw_tsdb_events'
        """
    )
    _RAW_EVENT_COLUMNS = {r[0] for r in cur.fetchall()}
    return _RAW_EVENT_COLUMNS


def _store_raw_event(
    cur,
    tsdb_event_id: str,
    payload: Dict[str, Any],
    verbose: bool = False,
) -> None:
    """
    Upsert into raw_tsdb_events by tsdb_event_id.

    If the table has a `raw_json` column (e.g. old experiments), we populate
    it with the same payload to satisfy NOT NULL constraints and keep old code
    happy.
    """
    if verbose:
        print(f"    [DB] upsert raw_tsdb_events.tsdb_event_id={tsdb_event_id}")

    cols = _get_raw_event_columns(cur)

    if "raw_json" in cols:
        # Table has an old raw_json column, likely NOT NULL.
        # Mirror payload into raw_json as well.
        cur.execute(
            """
            INSERT INTO raw_tsdb_events (
                tsdb_event_id,
                payload,
                source,
                fetched_at,
                raw_json
            ) VALUES (
                %s,
                %s,
                'thesportsdb',
                NOW(),
                %s
            )
            ON CONFLICT (tsdb_event_id)
            DO UPDATE SET
                payload    = EXCLUDED.payload,
                source     = EXCLUDED.source,
                fetched_at = EXCLUDED.fetched_at,
                raw_json   = EXCLUDED.raw_json;
            """,
            (tsdb_event_id, Json(payload), Json(payload)),
        )
    else:
        # New-style table with only payload/source/fetched_at
        cur.execute(
            """
            INSERT INTO raw_tsdb_events (
                tsdb_event_id,
                payload,
                source,
                fetched_at
            ) VALUES (
                %s,
                %s,
                'thesportsdb',
                NOW()
            )
            ON CONFLICT (tsdb_event_id)
            DO UPDATE SET
                payload    = EXCLUDED.payload,
                source     = EXCLUDED.source,
                fetched_at = EXCLUDED.fetched_at;
            """,
            (tsdb_event_id, Json(payload)),
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    import argparse

    _load_dotenv_if_available()

    parser = argparse.ArgumentParser(
        description=(
            "Fetch raw TSDB event JSON (lookupevent.php) for matches that "
            "have tsdb_event_id, and store them in raw_tsdb_events."
        )
    )
    parser.add_argument(
        "--only-tsdb-league",
        help="If set, only process matches where leagues.tsdb_league_id = this value.",
    )
    parser.add_argument(
        "--limit-seasons-back",
        type=int,
        help=(
            "If set, per TSDB league, only include matches from the last N seasons "
            "(by seasons.year) that have matches in the DB."
        ),
    )
    parser.add_argument(
        "--sleep-between",
        type=float,
        default=1.5,
        help="Seconds to sleep between API calls to TSDB (default: 1.5).",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Optional max number of events to process (for testing).",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose logging.",
    )
    args = parser.parse_args()

    verbose = args.verbose
    sleep_between = max(args.sleep_between, 0.0)
    max_events = args.max_events

    conn = _get_conn()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=DictCursor)

    try:
        _ensure_raw_table(cur, verbose=verbose)
        conn.commit()

        existing_ids = _load_already_fetched_ids(cur, verbose=verbose)

        matches = _load_candidate_matches(
            cur,
            only_tsdb_league=args.only_tsdb_league,
            limit_seasons_back=args.limit_seasons_back,
            verbose=verbose,
        )

        processed = 0
        skipped_existing = 0
        fetched_ok = 0
        fetch_failed = 0

        last_league: Optional[str] = None
        last_season: Optional[str] = None

        for m in matches:
            if max_events is not None and processed >= max_events:
                if verbose:
                    print(
                        f"[INFO] Reached max-events={max_events}, stopping early."
                    )
                break

            tsdb_event_id = m["tsdb_event_id"]
            lg = m["tsdb_league_id"]
            season_label = m["season_label"]

            if tsdb_event_id in existing_ids:
                skipped_existing += 1
                continue

            if verbose:
                if lg != last_league or season_label != last_season:
                    print(
                        f"\n[CTX] TSDB league {lg} ({m['league_name']}), season '{season_label}'"
                    )
                    last_league = lg
                    last_season = season_label
                print(f"[EVENT] match_id={m['match_id']} idEvent={tsdb_event_id}")

            data = _fetch_event_json(tsdb_event_id, verbose=verbose)
            if data is None:
                fetch_failed += 1
                continue

            _store_raw_event(cur, tsdb_event_id, data, verbose=verbose)

            existing_ids.add(tsdb_event_id)
            fetched_ok += 1
            processed += 1

            if sleep_between > 0:
                time.sleep(sleep_between)

        conn.commit()

        print(
            "[DONE] raw TSDB event ingest complete -> "
            f"processed={processed}, fetched_ok={fetched_ok}, "
            f"skipped_existing={skipped_existing}, fetch_failed={fetch_failed}"
        )

    except Exception as exc:
        conn.rollback()
        print(
            f"[ERROR] Ingestion failed, rolled back transaction: {exc}",
            file=sys.stderr,
        )
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
