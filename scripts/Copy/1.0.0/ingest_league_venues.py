#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ingest_league_venues.py
-----------------------

Generic venue ingest script for rugby leagues.

For one or more TheSportsDB league IDs, this script:

- Figures out a set of seasons to scan (current TSDB season and N seasons back).
- For each (league, season), fetches events via
  scr.ingest.tsdb_client.get_events_for_season_rugby().
- Extracts venue info from each event:
    - idVenue      -> venues.tsdb_venue_id
    - strVenue     -> venues.name
    - strCity      -> venues.city (if present)
    - strCountry   -> venues.country (if present)
- Upserts into the `venues` table in a schema-aware way:
    - Introspects available columns (name, city, country, latitude, longitude, tsdb_venue_id, created_at, updated_at).
    - Ensures `tsdb_venue_id` column exists and is indexed/unique.
- Optionally updates the `matches` table so that:
    - matches.venue_id is set based on matches.tsdb_event_id and venues.tsdb_venue_id.

Typical schema (from ingest_rugby_matches.py docstring):

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
      status         TEXT,
      kickoff_utc    TIMESTAMPTZ,
      home_score     INTEGER,
      away_score     INTEGER,
      attendance     INTEGER,
      tsdb_event_id  TEXT UNIQUE,
      source         TEXT,
      created_at     TIMESTAMPTZ,
      updated_at     TIMESTAMPTZ
  )

Usage examples (from C:\\rugby-analytics):

    # Just URC (4446), last ~5 seasons, and update matches. Sleep built-in.
    python -m scripts.ingest_league_venues --tsdb-leagues 4446 --limit-seasons-back 5 --update-matches -v

    # Multiple leagues at once (with polite sleeps between them)
    python -m scripts.ingest_league_venues --tsdb-leagues 4446 5167 5070 --limit-seasons-back 5 --update-matches -v

Run this AFTER:
    - ingest_rugby_seasons.py  (so seasons table is populated)
    - ingest_rugby_matches.py  (so matches + tsdb_event_id exist)
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any, Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# Make sure scr/ is importable
# ---------------------------------------------------------------------------

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# TheSportsDB client
try:
    from scr.ingest.tsdb_client import (
        get_current_season_label,
        get_events_for_season_rugby,
    )
except Exception as exc:
    print(f"[ERROR] Failed to import scr.ingest.tsdb_client: {exc}", file=sys.stderr)
    sys.exit(1)

# DB
try:
    import psycopg2
    from psycopg2.extras import DictCursor
except Exception as exc:  # pragma: no cover
    print("Missing psycopg2. Install: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)

# Optional db helper
try:
    from db.connection import get_db_connection  # type: ignore
except Exception:
    get_db_connection = None  # type: ignore


# ---------------------------------------------------------------------------
# DB connection helper
# ---------------------------------------------------------------------------

def _get_conn():
    """
    Prefer your db.connection.get_db_connection(), otherwise use DATABASE_URL.
    """
    if get_db_connection is not None:
        return get_db_connection()

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "DATABASE_URL is not set and db.connection.get_db_connection() "
            "is not available."
        )
    return psycopg2.connect(dsn)


# ---------------------------------------------------------------------------
# Helpers: seasons & schema introspection
# ---------------------------------------------------------------------------

def _previous_season_label(label: str) -> str:
    """
    Given '2025-2026' returns '2024-2025'. Fallback: treat as year string.
    """
    s = (label or "").strip()
    if not s:
        return ""
    if "-" in s:
        start = int(s.split("-")[0])
        return f"{start - 1}-{start}"
    # fallback: treat as year
    y = int(s[:4])
    return str(y - 1)


_VENUE_COLUMNS: Optional[Set[str]] = None
_MATCH_COLUMNS: Optional[Set[str]] = None


def _get_table_columns(cur, table_name: str) -> Set[str]:
    global _VENUE_COLUMNS, _MATCH_COLUMNS
    if table_name == "venues" and _VENUE_COLUMNS is not None:
        return _VENUE_COLUMNS
    if table_name == "matches" and _MATCH_COLUMNS is not None:
        return _MATCH_COLUMNS

    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
        """,
        (table_name,),
    )
    cols = {row[0] for row in cur.fetchall()}

    if table_name == "venues":
        _VENUE_COLUMNS = cols
    elif table_name == "matches":
        _MATCH_COLUMNS = cols

    return cols


def _ensure_tsdb_venue_column(cur, verbose: bool = False) -> None:
    """
    Ensure venues.tsdb_venue_id exists and has a unique index.
    """
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'venues'
          AND column_name = 'tsdb_venue_id'
        LIMIT 1
        """
    )
    if cur.fetchone():
        return

    if verbose:
        print("[INFO] Adding venues.tsdb_venue_id TEXT column + unique index")

    cur.execute("ALTER TABLE venues ADD COLUMN IF NOT EXISTS tsdb_venue_id TEXT;")
    cur.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                  FROM pg_indexes
                 WHERE schemaname = 'public'
                   AND tablename  = 'venues'
                   AND indexname  = 'venues_tsdb_venue_id_uk'
            ) THEN
                CREATE UNIQUE INDEX venues_tsdb_venue_id_uk
                    ON venues(tsdb_venue_id);
            END IF;
        END;
        $$;
        """
    )


# ---------------------------------------------------------------------------
# Venue upsert
# ---------------------------------------------------------------------------

def _upsert_venue(
    cur,
    vdata: Dict[str, Any],
    verbose: bool = False,
) -> int:
    """
    Upsert one venue based on tsdb_venue_id, using only columns that exist on
    the venues table.

    Returns: venue_id
    """
    cols = _get_table_columns(cur, "venues")

    tsdb_venue_id = vdata.get("tsdb_venue_id")
    if not tsdb_venue_id:
        if verbose:
            print("[SKIP] Venue missing tsdb_venue_id", file=sys.stderr)
        return 0

    name = (vdata.get("name") or "").strip()
    city = (vdata.get("city") or "").strip() or None
    country = (vdata.get("country") or "").strip() or None
    lat = vdata.get("latitude")
    lon = vdata.get("longitude")

    # Try to parse lat/lon if they look like strings
    def maybe_float(val):
        if val is None:
            return None
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    lat = maybe_float(lat)
    lon = maybe_float(lon)

    # 1) See if we have an existing venue for this tsdb_venue_id
    cur.execute(
        "SELECT venue_id FROM venues WHERE tsdb_venue_id = %s",
        (tsdb_venue_id,),
    )
    row = cur.fetchone()

    # -------------------------
    # UPDATE path
    # -------------------------
    if row:
        venue_id = row[0]
        if verbose:
            print(f"  [UPDATE] venue_id={venue_id}: {name or tsdb_venue_id}")

        set_fragments = []
        params: List[Any] = []

        if "name" in cols and name:
            set_fragments.append("name = %s")
            params.append(name)

        if "city" in cols:
            set_fragments.append("city = %s")
            params.append(city)

        if "country" in cols:
            set_fragments.append("country = %s")
            params.append(country)

        if "latitude" in cols and lat is not None:
            set_fragments.append("latitude = %s")
            params.append(lat)

        if "longitude" in cols and lon is not None:
            set_fragments.append("longitude = %s")
            params.append(lon)

        if "tsdb_venue_id" in cols:
            set_fragments.append("tsdb_venue_id = %s")
            params.append(tsdb_venue_id)

        if "updated_at" in cols:
            set_fragments.append("updated_at = NOW()")

        if not set_fragments:
            return venue_id

        sql = f"UPDATE venues SET {', '.join(set_fragments)} WHERE venue_id = %s"
        params.append(venue_id)
        cur.execute(sql, tuple(params))
        return venue_id

    # -------------------------
    # INSERT path
    # -------------------------
    if verbose:
        print(f"  [INSERT] {name or tsdb_venue_id}")

    insert_cols: List[str] = []
    placeholders: List[str] = []
    params2: List[Any] = []

    if "name" in cols:
        insert_cols.append("name")
        placeholders.append("%s")
        params2.append(name or tsdb_venue_id)

    if "city" in cols:
        insert_cols.append("city")
        placeholders.append("%s")
        params2.append(city)

    if "country" in cols:
        insert_cols.append("country")
        placeholders.append("%s")
        params2.append(country)

    if "latitude" in cols:
        insert_cols.append("latitude")
        placeholders.append("%s")
        params2.append(lat)

    if "longitude" in cols:
        insert_cols.append("longitude")
        placeholders.append("%s")
        params2.append(lon)

    if "tsdb_venue_id" in cols:
        insert_cols.append("tsdb_venue_id")
        placeholders.append("%s")
        params2.append(tsdb_venue_id)

    if "created_at" in cols:
        insert_cols.append("created_at")
        placeholders.append("NOW()")

    if "updated_at" in cols:
        insert_cols.append("updated_at")
        placeholders.append("NOW()")

    if not insert_cols:
        raise RuntimeError("No insertable columns detected for venues table.")

    sql = f"""
        INSERT INTO venues ({', '.join(insert_cols)})
        VALUES ({', '.join(placeholders)})
        RETURNING venue_id
    """
    cur.execute(sql, tuple(params2))
    row = cur.fetchone()
    return int(row[0])


# ---------------------------------------------------------------------------
# Match venue backfill
# ---------------------------------------------------------------------------

def _update_matches_with_venues(
    cur,
    events: List[Dict[str, Any]],
    tsdb_venue_to_id: Dict[str, int],
    verbose: bool = False,
) -> int:
    """
    For each event that has (idEvent, idVenue), set matches.venue_id
    where matches.tsdb_event_id = idEvent and venue_id IS NULL.

    Returns: number of matches updated.
    """
    cols = _get_table_columns(cur, "matches")
    if "venue_id" not in cols or "tsdb_event_id" not in cols:
        if verbose:
            print("[WARN] matches.venue_id or matches.tsdb_event_id missing; skip match updates")
        return 0

    updated = 0

    for e in events:
        tsdb_event_id = e.get("idEvent")
        tsdb_venue_id = e.get("idVenue")
        if not tsdb_event_id or not tsdb_venue_id:
            continue

        vid = tsdb_venue_to_id.get(str(tsdb_venue_id))
        if not vid:
            continue

        cur.execute(
            """
            UPDATE matches
               SET venue_id = %s
             WHERE tsdb_event_id = %s
               AND (venue_id IS NULL OR venue_id = 0)
            """,
            (vid, str(tsdb_event_id)),
        )
        if cur.rowcount > 0:
            updated += cur.rowcount

    if verbose:
        print(f"[INFO] Updated venue_id for {updated} matches")

    return updated


# ---------------------------------------------------------------------------
# MAIN ingest per league
# ---------------------------------------------------------------------------

def _ingest_venues_for_league(
    tsdb_league_id: int,
    limit_back: int,
    update_matches: bool,
    verbose: bool,
    season_sleep: float = 1.5,
) -> None:
    """
    Ingest venues for a single TSDB league id.
    """
    conn = _get_conn()
    cur = conn.cursor(cursor_factory=DictCursor)

    try:
        # Ensure venues.tsdb_venue_id is present
        _ensure_tsdb_venue_column(cur, verbose=verbose)
        conn.commit()

        tsdb_league_str = str(tsdb_league_id)
        if verbose:
            print(f"[INFO] Ingesting venues for league {tsdb_league_str}")

        # 1) Determine seasons to scan
        current = get_current_season_label(tsdb_league_str, verbose=verbose)
        if verbose:
            print(f"[TSDB] Current season for league {tsdb_league_str}: {current}")

        seasons: List[str] = []
        s = current
        for _ in range(max(1, limit_back)):
            if not s:
                break
            seasons.append(s)
            s = _previous_season_label(s)

        if verbose:
            print(f"[INFO] Seasons to scan for venues: {seasons}")

        # 2) Collect all events and unique venues across those seasons
        all_events: List[Dict[str, Any]] = []
        venue_map: Dict[str, Dict[str, Any]] = {}

        for season_label in seasons:
            if verbose:
                print(f"[INFO] Fetching events for season={season_label}")

            try:
                events = get_events_for_season_rugby(
                    tsdb_league_str,
                    season_label,
                    verbose=verbose,
                )
            except Exception as exc:
                print(
                    f"[WARN] Failed fetching events for league={tsdb_league_str}, "
                    f"season={season_label}: {exc}",
                    file=sys.stderr,
                )
                events = []

            all_events.extend(events)

            for e in events:
                vid = e.get("idVenue")
                if not vid:
                    continue
                vid_str = str(vid)

                # Only TSDB venue fields we're sure about from events
                vrec = venue_map.setdefault(
                    vid_str,
                    {
                        "tsdb_venue_id": vid_str,
                        "name": e.get("strVenue") or "",
                        "city": e.get("strCity") or "",
                        "country": e.get("strCountry") or "",
                        "latitude": None,
                        "longitude": None,
                    },
                )

                # If we later see events with a more complete name/city/country,
                # we can overwrite empties.
                if not vrec["name"] and e.get("strVenue"):
                    vrec["name"] = e["strVenue"]
                if not vrec["city"] and e.get("strCity"):
                    vrec["city"] = e["strCity"]
                if not vrec["country"] and e.get("strCountry"):
                    vrec["country"] = e["strCountry"]

            # be nice to TSDB
            time.sleep(season_sleep)

        if verbose:
            print(
                f"[INFO] Collected {len(all_events)} events and "
                f"{len(venue_map)} unique venues for league {tsdb_league_str}"
            )

        # 3) Upsert venues
        tsdb_venue_to_id: Dict[str, int] = {}

        for vid_str, vrec in sorted(venue_map.items(), key=lambda kv: kv[1]["name"]):
            venue_id = _upsert_venue(cur, vrec, verbose=verbose)
            if venue_id:
                tsdb_venue_to_id[vid_str] = venue_id

        conn.commit()
        if verbose:
            print(f"[INFO] Upserted {len(tsdb_venue_to_id)} venues for league {tsdb_league_str}")

        # 4) Optional: update matches.venue_id based on these events.
        if update_matches and all_events and tsdb_venue_to_id:
            if verbose:
                print("[INFO] Updating matches.venue_id where possible…")
            updated = _update_matches_with_venues(cur, all_events, tsdb_venue_to_id, verbose=verbose)
            conn.commit()
            if verbose:
                print(f"[INFO] Updated {updated} matches with venue_id")

    except Exception as exc:
        conn.rollback()
        print(f"[ERROR] Venue ingest failed for league {tsdb_league_id}: {exc}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Ingest venues for rugby leagues by scanning events, upserting into "
            "venues, and (optionally) backfilling matches.venue_id."
        )
    )
    parser.add_argument(
        "--tsdb-leagues",
        nargs="+",
        required=True,
        help="One or more TSDB league IDs (e.g. 4446 5167 5070)",
    )
    parser.add_argument(
        "--limit-seasons-back",
        type=int,
        default=5,
        help="How many seasons back to scan (default 5).",
    )
    parser.add_argument(
        "--update-matches",
        action="store_true",
        help="If set, also update matches.venue_id based on tsdb_event_id/idVenue.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose logging.",
    )

    args = parser.parse_args()

    for idx, lid in enumerate(args.tsdb_leagues, start=1):
        print(f"\n=== [{idx}/{len(args.tsdb_leagues)}] LEAGUE {lid} ===")
        _ingest_venues_for_league(
            tsdb_league_id=int(lid),
            limit_back=args.limit_seasons_back,
            update_matches=args.update_matches,
            verbose=args.verbose,
            season_sleep=1.5,
        )
        print(f"=== DONE league {lid} ===\n")
        # polite sleep between leagues
        time.sleep(3.0)


if __name__ == "__main__":
    main()
