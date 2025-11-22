#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ingest_urc_venues.py
--------------------

Discover all venues used in URC matches (for all URC seasons present in your DB)
via TheSportsDB, and upsert them into the `venues` table.

Strategy:
- Use DB to find all seasons for the URC league (leagues.tsdb_league_id = 4446).
- For each season, call TSDB eventsseason.php (Rugby-only events).
- Collect unique (idVenue, strVenue) combos.
- For each unique venue name, call TSDB searchvenues.php?v={strVenue}
  and pick the first result (typically contains city, country, lat/long, etc.).
- Upsert into `venues`:
    tsdb_venue_id
    name
    city
    country
    latitude
    longitude
- Optionally write a CSV snapshot of the venues we processed.

Usage:
    python .\scripts\ingest_urc_venues.py --write-csv -v

Arguments:
    --league-id       (default: 4446 = URC)
    --sleep-seconds   delay between TSDB calls (default: 0.3)
    --write-csv       write ./data/venues_urc_all.csv
"""

import os
import sys
import time
import csv
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Make sure project root is on sys.path
# ---------------------------------------------------------------------------

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ---------------------------------------------------------------------------
# DB imports
# ---------------------------------------------------------------------------

try:
    import psycopg2
    from psycopg2.extras import DictCursor
except Exception:
    print("Missing psycopg2. Install: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# HTTP imports
# ---------------------------------------------------------------------------

try:
    import requests
except Exception:
    print("Missing requests. Install: pip install requests", file=sys.stderr)
    sys.exit(1)

# Optional db helper
try:
    from db.connection import get_db_connection  # type: ignore
except Exception:
    get_db_connection = None  # type: ignore


# ---------------------------------------------------------------------------
# TSDB helpers
# ---------------------------------------------------------------------------

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
    delay = 0.8
    for attempt in range(1, max_retries + 1):
        resp = session.get(url, params=params, timeout=45)
        if resp.status_code in (502, 503, 504, 500, 429):
            if verbose:
                print(
                    f"[TSDB] HTTP {resp.status_code} on {url} {params} "
                    f"(attempt {attempt}/{max_retries}); sleeping {delay:.1f}s…",
                    file=sys.stderr,
                )
            time.sleep(delay)
            delay *= 1.6
            continue
        resp.raise_for_status()
        return resp
    resp.raise_for_status()
    return resp


def _events_for_season_rugby(
    session: requests.Session,
    api_key: str,
    league_id: str,
    season: str,
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    """
    Wrap eventsseason.php; filter where strSport starts with 'rugby'.
    """
    url = f"{_tsdb_base(api_key)}/eventsseason.php"
    resp = _get_with_backoff(
        session,
        url,
        {"id": league_id, "s": season},
        verbose=verbose,
    )
    data = resp.json() or {}
    events = data.get("events") or []
    rugby_events: List[Dict[str, Any]] = []
    for e in events:
        sport = (e.get("strSport") or "").lower()
        if sport.startswith("rugby"):
            rugby_events.append(e)
    if verbose:
        print(
            f"[TSDB] eventsseason id={league_id} season={season} "
            f"-> {len(rugby_events)} rugby events"
        )
    return rugby_events


def _search_venue_by_name(
    session: requests.Session,
    api_key: str,
    name: str,
    verbose: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Use TSDB searchvenues.php?v=Name to get venue details.
    Returns first venue dict, or None if nothing found.
    """
    url = f"{_tsdb_base(api_key)}/searchvenues.php"
    resp = _get_with_backoff(
        session,
        url,
        {"v": name},
        verbose=verbose,
    )
    data = resp.json() or {}
    venues = data.get("venues") or []
    if not venues:
        if verbose:
            print(f"[TSDB] searchvenues v={name!r} -> no results")
        return None
    v0 = venues[0]
    if verbose:
        print(
            f"[TSDB] searchvenues v={name!r} -> picked idVenue={v0.get('idVenue')} "
            f"({v0.get('strVenue')}, {v0.get('strCity')}, {v0.get('strCountry')})"
        )
    return v0


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
# Venues table helpers
# ---------------------------------------------------------------------------

_VENUES_HAS_TSDB_COLUMN: Optional[bool] = None


def _venues_has_tsdb_column(cur) -> bool:
    """
    Check once whether venues.tsdb_venue_id exists, cache result.
    """
    global _VENUES_HAS_TSDB_COLUMN
    if _VENUES_HAS_TSDB_COLUMN is not None:
        return _VENUES_HAS_TSDB_COLUMN

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
    _VENUES_HAS_TSDB_COLUMN = cur.fetchone() is not None
    return _VENUES_HAS_TSDB_COLUMN


def _upsert_venue(
    cur,
    tsdb_venue_id: Optional[str],
    name: str,
    city: Optional[str],
    country: Optional[str],
    latitude: Optional[float],
    longitude: Optional[float],
    verbose: bool = False,
) -> int:
    """
    Upsert a venue.

    Match priority:
      1. tsdb_venue_id (if column exists)
      2. (name, city, country) fallback
    """
    name = name.strip()
    if not name:
        raise ValueError("Venue name is required")

    has_tsdb = _venues_has_tsdb_column(cur)

    # 1) Try lookup by tsdb_venue_id
    if has_tsdb and tsdb_venue_id:
        cur.execute(
            "SELECT venue_id FROM venues WHERE tsdb_venue_id = %s",
            (tsdb_venue_id,),
        )
        row = cur.fetchone()
        if row:
            venue_id = row["venue_id"]
            if verbose:
                print(f"  [UPDATE] venue_id={venue_id} (match tsdb_venue_id={tsdb_venue_id})")
            cur.execute(
                """
                UPDATE venues
                SET name=%s,
                    city=%s,
                    country=%s,
                    latitude=%s,
                    longitude=%s,
                    tsdb_venue_id=%s,
                    updated_at=NOW()
                WHERE venue_id=%s
                """,
                (
                    name,
                    city,
                    country,
                    latitude,
                    longitude,
                    tsdb_venue_id,
                    venue_id,
                ),
            )
            return venue_id

    # 2) Fallback by name + city + country
    cur.execute(
        """
        SELECT venue_id
        FROM venues
        WHERE LOWER(name)=LOWER(%s)
          AND (city IS NULL OR city=%s)
          AND (country IS NULL OR country=%s)
        LIMIT 1
        """,
        (name, city, country),
    )
    row = cur.fetchone()

    if row:
        venue_id = row["venue_id"]
        if verbose:
            print(f"  [UPDATE] venue_id={venue_id} (match name/city/country)")
        if has_tsdb:
            cur.execute(
                """
                UPDATE venues
                SET name=%s,
                    city=%s,
                    country=%s,
                    latitude=%s,
                    longitude=%s,
                    tsdb_venue_id=COALESCE(tsdb_venue_id, %s),
                    updated_at=NOW()
                WHERE venue_id=%s
                """,
                (
                    name,
                    city,
                    country,
                    latitude,
                    longitude,
                    tsdb_venue_id,
                    venue_id,
                ),
            )
        else:
            cur.execute(
                """
                UPDATE venues
                SET name=%s,
                    city=%s,
                    country=%s,
                    latitude=%s,
                    longitude=%s,
                    updated_at=NOW()
                WHERE venue_id=%s
                """,
                (
                    name,
                    city,
                    country,
                    latitude,
                    longitude,
                    venue_id,
                ),
            )
        return venue_id

    # 3) INSERT new row (TSDB-only, no ESPN columns)
    if verbose:
        print(f"  [INSERT] venue '{name}' ({city}, {country}) tsdb_venue_id={tsdb_venue_id}")

    if has_tsdb:
        cur.execute(
            """
            INSERT INTO venues (
                name,
                city,
                country,
                latitude,
                longitude,
                tsdb_venue_id,
                created_at,
                updated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s, NOW(), NOW())
            RETURNING venue_id
            """,
            (
                name,
                city,
                country,
                latitude,
                longitude,
                tsdb_venue_id,
            ),
        )
    else:
        cur.execute(
            """
            INSERT INTO venues (
                name,
                city,
                country,
                latitude,
                longitude,
                created_at,
                updated_at
            )
            VALUES (%s,%s,%s,%s,%s, NOW(), NOW())
            RETURNING venue_id
            """,
            (
                name,
                city,
                country,
                latitude,
                longitude,
            ),
        )

    return cur.fetchone()[0]


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------

def _write_csv(venues: List[Dict[str, Any]]) -> str:
    os.makedirs("data", exist_ok=True)
    path = os.path.join("data", "venues_urc_all.csv")
    cols = [
        "tsdb_venue_id",
        "name",
        "city",
        "country",
        "latitude",
        "longitude",
        "source_league_ids",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for v in venues:
            w.writerow(v)
    return path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse

    _load_dotenv_if_available()

    parser = argparse.ArgumentParser(description="Ingest URC venues from TheSportsDB into Postgres.")
    parser.add_argument(
        "--league-id",
        default="4446",
        help="TSDB league id to scan (default: 4446 for URC).",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("THESPORTSDB_API_KEY", "123"),
        help="TheSportsDB V1 API key (default from THESPORTSDB_API_KEY or '123').",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.3,
        help="Sleep between TSDB calls to be nice to the API (default: 0.3).",
    )
    parser.add_argument(
        "--write-csv",
        action="store_true",
        help="Write CSV snapshot to ./data/venues_urc_all.csv",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose logging",
    )

    args = parser.parse_args()
    tsdb_league_id = str(args.league_id)
    api_key = args.api_key
    sleep_seconds = max(args.sleep_seconds, 0.0)
    verbose = args.verbose

    shown_key = api_key if len(api_key) <= 4 else api_key[:2] + "***" + api_key[-2:]
    print(f"[INFO] Using TSDB API key: '{shown_key}'")
    print(f"[INFO] Target TSDB league_id={tsdb_league_id}")

    # DB connection
    conn = _get_conn()
    cur = conn.cursor(cursor_factory=DictCursor)

    # 1) Find all seasons in DB for this league
    cur.execute(
        """
        SELECT s.season_id, s.label, s.tsdb_season_key
        FROM seasons s
        JOIN leagues l ON l.league_id = s.league_id
        WHERE l.tsdb_league_id = %s
        ORDER BY s.year ASC
        """,
        (tsdb_league_id,),
    )
    seasons = cur.fetchall()
    if not seasons:
        conn.close()
        raise SystemExit(
            f"No seasons found in DB for TSDB league_id={tsdb_league_id}. "
            "Ingest matches/seasons first."
        )

    print(f"[INFO] Found {len(seasons)} seasons in DB for TSDB league_id={tsdb_league_id}")

    # 2) For each season, pull events and collect venues
    sess = _session_with_retries()
    seen_venues: Dict[str, Dict[str, Any]] = {}  # key: normalized venue name

    for srow in seasons:
        season_label = srow["tsdb_season_key"] or srow["label"]
        if not season_label:
            continue
        season_label = season_label.strip()
        print(f"[INFO] Fetching events for season={season_label!r}")

        events = _events_for_season_rugby(
            sess,
            api_key,
            tsdb_league_id,
            season_label,
            verbose=verbose,
        )
        print(f"[INFO]  -> got {len(events)} rugby events")

        for e in events:
            id_venue = (e.get("idVenue") or "").strip()
            str_venue = (e.get("strVenue") or "").strip()
            if not str_venue:
                continue
            key = str_venue.lower()
            if key not in seen_venues:
                seen_venues[key] = {
                    "tsdb_venue_id": id_venue or None,
                    "name": str_venue,
                    "city": None,
                    "country": None,
                    "latitude": None,
                    "longitude": None,
                    "source_league_ids": {tsdb_league_id},
                }
            else:
                seen_venues[key]["source_league_ids"].add(tsdb_league_id)

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    print(f"[INFO] Discovered {len(seen_venues)} unique venue names from events.")

    # 3) Enrich each venue via searchvenues, then upsert into DB
    enriched: List[Dict[str, Any]] = []
    try:
        for i, (key, vinfo) in enumerate(seen_venues.items(), start=1):
            name = vinfo["name"]
            tsdb_venue_id = vinfo["tsdb_venue_id"]

            print(f"[VENUE {i}/{len(seen_venues)}] {name} (initial tsdb_venue_id={tsdb_venue_id})")
            venue_details = _search_venue_by_name(sess, api_key, name, verbose=verbose)

            city = None
            country = None
            lat = None
            lon = None
            if venue_details:
                tsdb_venue_id = (
                    (venue_details.get("idVenue") or tsdb_venue_id or "").strip()
                    or None
                )
                city = (venue_details.get("strCity") or "").strip() or None
                country = (venue_details.get("strCountry") or "").strip() or None

                def _to_float(x):
                    try:
                        return float(x) if x not in (None, "", "null") else None
                    except Exception:
                        return None

                lat = _to_float(venue_details.get("strLatitude"))
                lon = _to_float(venue_details.get("strLongitude"))

            venue_id_db = _upsert_venue(
                cur,
                tsdb_venue_id,
                name,
                city,
                country,
                lat,
                lon,
                verbose=verbose,
            )

            enriched.append(
                {
                    "tsdb_venue_id": tsdb_venue_id,
                    "name": name,
                    "city": city,
                    "country": country,
                    "latitude": lat,
                    "longitude": lon,
                    "source_league_ids": ",".join(sorted(vinfo["source_league_ids"])),
                }
            )

        conn.commit()
        print(f"[DONE] Ingested/updated {len(enriched)} venues into DB.")

        if args.write_csv:
            path = _write_csv(enriched)
            print(f"[OK] Wrote CSV: {path}")

    except Exception as exc:
        conn.rollback()
        print(f"[ERROR] Ingestion failed, rolled back transaction: {exc}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
