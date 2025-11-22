#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
fetch_urc_teams_by_name.py
--------------------------

Robust URC teams loader that *does not* rely on numeric team IDs, since TheSportsDB
is currently returning Arsenal for some of those IDs.

Instead, we:
  * Hard-code the 16 URC team NAMES.
  * Use v1 search endpoint: /searchteams.php?t={name}.
  * Filter results to sport starting with "Rugby" to avoid soccer teams.
  * Prefer an exact strTeam match on the expected name.
  * Upsert into your `teams` table with a tsdb_team_id column.

Usage:
    python .\scripts\fetch_urc_teams_by_name.py --write-csv -v

"""

import os
import re
import sys
import csv
import time
from typing import Any, Dict, List, Optional

# --- HTTP --------------------------------------------------------------------
try:
    import requests
except ImportError:
    print("Missing dependency: requests (pip install requests)", file=sys.stderr)
    sys.exit(1)

# --- DB: psycopg v3 preferred, fallback to psycopg2 --------------------------
try:
    import psycopg  # v3
    _PSYCOPG_VERSION = 3
except ImportError:
    try:
        import psycopg2 as psycopg  # v2
        _PSYCOPG_VERSION = 2
    except ImportError:
        print(
            "Missing dependency: psycopg or psycopg2-binary\n"
            "Install one of:\n"
            "  pip install psycopg[binary]\n"
            "  or\n"
            "  pip install psycopg2-binary",
            file=sys.stderr,
        )
        sys.exit(1)


# --- Config: our 16 URC teams by name ----------------------------------------
URC_TEAM_TARGETS: List[Dict[str, str]] = [
    {"expected_name": "Benetton",        "search": "Benetton"},
    {"expected_name": "Bulls",           "search": "Bulls"},
    {"expected_name": "Cardiff Rugby",   "search": "Cardiff Rugby"},
    {"expected_name": "Connacht",        "search": "Connacht"},
    {"expected_name": "Dragons",         "search": "Dragons"},
    {"expected_name": "Edinburgh",       "search": "Edinburgh"},
    {"expected_name": "Glasgow",         "search": "Glasgow"},
    {"expected_name": "Leinster",        "search": "Leinster"},
    {"expected_name": "Lions",           "search": "Lions"},
    {"expected_name": "Munster",         "search": "Munster"},
    {"expected_name": "Ospreys",         "search": "Ospreys"},
    {"expected_name": "Scarlets",        "search": "Scarlets"},
    {"expected_name": "Stormers",        "search": "Stormers"},
    {"expected_name": "The Sharks",      "search": "The Sharks"},
    {"expected_name": "Ulster",          "search": "Ulster"},
    {"expected_name": "Zebre",           "search": "Zebre"},
]


def _load_dotenv_if_available() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        # dotenv is optional
        pass


def _clean(x: Optional[str]) -> Optional[str]:
    if x is None:
        return None
    x = x.strip()
    return x or None


def _derive_short_and_abbr(full_name: str,
                           str_team_short: Optional[str],
                           _str_alt: Optional[str]) -> (Optional[str], Optional[str]):
    """
    Heuristics to derive short_name and abbreviation for rugby teams.
    """
    short = _clean(str_team_short)
    name = _clean(full_name) or ""
    if not short:
        # Strip common suffixes like "Rugby", "RFC", "RC"
        short = re.sub(
            r"\s+(Rugby(?: Club| Football Club)?|RFC|RC)$",
            "",
            name,
            flags=re.IGNORECASE,
        )
        if short == name and "Rugby" in name:
            short = name.split("Rugby")[0].strip()
        if not short:
            short = name

    tokens = re.findall(r"[A-Za-z]+", short)
    abbr = "".join(tok[0] for tok in tokens).upper()
    if len(abbr) < 2:
        abbr = (short[:3]).upper()
    if len(abbr) > 4:
        abbr = abbr[:4]
    return short, abbr


# --- TheSportsDB helpers -----------------------------------------------------
def _tsdb_base(api_key: str) -> str:
    return f"https://www.thesportsdb.com/api/v1/json/{api_key}"


def _search_team(api_key: str, search: str, expected_name: str, verbose: bool = False) -> Optional[Dict[str, Any]]:
    """
    Use searchteams.php?t={search} and pick the best rugby match.

    Selection:
      1) Filter to strSport starting with 'Rugby'
      2) Prefer exact strTeam == expected_name (case-insensitive)
      3) Else return first rugby match, or None if none found.
    """
    url = f"{_tsdb_base(api_key)}/searchteams.php"
    resp = requests.get(url, params={"t": search}, timeout=30)
    resp.raise_for_status()
    payload = resp.json() or {}
    teams = payload.get("teams") or []
    if not teams:
        if verbose:
            print(f"[WARN] No teams found for search={search!r}", file=sys.stderr)
        return None

    rugby_teams = [
        t for t in teams
        if (t.get("strSport") or "").lower().startswith("rugby")
    ]
    if not rugby_teams:
        if verbose:
            print(f"[WARN] No rugby teams found for search={search!r}", file=sys.stderr)
        return None

    # Prefer exact name match
    expected_lower = expected_name.lower()
    for t in rugby_teams:
        if (t.get("strTeam") or "").lower() == expected_lower:
            return t

    # Fallback: first rugby match
    if verbose and rugby_teams:
        names = ", ".join(t.get("strTeam") or "?" for t in rugby_teams)
        print(f"[INFO] Using first rugby hit for search={search!r}: candidates={names}", file=sys.stderr)
    return rugby_teams[0]


# --- DB helpers --------------------------------------------------------------
def _get_db_connection():
    """
    Prefer your local db module, else use env.

    db module (rugby-analytics/db) must expose one of:
      - get_connection()
      - get_db()
      - connect()

    Otherwise we use DATABASE_URL or PG* env vars.
    """
    try:
        import db  # your project module
        for fn_name in ("get_connection", "get_db", "connect"):
            if hasattr(db, fn_name):
                return getattr(db, fn_name)()
    except Exception:
        pass

    dsn = os.getenv("DATABASE_URL") or os.getenv("PG_DSN")
    if dsn:
        return psycopg.connect(dsn)

    params = {
        "host": os.getenv("PGHOST"),
        "port": os.getenv("PGPORT"),
        "user": os.getenv("PGUSER"),
        "password": os.getenv("PGPASSWORD"),
        "dbname": os.getenv("PGDATABASE"),
    }
    if params["host"]:
        return psycopg.connect(**params)

    raise RuntimeError(
        "No DB connection available. Provide a `db` module or set DATABASE_URL / PG* env vars."
    )


def _ensure_tsdb_columns(conn) -> None:
    """
    Ensure tsdb_team_id BIGINT and unique index exist on teams.
    """
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE teams ADD COLUMN IF NOT EXISTS tsdb_team_id BIGINT")
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS teams_tsdb_team_id_uk "
            "ON teams(tsdb_team_id)"
        )
    conn.commit()


def _upsert_team(conn, t: Dict[str, Any]) -> str:
    """
    Upsert a team into teams.

    Returns: 'updated_by_tsdb_id' | 'matched_by_name' | 'inserted'
    """
    tsdb_team_id = t.get("idTeam")
    name = _clean(t.get("strTeam")) or ""
    country = _clean(t.get("strCountry"))
    str_team_short = _clean(t.get("strTeamShort"))
    str_alt = _clean(t.get("strAlternate"))
    short_name, abbreviation = _derive_short_and_abbr(name, str_team_short, str_alt)

    with conn.cursor() as cur:
        # 1) Update by tsdb_team_id
        cur.execute(
            """
            UPDATE teams
               SET name = %s,
                   short_name = COALESCE(%s, short_name),
                   abbreviation = COALESCE(%s, abbreviation),
                   country = COALESCE(%s, country)
             WHERE tsdb_team_id = %s
            """,
            (name, short_name, abbreviation, country, tsdb_team_id),
        )
        if getattr(cur, "rowcount", 0) > 0:
            return "updated_by_tsdb_id"

        # 2) Attach tsdb id to existing team by name (case-insensitive)
        cur.execute(
            """
            UPDATE teams
               SET tsdb_team_id = %s,
                   short_name = COALESCE(%s, short_name),
                   abbreviation = COALESCE(%s, abbreviation),
                   country = COALESCE(%s, country)
             WHERE LOWER(name) = LOWER(%s) AND tsdb_team_id IS NULL
            """,
            (tsdb_team_id, short_name, abbreviation, country, name),
        )
        if getattr(cur, "rowcount", 0) > 0:
            return "matched_by_name"

        # 3) Insert new row
        cur.execute(
            """
            INSERT INTO teams (name, short_name, abbreviation, country, tsdb_team_id)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (name, short_name, abbreviation, country, tsdb_team_id),
        )
        return "inserted"


def _write_csv(teams: List[Dict[str, Any]], out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    cols = ["idTeam", "strTeam", "strTeamShort", "strAlternate", "strCountry", "strSport"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for t in teams:
            row = {c: t.get(c) for c in cols}
            w.writerow(row)


# --- Main --------------------------------------------------------------------
def main() -> None:
    import argparse

    _load_dotenv_if_available()

    parser = argparse.ArgumentParser(
        description="Fetch URC teams by name from TheSportsDB and upsert into teams table."
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("THESPORTSDB_API_KEY", "1"),
        help="TheSportsDB API key (default: 1 = public test key)",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=1.5,
        help="Seconds to sleep between API calls to avoid 429 (default: 1.5)",
    )
    parser.add_argument(
        "--write-csv",
        action="store_true",
        help="Write CSV snapshot to ./data/urc_teams_by_name.csv",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose logging",
    )
    args = parser.parse_args()

    api_key = args.api_key
    sleep_seconds = max(args.sleep_seconds, 0.0)

    if args.verbose:
        print(f"[INFO] Using TheSportsDB key={api_key!r}")
        print(f"[INFO] Sleep between calls: {sleep_seconds:.2f}s")

    # 1) Fetch teams via search
    teams: List[Dict[str, Any]] = []
    for idx, target in enumerate(URC_TEAM_TARGETS, start=1):
        expected = target["expected_name"]
        search = target["search"]
        if args.verbose:
            print(f"[INFO] ({idx}/{len(URC_TEAM_TARGETS)}) search={search!r} expected={expected!r}")
        try:
            team = _search_team(api_key, search=search, expected_name=expected, verbose=args.verbose)
        except requests.HTTPError as e:
            print(f"[WARN] HTTP error during search for {search!r}: {e}", file=sys.stderr)
            team = None

        if team:
            teams.append(team)
        else:
            print(f"[WARN] No rugby team resolved for {expected!r}", file=sys.stderr)

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    if not teams:
        raise SystemExit("No URC teams fetched from TheSportsDB (by name). Aborting.")

    if args.verbose:
        names = [t.get("strTeam") for t in teams]
        print(f"[INFO] Successfully resolved {len(teams)} teams: {', '.join(filter(None, names))}")

    # 2) Upsert into DB
    conn = _get_db_connection()
    try:
        _ensure_tsdb_columns(conn)
        inserted = updated_by_id = matched_by_name = 0

        for t in teams:
            outcome = _upsert_team(conn, t)
            if outcome == "inserted":
                inserted += 1
            elif outcome == "updated_by_tsdb_id":
                updated_by_id += 1
            elif outcome == "matched_by_name":
                matched_by_name += 1

        conn.commit()
        print(
            "[OK] Teams upsert complete -> "
            f"inserted={inserted}, updated_by_tsdb_id={updated_by_id}, matched_by_name={matched_by_name}"
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass

    # 3) Optional CSV snapshot
    if args.write_csv:
        out_csv = os.path.join(os.getcwd(), "data", "urc_teams_by_name.csv")
        _write_csv(teams, out_csv)
        print(f"[OK] Wrote CSV snapshot: {out_csv}")


if __name__ == "__main__":
    main()
