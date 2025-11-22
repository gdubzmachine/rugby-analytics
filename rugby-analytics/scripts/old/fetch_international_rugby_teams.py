#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""
fetch_international_rugby_teams.py
----------------------------------

Fetch ALL rugby international teams (as far as TheSportsDB knows) by
discovering teams from a set of international competitions, then upsert
them into your `teams` table and write a CSV snapshot.

Strategy:
- Use the v1 endpoint: search_all_teams.php?id={idLeague}
- Target ONLY international rugby union competitions:
    * Rugby World Cup                (4574)
    * Six Nations Championship       (4714)
    * Rugby Championship             (4986)
    * Pacific Nations Cup            (4985)
    * Rugby Europe Championship      (4983)
    * Autumn Nations Cup             (4984)
    * Six Nations Women              (5563)
    * Six Nations Under 20s          (5082)
- Filter results where strSport starts with "Rugby".
- Dedupe by idTeam (tsdb_team_id).
- Upsert each unique team into `teams` using tsdb_team_id BIGINT + unique index.
- Write CSV to ./data/international_rugby_teams_all.csv

This should give you a much broader *international teams* set than the
previous hard-coded Tier 1 list.
"""

import os
import re
import sys
import csv
import time
from typing import Any, Dict, List, Optional

# --- HTTP client -------------------------------------------------------------
try:
    import requests
except ImportError:
    print("Missing dependency: requests (pip install requests)", file=sys.stderr)
    sys.exit(1)

# --- DB client: psycopg v3 preferred, fallback to psycopg2 -------------------
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


# --- Config: international rugby leagues to mine -----------------------------

INTERNATIONAL_RUGBY_LEAGUES: List[Dict[str, str]] = [
    # World-level
    {"idLeague": "4574", "name": "Rugby World Cup"},
    # Tier 1 North
    {"idLeague": "4714", "name": "Six Nations Championship"},
    # Tier 1 South
    {"idLeague": "4986", "name": "Rugby Championship"},
    # Tier 2 / Tier 3
    {"idLeague": "4983", "name": "Rugby Europe Championship"},
    {"idLeague": "4985", "name": "Pacific Nations Cup"},
    {"idLeague": "4984", "name": "Autumn Nations Cup"},
    # Women & age grade (still national teams)
    {"idLeague": "5563", "name": "Six Nations Women"},
    {"idLeague": "5082", "name": "Six Nations Under 20s Championship"},
]


# --- Helpers -----------------------------------------------------------------
def _load_dotenv_if_available() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        # optional
        pass


def _clean(x: Optional[str]) -> Optional[str]:
    if x is None:
        return None
    x = x.strip()
    return x or None


def _derive_short_and_abbr(
    full_name: str,
    str_team_short: Optional[str],
    _str_alt: Optional[str],
) -> (Optional[str], Optional[str]):
    """
    Simple heuristics for short_name and abbreviation for rugby teams.
    """
    short = _clean(str_team_short)
    name = _clean(full_name) or ""
    if not short:
        # Remove suffixes like "Rugby", "Rugby Union", "Rugby Team", "RFC", "RC"
        short = re.sub(
            r"\s+(Rugby(?: Union)?|Rugby Team|RFC|RC)$",
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


def _search_all_teams_for_league(
    api_key: str,
    id_league: str,
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    """
    search_all_teams.php?id={idLeague}
    Return a list of teams for that league, filtered to rugby.
    """
    url = f"{_tsdb_base(api_key)}/search_all_teams.php"
    resp = requests.get(url, params={"id": id_league}, timeout=45)
    resp.raise_for_status()
    payload = resp.json() or {}
    teams = payload.get("teams") or []
    if not teams:
        if verbose:
            print(f"[WARN] No teams returned for league id={id_league}", file=sys.stderr)
        return []

    rugby_teams = [
        t for t in teams
        if (t.get("strSport") or "").lower().startswith("rugby")
    ]
    if verbose:
        all_names = ", ".join(t.get("strTeam") or "?" for t in rugby_teams)
        print(
            f"[INFO]  -> league id={id_league} rugby teams: "
            f"{len(rugby_teams)} [{all_names}]"
        )
    return rugby_teams


# --- DB helpers --------------------------------------------------------------
def _get_db_connection():
    """
    Prefer your local db module (rugby-analytics/db) if present, else use env.
    """
    try:
        import db  # project-local module
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
        "No DB connection available. Provide a db module or set DATABASE_URL / PG* env vars."
    )


def _ensure_tsdb_columns(conn) -> None:
    """
    Ensure a tsdb_team_id column + unique index exists on teams.
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
    Upsert a team into `teams` by tsdb_team_id, then by name.
    Returns one of: 'inserted', 'updated_by_tsdb_id', 'matched_by_name'.
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

        # 2) Attach tsdb id to existing row by case-insensitive name
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
    cols = [
        "idTeam",
        "strTeam",
        "strTeamShort",
        "strAlternate",
        "strCountry",
        "strLeague",
        "idLeague",
        "strStadium",
        "strStadiumLocation",
        "intStadiumCapacity",
        "strWebsite",
        "strFacebook",
        "strTwitter",
        "strInstagram",
        "strSport",
        "strDescriptionEN",
    ]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        for t in teams:
            row = {c: t.get(c) for c in cols}
            writer.writerow(row)


# --- Main --------------------------------------------------------------------
def main() -> None:
    import argparse

    _load_dotenv_if_available()

    parser = argparse.ArgumentParser(
        description="Fetch international rugby union teams from TheSportsDB (via multiple leagues) and upsert into teams table."
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("THESPORTSDB_API_KEY", "1"),
        help="TheSportsDB API key (default: 1 = public test key).",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=1.5,
        help="Seconds to sleep between API calls per league (default: 1.5).",
    )
    parser.add_argument(
        "--write-csv",
        dest="write_csv",
        action="store_true",
        help="Write CSV snapshot to ./data/international_rugby_teams_all.csv",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose logging.",
    )
    args = parser.parse_args()

    api_key = args.api_key
    sleep_seconds = max(args.sleep_seconds, 0.0)

    if args.verbose:
        print(f"[INFO] Using TheSportsDB key={api_key!r}")
        print(f"[INFO] Sleep between league calls: {sleep_seconds:.2f}s")
        print(f"[INFO] International leagues configured: {len(INTERNATIONAL_RUGBY_LEAGUES)}")

    # 1) Fetch all teams from all configured international leagues
    teams_by_id: Dict[str, Dict[str, Any]] = {}

    for idx, league in enumerate(INTERNATIONAL_RUGBY_LEAGUES, start=1):
        lid = league["idLeague"]
        lname = league["name"]
        if args.verbose:
            print(f"[INFO] League {idx}/{len(INTERNATIONAL_RUGBY_LEAGUES)}: {lname} (id={lid})")
        try:
            rugby_teams = _search_all_teams_for_league(api_key, lid, verbose=args.verbose)
        except requests.HTTPError as e:
            print(f"[WARN] HTTP error while fetching teams for league id={lid}: {e}", file=sys.stderr)
            rugby_teams = []

        for t in rugby_teams:
            tid = str(t.get("idTeam"))
            # If the team already exists from another league, keep the first one
            if tid not in teams_by_id:
                teams_by_id[tid] = t

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    if not teams_by_id:
        raise SystemExit("No international rugby teams fetched from any league. Aborting.")

    all_teams: List[Dict[str, Any]] = list(teams_by_id.values())
    if args.verbose:
        names = ", ".join(sorted(set(filter(None, (t.get("strTeam") for t in all_teams)))))
        print(f"[INFO] Unique international rugby teams discovered: {len(all_teams)}")
        print(f"[INFO] Team names: {names}")

    # 2) Upsert into DB
    conn = _get_db_connection()
    try:
        _ensure_tsdb_columns(conn)
        inserted = updated_by_id = matched_by_name = 0

        for t in all_teams:
            outcome = _upsert_team(conn, t)
            if outcome == "inserted":
                inserted += 1
            elif outcome == "updated_by_tsdb_id":
                updated_by_id += 1
            elif outcome == "matched_by_name":
                matched_by_name += 1

        conn.commit()
        print(
            "[OK] International teams upsert complete -> "
            f"inserted={inserted}, updated_by_tsdb_id={updated_by_id}, matched_by_name={matched_by_name}"
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass

    # 3) Optional CSV snapshot
    if args.write_csv:
        out_csv = os.path.join(os.getcwd(), "data", "international_rugby_teams_all.csv")
        _write_csv(all_teams, out_csv)
        print(f"[OK] Wrote CSV snapshot: {out_csv}")


if __name__ == "__main__":
    main()
