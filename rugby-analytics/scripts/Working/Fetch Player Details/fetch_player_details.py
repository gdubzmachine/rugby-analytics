#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
fetch_player_details.py
-----------------------

Given a TheSportsDB player ID (idPlayer), fetch as much detail as the API
exposes for that player and write it out to:

  - JSON: ./data/player_{idPlayer}.json  (full raw payload)
  - CSV:  ./data/player_{idPlayer}.csv   (selected columns)

This script is **read-only** with respect to your database: it does not
insert or update anything. It's a data exploration / inspection tool so
you can see exactly what is available per player before wiring it into
your ingest pipeline.
"""

import os
import sys
import json
import csv
from typing import Any, Dict, List, Optional

# --- HTTP client -------------------------------------------------------------
try:
    import requests
except ImportError:
    print("Missing dependency: requests (pip install requests)", file=sys.stderr)
    sys.exit(1)


# --- Env helpers -------------------------------------------------------------
def _load_dotenv_if_available() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        # optional
        pass


def _tsdb_base(api_key: str) -> str:
    return f"https://www.thesportsdb.com/api/v1/json/{api_key}"


# --- TheSportsDB calls -------------------------------------------------------
def _lookup_player(api_key: str, player_id: str) -> Optional[Dict[str, Any]]:
    """
    Call v1 /lookupplayer.php?id={player_id} and return the player dict, or None.
    """
    url = f"{_tsdb_base(api_key)}/lookupplayer.php"
    resp = requests.get(url, params={"id": player_id}, timeout=30)
    resp.raise_for_status()
    payload = resp.json() or {}
    players = payload.get("players") or payload.get("player") or []
    if not players:
        return None
    # TheSportsDB returns a list; first element is the one we want
    return players[0]


# --- File writers ------------------------------------------------------------
def _ensure_data_dir() -> str:
    root = os.getcwd()
    data_dir = os.path.join(root, "data")
    os.makedirs(data_dir, exist_ok=True)
    return data_dir


def _write_json(player: Dict[str, Any], player_id: str) -> str:
    data_dir = _ensure_data_dir()
    path = os.path.join(data_dir, f"player_{player_id}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(player, f, ensure_ascii=False, indent=2)
    return path


def _write_csv(player: Dict[str, Any], player_id: str) -> str:
    """
    Write a 1-row CSV with the most interesting fields.
    You can extend this list later once you see the JSON.
    """
    data_dir = _ensure_data_dir()
    path = os.path.join(data_dir, f"player_{player_id}.csv")

    # Common fields exposed by TheSportsDB for players
    # (We'll safely .get() them; if missing, they will be empty.)
    cols = [
        "idPlayer",
        "strPlayer",
        "strNationality",
        "dateBorn",
        "strBirthLocation",
        "strNumber",
        "strPosition",
        "strTeam",
        "strTeam2",
        "strSport",
        "strHeight",
        "strWeight",
        "strThumb",
        "strCutout",
        "strRender",
        "strBanner",
        "strDescriptionEN",
        "strSide",
        "strFacebook",
        "strInstagram",
        "strTwitter",
        "strWage",
        "strKit",
    ]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        row = {c: player.get(c) for c in cols}
        writer.writerow(row)

    return path


# --- Pretty-print summary ----------------------------------------------------
def _print_summary(player: Dict[str, Any]) -> None:
    def g(key: str) -> Optional[str]:
        val = player.get(key)
        if val is None:
            return None
        if isinstance(val, str):
            val = val.strip()
        return str(val) or None

    print("\n=== Player summary ===")
    print(f"  idPlayer       : {g('idPlayer')}")
    print(f"  Name           : {g('strPlayer')}")
    print(f"  Sport          : {g('strSport')}")
    print(f"  Nationality    : {g('strNationality')}")
    print(f"  Date of Birth  : {g('dateBorn')}")
    print(f"  Birthplace     : {g('strBirthLocation')}")
    print(f"  Position       : {g('strPosition')}")
    print(f"  Team           : {g('strTeam')}")
    print(f"  Team2          : {g('strTeam2')}")
    print(f"  Height         : {g('strHeight')}")
    print(f"  Weight         : {g('strWeight')}")
    desc = g("strDescriptionEN")
    if desc:
        snippet = (desc[:180] + "…") if len(desc) > 180 else desc
        print(f"  DescriptionEN  : {snippet}")
    thumb = g("strThumb")
    if thumb:
        print(f"  Thumbnail URL  : {thumb}")
    print("======================\n")


# --- Main --------------------------------------------------------------------
def main() -> None:
    import argparse

    _load_dotenv_if_available()

    parser = argparse.ArgumentParser(
        description="Fetch full TheSportsDB details for a single player."
    )
    parser.add_argument(
        "--player-id",
        required=True,
        help="TheSportsDB player ID (idPlayer).",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("THESPORTSDB_API_KEY", "1"),
        help="TheSportsDB API key (default: 1 = public test key).",
    )
    parser.add_argument(
        "--write-json",
        action="store_true",
        help="Write full JSON payload to ./data/player_{id}.json",
    )
    parser.add_argument(
        "--write-csv",
        action="store_true",
        help="Write selected fields to ./data/player_{id}.csv",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose logging.",
    )
    args = parser.parse_args()

    player_id = str(args.player_id)
    api_key = args.api_key

    if args.verbose:
        print(f"[INFO] Using TheSportsDB key={api_key!r}")
        print(f"[INFO] Fetching details for idPlayer={player_id}")

    try:
        player = _lookup_player(api_key, player_id)
    except requests.HTTPError as e:
        print(f"[ERROR] HTTP error from TheSportsDB for idPlayer={player_id}: {e}", file=sys.stderr)
        raise SystemExit(1)

    if not player:
        print(f"[ERROR] No player record found for idPlayer={player_id}", file=sys.stderr)
        raise SystemExit(1)

    # Console summary
    _print_summary(player)

    # Optional outputs
    if args.write_json:
        json_path = _write_json(player, player_id)
        print(f"[OK] Wrote JSON: {json_path}")

    if args.write_csv:
        csv_path = _write_csv(player, player_id)
        print(f"[OK] Wrote CSV:  {csv_path}")


if __name__ == "__main__":
    main()
