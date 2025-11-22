# src/etl/import_players.py
import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from typing import Dict, Any, List

load_dotenv()

# Config
DB_URL = os.getenv("DATABASE_URL")
THESPORTSDB_API_KEY = os.getenv("THESPORTSDB_API_KEY")
ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/rugby"
HEADERS = {"User-Agent": "RugbyAnalytics/1.0"}
SIX_NATIONS_LEAGUE_ID = "13"  # Confirmed working ID for Six Nations

# Pre-defined date windows for Six Nations (avoids 400 errors on full years)
SIX_NATIONS_WINDOWS = [
    {"year": 2021, "start": "20210201", "end": "20210331"},
    {"year": 2022, "start": "20220201", "end": "20220331"},
    {"year": 2023, "start": "20230201", "end": "20230331"},
    {"year": 2024, "start": "20240201", "end": "20240331"},
    {"year": 2025, "start": "20250101", "end": "20250331"},  # Future, but API may have previews
]

# Fallback: Known event IDs from recent Six Nations (if scoreboard fails)
KNOWN_EVENT_IDS = ["401456789", "401456790", "401456791"]  # Examples from 2023; expand via manual lookup

engine = create_engine(DB_URL)

def safe_request(url: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=10)
        if resp.status_code == 400:
            print(f"❌ 400 Bad Request for {url}. Skipping window.")
            return {}
        if resp.status_code == 404:
            print(f"❌ 404 Not Found for {url}. Invalid league/date.")
            return {}
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return {}

def get_the_sportsdb_player(player_name: str, nationality: str = None) -> Dict[str, Any]:
    if not THESPORTSDB_API_KEY:
        return {}
    search_url = f"https://www.thesportsdb.com/api/v1/json/{THESPORTSDB_API_KEY}/searchplayers.php?p={player_name.replace(' ', '%20')}"
    data = safe_request(search_url)
    players = data.get("player", [])
    if nationality and isinstance(players, list):
        players = [p for p in players if p.get("strNationality") == nationality]
    return players[0] if players else {}

def upsert_position(code: str, name: str, category: str = "other") -> int:
    sql = """
    INSERT INTO positions (code, name, category)
    VALUES (:code, :name, :category::position_category)
    ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name, category = EXCLUDED.category
    RETURNING position_id
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql), {"code": code, "name": name, "category": category})
        conn.commit()
        return result.scalar_one()

def upsert_team(espn_id: str, name: str, country: str = None) -> int:
    sql = """
    INSERT INTO teams (espn_team_id, name, country)
    VALUES (:espn_id, :name, :country)
    ON CONFLICT (espn_team_id) DO NOTHING
    RETURNING team_id
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql), {"espn_id": espn_id, "name": name, "country": country})
        if result.rowcount == 0:
            result = conn.execute(text("SELECT team_id FROM teams WHERE espn_team_id = :espn_id"), {"espn_id": espn_id})
            team_id = result.scalar_one()
        else:
            team_id = result.scalar_one()
        conn.commit()
        return team_id

def upsert_player(player_data: Dict[str, Any], team_espn_id: str, season_year: int) -> bool:
    espn_id = player_data.get("id")
    if not espn_id:
        return False

    full_name = player_data.get("displayName") or player_data.get("full_name", "").strip()
    if not full_name:
        return False

    first_name, last_name = full_name.rsplit(" ", 1) if " " in full_name else (full_name, "")
    dob_str = player_data.get("dateOfBirth")
    dob = datetime.strptime(dob_str, "%Y-%m-%d").date() if dob_str else None
    nationality = player_data.get("nationality") or player_data.get("strNationality")
    pos_obj = player_data.get("position", {})
    pos_code = pos_obj.get("abbreviation", "UTIL")
    pos_name = pos_obj.get("name", "Utility")

    # Upsert position
    position_id = upsert_position(pos_code, pos_name)

    # Upsert player
    sql = """
    INSERT INTO players (espn_player_id, full_name, first_name, last_name, date_of_birth, nationality, preferred_position_id)
    VALUES (:espn_id, :full_name, :first_name, :last_name, :dob, :nationality, :pos_id)
    ON CONFLICT (espn_player_id) DO UPDATE SET
        full_name = EXCLUDED.full_name, nationality = EXCLUDEDa