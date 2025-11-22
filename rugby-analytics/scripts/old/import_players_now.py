# scripts/import_big_names.py
import os
import csv
import requests
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DB_URL = os.getenv("DATABASE_URL")
API_KEY = os.getenv("THESPORTSDB_API_KEY", "123")
BASE_URL = f"https://www.thesportsdb.com/api/v1/json/{API_KEY}"

engine = create_engine(DB_URL)

CSV_FILE = "data/big_rugby_players_2021_2025.csv"
os.makedirs("data", exist_ok=True)

# ----------------------------------------------------------------------
# CSV header
with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["full_name", "dob", "nationality", "position", "club", "team"])

# ----------------------------------------------------------------------
# BIG NAME LISTS
SPRINGBOKS_2023_RWC = [
    "Siya Kolisi", "Eben Etzebeth", "Cheslin Kolbe", "Handre Pollard", "Damian de Allende",
    "Kurt-Lee Arendse", "Willie le Roux", "Franco Mostert", "Steven Kitshoff", "Malcolm Marx",
    "Bongi Mbonambi", "Frans Malherbe", "Vincent Koch", "Ox Nche", "Trevor Nyakane",
    "RG Snyman", "Pieter-Steph du Toit", "Duane Vermeulen", "Kwagga Smith", "Marco van Staden",
    "Jasper Wiese", "Faf de Klerk", "Grant Williams", "Cobus Reinach", "Damian Willemse",
    "Jesse Kriel", "Lukhanyo Am", "Canan Moodie", "Makazole Mapimpi", "Andre Esterhuizen"
]

ALL_BLACKS_2023_RWC = [
    "Ardie Savea", "Sam Cane", "Aaron Smith", "Beauden Barrett", "Richie Mo'unga",
    "Jordie Barrett", "Will Jordan", "Rieko Ioane", "Caleb Clarke", "Mark Telea",
    "Codie Taylor", "Dane Coles", "Ethan de Groot", "Nepo Laulala", "Tyrel Lomax",
    "Fletcher Newell", "Scott Barrett", "Brodie Retallick", "Sam Whitelock", "Shannon Frizell",
    "Dalton Papalii", "Samipeni Finau", "Luke Jacobson", "Damian McKenzie", "David Havili",
    "Anton Lienert-Brown", "Braydon Ennor", "Leicester Fainga'anuku"
]

OTHER_BIG_NAMES = [
    "Antoine Dupont", "Owen Farrell", "Maro Itoje", "Marcus Smith", "Johnny Sexton",
    "Tadhg Furlong", "Gregory Alldritt", "Josh van der Flier", "Bundee Aki", "Garry Ringrose"
]

ALL_BIG_NAMES = list(set(SPRINGBOKS_2023_RWC + ALL_BLACKS_2023_RWC + OTHER_BIG_NAMES))

# ----------------------------------------------------------------------
def safe_request(url):
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"API error: {e}")
        return {}

# ----------------------------------------------------------------------
def upsert_position(name):
    name = name or "Unknown"
    code = "".join(c for c in name if c.isalnum()).upper()[:4] or "UNK"
    sql = """
    INSERT INTO positions (code, name, category)
    VALUES (:c, :n, 'other'::position_category)
    ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name
    RETURNING position_id
    """
    with engine.connect() as conn:
        return conn.execute(text(sql), {"c": code, "n": name}).scalar_one()

# ----------------------------------------------------------------------
def upsert_team(name, country):
    name = name or "Unknown Club"
    country = country or "Unknown"
    sql = """
    INSERT INTO teams (name, country)
    VALUES (:n, :c)
    ON CONFLICT (name) DO NOTHING
    RETURNING team_id
    """
    with engine.connect() as conn:
        res = conn.execute(text(sql), {"n": name, "c": country})
        if res.rowcount == 0:                     # already exists
            res = conn.execute(text("SELECT team_id FROM teams WHERE name = :n"), {"n": name})
            team_id = res.scalar_one()
        else:
            team_id = res.scalar_one()
        conn.commit()
        return team_id

# ----------------------------------------------------------------------
def get_or_create_season():
    """Return season_id for league_id=1, year=2024."""
    sql_insert = """
    INSERT INTO seasons (league_id, year) VALUES (1, 2024)
    ON CONFLICT (league_id, year) DO NOTHING
    RETURNING season_id
    """
    sql_select = "SELECT season_id FROM seasons WHERE league_id = 1 AND year = 2024"
    with engine.connect() as conn:
        res = conn.execute(text(sql_insert)).scalar_one_or_none()
        if res is None:
            res = conn.execute(text(sql_select)).scalar_one()
        conn.commit()
        return res

# ----------------------------------------------------------------------
def import_player_from_api(name):
    url = f"{BASE_URL}/searchplayers.php?p={name.replace(' ', '%20')}"
    data = safe_request(url)
    if not data or not data.get("player"):
        return False

    p = data["player"][0]
    if p.get("strSport") != "Rugby":
        return False

    # ---- DOB -------------------------------------------------------
    dob_str = p.get("dateBorn", "")
    dob = None
    if dob_str and len(dob_str) >= 4:
        try:
            dob = datetime.strptime(dob_str.split(" ")[0], "%Y-%m-%d").date()
        except:
            pass

    nationality = p.get("strNationality", "Unknown")
    position = p.get("strPosition", "Unknown")
    club = p.get("strTeam", "Unknown Club")

    # ---- Name split ------------------------------------------------
    parts = name.split()
    first = parts[0]
    last = " ".join(parts[1:]) if len(parts) > 1 else ""

    # ---- DB upserts ------------------------------------------------
    with engine.connect() as conn:
        # 1. Position
        pos_id = upsert_position(position)

        # 2. **Team first** – guarantees a valid team_id
        team_id = upsert_team(club, nationality)

        # 3. Player
        player_sql = """
        INSERT INTO players (full_name, first_name, last_name, date_of_birth, nationality, preferred_position_id)
        VALUES (:fn, :fi, :la, :dob, :nat, :pos)
        ON CONFLICT (full_name) DO UPDATE SET
            date_of_birth = COALESCE(EXCLUDED.date_of_birth, players.date_of_birth),
            nationality   = COALESCE(EXCLUDED.nationality,   players.nationality)
        RETURNING player_id
        """
        player_id = conn.execute(text(player_sql), {
            "fn": name, "fi": first, "la": last,
            "dob": dob, "nat": nationality, "pos": pos_id
        }).scalar_one()

        # 4. Season
        season_id = get_or_create_season()

        # 5. Player-team link
        conn.execute(text("""
            INSERT INTO player_teams (player_id, team_id, season_id)
            VALUES (:p, :t, :s)
            ON CONFLICT (player_id, team_id, season_id) DO NOTHING
        """), {"p": player_id, "t": team_id, "s": season_id})
        conn.commit()

    # ---- CSV -------------------------------------------------------
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([name, dob, nationality, position, club, "International"])

    print(f"Imported: {name} ({nationality}) – {position}")
    return True

# ----------------------------------------------------------------------
def main():
    print(f"Starting import of {len(ALL_BIG_NAMES)} big-name rugby players...")

    # Add missing unique constraints (ignore errors)
    with engine.connect() as conn:
        for stmt in [
            "ALTER TABLE players ADD CONSTRAINT uq_players_full_name UNIQUE (full_name)",
            "ALTER TABLE teams   ADD CONSTRAINT uq_teams_name       UNIQUE (name)"
        ]:
            try:
                conn.execute(text(stmt))
                conn.commit()
            except:
                pass

    imported = 0
    for name in ALL_BIG_NAMES:
        if import_player_from_api(name):
            imported += 1

    print(f"\nDone! Imported {imported} players.")
    print(f"CSV saved to: {CSV_FILE}")
    print("\nVerify:")
    print("  SELECT COUNT(*) FROM players;")
    print("  SELECT full_name, nationality FROM players WHERE nationality IN ('South Africa','New Zealand') LIMIT 10;")

if __name__ == "__main__":
    main()