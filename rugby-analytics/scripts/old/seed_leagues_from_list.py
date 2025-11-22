# scripts/seed_leagues_from_list.py

import sys, os, csv
from io import StringIO
from sqlalchemy import text

# Add project root so we can import db.connection
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.connection import engine


CSV_DATA = """name,short_name,slug,country_code,espn_league_id
Six Nations,Six Nations,six-nations,INTL,
Rugby Championship,TRC,rugby-championship,INTL,
Rugby World Cup,RWC,rugby-world-cup,INTL,
United Rugby Championship,URC,urc,EUR,
Super Rugby,Super Rugby,super-rugby,INTL,
Top 14,Top 14,top-14,FRA,
Premiership Rugby,Premiership,premiership,ENG,
Heineken Champions Cup,Champions Cup,champions-cup,EUR,
European Challenge Cup,Challenge Cup,challenge-cup,EUR,
Major League Rugby,MLR,mlr,USA,
Currie Cup,Currie Cup,currie-cup,ZAF,
NPC (National Provincial Championship),NPC,npc,NZL,
Bunnings NPC,Bunnings NPC,bunnings-npc,NZL,
Pro D2,Pro D2,pro-d2,FRA,
Pacific Nations Cup,PNC,pacific-nations-cup,INTL,
Women's Rugby World Cup,WRWC,womens-rugby-world-cup,INTL,
Women's Six Nations,W6N,womens-six-nions,INTL,
World Rugby Sevens Series,Sevens,world-rugby-sevens,INTL,
Rugby Europe Championship,REC,rugby-europe-championship,EUR,
Rugby Europe Trophy,RET,rugby-europe-trophy,EUR,
Rugby Africa Cup,Africa Cup,rugby-africa-cup,AFR,
Asian Rugby Championship,ARC,asian-rugby-championship,ASIA,
Americas Rugby Championship,ARCh,americas-rugby-championship,AMR,
Rugby Americas North Championship,RAN,rugby-americas-north,AMR,
South American Rugby Championship,SARC,south-american-rugby,AMR,
Japan Rugby League One,League One,japan-rugby-league-one,JPN,
Mitre 10 Cup (historic),Mitre 10,mitre-10-cup,NZL,
Super Rugby Aupiki,Aupiki,super-rugby-aupiki,NZL,
Super W,Super W,super-w,AUS,
Shute Shield,Shute Shield,shute-shield,AUS,
Hospital Cup,Hospital Cup,hospital-cup,AUS,
ITM Cup (historic),ITM Cup,itm-cup,NZL,
Anglo-Welsh Cup,Anglo-Welsh,anglo-welsh-cup,ENG,
RFU Championship,RFU Champ,rfu-championship,ENG,
Greene King IPA Championship,IPA Championship,ipa-championship,ENG,
Bok Women’s Premier Division,W Premier Division,womens-premier-division,ZAF,
Varsity Cup,Varsity Cup,varsity-cup,ZAF,
Vodacom Cup,Vodacom Cup,vodacom-cup,ZAF,
Top League (historic),Top League,japan-top-league,JPN,
Super 6,S6,super-6,SCOT,
Allianz Premier 15s,Premier 15s,premier-15s,ENG,
Elite 1 Féminine,Elite 1,elite-1-feminine,FRA,
World Rugby U20 Championship,U20,u20-championship,INTL,
World Rugby U20 Trophy,U20 Trophy,u20-trophy,INTL,
Rugby Championship U20,TRC U20,trc-u20,INTL,
Six Nations U20,U20 Six Nations,u20-six-nations,EUR,
"""


def get_or_create_sport(conn, sport_name: str, code: str) -> int:
    """Return sport_id for the given sport name, creating it with a code if needed."""
    sport_id = conn.execute(
        text("SELECT sport_id FROM sports WHERE name = :name"),
        {"name": sport_name},
    ).scalar_one_or_none()

    if sport_id is None:
        sport_id = conn.execute(
            text(
                """
                INSERT INTO sports (code, name)
                VALUES (:code, :name)
                RETURNING sport_id
                """
            ),
            {"code": code, "name": sport_name},
        ).scalar_one()
        print(f"Inserted sport '{sport_name}' (code={code}) with id={sport_id}")
    else:
        print(f"Using existing sport '{sport_name}' with id={sport_id}")

    return sport_id


def infer_sport_name(league_name: str) -> str:
    """Decide whether this league is Rugby Union or Rugby Sevens."""
    lname = league_name.lower()
    if "sevens" in lname or "7s" in lname:
        return "Rugby Sevens"
    return "Rugby Union"


def main():
    reader = csv.DictReader(StringIO(CSV_DATA))

    inserted = 0
    skipped = 0

    with engine.begin() as conn:
        # Ensure sports exist WITH codes
        ru_id = get_or_create_sport(conn, "Rugby Union", "RU")
        rs_id = get_or_create_sport(conn, "Rugby Sevens", "RS")

        for row in reader:
            name = (row.get("name") or "").strip()
            if not name:
                continue

            short_name = (row.get("short_name") or "").strip() or None
            slug = (row.get("slug") or "").strip()

            raw_cc = (row.get("country_code") or "").strip().upper()
            # country_code column is CHAR(2) → anything longer becomes NULL for now
            if len(raw_cc) == 2:
                country_code = raw_cc
            else:
                country_code = None

            espn_raw = (row.get("espn_league_id") or "").strip()
            espn_league_id = espn_raw if espn_raw else None

            # Choose sport_id based on league name
            sport_name = infer_sport_name(name)
            sport_id = ru_id if sport_name == "Rugby Union" else rs_id

            # Check for existing league by slug or name
            existing = conn.execute(
                text(
                    """
                    SELECT league_id
                    FROM leagues
                    WHERE slug = :slug OR name = :name
                    """
                ),
                {"slug": slug, "name": name},
            ).scalar_one_or_none()

            if existing:
                print(f"Skipping existing league: {name} (id={existing})")
                skipped += 1
                continue

            conn.execute(
                text(
                    """
                    INSERT INTO leagues (sport_id, name, short_name, slug, espn_league_id, country_code)
                    VALUES (:sport_id, :name, :short_name, :slug, :espn_league_id, :country_code)
                    """
                ),
                {
                    "sport_id": sport_id,
                    "name": name,
                    "short_name": short_name,
                    "slug": slug,
                    "espn_league_id": espn_league_id,
                    "country_code": country_code,
                },
            )

            print(f"Inserted league: {name} (sport={sport_name}, slug={slug}, cc={country_code})")
            inserted += 1

    print(f"\nDone. Inserted {inserted} leagues, skipped {skipped}.")


if __name__ == "__main__":
    main()
