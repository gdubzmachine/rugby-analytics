# scripts/seed_matches_2023_from_csv.py
import os
import sys
from datetime import datetime, timezone
from typing import Dict
import pandas as pd
from sqlalchemy import text

# ---------------------------------------------------------------------------
# Project root & DB engine
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)
from db.connection import engine  # type: ignore

# ---------------------------------------------------------------------------
# LEAGUE ID MAP
# ---------------------------------------------------------------------------
SHORT_CODE_TO_ID: Dict[str, int] = {
    "SN": 1,
    "WSN": 42,
    "RC": 2,
    "RWC": 3,
    "WXV": 999,
    "P4": 998,
}

LEAGUE_NAME_MAP: Dict[str, int] = {
    "Six Nations": 1,
    "Women’s Six Nations": 42,
    "Rugby Championship": 2,
    "Rugby World Cup": 3,
    "WXV": 999,
    "Pacific Four Series": 998,
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def clean_competition_name(name: str) -> str:
    name = name.strip()
    for y in range(2020, 2030):
        name = name.replace(f" {y}", "").replace(f"{y} ", "")
    name = name.split(" (")[0]
    name = name.replace("–", "-").replace("’", "'")
    return name.strip()

def get_or_create_season_2023(conn, league_id: int) -> int:
    season_id = conn.execute(
        text("SELECT season_id FROM seasons WHERE league_id = :league_id AND year = 2023"),
        {"league_id": league_id}
    ).scalar_one_or_none()

    if season_id is None:
        season_id = conn.execute(
            text("""
                INSERT INTO seasons (league_id, year, start_date, end_date)
                VALUES (:league_id, 2023, '2023-01-01', '2023-12-31')
                RETURNING season_id
            """),
            {"league_id": league_id}
        ).scalar_one()
        print(f"Created season 2023 for league_id={league_id} → season_id={season_id}")
    return season_id

def parse_kickoff(date_str: str) -> datetime:
    d = datetime.fromisoformat(date_str.split(" ")[0])
    return datetime(d.year, d.month, d.day, 15, 0, tzinfo=timezone.utc)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    csv_path = os.path.join(PROJECT_ROOT, "rugby_matches_full.csv")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found at {csv_path}")

    df = pd.read_csv(csv_path, encoding="latin1")
    df_2023 = df[df["match_date"].astype(str).str.startswith("2023")].copy()
    print(f"Loaded {len(df_2023)} matches for 2023 from CSV.")

    inserted = 0
    skipped = 0
    skip_reasons: Dict[str, int] = {}

    with engine.begin() as conn:
        for _, row in df_2023.iterrows():
            comp_name = str(row.get("competition_name", "")).strip()
            comp_short = str(row.get("competition_short_name", "")).strip()
            date_str = str(row.get("match_date", "")).strip()

            # 1. Year
            try:
                year = int(date_str[:4])
                if year != 2023: raise ValueError()
            except Exception:
                reason = f"invalid year in date '{date_str}'"
                skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
                skipped += 1
                continue

            # 2. League
            league_id = None
            if comp_short in SHORT_CODE_TO_ID:
                league_id = SHORT_CODE_TO_ID[comp_short]
            else:
                cleaned = clean_competition_name(comp_name)
                if cleaned in LEAGUE_NAME_MAP:
                    league_id = LEAGUE_NAME_MAP[cleaned]

            if league_id is None:
                reason = f"unknown competition: '{comp_name}' (short: '{comp_short}')"
                skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
                skipped += 1
                continue

            # 3. Season
            season_id = get_or_create_season_2023(conn, league_id)

            # 4. Kickoff
            try:
                kickoff_utc = parse_kickoff(date_str)
            except Exception as e:
                reason = f"bad date '{date_str}': {e}"
                skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
                skipped += 1
                continue

            # 5. Scores
            try:
                home_score = int(row["home_score"])
                away_score = int(row["away_score"])
            except Exception:
                reason = "invalid score"
                skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
                skipped += 1
                continue

            # 6. INSERT – home_team_id and away_team_id = NULL
            conn.execute(
                text("""
                    INSERT INTO matches (
                        league_id, season_id, espn_event_id, status, kickoff_utc,
                        round_label, stage, venue_id,
                        home_team_id, away_team_id,
                        home_score, away_score, attendance, source,
                        created_at, updated_at
                    ) VALUES (
                        :league_id, :season_id, NULL, :status, :kickoff_utc,
                        NULL, NULL, NULL,
                        NULL, NULL,
                        :home_score, :away_score, NULL, 'manual',
                        NOW(), NOW()
                    )
                """),
                {
                    "league_id": league_id,
                    "season_id": season_id,
                    "status": "scheduled",
                    "kickoff_utc": kickoff_utc,
                    "home_score": home_score,
                    "away_score": away_score,
                },
            )
            inserted += 1

    print(f"\nDone. Inserted {inserted} matches, skipped {skipped}.")
    if skip_reasons:
        print("Skip reasons:")
        for r, c in sorted(skip_reasons.items(), key=lambda x: x[1], reverse=True):
            print(f"  • {r}: {c}")


if __name__ == "__main__":
    main()