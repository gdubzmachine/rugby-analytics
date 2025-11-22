# scripts/seed_example_team_stats.py

import sys, os
from sqlalchemy import text

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.connection import engine


def get_match_id(conn):
    """
    Fetch the example Six Nations 2023 Wales vs Ireland match
    we inserted in seed_example_match.py.
    """
    row = conn.execute(
        text(
            """
            SELECT m.match_id,
                   ht.name AS home_team,
                   at.name AS away_team,
                   m.home_score,
                   m.away_score
            FROM matches m
            JOIN teams ht ON m.home_team_id = ht.team_id
            JOIN teams at ON m.away_team_id = at.team_id
            JOIN seasons s ON m.season_id = s.season_id
            JOIN leagues l ON m.league_id = l.league_id
            WHERE l.name = :league_name
              AND s.year = :season_year
              AND ht.name = :home_name
              AND at.name = :away_name
            ORDER BY m.match_id DESC
            LIMIT 1
            """
        ),
        {
            "league_name": "Six Nations",
            "season_year": 2023,
            "home_name": "Wales",
            "away_name": "Ireland",
        },
    ).mappings().first()

    return row


def main():
    with engine.begin() as conn:
        match_row = get_match_id(conn)
        if not match_row:
            print("Could not find the Wales vs Ireland match. Run seed_example_match.py first.")
            return

        match_id = match_row["match_id"]
        home_score = match_row["home_score"]
        away_score = match_row["away_score"]

        print(f"Using match_id={match_id}, score {home_score}-{away_score}")

        # Look up team IDs
        home_team_id = conn.execute(
            text("SELECT team_id FROM teams WHERE name = :name"),
            {"name": "Wales"},
        ).scalar_one()
        away_team_id = conn.execute(
            text("SELECT team_id FROM teams WHERE name = :name"),
            {"name": "Ireland"},
        ).scalar_one()

        # Check if stats already exist
        existing = conn.execute(
            text(
                """
                SELECT COUNT(*) FROM team_match_stats
                WHERE match_id = :match_id
                """
            ),
            {"match_id": match_id},
        ).scalar_one()

        if existing > 0:
            print("team_match_stats already exist for this match.")
            return

        # Very rough example stats just to populate the table
        # Wales 10 = 1 try (5) + 1 conv (2) + 1 pen (3)  => 5+2+3 = 10
        # Ireland 34 = 4 tries (20) + 3 conv (6) + 2 pens (6) + 1 drop (3) => 35 (but we'll keep it simple)
        # We'll keep it approximate and let "points_scored" generated column do its work.
        conn.execute(
            text(
                """
                INSERT INTO team_match_stats (
                    match_id, team_id, is_home,
                    tries, conversions, penalty_goals, drop_goals,
                    yellow_cards, red_cards,
                    scrums_won, scrums_lost,
                    lineouts_won, lineouts_lost,
                    tackles_made, tackles_missed,
                    metres_gained, possession_pct, territory_pct
                )
                VALUES (
                    :match_id, :team_id, :is_home,
                    :tries, :conversions, :penalty_goals, :drop_goals,
                    :yellow_cards, :red_cards,
                    :scrums_won, :scrums_lost,
                    :lineouts_won, :lineouts_lost,
                    :tackles_made, :tackles_missed,
                    :metres_gained, :possession_pct, :territory_pct
                )
                """
            ),
            {
                "match_id": match_id,
                "team_id": home_team_id,
                "is_home": True,
                "tries": 1,
                "conversions": 1,
                "penalty_goals": 1,
                "drop_goals": 0,
                "yellow_cards": 0,
                "red_cards": 0,
                "scrums_won": 6,
                "scrums_lost": 1,
                "lineouts_won": 8,
                "lineouts_lost": 2,
                "tackles_made": 120,
                "tackles_missed": 25,
                "metres_gained": 280,
                "possession_pct": 47.5,
                "territory_pct": 45.0,
            },
        )

        conn.execute(
            text(
                """
                INSERT INTO team_match_stats (
                    match_id, team_id, is_home,
                    tries, conversions, penalty_goals, drop_goals,
                    yellow_cards, red_cards,
                    scrums_won, scrums_lost,
                    lineouts_won, lineouts_lost,
                    tackles_made, tackles_missed,
                    metres_gained, possession_pct, territory_pct
                )
                VALUES (
                    :match_id, :team_id, :is_home,
                    :tries, :conversions, :penalty_goals, :drop_goals,
                    :yellow_cards, :red_cards,
                    :scrums_won, :scrums_lost,
                    :lineouts_won, :lineouts_lost,
                    :tackles_made, :tackles_missed,
                    :metres_gained, :possession_pct, :territory_pct
                )
                """
            ),
            {
                "match_id": match_id,
                "team_id": away_team_id,
                "is_home": False,
                "tries": 4,
                "conversions": 3,
                "penalty_goals": 2,
                "drop_goals": 0,
                "yellow_cards": 0,
                "red_cards": 0,
                "scrums_won": 9,
                "scrums_lost": 0,
                "lineouts_won": 13,
                "lineouts_lost": 1,
                "tackles_made": 135,
                "tackles_missed": 18,
                "metres_gained": 410,
                "possession_pct": 52.5,
                "territory_pct": 55.0,
            },
        )

        print("Inserted team_match_stats rows for Wales and Ireland.")


if __name__ == "__main__":
    main()
