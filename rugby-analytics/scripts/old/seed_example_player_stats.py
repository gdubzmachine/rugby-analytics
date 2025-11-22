# scripts/seed_example_player_stats.py

import sys, os
from sqlalchemy import text

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.connection import engine


def get_example_match(conn):
    """Fetch our Wales vs Ireland Six Nations 2023 match."""
    row = conn.execute(
        text(
            """
            SELECT m.match_id,
                   ht.team_id AS home_team_id,
                   at.team_id AS away_team_id,
                   ht.name    AS home_team_name,
                   at.name    AS away_team_name,
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


def get_or_create_position(conn, code: str, name: str, category: str, num_min: int, num_max: int):
    """
    Insert position ignoring category (enum mismatch safe).
    Postgres default category ('OTHER') will be used.
    """
    pos_id = conn.execute(
        text("SELECT position_id FROM positions WHERE code = :code"),
        {"code": code},
    ).scalar_one_or_none()

    if pos_id is None:
        pos_id = conn.execute(
            text(
                """
                INSERT INTO positions (code, name, number_min, number_max)
                VALUES (:code, :name, :num_min, :num_max)
                RETURNING position_id
                """
            ),
            {
                "code": code,
                "name": name,
                "num_min": num_min,
                "num_max": num_max,
            },
        ).scalar_one()

    return pos_id


def get_or_create_player(conn, full_name: str, position_id: int | None = None):
    player_id = conn.execute(
        text("SELECT player_id FROM players WHERE full_name = :full_name"),
        {"full_name": full_name},
    ).scalar_one_or_none()

    if player_id is None:
        player_id = conn.execute(
            text(
                """
                INSERT INTO players (full_name, preferred_position_id)
                VALUES (:full_name, :position_id)
                RETURNING player_id
                """
            ),
            {"full_name": full_name, "position_id": position_id},
        ).scalar_one()

    return player_id


def player_stats_exist(conn, match_id: int, player_id: int) -> bool:
    return (
        conn.execute(
            text(
                """
                SELECT 1
                FROM player_match_stats
                WHERE match_id = :match_id AND player_id = :player_id
                """
            ),
            {"match_id": match_id, "player_id": player_id},
        ).scalar_one_or_none()
        is not None
    )


def main():
    with engine.begin() as conn:
        match_row = get_example_match(conn)
        if not match_row:
            print("Example match not found. Run seed_example_match.py first.")
            return

        match_id = match_row["match_id"]
        home_team_id = match_row["home_team_id"]
        away_team_id = match_row["away_team_id"]

        print(
            f"Using match_id={match_id} ({match_row['home_team_name']} vs {match_row['away_team_name']})"
        )

        # 1) Ensure positions exist
        sh_id = get_or_create_position(
            conn, code="SH", name="Scrum-half", category="BACK", num_min=9, num_max=9
        )
        fh_id = get_or_create_position(
            conn, code="FH", name="Fly-half", category="BACK", num_min=10, num_max=10
        )

        # 2) Example players
        wales_9_id = get_or_create_player(conn, "Example Wales Scrum-half", sh_id)
        ireland_10_id = get_or_create_player(conn, "Example Ireland Fly-half", fh_id)

        # 3) Insert stats for Wales 9
        if not player_stats_exist(conn, match_id, wales_9_id):
            conn.execute(
                text(
                    """
                    INSERT INTO player_match_stats (
                        match_id, team_id, player_id,
                        position_id, is_starting, jersey_number,
                        minutes_played, tries, conversions, penalty_goals, drop_goals,
                        yellow_cards, red_cards,
                        tackles_made, tackles_missed,
                        carries, metres_gained,
                        linebreaks, offloads,
                        turnovers_won, turnovers_conceded
                    )
                    VALUES (
                        :match_id, :team_id, :player_id,
                        :position_id, :is_starting, :jersey_number,
                        :minutes_played, :tries, :conversions, :penalty_goals, :drop_goals,
                        :yellow_cards, :red_cards,
                        :tackles_made, :tackles_missed,
                        :carries, :metres_gained,
                        :linebreaks, :offloads,
                        :turnovers_won, :turnovers_conceded
                    )
                    """
                ),
                {
                    "match_id": match_id,
                    "team_id": home_team_id,
                    "player_id": wales_9_id,
                    "position_id": sh_id,
                    "is_starting": True,
                    "jersey_number": 9,
                    "minutes_played": 80,
                    "tries": 0,
                    "conversions": 0,
                    "penalty_goals": 0,
                    "drop_goals": 0,
                    "yellow_cards": 0,
                    "red_cards": 0,
                    "tackles_made": 12,
                    "tackles_missed": 3,
                    "carries": 6,
                    "metres_gained": 45,
                    "linebreaks": 1,
                    "offloads": 2,
                    "turnovers_won": 1,
                    "turnovers_conceded": 1,
                },
            )
            print("Inserted stats for Example Wales Scrum-half")
        else:
            print("Stats already exist for Example Wales Scrum-half")

        # 4) Insert stats for Ireland 10
        if not player_stats_exist(conn, match_id, ireland_10_id):
            conn.execute(
                text(
                    """
                    INSERT INTO player_match_stats (
                        match_id, team_id, player_id,
                        position_id, is_starting, jersey_number,
                        minutes_played, tries, conversions, penalty_goals, drop_goals,
                        yellow_cards, red_cards,
                        tackles_made, tackles_missed,
                        carries, metres_gained,
                        linebreaks, offloads,
                        turnovers_won, turnovers_conceded
                    )
                    VALUES (
                        :match_id, :team_id, :player_id,
                        :position_id, :is_starting, :jersey_number,
                        :minutes_played, :tries, :conversions, :penalty_goals, :drop_goals,
                        :yellow_cards, :red_cards,
                        :tackles_made, :tackles_missed,
                        :carries, :metres_gained,
                        :linebreaks, :offloads,
                        :turnovers_won, :turnovers_conceded
                    )
                    """
                ),
                {
                    "match_id": match_id,
                    "team_id": away_team_id,
                    "player_id": ireland_10_id,
                    "position_id": fh_id,
                    "is_starting": True,
                    "jersey_number": 10,
                    "minutes_played": 75,
                    "tries": 1,
                    "conversions": 4,
                    "penalty_goals": 2,
                    "drop_goals": 0,
                    "yellow_cards": 0,
                    "red_cards": 0,
                    "tackles_made": 9,
                    "tackles_missed": 1,
                    "carries": 8,
                    "metres_gained": 65,
                    "linebreaks": 2,
                    "offloads": 1,
                    "turnovers_won": 0,
                    "turnovers_conceded": 1,
                },
            )
            print("Inserted stats for Example Ireland Fly-half")
        else:
            print("Stats already exist for Example Ireland Fly-half")


if __name__ == "__main__":
    main()
