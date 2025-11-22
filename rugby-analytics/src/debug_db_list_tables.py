from db.connection import get_engine

def main():
    engine = get_engine()
    with engine.connect() as conn:
        print("\nTables in rugby_analytics.public:")
        rows = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public';
        """).fetchall()

        for r in rows:
            print("-", r[0])

        print("\nTeams count:")
        count = conn.execute("SELECT COUNT(*) FROM teams;").scalar_one()
        print("teams table row count =", count)

if __name__ == "__main__":
    main()
