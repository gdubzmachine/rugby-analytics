import sys
from pathlib import Path

from sqlalchemy import text

# --- Ensure project root is on sys.path ---
ROOT_DIR = Path(__file__).resolve().parent.parent  # src -> rugby-analytics
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db.connection import get_engine


def main():
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT current_database();"))
        db_name = result.scalar_one()
        print("Python is writing to database:", db_name)


if __name__ == "__main__":
    main()
