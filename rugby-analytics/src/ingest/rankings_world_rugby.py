import pandas as pd

WIKI_URL = "https://en.wikipedia.org/wiki/World_Rugby_Rankings"


def load_rankings_tables():
    print(f"Loading tables from: {WIKI_URL}")
    tables = pd.read_html(WIKI_URL)
    print(f"Found {len(tables)} tables on the page")
    return tables


def pick_current_rankings_table(tables):
    """
    Heuristic: find a table that has both 'Rank' and 'Team' columns.
    We'll refine this once we see the actual structure.
    """
    for i, df in enumerate(tables):
        cols = [str(c) for c in df.columns]
        if any("Rank" in c for c in cols) and any("Team" in c for c in cols):
            print(f"Using table #{i} as the current rankings table")
            return df

    raise ValueError("Could not find a table with Rank and Team columns")


def main():
    tables = load_rankings_tables()
    rankings_df = pick_current_rankings_table(tables)

    print("\n=== Sample of rankings table ===")
    print(rankings_df.head(10))


if __name__ == "__main__":
    main()
