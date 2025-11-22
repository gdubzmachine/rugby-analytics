import requests
import pandas as pd

WIKI_URL = "https://en.wikipedia.org/wiki/World_Rugby_Rankings"

HEADERS = {
    # Pretend to be a normal browser so Wikipedia doesn't block us
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0 Safari/537.36"
    )
}


def load_rankings_tables():
    print(f"Requesting: {WIKI_URL}")
    resp = requests.get(WIKI_URL, headers=HEADERS, timeout=30)
    print("HTTP status code:", resp.status_code)
    resp.raise_for_status()

    html = resp.text
    tables = pd.read_html(html)
    print(f"Found {len(tables)} tables on the page")
    return tables


def pick_current_rankings_table(tables):
    """
    Find a table that looks like the current rankings:
    it should have columns containing 'Rank' and 'Team'.
    """
    for i, df in enumerate(tables):
        cols = [str(c) for c in df.columns]
        print(f"Table {i} columns: {cols}")
        if any("Rank" in c for c in cols) and any("Team" in c for c in cols):
            print(f"\n Using table #{i} as the current rankings table")
            return df

    raise ValueError("Could not find a table with Rank and Team columns")


def main():
    tables = load_rankings_tables()
    rankings_df = pick_current_rankings_table(tables)

    print("\n=== Sample of rankings table (first 10 rows) ===")
    print(rankings_df.head(10))


if __name__ == "__main__":
    main()
