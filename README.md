# rugby-analytics

- Virtual env: .\.venv
- Data:        .\data
- Scripts:     .\scripts
- DB helpers:  .\db
- Reusable pkg: .\scr\ingest

After setup:
1) Set your TSDB API key in .env
2) Ensure DATABASE_URL points to your Postgres
3) Use scripts under .\scripts (run with: .\.venv\Scripts\python.exe .\scripts\your_script.py)