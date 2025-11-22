import os
import psycopg2
from dotenv import load_dotenv

def get_db_connection():
    \"\"\"Return a psycopg2 connection using DATABASE_URL from .env\"\"\"
    load_dotenv()
    dsn = os.getenv('DATABASE_URL')
    if not dsn:
        raise RuntimeError('DATABASE_URL is not set in .env')
    return psycopg2.connect(dsn)