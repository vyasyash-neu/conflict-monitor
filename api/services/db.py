import os, psycopg2
from psycopg2.extras import RealDictCursor

PG_URL = os.getenv("POSTGRES_URL", "postgresql://conflict:conflict_secret@localhost:5433/conflict_monitor")
_conn = None

def init_db():
    global _conn
    _conn = psycopg2.connect(PG_URL)
    _conn.autocommit = True

def get_conn():
    global _conn
    if _conn is None or _conn.closed:
        init_db()
    return _conn

def query(sql, params=None):
    conn = get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()

def query_one(sql, params=None):
    conn = get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchone()