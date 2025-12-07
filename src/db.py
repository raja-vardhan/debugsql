import os
import psycopg
from contextlib import contextmanager
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("PG_DATABASE"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "host": "localhost",
    "port": int(os.getenv("PG_PORT")),
}

@contextmanager
def get_conn():
  conn = psycopg.connect(**DB_CONFIG)
  try:
    yield conn
  finally:
    conn.close()

@contextmanager
def get_cursor():
  with get_conn() as conn:
    cur = conn.cursor()
    try:
      yield cur
      conn.commit()
    except Exception:
      conn.rollback()
      raise
    finally:
      cur.close()


def run_query(sql, params=None):
  with get_cursor() as cur:
    cur.execute(sql, params or ())
    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
  return colnames, rows


def run_scalar(sql, params=None):
  with get_cursor() as cur:
    cur.execute(sql, params or ())
    value = cur.fetchone()[0]
  return value
