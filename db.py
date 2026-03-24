"""
db.py – MySQL persistence layer (InfinityFree / any MySQL host)
Tables:
  batches   – one row per course: course_id, channel_id, status
  sent_files – one row per (course_id, content_id) pair already posted
"""

import os
import mysql.connector
from mysql.connector import pooling

# ─────────────── DB CONFIG ───────────────
DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "sql200.infinityfree.com"),
    "port":     int(os.getenv("DB_PORT", "3306")),
    "user":     os.getenv("DB_USER",     "if0_40695956"),
    "password": os.getenv("DB_PASSWORD", "yourpassword"),
    "database": os.getenv("DB_NAME",     "if0_40695956_nt"),
}
# ─────────────────────────────────────────

_pool: pooling.MySQLConnectionPool | None = None


def _get_pool() -> pooling.MySQLConnectionPool:
    global _pool
    if _pool is None:
        _pool = pooling.MySQLConnectionPool(
            pool_name="ntbot",
            pool_size=5,
            **DB_CONFIG
        )
    return _pool


def _conn():
    return _get_pool().get_connection()


# ──────────────── SCHEMA INIT ────────────────

def init():
    con = _conn()
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS batches (
            course_id   INT          NOT NULL PRIMARY KEY,
            channel_id  BIGINT       DEFAULT NULL,
            status      VARCHAR(10)  NOT NULL DEFAULT 'active'
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sent_files (
            id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            course_id   INT    NOT NULL,
            content_id  INT    NOT NULL,
            sent_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_sent (course_id, content_id)
        )
    """)
    con.commit()
    cur.close()
    con.close()


# ──────────────── BATCH CRUD ────────────────

def get_batch(course_id: int) -> dict | None:
    con = _conn()
    cur = con.cursor(dictionary=True)
    cur.execute("SELECT * FROM batches WHERE course_id = %s", (course_id,))
    row = cur.fetchone()
    cur.close()
    con.close()
    return row


def get_all_active_batches() -> list[dict]:
    con = _conn()
    cur = con.cursor(dictionary=True)
    cur.execute("SELECT * FROM batches WHERE status = 'active' AND channel_id IS NOT NULL")
    rows = cur.fetchall()
    cur.close()
    con.close()
    return rows


def upsert_batch(course_id: int, channel_id: int | None = None, status: str | None = None):
    """Create or update a batch row."""
    con = _conn()
    cur = con.cursor()
    # Ensure row exists
    cur.execute(
        "INSERT IGNORE INTO batches (course_id) VALUES (%s)",
        (course_id,)
    )
    if channel_id is not None:
        cur.execute(
            "UPDATE batches SET channel_id = %s WHERE course_id = %s",
            (channel_id, course_id)
        )
    elif channel_id is None and status is None:
        # Explicit "remove channel"
        cur.execute(
            "UPDATE batches SET channel_id = NULL WHERE course_id = %s",
            (course_id,)
        )
    if status is not None:
        cur.execute(
            "UPDATE batches SET status = %s WHERE course_id = %s",
            (status, course_id)
        )
    con.commit()
    cur.close()
    con.close()


def set_status(course_id: int, status: str):
    con = _conn()
    cur = con.cursor()
    cur.execute(
        "INSERT INTO batches (course_id, status) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE status = VALUES(status)",
        (course_id, status)
    )
    con.commit()
    cur.close()
    con.close()


# ──────────────── SENT FILES ────────────────

def is_sent(course_id: int, content_id: int) -> bool:
    con = _conn()
    cur = con.cursor()
    cur.execute(
        "SELECT 1 FROM sent_files WHERE course_id = %s AND content_id = %s",
        (course_id, content_id)
    )
    found = cur.fetchone() is not None
    cur.close()
    con.close()
    return found


def mark_sent(course_id: int, content_id: int):
    con = _conn()
    cur = con.cursor()
    cur.execute(
        "INSERT IGNORE INTO sent_files (course_id, content_id) VALUES (%s, %s)",
        (course_id, content_id)
    )
    con.commit()
    cur.close()
    con.close()


def reset_sent(course_id: int):
    """Delete all sent-file records for a batch (restart)."""
    con = _conn()
    cur = con.cursor()
    cur.execute("DELETE FROM sent_files WHERE course_id = %s", (course_id,))
    con.commit()
    cur.close()
    con.close()
