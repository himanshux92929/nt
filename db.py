"""
db.py – Aiven MySQL persistence layer

Aiven MySQL rejects connections that don't present a valid CA cert with a
misleading "Access denied" (1045) error instead of an SSL error.

Fix: we connect with ssl_disabled=False but ssl_verify_cert=False so the
connection is still encrypted but we skip cert validation. This is the
standard approach for managed cloud MySQL when you don't want to bundle the
CA cert file into the container.

If you DO want full cert validation, download the CA cert from:
  Aiven Console → your MySQL service → "Connection information" → CA Certificate
and set  AIVEN_CA_CERT  env var to its full PEM contents (Render allows multi-line).

Required env vars (set in Render → Environment):
    DB_PASSWORD   Aiven MySQL password
    BOT_TOKEN     Telegram bot token
    OWNER_ID      Telegram owner user ID

Everything else is hardcoded below.
"""

import os
import logging
import tempfile
import mysql.connector
from mysql.connector import pooling

log = logging.getLogger(__name__)

# ── Hardcoded Aiven connection details ────────────────────────
_HOST     = "mysql-3369278f-cathycarter-c7c2.c.aivencloud.com"
_PORT     = 11860
_USER     = "avnadmin"
_DATABASE = "defaultdb"

# ── Sensitive – must be set as env var ───────────────────────
_PASSWORD = os.environ["DB_PASSWORD"]

# ── Optional: full CA cert PEM contents in env var ───────────
# If set, we use full SSL verification (most secure).
# If not set, we use encrypted SSL without cert verification.
_CA_CERT_INLINE = os.getenv("AIVEN_CA_CERT", "").strip()


def _build_ssl_args() -> dict:
    """Return the SSL kwargs for mysql.connector."""
    if _CA_CERT_INLINE:
        # Write inline PEM to a temp file (mysql-connector needs a file path)
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".pem", delete=False, prefix="aiven_ca_"
        )
        tmp.write(_CA_CERT_INLINE.replace("\\n", "\n"))
        tmp.flush()
        tmp.close()
        log.info(f"Using Aiven CA cert from AIVEN_CA_CERT env var → {tmp.name}")
        return {
            "ssl_ca":              tmp.name,
            "ssl_verify_cert":     True,
            "ssl_verify_identity": False,   # hostname check can cause issues
        }

    # No CA cert provided – still use SSL (encrypted) but skip cert verification.
    # This resolves the 1045 "Access denied" caused by missing CA cert on Aiven.
    log.info("No AIVEN_CA_CERT set – using SSL without cert verification.")
    return {
        "ssl_disabled":    False,
        "ssl_verify_cert": False,
    }


_SSL_ARGS = _build_ssl_args()

_CONNECT_ARGS = {
    "host":               _HOST,
    "port":               _PORT,
    "user":               _USER,
    "password":           _PASSWORD,
    "database":           _DATABASE,
    "connection_timeout": 15,
    "autocommit":         False,
    **_SSL_ARGS,
}

_pool: pooling.MySQLConnectionPool | None = None


def _get_pool() -> pooling.MySQLConnectionPool:
    global _pool
    if _pool is None:
        _pool = pooling.MySQLConnectionPool(
            pool_name="ntbot",
            pool_size=5,
            **_CONNECT_ARGS,
        )
        log.info("MySQL connection pool created.")
    return _pool


def _conn():
    """Get a connection from the pool, reconnecting if stale."""
    return _get_pool().get_connection()


# ─────────────── SCHEMA ─────────────────────────────────────

def init():
    """Create tables if they don't exist."""
    con = _conn()
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS batches (
            platform   VARCHAR(10)  NOT NULL,
            course_id  INT          NOT NULL,
            channel_id BIGINT       DEFAULT NULL,
            status     VARCHAR(10)  NOT NULL DEFAULT 'active',
            PRIMARY KEY (platform, course_id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sent_files (
            id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            platform   VARCHAR(10) NOT NULL,
            course_id  INT         NOT NULL,
            content_id INT         NOT NULL,
            sent_at    DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_sent (platform, course_id, content_id)
        )
    """)
    con.commit()
    cur.close()
    con.close()
    log.info("DB schema ready.")


def ping():
    """
    Send a lightweight ping to keep the connection pool alive.
    Called every 2 hours by the bot's job queue.
    """
    try:
        con = _conn()
        cur = con.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        con.close()
        log.info("DB ping OK.")
    except Exception as e:
        log.warning(f"DB ping failed (pool will reconnect on next query): {e}")
        # Reset pool so next real query gets a fresh connection
        global _pool
        _pool = None


# ─────────────── BATCH CRUD ─────────────────────────────────

def get_batch(platform: str, course_id: int) -> dict | None:
    con = _conn()
    cur = con.cursor(dictionary=True)
    cur.execute(
        "SELECT * FROM batches WHERE platform=%s AND course_id=%s",
        (platform, course_id),
    )
    row = cur.fetchone()
    cur.close(); con.close()
    return row


def get_all_active_batches() -> list[dict]:
    con = _conn()
    cur = con.cursor(dictionary=True)
    cur.execute(
        "SELECT * FROM batches WHERE status='active' AND channel_id IS NOT NULL"
    )
    rows = cur.fetchall()
    cur.close(); con.close()
    return rows


def upsert_batch(
    platform: str,
    course_id: int,
    channel_id: int | None = None,
    remove_channel: bool = False,
    status: str | None = None,
):
    con = _conn()
    cur = con.cursor()
    cur.execute(
        "INSERT IGNORE INTO batches (platform, course_id) VALUES (%s, %s)",
        (platform, course_id),
    )
    if remove_channel:
        cur.execute(
            "UPDATE batches SET channel_id=NULL WHERE platform=%s AND course_id=%s",
            (platform, course_id),
        )
    elif channel_id is not None:
        cur.execute(
            "UPDATE batches SET channel_id=%s WHERE platform=%s AND course_id=%s",
            (channel_id, platform, course_id),
        )
    if status is not None:
        cur.execute(
            "UPDATE batches SET status=%s WHERE platform=%s AND course_id=%s",
            (status, platform, course_id),
        )
    con.commit()
    cur.close(); con.close()


def set_status(platform: str, course_id: int, status: str):
    con = _conn()
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO batches (platform, course_id, status) VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE status=VALUES(status)
        """,
        (platform, course_id, status),
    )
    con.commit()
    cur.close(); con.close()


# ─────────────── SENT FILES ─────────────────────────────────

def is_sent(platform: str, course_id: int, content_id: int) -> bool:
    con = _conn()
    cur = con.cursor()
    cur.execute(
        "SELECT 1 FROM sent_files WHERE platform=%s AND course_id=%s AND content_id=%s",
        (platform, course_id, content_id),
    )
    found = cur.fetchone() is not None
    cur.close(); con.close()
    return found


def mark_sent(platform: str, course_id: int, content_id: int):
    con = _conn()
    cur = con.cursor()
    cur.execute(
        "INSERT IGNORE INTO sent_files (platform, course_id, content_id) VALUES (%s,%s,%s)",
        (platform, course_id, content_id),
    )
    con.commit()
    cur.close(); con.close()


def reset_sent(platform: str, course_id: int):
    con = _conn()
    cur = con.cursor()
    cur.execute(
        "DELETE FROM sent_files WHERE platform=%s AND course_id=%s",
        (platform, course_id),
    )
    con.commit()
    cur.close(); con.close()
