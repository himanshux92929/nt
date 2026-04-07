"""
bot.py – NextToppers + MissionJeet Telegram Bot
Combined single-file bot with MySQL persistence, auto-posting, broadcast,
force-all-update, daily logs, midnight DB backup, and beautiful UI.
"""

import os
import io
import csv
import asyncio
import logging
import threading
import base64
import datetime
from urllib.parse import quote

import aiohttp
import mysql.connector
from mysql.connector import pooling
import tempfile
from flask import Flask
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ═══════════════════════════════════════════════════════════════
#  CONFIG  –  Sensitive values come from environment variables
# ═══════════════════════════════════════════════════════════════
BOT_TOKEN = os.environ["BOT_TOKEN"]
OWNER_ID = int(os.environ["OWNER_ID"])
DB_PASSWORD = os.environ["DB_PASSWORD"]

# ── Non-sensitive / tweak here ─────────────────────────────────
CHECK_INTERVAL_HOURS = 24
FLASK_PORT = int(os.getenv("PORT", "8080"))
WATERMARK = "\n\n<i>Provided by @smartrz</i>"

PLAYER_BASE = "https://smarterz.netlify.app/ntplayer"
PLAYER_SECRET = "smarterzpro"

# ── Single source of truth for API base URL ────────────────────
API_BASE = "https://apiserver-phi-topaz.vercel.app/api"

# ─── Platform definitions (built on top of API_BASE) ──────────
PLATFORMS = {
    "nt": {
        "label": "NextToppers",
        "emoji": "📘",
        "batches_url": f"{API_BASE}/nexttoppers/batches",
        "content_url": f"{API_BASE}/nexttoppers/all-content",
        "details_url": f"{API_BASE}/nexttoppers/content-details",
        "content_qs": lambda cid, fid=None: f"?courseid={cid}"
        + (f"&id={fid}" if fid else ""),
        "details_qs": lambda cid, course_id: f"?content_id={cid}&courseid={course_id}",
    },
    "mj": {
        "label": "MissionJeet",
        "emoji": "🎯",
        "batches_url": f"{API_BASE}/missionjeet/batches",
        "content_url": f"{API_BASE}/missionjeet/all-content",
        "details_url": f"{API_BASE}/missionjeet/content-details",
        "content_qs": lambda cid, fid=None: f"/{cid}" + (f"?id={fid}" if fid else ""),
        "details_qs": lambda cid, course_id: f"?content_id={cid}&course_id={course_id}",
    },
}

# ═══════════════════════════════════════════════════════════════
#  DATABASE  –  Aiven MySQL
# ═══════════════════════════════════════════════════════════════
_DB_HOST = "mysql-3369278f-cathycarter-c7c2.c.aivencloud.com"
_DB_PORT = 11860
_DB_USER = "avnadmin"
_DB_DATABASE = "defaultdb"
_CA_CERT_INLINE = os.getenv("AIVEN_CA_CERT", "").strip()

_pool: pooling.MySQLConnectionPool | None = None


def _build_ssl_args() -> dict:
    if _CA_CERT_INLINE:
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".pem", delete=False, prefix="aiven_ca_"
        )
        tmp.write(_CA_CERT_INLINE.replace("\\n", "\n"))
        tmp.flush()
        tmp.close()
        return {
            "ssl_ca": tmp.name,
            "ssl_verify_cert": True,
            "ssl_verify_identity": False,
        }
    return {"ssl_disabled": False, "ssl_verify_cert": False}


_CONNECT_ARGS = {
    "host": _DB_HOST,
    "port": _DB_PORT,
    "user": _DB_USER,
    "password": DB_PASSWORD,
    "database": _DB_DATABASE,
    "connection_timeout": 15,
    "autocommit": False,
    **_build_ssl_args(),
}


def _get_pool() -> pooling.MySQLConnectionPool:
    global _pool
    if _pool is None:
        _pool = pooling.MySQLConnectionPool(
            pool_name="ntbot", pool_size=5, **_CONNECT_ARGS
        )
    return _pool


def _conn():
    return _get_pool().get_connection()


# ── Schema ──────────────────────────────────────────────────────
def db_init():
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


def db_ping():
    try:
        con = _conn()
        cur = con.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        con.close()
        log.info("DB ping OK.")
    except Exception as e:
        log.warning(f"DB ping failed: {e}")
        global _pool
        _pool = None


def db_get_batch(platform, course_id):
    con = _conn()
    cur = con.cursor(dictionary=True)
    cur.execute(
        "SELECT * FROM batches WHERE platform=%s AND course_id=%s",
        (platform, course_id),
    )
    row = cur.fetchone()
    cur.close()
    con.close()
    return row


def db_get_all_active_batches():
    con = _conn()
    cur = con.cursor(dictionary=True)
    cur.execute(
        "SELECT * FROM batches WHERE status='active' AND channel_id IS NOT NULL"
    )
    rows = cur.fetchall()
    cur.close()
    con.close()
    return rows


def db_get_all_batches():
    con = _conn()
    cur = con.cursor(dictionary=True)
    cur.execute("SELECT * FROM batches ORDER BY platform, course_id")
    rows = cur.fetchall()
    cur.close()
    con.close()
    return rows


def db_upsert_batch(
    platform, course_id, channel_id=None, remove_channel=False, status=None
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
    cur.close()
    con.close()


def db_set_status(platform, course_id, status):
    con = _conn()
    cur = con.cursor()
    cur.execute(
        "INSERT INTO batches (platform, course_id, status) VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE status=VALUES(status)",
        (platform, course_id, status),
    )
    con.commit()
    cur.close()
    con.close()


def db_is_sent(platform, course_id, content_id):
    con = _conn()
    cur = con.cursor()
    cur.execute(
        "SELECT 1 FROM sent_files WHERE platform=%s AND course_id=%s AND content_id=%s",
        (platform, course_id, content_id),
    )
    found = cur.fetchone() is not None
    cur.close()
    con.close()
    return found


def db_mark_sent(platform, course_id, content_id):
    con = _conn()
    cur = con.cursor()
    cur.execute(
        "INSERT IGNORE INTO sent_files (platform, course_id, content_id) VALUES (%s,%s,%s)",
        (platform, course_id, content_id),
    )
    con.commit()
    cur.close()
    con.close()


def db_reset_sent(platform, course_id):
    con = _conn()
    cur = con.cursor()
    cur.execute(
        "DELETE FROM sent_files WHERE platform=%s AND course_id=%s",
        (platform, course_id),
    )
    con.commit()
    cur.close()
    con.close()


def db_dump_csv() -> bytes:
    """Export both tables as a combined CSV for backup."""
    buf = io.StringIO()
    writer = csv.writer(buf)

    con = _conn()
    cur = con.cursor()

    writer.writerow(["=== TABLE: batches ==="])
    cur.execute("SELECT * FROM batches")
    writer.writerow([d[0] for d in cur.description])
    writer.writerows(cur.fetchall())

    writer.writerow([])
    writer.writerow(["=== TABLE: sent_files ==="])
    cur.execute("SELECT * FROM sent_files")
    writer.writerow([d[0] for d in cur.description])
    writer.writerows(cur.fetchall())

    cur.close()
    con.close()
    return buf.getvalue().encode()


# ═══════════════════════════════════════════════════════════════
#  FLASK KEEP-ALIVE
# ═══════════════════════════════════════════════════════════════
flask_app = Flask(__name__)


@flask_app.route("/")
def health():
    return "OK – Bot is running hehe", 200


@flask_app.route("/ping")
def ping():
    return "pong", 200


def start_flask():
    def _run():
        from werkzeug.serving import make_server

        server = make_server("0.0.0.0", FLASK_PORT, flask_app)
        log.info(f"Flask keep-alive on port {FLASK_PORT}")
        server.serve_forever()

    threading.Thread(target=_run, daemon=True).start()


# ═══════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s"
)
log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════
def owner_only(func):
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE, *a, **kw):
        if (
            update.effective_chat.type == "private"
            and update.effective_user.id != OWNER_ID
        ):
            await update.effective_message.reply_text("⛔ Access denied.")
            return
        return await func(update, ctx, *a, **kw)

    return wrapper


async def api_get(session: aiohttp.ClientSession, url: str, _retries: int = 3) -> dict:
    last_exc = None
    for attempt in range(1, _retries + 1):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
                r.raise_for_status()
                data = await r.json()
            # Retry if API explicitly signals failure
            if isinstance(data, dict) and data.get("success") is False:
                log.warning(f"API returned success=false (attempt {attempt}/3): {url}")
                if attempt < _retries:
                    await asyncio.sleep(2**attempt)  # 2s, 4s back-off
                    continue
                raise RuntimeError(
                    f"API success=false after {_retries} attempts: {url}"
                )
            return data
        except RuntimeError:
            raise
        except Exception as e:
            last_exc = e
            log.warning(f"API error attempt {attempt}/3 [{url}]: {e}")
            if attempt < _retries:
                await asyncio.sleep(2**attempt)
    raise last_exc


def encode_token(url: str) -> str:
    key = PLAYER_SECRET
    xored = bytes([ord(url[i]) ^ ord(key[i % len(key)]) for i in range(len(url))])
    return base64.b64encode(xored).decode()


def make_player_url(file_url: str) -> str:
    return f"{PLAYER_BASE}?token={quote(encode_token(file_url))}"


def pcfg(platform: str) -> dict:
    return PLATFORMS[platform]


def back_btn(platform: str) -> list:
    return [
        [InlineKeyboardButton("« Back to Courses", callback_data=f"back:{platform}")]
    ]


# ═══════════════════════════════════════════════════════════════
#  BEAUTIFUL UI BUILDER
# ═══════════════════════════════════════════════════════════════
def build_course_keyboard(
    courses: list, platform: str, cfg: dict
) -> InlineKeyboardMarkup:
    """
    Split courses into two visual sections:
    ✅ CONNECTED  – has a channel_id set
    ○  NOT CONNECTED
    """
    connected = []
    not_connected = []

    for c in courses:
        batch = db_get_batch(platform, c["id"])
        if batch and batch.get("channel_id"):
            connected.append((c, batch))
        else:
            not_connected.append((c, batch))

    keyboard = []

    if connected:
        keyboard.append(
            [InlineKeyboardButton("━━━━  ✅  CONNECTED  ━━━━", callback_data="noop")]
        )
        for c, batch in connected:
            status = (batch or {}).get("status", "active")
            s_icon = "▶️" if status == "active" else "⏸"
            trending = "🔥 " if c.get("is_trending") else ""
            label = f"{s_icon} {trending}{cfg['emoji']} {str(c['title'])[:38]}"
            keyboard.append(
                [
                    InlineKeyboardButton(
                        label, callback_data=f"course:{platform}:{c['id']}"
                    )
                ]
            )

    if not_connected:
        keyboard.append(
            [InlineKeyboardButton("━━━  ○  NOT CONNECTED  ━━━", callback_data="noop")]
        )
        for c, batch in not_connected:
            trending = "🔥 " if c.get("is_trending") else ""
            label = f"➕ {trending}{cfg['emoji']} {str(c['title'])[:38]}"
            keyboard.append(
                [
                    InlineKeyboardButton(
                        label, callback_data=f"course:{platform}:{c['id']}"
                    )
                ]
            )

    keyboard.append(
        [
            InlineKeyboardButton("📘 NextToppers", callback_data="pick:nt"),
            InlineKeyboardButton("🎯 MissionJeet", callback_data="pick:mj"),
        ]
    )
    keyboard.append(
        [
            InlineKeyboardButton("📡 Broadcast All", callback_data="broadcast_prompt"),
            InlineKeyboardButton("⚡ Force All", callback_data="forceall_confirm"),
        ]
    )
    keyboard.append(
        [
            InlineKeyboardButton("🗄️ Backup Now", callback_data="backup_now"),
        ]
    )
    return InlineKeyboardMarkup(keyboard)


# ═══════════════════════════════════════════════════════════════
#  COURSE LIST
# ═══════════════════════════════════════════════════════════════
async def show_courses(edit_fn, platform: str):
    cfg = pcfg(platform)
    async with aiohttp.ClientSession() as session:
        try:
            data = await api_get(session, cfg["batches_url"])
        except Exception as e:
            await edit_fn(f"❌ API error: {e}")
            return

    courses = []
    for item in data.get("data", []):
        if isinstance(item, dict):
            if "list" in item:
                courses.extend(item["list"])
            elif "id" in item:
                courses.append(item)

    if not courses:
        await edit_fn("No courses found.")
        return

    kb = build_course_keyboard(courses, platform, cfg)
    total_connected = sum(
        1
        for c in courses
        if db_get_batch(platform, c["id"])
        and db_get_batch(platform, c["id"]).get("channel_id")
    )

    await edit_fn(
        f"{'─' * 28}\n"
        f"{cfg['emoji']}  *{cfg['label']} — Courses*\n"
        f"{'─' * 28}\n"
        f"📊  Total: `{len(courses)}`  |  ✅ Connected: `{total_connected}`  |  ○ Pending: `{len(courses) - total_connected}`\n"
        f"{'─' * 28}\n\n"
        f"Tap a course to manage it:",
        reply_markup=kb,
        parse_mode="Markdown",
    )


# ═══════════════════════════════════════════════════════════════
#  COMMANDS
# ═══════════════════════════════════════════════════════════════
@owner_only
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    kb = [
        [
            InlineKeyboardButton("📘 NextToppers", callback_data="pick:nt"),
            InlineKeyboardButton("🎯 MissionJeet", callback_data="pick:mj"),
        ],
        [
            InlineKeyboardButton("📡 Broadcast All", callback_data="broadcast_prompt"),
            InlineKeyboardButton("⚡ Force All", callback_data="forceall_confirm"),
        ],
        [
            InlineKeyboardButton("🗄️ Backup Now", callback_data="backup_now"),
        ],
    ]
    await update.message.reply_text(
        "╔══════════════════════════╗\n"
        "║   🤖  *Content Manager*   ║\n"
        "╚══════════════════════════╝\n\n"
        "Welcome back, Admin!\n"
        "Choose a platform to manage courses,\n"
        "or use the actions below.",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="Markdown",
    )


@owner_only
async def cmd_batches(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    platform = args[0].lower() if args and args[0].lower() in PLATFORMS else None
    if not platform:
        kb = [
            [
                InlineKeyboardButton("📘 NextToppers", callback_data="pick:nt"),
                InlineKeyboardButton("🎯 MissionJeet", callback_data="pick:mj"),
            ]
        ]
        await update.message.reply_text(
            "Which platform?", reply_markup=InlineKeyboardMarkup(kb)
        )
        return
    msg = await update.message.reply_text("⏳ Fetching courses…")
    await show_courses(msg.edit_text, platform)


@owner_only
async def cmd_backup(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/backup – send DB backup immediately."""
    msg = await update.message.reply_text("🗄️ Generating backup…")
    await _do_db_backup(ctx.application)
    await msg.delete()


async def cb_backup_now(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("🗄️ Sending backup…", show_alert=False)
    if query.from_user.id != OWNER_ID:
        return
    await _do_db_backup(ctx.application)
    await query.answer("✅ Backup sent to your DM!", show_alert=True)


async def _do_db_backup(app: Application):
    try:
        csv_bytes = db_dump_csv()
        now = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M")
        filename = f"db_backup_{now}.csv"
        bio = io.BytesIO(csv_bytes)
        bio.name = filename
        await app.bot.send_document(
            chat_id=OWNER_ID,
            document=bio,
            filename=filename,
            caption=(
                f"🗄️ <b>Database Backup</b>\n"
                f"🕛 {now} UTC\n\n"
                f"Contains: <code>batches</code> + <code>sent_files</code> tables."
            ),
            parse_mode="HTML",  # ← was Markdown, underscores in filename broke it
        )
        log.info("DB backup sent.")
    except Exception as e:
        log.error(f"DB backup failed: {e}")
        try:
            await app.bot.send_message(
                chat_id=OWNER_ID, text=f"❌ DB backup failed: {e}"
            )
        except Exception:
            pass
    text = " ".join(ctx.args) if ctx.args else ""
    if not text:
        await update.message.reply_text("Usage: /broadcast <your message>")
        return
    await _do_broadcast(update.message.reply_text, ctx.application, text)


@owner_only
async def cmd_forceall(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/forceall – force update every connected batch."""
    msg = await update.message.reply_text(
        "⚡ Starting force-update for all connected batches…"
    )
    await _do_forceall(msg.edit_text, ctx.application)


# ═══════════════════════════════════════════════════════════════
#  BROADCAST LOGIC
# ═══════════════════════════════════════════════════════════════
async def _do_broadcast(reply_fn, app: Application, text: str):
    batches = db_get_all_active_batches()
    if not batches:
        await reply_fn("⚠️ No active connected batches to broadcast to.")
        return

    full_text = text + WATERMARK
    success, fail = 0, 0
    fail_details = []

    for batch in batches:
        try:
            msg = await app.bot.send_message(
                chat_id=batch["channel_id"],
                text=full_text,
                parse_mode="HTML",
            )
            await app.bot.pin_chat_message(
                chat_id=batch["channel_id"],
                message_id=msg.message_id,
                disable_notification=True,
            )
            success += 1
        except Exception as e:
            fail += 1
            fail_details.append(
                f"  • `{batch['channel_id']}` ({batch['platform']} / {batch['course_id']}): {e}"
            )

    report = (
        f"📡 *Broadcast Complete*\n"
        f"{'─' * 28}\n"
        f"✅ Sent & Pinned : `{success}`\n"
        f"❌ Failed        : `{fail}`\n"
    )
    if fail_details:
        report += "\n*Failures:*\n" + "\n".join(fail_details)

    await reply_fn(report, parse_mode="Markdown")


# ═══════════════════════════════════════════════════════════════
#  FORCE-ALL LOGIC
# ═══════════════════════════════════════════════════════════════
async def _do_forceall(status_fn, app: Application):
    batches = db_get_all_active_batches()
    if not batches:
        await status_fn("⚠️ No active connected batches found.")
        return

    await status_fn(
        f"⚡ *Force Update – All Batches*\n"
        f"{'─' * 28}\n"
        f"🔍 Found `{len(batches)}` active batch(es)…\nThis may take a while.",
        parse_mode="Markdown",
    )

    results = []
    async with aiohttp.ClientSession() as session:
        for batch in batches:
            platform = batch["platform"]
            course_id = batch["course_id"]
            channel_id = batch["channel_id"]
            cfg = pcfg(platform)
            posted = skipped = errors = 0

            try:
                files = await fetch_files_recursive(session, platform, course_id)
                for f in files:
                    cid = f["entity_id"]
                    if db_is_sent(platform, course_id, cid):
                        skipped += 1
                        continue
                    detail = await get_content_detail(
                        session, platform, cid, course_id, f.get("data")
                    )
                    if not detail:
                        log.warning(
                            f"[{platform}] No detail for content_id={cid}, will retry next run."
                        )
                        skipped += 1
                        continue  # ← do NOT mark as sent, retry next time

                    file_url = (detail.get("file_url") or "").strip()
                    if not file_url:
                        # Genuinely no file (e.g. live class placeholder) — safe to skip forever
                        log.info(
                            f"[{platform}] content_id={cid} has no file_url, marking done."
                        )
                        db_mark_sent(platform, course_id, cid)
                        skipped += 1
                        continue
                    await post_content(app, platform, channel_id, detail, f["title"])
                    db_mark_sent(platform, course_id, cid)
                    posted += 1
                    await asyncio.sleep(1.2)

                results.append(
                    {
                        "platform": platform,
                        "course_id": course_id,
                        "channel_id": channel_id,
                        "posted": posted,
                        "skipped": skipped,
                        "errors": 0,
                        "ok": True,
                    }
                )
            except Exception as e:
                log.error(f"[{platform}] forceall error for course {course_id}: {e}")
                results.append(
                    {
                        "platform": platform,
                        "course_id": course_id,
                        "channel_id": channel_id,
                        "posted": posted,
                        "skipped": skipped,
                        "errors": 1,
                        "ok": False,
                        "err_msg": str(e),
                    }
                )

    # Build summary
    total_posted = sum(r["posted"] for r in results)
    total_errors = sum(r["errors"] for r in results)
    lines = [
        f"⚡ *Force Update – Summary*",
        f"{'─' * 28}",
        f"📦 Batches processed : `{len(results)}`",
        f"📤 Total posted      : `{total_posted}`",
        f"❌ Total errors      : `{total_errors}`",
        f"{'─' * 28}",
    ]
    for r in results:
        icon = "✅" if r["ok"] else "❌"
        cfg = pcfg(r["platform"])
        lines.append(
            f"{icon} {cfg['emoji']} Course `{r['course_id']}` → `{r['channel_id']}`\n"
            f"   📤 {r['posted']} posted  |  ⏭ {r['skipped']} skipped"
            + (f"\n   ⚠️ {r.get('err_msg', '')}" if not r["ok"] else "")
        )

    await status_fn("\n".join(lines), parse_mode="Markdown")


# ═══════════════════════════════════════════════════════════════
#  CALLBACKS
# ═══════════════════════════════════════════════════════════════
async def cb_noop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()


async def cb_pick(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        await query.answer("⛔ Access denied.", show_alert=True)
        return
    platform = query.data.split(":")[1]
    await show_courses(query.edit_message_text, platform)


async def cb_back(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    platform = query.data.split(":")[1]
    await show_courses(query.edit_message_text, platform)


async def cb_course(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        await query.answer("⛔ Access denied.", show_alert=True)
        return

    _, platform, cid = query.data.split(":")
    course_id = int(cid)
    cfg = pcfg(platform)
    batch = db_get_batch(platform, course_id)

    channel_id = batch["channel_id"] if batch else None
    status = (batch["status"] if batch else "active") or "active"
    s_icon = "▶️" if status == "active" else "⏸"
    chan_txt = f"`{channel_id}`" if channel_id else "─ _not set_ ─"
    conn_badge = "✅ Connected" if channel_id else "○  Not Connected"

    text = (
        f"{'─' * 30}\n"
        f"{cfg['emoji']}  *{cfg['label']}*\n"
        f"Course ID :  `{course_id}`\n"
        f"{'─' * 30}\n"
        f"📡  Channel  :  {chan_txt}\n"
        f"🔗  Status   :  {conn_badge}\n"
        f"▶️  Mode     :  {s_icon} {status}\n"
        f"{'─' * 30}"
    )

    kb = [
        [
            InlineKeyboardButton(
                "➕ Set Channel", callback_data=f"setchan:{platform}:{course_id}"
            ),
            InlineKeyboardButton(
                "➖ Remove Channel", callback_data=f"rmchan:{platform}:{course_id}"
            ),
        ],
        [
            InlineKeyboardButton(
                "⏸ Pause", callback_data=f"pause:{platform}:{course_id}"
            ),
            InlineKeyboardButton(
                "▶️ Resume", callback_data=f"resume:{platform}:{course_id}"
            ),
        ],
        [
            InlineKeyboardButton(
                "🔄 Restart (reset sent)",
                callback_data=f"rst_confirm:{platform}:{course_id}",
            )
        ],
        [
            InlineKeyboardButton(
                "⚡ Force Update Now",
                callback_data=f"forceupdate:{platform}:{course_id}",
            )
        ],
    ] + back_btn(platform)

    await query.edit_message_text(
        text, reply_markup=InlineKeyboardMarkup(kb), parse_mode="Markdown"
    )


async def cb_setchan(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    _, platform, cid = query.data.split(":")
    ctx.user_data["awaiting_channel"] = (platform, int(cid))
    await query.edit_message_text(
        f"📩  Send the *channel ID* (e.g. `-1001234567890`)\nto link to course `{cid}`:",
        parse_mode="Markdown",
    )


async def cb_rmchan(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    _, platform, cid = query.data.split(":")
    db_upsert_batch(platform, int(cid), remove_channel=True)
    await query.edit_message_text(
        f"✅ Channel removed from course `{cid}`.\nUse /start to go back.",
        parse_mode="Markdown",
    )


async def cb_pause(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    _, platform, cid = query.data.split(":")
    db_set_status(platform, int(cid), "paused")
    await query.edit_message_text(
        f"⏸ Course `{cid}` paused.\nUse /start to go back.", parse_mode="Markdown"
    )


async def cb_resume(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    _, platform, cid = query.data.split(":")
    db_set_status(platform, int(cid), "active")
    await query.edit_message_text(
        f"▶️ Course `{cid}` resumed.\nUse /start to go back.", parse_mode="Markdown"
    )


async def cb_rst_confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    _, platform, cid = query.data.split(":")
    kb = [
        [
            InlineKeyboardButton(
                "✅ Yes, reset", callback_data=f"restart:{platform}:{cid}"
            ),
            InlineKeyboardButton("❌ Cancel", callback_data=f"course:{platform}:{cid}"),
        ]
    ]
    await query.edit_message_text(
        f"⚠️ *Restart course {cid}?*\n\n"
        "All sent-file records will be deleted.\n"
        "Content will re-post on the next check.",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="Markdown",
    )


async def cb_restart(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    _, platform, cid = query.data.split(":")
    db_reset_sent(platform, int(cid))
    await query.edit_message_text(
        f"🔄 Course `{cid}` reset.\nContent re-posts on next scheduled check.",
        parse_mode="Markdown",
    )


async def cb_broadcast_prompt(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    ctx.user_data["awaiting_broadcast"] = True
    await query.edit_message_text(
        "📡 *Broadcast Message*\n\n"
        "Send the message text you want to broadcast to *all connected channels*.\n"
        "It will be pinned automatically.\n\n"
        "You can use HTML formatting.",
        parse_mode="Markdown",
    )


async def cb_forceall_confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    kb = [
        [
            InlineKeyboardButton("⚡ Yes, force all", callback_data="forceall_go"),
            InlineKeyboardButton("❌ Cancel", callback_data="cancel_action"),
        ]
    ]
    await query.edit_message_text(
        "⚡ *Force Update All Batches?*\n\n"
        "This will scan every connected course and post any new content.\n"
        "This may take several minutes.",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="Markdown",
    )


async def cb_forceall_go(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("⚡ Starting…", show_alert=False)
    if query.from_user.id != OWNER_ID:
        return
    await _do_forceall(query.edit_message_text, ctx.application)


async def cb_cancel_action(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    kb = [
        [
            InlineKeyboardButton("📘 NextToppers", callback_data="pick:nt"),
            InlineKeyboardButton("🎯 MissionJeet", callback_data="pick:mj"),
        ],
        [
            InlineKeyboardButton("📡 Broadcast All", callback_data="broadcast_prompt"),
            InlineKeyboardButton("⚡ Force All", callback_data="forceall_confirm"),
        ],
        [
            InlineKeyboardButton("🗄️ Backup Now", callback_data="backup_now"),
        ],
    ]
    await query.edit_message_text(
        "╔══════════════════════════╗\n"
        "║   🤖  *Content Manager*   ║\n"
        "╚══════════════════════════╝\n\n"
        "Action cancelled. Choose a platform:",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="Markdown",
    )


async def cb_forceupdate(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("⚡ Starting…", show_alert=False)
    if query.from_user.id != OWNER_ID:
        return

    _, platform, cid = query.data.split(":")
    course_id = int(cid)
    cfg = pcfg(platform)
    batch = db_get_batch(platform, course_id)

    if not batch or not batch.get("channel_id"):
        await query.edit_message_text(
            f"⚠️ Course {course_id} has no channel. Set one first."
        )
        return

    channel_id = batch["channel_id"]
    await query.edit_message_text(
        f"{cfg['emoji']} *{cfg['label']} – Course {course_id}*\n\n"
        f"⚡ Force update running…\n📂 Fetching content list…",
        parse_mode="Markdown",
    )

    posted = skipped = errors = 0
    try:
        async with aiohttp.ClientSession() as session:
            files = await fetch_files_recursive(session, platform, course_id)
            for f in files:
                content_id = f["entity_id"]
                if db_is_sent(platform, course_id, content_id):
                    skipped += 1
                    continue
                detail = await get_content_detail(
                    session, platform, content_id, course_id, f.get("data")
                )
                if not detail:
                    log.warning(
                        f"[{platform}] No detail for content_id={cid}, will retry next run."
                    )
                    skipped += 1
                    continue  # ← do NOT mark as sent, retry next time

                file_url = (detail.get("file_url") or "").strip()
                if not file_url:
                    # Genuinely no file (e.g. live class placeholder) — safe to skip forever
                    log.info(
                        f"[{platform}] content_id={cid} has no file_url, marking done."
                    )
                    db_mark_sent(platform, course_id, cid)
                    skipped += 1
                    continue
                await post_content(
                    ctx.application, platform, channel_id, detail, f["title"]
                )
                db_mark_sent(platform, course_id, content_id)
                posted += 1
                await asyncio.sleep(1.2)
    except Exception as e:
        log.error(f"[{platform}] force update error {course_id}: {e}")
        errors += 1

    status_line = "✅ Done" if not errors else "⚠️ Finished with errors"
    kb = [
        [
            InlineKeyboardButton(
                "➕ Set Channel", callback_data=f"setchan:{platform}:{course_id}"
            ),
            InlineKeyboardButton(
                "➖ Remove Channel", callback_data=f"rmchan:{platform}:{course_id}"
            ),
        ],
        [
            InlineKeyboardButton(
                "⏸ Pause", callback_data=f"pause:{platform}:{course_id}"
            ),
            InlineKeyboardButton(
                "▶️ Resume", callback_data=f"resume:{platform}:{course_id}"
            ),
        ],
        [
            InlineKeyboardButton(
                "🔄 Restart (reset sent)",
                callback_data=f"rst_confirm:{platform}:{course_id}",
            )
        ],
        [
            InlineKeyboardButton(
                "⚡ Force Update Now",
                callback_data=f"forceupdate:{platform}:{course_id}",
            )
        ],
    ] + back_btn(platform)

    await query.edit_message_text(
        f"{'─' * 30}\n"
        f"{cfg['emoji']} *{cfg['label']} – Course {course_id}*\n"
        f"{'─' * 30}\n"
        f"📡 Channel : `{channel_id}`\n"
        f"{'─' * 30}\n\n"
        f"{status_line}\n"
        f"📤 Posted  : `{posted}`\n"
        f"⏭ Skipped : `{skipped}`\n" + (f"❌ Errors  : `{errors}`\n" if errors else ""),
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="Markdown",
    )


async def msg_text_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle free-text: channel ID input OR broadcast text."""
    if update.effective_user.id != OWNER_ID:
        return

    # ── Awaiting broadcast message ──────────────────────────────
    if ctx.user_data.get("awaiting_broadcast"):
        ctx.user_data.pop("awaiting_broadcast")
        text = update.message.text.strip()
        reply = await update.message.reply_text("📡 Broadcasting…")
        await _do_broadcast(reply.edit_text, ctx.application, text)
        return

    # ── Awaiting channel ID ─────────────────────────────────────
    pending = ctx.user_data.get("awaiting_channel")
    if not pending:
        return
    platform, course_id = pending
    raw = update.message.text.strip()
    try:
        channel_id = int(raw)
    except ValueError:
        await update.message.reply_text("❌ Must be a number like `-1001234567890`.")
        return
    db_upsert_batch(platform, course_id, channel_id=channel_id)
    ctx.user_data.pop("awaiting_channel", None)
    cfg = pcfg(platform)
    await update.message.reply_text(
        f"✅ Channel `{channel_id}` linked to *{cfg['label']}* course `{course_id}`.",
        parse_mode="Markdown",
    )


# ═══════════════════════════════════════════════════════════════
#  CONTENT POLLER
# ═══════════════════════════════════════════════════════════════
async def fetch_files_recursive(session, platform, course_id, folder_id=None, _depth=0):
    if _depth > 10:
        log.warning(f"[{platform}] Max recursion depth hit for course {course_id}")
        return []

    cfg = pcfg(platform)
    qs = cfg["content_qs"](course_id, folder_id)
    url = cfg["content_url"] + qs
    log.debug(f"[{platform}] fetch url={url}")

    try:
        resp = await api_get(session, url)  # already retries 3x
    except Exception as e:
        log.warning(
            f"[{platform}] content fetch failed cid={course_id} fid={folder_id}: {e}"
        )
        return []  # folder fetch failed even after retries, skip gracefully

    items = resp.get("data") or []
    result = []
    for item in items:
        itype = item.get("type")
        if itype == "folder":
            fid = item.get("entity_id")
            if fid:
                sub = await fetch_files_recursive(
                    session, platform, course_id, fid, _depth=_depth + 1
                )
                result.extend(sub)
        elif itype == "file":
            result.append(item)
        else:
            # Some APIs return items without a type — treat as file
            if item.get("entity_id"):
                log.debug(
                    f"[{platform}] Unknown item type '{itype}' for entity {item.get('entity_id')}, treating as file"
                )
                result.append(item)
    return result


async def get_content_detail(
    session, platform, content_id, course_id, inline_data: dict | None = None
):
    """
    Return the content detail dict for a file item.
    If the tree-walk already gave us the detail payload inside item["data"],
    we use that directly to save an extra API call.
    Falls back to the dedicated content-details endpoint.
    """
    if inline_data and inline_data.get("file_url") is not None:
        return inline_data

    cfg = pcfg(platform)
    qs = cfg["details_qs"](content_id, course_id)
    url = cfg["details_url"] + qs
    try:
        resp = await api_get(session, url)
        return resp.get("data") or None
    except Exception as e:
        log.warning(f"[{platform}] details failed content_id={content_id}: {e}")
        return None


async def post_content(app: Application, platform, channel_id, detail, title):
    file_url = (detail.get("file_url") or "").strip()
    file_type = detail.get("file_type")
    thumb = (detail.get("thumbnail") or "").strip()
    duration = detail.get("duration")

    if not file_url:
        return

    is_video = (file_type == 2) or bool(detail.get("video_type"))
    open_url = make_player_url(file_url) if is_video else file_url
    tag = "🎬 Video" if is_video else "📄 PDF"

    # Duration only makes sense for videos
    dur_txt = ""
    if is_video and duration:
        try:
            s = int(duration)
            if s > 0:
                dur_txt = f" • {s // 60}m {s % 60}s"
        except (ValueError, TypeError):
            pass

    cfg = pcfg(platform)
    caption = (
        f"{tag}{dur_txt}\n" f"<b>{title}</b>\n" f"<i>{cfg['label']}</i>" f"{WATERMARK}"
    )
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Open", url=open_url)]])

    try:
        if thumb and thumb.startswith("http"):
            await app.bot.send_photo(
                chat_id=channel_id,
                photo=thumb,
                caption=caption,
                reply_markup=kb,
                parse_mode="HTML",
            )
        else:
            await app.bot.send_message(
                chat_id=channel_id, text=caption, reply_markup=kb, parse_mode="HTML"
            )
    except Exception as e:
        log.error(f"[{platform}] post failed → channel {channel_id}: {e}")
        raise


# ═══════════════════════════════════════════════════════════════
#  SCHEDULED DAILY CHECK + LOGS
# ═══════════════════════════════════════════════════════════════
async def check_and_post(app: Application):
    log.info("=== Daily content check started ===")
    batches = db_get_all_active_batches()
    if not batches:
        log.info("No active batches.")
        return

    results = []
    async with aiohttp.ClientSession() as session:
        for batch in batches:
            platform = batch["platform"]
            course_id = batch["course_id"]
            channel_id = batch["channel_id"]
            cfg = pcfg(platform)
            posted = skipped = 0
            err_msgs = []

            log.info(f"[{platform}] Scanning course {course_id} → {channel_id}")
            try:
                files = await fetch_files_recursive(session, platform, course_id)
                for f in files:
                    cid = f["entity_id"]
                    if db_is_sent(platform, course_id, cid):
                        skipped += 1
                        continue
                    detail = await get_content_detail(
                        session, platform, cid, course_id, f.get("data")
                    )
                    if not detail:
                        log.warning(
                            f"[{platform}] No detail for content_id={cid}, will retry next run."
                        )
                        skipped += 1
                        continue  # ← do NOT mark as sent, retry next time

                    file_url = (detail.get("file_url") or "").strip()
                    if not file_url:
                        # Genuinely no file (e.g. live class placeholder) — safe to skip forever
                        log.info(
                            f"[{platform}] content_id={cid} has no file_url, marking done."
                        )
                        db_mark_sent(platform, course_id, cid)
                        skipped += 1
                        continue
                    try:
                        await post_content(
                            app, platform, channel_id, detail, f["title"]
                        )
                        db_mark_sent(platform, course_id, cid)
                        posted += 1
                        await asyncio.sleep(1.2)
                    except Exception as e:
                        err_msgs.append(str(e))
            except Exception as e:
                err_msgs.append(f"Scan error: {e}")

            results.append(
                {
                    "platform": platform,
                    "course_id": course_id,
                    "channel_id": channel_id,
                    "posted": posted,
                    "skipped": skipped,
                    "errors": err_msgs,
                }
            )
            log.info(
                f"[{platform}] Course {course_id}: {posted} posted, {len(err_msgs)} errors."
            )

    # ── Send daily log to owner ──────────────────────────────────
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"📋 *Daily Update Log*",
        f"🕛 {now}",
        f"{'─' * 30}",
    ]
    for r in results:
        cfg = pcfg(r["platform"])
        icon = "✅" if not r["errors"] else "⚠️"
        lines.append(
            f"{icon} {cfg['emoji']} Course `{r['course_id']}` → `{r['channel_id']}`\n"
            f"   📤 {r['posted']} posted  |  ⏭ {r['skipped']} skipped"
        )
        for err in r["errors"]:
            lines.append(f"   ❌ `{err[:80]}`")

    total_posted = sum(r["posted"] for r in results)
    total_errors = sum(len(r["errors"]) for r in results)
    lines += [
        f"{'─' * 30}",
        f"📦 Batches : `{len(results)}`  |  📤 Posted : `{total_posted}`  |  ❌ Errors : `{total_errors}`",
    ]

    try:
        await app.bot.send_message(
            chat_id=OWNER_ID,
            text="\n".join(lines),
            parse_mode="Markdown",
        )
    except Exception as e:
        log.error(f"Failed to send daily log to owner: {e}")


# ═══════════════════════════════════════════════════════════════
#  NOTE: DB backup logic lives in _do_db_backup() (defined above)
#        and is triggered by the nightly job + /backup + Backup Now button.
# ═══════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    start_flask()
    db_init()

    app = Application.builder().token(BOT_TOKEN).build()

    # ── Commands ────────────────────────────────────────────────
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("batches", cmd_batches))
    app.add_handler(CommandHandler("broadcast", _do_broadcast))
    app.add_handler(CommandHandler("forceall", cmd_forceall))
    app.add_handler(CommandHandler("backup", cmd_backup))

    # ── Callbacks ───────────────────────────────────────────────
    app.add_handler(CallbackQueryHandler(cb_noop, pattern=r"^noop$"))
    app.add_handler(CallbackQueryHandler(cb_pick, pattern=r"^pick:(nt|mj)$"))
    app.add_handler(CallbackQueryHandler(cb_back, pattern=r"^back:(nt|mj)$"))
    app.add_handler(CallbackQueryHandler(cb_course, pattern=r"^course:(nt|mj):\d+$"))
    app.add_handler(CallbackQueryHandler(cb_setchan, pattern=r"^setchan:(nt|mj):\d+$"))
    app.add_handler(CallbackQueryHandler(cb_rmchan, pattern=r"^rmchan:(nt|mj):\d+$"))
    app.add_handler(CallbackQueryHandler(cb_pause, pattern=r"^pause:(nt|mj):\d+$"))
    app.add_handler(CallbackQueryHandler(cb_resume, pattern=r"^resume:(nt|mj):\d+$"))
    app.add_handler(
        CallbackQueryHandler(cb_rst_confirm, pattern=r"^rst_confirm:(nt|mj):\d+$")
    )
    app.add_handler(CallbackQueryHandler(cb_restart, pattern=r"^restart:(nt|mj):\d+$"))
    app.add_handler(
        CallbackQueryHandler(cb_forceupdate, pattern=r"^forceupdate:(nt|mj):\d+$")
    )
    app.add_handler(
        CallbackQueryHandler(cb_broadcast_prompt, pattern=r"^broadcast_prompt$")
    )
    app.add_handler(
        CallbackQueryHandler(cb_forceall_confirm, pattern=r"^forceall_confirm$")
    )
    app.add_handler(CallbackQueryHandler(cb_forceall_go, pattern=r"^forceall_go$"))
    app.add_handler(CallbackQueryHandler(cb_cancel_action, pattern=r"^cancel_action$"))
    app.add_handler(CallbackQueryHandler(cb_backup_now, pattern=r"^backup_now$"))

    # ── Free-text handler ───────────────────────────────────────
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, msg_text_handler))

    # ── DB keep-alive ping every 2 hours ────────────────────────
    app.job_queue.run_repeating(
        lambda ctx: db_ping(),
        interval=7200,
        first=60,
    )

    # ── Daily content check at midnight UTC ─────────────────────
    # Uses run_daily so it always fires at exactly 00:00 UTC,
    # not "N hours after boot" which drifts over time.
    app.job_queue.run_daily(
        lambda ctx: asyncio.create_task(check_and_post(app)),
        time=datetime.time(hour=0, minute=0, second=0, tzinfo=datetime.timezone.utc),
    )

    # ── Nightly DB backup at 00:05 UTC ──────────────────────────
    app.job_queue.run_daily(
        lambda ctx: asyncio.create_task(_do_db_backup(app)),
        time=datetime.time(hour=0, minute=5, second=0, tzinfo=datetime.timezone.utc),
    )

    log.info("Bot polling started.")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
