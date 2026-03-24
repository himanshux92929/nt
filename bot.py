"""
bot.py – NextToppers + MissionJeet Telegram Bot
Runs alongside a dummy Flask server so Render keeps the dyno alive.
"""

import os
import asyncio
import logging
import threading
import base64
from urllib.parse import quote

import aiohttp
from flask import Flask
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes, MessageHandler, filters,
)

import db

# ═══════════════════════ CONFIG ════════════════════════
# Sensitive – must be set as env vars (Render → Environment)
BOT_TOKEN = os.environ["BOT_TOKEN"]        # from @BotFather
OWNER_ID  = int(os.environ["OWNER_ID"])    # your Telegram numeric user ID

# Non-sensitive – hardcoded, change here if needed
CHECK_INTERVAL_HOURS = 24          # hours between content checks
FLASK_PORT           = int(os.getenv("PORT", "8080"))  # Render injects $PORT

PLAYER_BASE   = "https://smarterz.netlify.app/ntplayer"
PLAYER_SECRET = "smarterzpro"

# ── Platform definitions ────────────────────────────────
# content_qs(course_id, folder_id)  → query/path suffix for all-content
# details_qs(content_id, course_id) → query string for content-details
PLATFORMS = {
    "nt": {
        "label":      "NextToppers",
        "emoji":      "📘",
        "batches_url": "https://apiserverpro.onrender.com/api/nexttoppers/batches",
        "content_url": "https://apiserverpro.onrender.com/api/nexttoppers/all-content",
        "details_url": "https://apiserverpro.onrender.com/api/nexttoppers/content-details",
        "content_qs":  lambda cid, fid=None: f"?courseid={cid}" + (f"&id={fid}" if fid else ""),
        "details_qs":  lambda cid, course_id: f"?content_id={cid}&courseid={course_id}",
    },
    "mj": {
        "label":      "MissionJeet",
        "emoji":      "🎯",
        "batches_url": "https://apiserverpro.onrender.com/api/missionjeet/batches",
        "content_url": "https://apiserverpro.onrender.com/api/missionjeet/all-content",
        "details_url": "https://apiserverpro.onrender.com/api/missionjeet/content-details",
        "content_qs":  lambda cid, fid=None: f"/{cid}" + (f"?id={fid}" if fid else ""),
        "details_qs":  lambda cid, course_id: f"?content_id={cid}&course_id={course_id}",
    },
}
# ════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)


# ═══════════════════════ FLASK KEEP-ALIVE ══════════════

flask_app = Flask(__name__)

@flask_app.route("/")
def health():
    return "OK – Bot is running", 200

@flask_app.route("/ping")
def ping():
    return "pong", 200

def _run_flask():
    # Use werkzeug directly so we can suppress the noisy reloader
    from werkzeug.serving import make_server
    server = make_server("0.0.0.0", FLASK_PORT, flask_app)
    log.info(f"Flask keep-alive listening on port {FLASK_PORT}")
    server.serve_forever()

def start_flask():
    t = threading.Thread(target=_run_flask, daemon=True)
    t.start()

# ════════════════════════════════════════════════════════


# ═══════════════════════ HELPERS ═══════════════════════

def owner_only(func):
    """Restrict private-chat commands to OWNER_ID."""
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if (
            update.effective_chat.type == "private"
            and update.effective_user.id != OWNER_ID
        ):
            await update.effective_message.reply_text("⛔ Access denied.")
            return
        return await func(update, ctx, *args, **kwargs)
    return wrapper


async def api_get(session: aiohttp.ClientSession, url: str) -> dict:
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
        r.raise_for_status()
        return await r.json()


def encode_token(url: str) -> str:
    """XOR each character of the URL against the cycling key, then base64-encode."""
    key    = PLAYER_SECRET
    xored  = bytes([ord(url[i]) ^ ord(key[i % len(key)]) for i in range(len(url))])
    return base64.b64encode(xored).decode()


def make_player_url(file_url: str) -> str:
    return f"{PLAYER_BASE}?token={quote(encode_token(file_url))}"


def pcfg(platform: str) -> dict:
    return PLATFORMS[platform]


def back_btn(platform: str) -> list:
    return [[InlineKeyboardButton("« Back to Courses", callback_data=f"back:{platform}")]]


# ═══════════════════════ COURSE LIST HELPER ════════════

async def show_courses(edit_fn, platform: str):
    """Fetch & render the course list. edit_fn = query.edit_message_text or msg.edit_text."""
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
            if "list" in item:               # layout-wrapped list
                courses.extend(item["list"])
            elif "id" in item:               # flat item
                courses.append(item)

    if not courses:
        await edit_fn("No courses found.")
        return

    keyboard = []
    for c in courses:
        trending = "🔥 " if c.get("is_trending") else ""
        label = f"{trending}{cfg['emoji']} [{c['id']}] {str(c['title'])[:42]}"
        keyboard.append([InlineKeyboardButton(label, callback_data=f"course:{platform}:{c['id']}")])

    keyboard.append([
        InlineKeyboardButton("📘 NextToppers", callback_data="pick:nt"),
        InlineKeyboardButton("🎯 MissionJeet",  callback_data="pick:mj"),
    ])

    await edit_fn(
        f"{cfg['emoji']} *{cfg['label']} – All Courses*\nTap a course to manage:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown",
    )


# ═══════════════════════ COMMANDS ══════════════════════

@owner_only
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    kb = [[
        InlineKeyboardButton("📘 NextToppers", callback_data="pick:nt"),
        InlineKeyboardButton("🎯 MissionJeet",  callback_data="pick:mj"),
    ]]
    await update.message.reply_text(
        "👋 *Content Bot*\n\nChoose a platform:",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="Markdown",
    )


@owner_only
async def cmd_batches(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/batches nt   or   /batches mj"""
    args = ctx.args
    platform = args[0].lower() if args and args[0].lower() in PLATFORMS else None
    if not platform:
        kb = [[
            InlineKeyboardButton("📘 NextToppers", callback_data="pick:nt"),
            InlineKeyboardButton("🎯 MissionJeet",  callback_data="pick:mj"),
        ]]
        await update.message.reply_text(
            "Which platform?",
            reply_markup=InlineKeyboardMarkup(kb),
        )
        return
    msg = await update.message.reply_text("⏳ Fetching courses…")
    await show_courses(msg.edit_text, platform)


# ═══════════════════════ CALLBACKS ═════════════════════

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
    cfg   = pcfg(platform)
    batch = db.get_batch(platform, course_id)

    channel_id  = batch["channel_id"] if batch else None
    status      = (batch["status"]    if batch else "active") or "active"
    s_icon      = "▶️" if status == "active" else "⏸"
    chan_txt     = f"`{channel_id}`" if channel_id else "_none_"

    text = (
        f"{cfg['emoji']} *{cfg['label']} – Course {course_id}*\n\n"
        f"Channel : {chan_txt}\n"
        f"Status  : {s_icon} {status}\n"
    )

    kb = [
        [InlineKeyboardButton("➕ Set Channel",    callback_data=f"setchan:{platform}:{course_id}"),
         InlineKeyboardButton("➖ Remove Channel", callback_data=f"rmchan:{platform}:{course_id}")],
        [InlineKeyboardButton("⏸ Pause",          callback_data=f"pause:{platform}:{course_id}"),
         InlineKeyboardButton("▶️ Resume",         callback_data=f"resume:{platform}:{course_id}")],
        [InlineKeyboardButton("🔄 Restart (reset sent)", callback_data=f"rst_confirm:{platform}:{course_id}")],
        [InlineKeyboardButton("⚡ Force Update Now",     callback_data=f"forceupdate:{platform}:{course_id}")],
    ] + back_btn(platform)

    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode="Markdown")


async def cb_setchan(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    _, platform, cid = query.data.split(":")
    ctx.user_data["awaiting_channel"] = (platform, int(cid))
    await query.edit_message_text(
        f"Send the *channel ID* (e.g. `-1001234567890`) to link to course {cid}:",
        parse_mode="Markdown",
    )


async def cb_rmchan(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    _, platform, cid = query.data.split(":")
    db.upsert_batch(platform, int(cid), remove_channel=True)
    await query.edit_message_text(f"✅ Channel removed from course {cid}. /start to go back.")


async def cb_pause(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    _, platform, cid = query.data.split(":")
    db.set_status(platform, int(cid), "paused")
    await query.edit_message_text(f"⏸ Course {cid} paused. /start to go back.")


async def cb_resume(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    _, platform, cid = query.data.split(":")
    db.set_status(platform, int(cid), "active")
    await query.edit_message_text(f"▶️ Course {cid} resumed. /start to go back.")


async def cb_rst_confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    _, platform, cid = query.data.split(":")
    kb = [[
        InlineKeyboardButton("✅ Yes, reset",   callback_data=f"restart:{platform}:{cid}"),
        InlineKeyboardButton("❌ Cancel",        callback_data=f"course:{platform}:{cid}"),
    ]]
    await query.edit_message_text(
        f"⚠️ *Restart course {cid}?*\n\n"
        "All sent-file records will be deleted and content will re-post on next check.",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="Markdown",
    )


async def cb_restart(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    _, platform, cid = query.data.split(":")
    db.reset_sent(platform, int(cid))
    await query.edit_message_text(f"🔄 Course {cid} reset. Content re-posts on next check.")


async def msg_channel_input(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Accept free-text channel ID after 'Set Channel' is tapped."""
    if update.effective_user.id != OWNER_ID:
        return
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
    db.upsert_batch(platform, course_id, channel_id=channel_id)
    ctx.user_data.pop("awaiting_channel", None)
    cfg = pcfg(platform)
    await update.message.reply_text(
        f"✅ Channel `{channel_id}` linked to {cfg['label']} course {course_id}.",
        parse_mode="Markdown",
    )


# ═══════════════════════ CONTENT POLLER ════════════════

async def fetch_files_recursive(
    session: aiohttp.ClientSession,
    platform: str,
    course_id: int,
    folder_id: int = 0,
) -> list:
    cfg = pcfg(platform)
    qs  = cfg["content_qs"](course_id, folder_id if folder_id else None)
    url = cfg["content_url"] + qs
    try:
        data = await api_get(session, url)
    except Exception as e:
        log.warning(f"[{platform}] content fetch failed {course_id}/{folder_id}: {e}")
        return []

    result = []
    for item in data.get("data", []):
        if item.get("type") == "folder":
            sub = await fetch_files_recursive(session, platform, course_id, item["entity_id"])
            result.extend(sub)
        elif item.get("type") == "file":
            result.append(item)
    return result


async def get_content_detail(
    session: aiohttp.ClientSession,
    platform: str,
    content_id: int,
    course_id: int,
) -> dict | None:
    cfg = pcfg(platform)
    qs  = cfg["details_qs"](content_id, course_id)
    url = cfg["details_url"] + qs
    try:
        data = await api_get(session, url)
        return data.get("data")
    except Exception as e:
        log.warning(f"[{platform}] details failed {content_id}: {e}")
        return None


async def post_content(
    app: Application,
    platform: str,
    channel_id: int,
    detail: dict,
    title: str,
):
    file_url  = detail.get("file_url", "")
    file_type = detail.get("file_type")
    # Always use the thumbnail returned by content-details API
    thumb     = (detail.get("thumbnail") or "").strip()
    duration  = detail.get("duration", "")

    if not file_url:
        return

    is_video = (file_type == 2) or bool(detail.get("video_type"))
    open_url = make_player_url(file_url) if is_video else file_url
    tag      = "🎬 Video" if is_video else "📄 PDF"

    dur_txt = ""
    if duration:
        try:
            s = int(duration)
            dur_txt = f" • {s // 60}m {s % 60}s"
        except (ValueError, TypeError):
            pass

    cfg     = pcfg(platform)
    caption = f"{tag}{dur_txt}\n<b>{title}</b>\n<i>{cfg['label']}</i>"
    kb      = InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Open", url=open_url)]])

    try:
        if thumb and thumb.startswith("http"):
            # Always send as photo with the thumbnail from content details
            await app.bot.send_photo(
                chat_id=channel_id, photo=thumb,
                caption=caption, reply_markup=kb, parse_mode="HTML",
            )
        else:
            # No thumbnail available — fall back to plain message
            await app.bot.send_message(
                chat_id=channel_id, text=caption,
                reply_markup=kb, parse_mode="HTML",
            )
    except Exception as e:
        log.error(f"[{platform}] post failed → channel {channel_id}: {e}")


async def cb_forceupdate(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Immediately run the content check for a single batch and report results."""
    query = update.callback_query
    await query.answer("⚡ Starting force update…", show_alert=False)
    if query.from_user.id != OWNER_ID:
        return

    _, platform, cid = query.data.split(":")
    course_id = int(cid)
    cfg   = pcfg(platform)
    batch = db.get_batch(platform, course_id)

    if not batch or not batch.get("channel_id"):
        await query.edit_message_text(
            f"⚠️ Course {course_id} has no channel linked. Set a channel first.",
        )
        return

    channel_id = batch["channel_id"]

    # Edit message to show progress
    await query.edit_message_text(
        f"{cfg['emoji']} *{cfg['label']} – Course {course_id}*\n\n"
        f"⚡ Force update running…\nFetching content list, please wait.",
        parse_mode="Markdown",
    )

    posted  = 0
    skipped = 0
    errors  = 0

    try:
        async with aiohttp.ClientSession() as session:
            files = await fetch_files_recursive(session, platform, course_id)

            for f in files:
                content_id = f["entity_id"]
                if db.is_sent(platform, course_id, content_id):
                    skipped += 1
                    continue

                detail = await get_content_detail(session, platform, content_id, course_id)
                if not detail or not detail.get("file_url"):
                    db.mark_sent(platform, course_id, content_id)
                    skipped += 1
                    continue

                await post_content(ctx.application, platform, channel_id, detail, f["title"])
                db.mark_sent(platform, course_id, content_id)
                posted += 1
                await asyncio.sleep(1.2)

    except Exception as e:
        log.error(f"[{platform}] force update error for course {course_id}: {e}")
        errors += 1

    # Report result back in the same message
    status_line = "✅ Done" if not errors else "⚠️ Finished with errors"
    chan_txt = f"`{channel_id}`"

    kb = [
        [InlineKeyboardButton("➕ Set Channel",    callback_data=f"setchan:{platform}:{course_id}"),
         InlineKeyboardButton("➖ Remove Channel", callback_data=f"rmchan:{platform}:{course_id}")],
        [InlineKeyboardButton("⏸ Pause",          callback_data=f"pause:{platform}:{course_id}"),
         InlineKeyboardButton("▶️ Resume",         callback_data=f"resume:{platform}:{course_id}")],
        [InlineKeyboardButton("🔄 Restart (reset sent)", callback_data=f"rst_confirm:{platform}:{course_id}")],
        [InlineKeyboardButton("⚡ Force Update Now",     callback_data=f"forceupdate:{platform}:{course_id}")],
    ] + back_btn(platform)

    await query.edit_message_text(
        f"{cfg['emoji']} *{cfg['label']} – Course {course_id}*\n\n"
        f"Channel : {chan_txt}\n"
        f"Status  : {'▶️' if batch.get('status','active') == 'active' else '⏸'} {batch.get('status','active')}\n\n"
        f"{status_line}\n"
        f"📤 Posted : {posted}\n"
        f"⏭ Skipped : {skipped} (already sent)\n"
        + (f"❌ Errors  : {errors}\n" if errors else ""),
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="Markdown",
    )



    batches = db.get_all_active_batches()
    if not batches:
        log.info("No active batches with channels configured.")
        return

    async with aiohttp.ClientSession() as session:
        for batch in batches:
            platform   = batch["platform"]
            course_id  = batch["course_id"]
            channel_id = batch["channel_id"]

            log.info(f"[{platform}] Scanning course {course_id} → channel {channel_id}")
            files  = await fetch_files_recursive(session, platform, course_id)
            posted = 0

            for f in files:
                content_id = f["entity_id"]
                if db.is_sent(platform, course_id, content_id):
                    continue

                detail = await get_content_detail(session, platform, content_id, course_id)
                if not detail or not detail.get("file_url"):
                    db.mark_sent(platform, course_id, content_id)   # skip permanently
                    continue

                await post_content(app, platform, channel_id, detail, f["title"])
                db.mark_sent(platform, course_id, content_id)
                posted += 1
                await asyncio.sleep(1.2)

            log.info(f"[{platform}] Course {course_id}: {posted} new item(s) posted.")


# ═══════════════════════ MAIN ══════════════════════════

def main():
    start_flask()
    db.init()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("batches", cmd_batches))

    app.add_handler(CallbackQueryHandler(cb_pick,        pattern=r"^pick:(nt|mj)$"))
    app.add_handler(CallbackQueryHandler(cb_back,        pattern=r"^back:(nt|mj)$"))
    app.add_handler(CallbackQueryHandler(cb_course,      pattern=r"^course:(nt|mj):\d+$"))
    app.add_handler(CallbackQueryHandler(cb_setchan,     pattern=r"^setchan:(nt|mj):\d+$"))
    app.add_handler(CallbackQueryHandler(cb_rmchan,      pattern=r"^rmchan:(nt|mj):\d+$"))
    app.add_handler(CallbackQueryHandler(cb_pause,       pattern=r"^pause:(nt|mj):\d+$"))
    app.add_handler(CallbackQueryHandler(cb_resume,      pattern=r"^resume:(nt|mj):\d+$"))
    app.add_handler(CallbackQueryHandler(cb_rst_confirm, pattern=r"^rst_confirm:(nt|mj):\d+$"))
    app.add_handler(CallbackQueryHandler(cb_restart,     pattern=r"^restart:(nt|mj):\d+$"))
    app.add_handler(CallbackQueryHandler(cb_forceupdate, pattern=r"^forceupdate:(nt|mj):\d+$"))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, msg_channel_input))

    # DB keep-alive ping every 2 hours (prevents Aiven idle-timeout drops)
    app.job_queue.run_repeating(
        lambda ctx: db.ping(),
        interval=7200,
        first=60,
    )

    # Periodic content check
    interval_secs = CHECK_INTERVAL_HOURS * 3600
    app.job_queue.run_repeating(
        lambda ctx: asyncio.create_task(check_and_post(app)),
        interval=interval_secs,
        first=30,
    )

    log.info("Bot polling started.")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
