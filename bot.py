import os
import asyncio
import logging
import aiohttp
import json
import base64
from datetime import datetime, timezone
from urllib.parse import quote

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes, MessageHandler, filters
)

import db

# ─────────────────────── CONFIG ───────────────────────
BOT_TOKEN    = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
OWNER_ID     = int(os.getenv("OWNER_ID", "123456789"))          # Telegram user ID of owner
CHECK_INTERVAL_HOURS = int(os.getenv("CHECK_INTERVAL_HOURS", "24"))  # hours between content checks
API_BASE     = "https://apiserverpro.onrender.com/api/nexttoppers"
PLAYER_BASE  = "https://smarterz.netlify.app/ntplayer"
PLAYER_SECRET = "smarterzpro"
# ───────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ═══════════════════════ HELPERS ═══════════════════════

def owner_only(func):
    """Decorator – DM commands restricted to owner."""
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id
        chat_type = update.effective_chat.type
        if chat_type == "private" and user_id != OWNER_ID:
            await update.effective_message.reply_text("⛔ Access denied.")
            return
        return await func(update, ctx, *args, **kwargs)
    return wrapper


async def api_get(session: aiohttp.ClientSession, url: str) -> dict:
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
        r.raise_for_status()
        return await r.json()


def encode_token(url: str) -> str:
    """Base64-encode the URL with the shared secret prefix."""
    raw = f"{PLAYER_SECRET}:{url}"
    return base64.b64encode(raw.encode()).decode()


def make_player_url(file_url: str) -> str:
    return f"{PLAYER_BASE}?token={quote(encode_token(file_url))}"


# ═══════════════════════ COMMANDS ══════════════════════

@owner_only
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 *NextToppers Bot*\n\nUse /batches to see all courses.",
        parse_mode="Markdown"
    )


@owner_only
async def cmd_batches(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("⏳ Fetching courses…")
    async with aiohttp.ClientSession() as session:
        try:
            data = await api_get(session, f"{API_BASE}/batches")
        except Exception as e:
            await msg.edit_text(f"❌ API error: {e}")
            return

    courses = []
    for layout in data.get("data", []):
        for item in layout.get("list", []):
            courses.append(item)

    if not courses:
        await msg.edit_text("No courses found.")
        return

    keyboard = []
    for c in courses:
        label = f"{'🔥 ' if c.get('is_trending') else ''}[{c['id']}] {c['title'][:45]}"
        keyboard.append([InlineKeyboardButton(label, callback_data=f"course:{c['id']}")])

    await msg.edit_text(
        "📚 *All Courses* – tap one to manage:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


# ═══════════════════════ CALLBACKS ═════════════════════

async def cb_course(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Show manage panel for a course."""
    query = update.callback_query
    await query.answer()

    if query.from_user.id != OWNER_ID:
        await query.answer("⛔ Access denied.", show_alert=True)
        return

    course_id = int(query.data.split(":")[1])
    batch = db.get_batch(course_id)
    channel_id = batch["channel_id"] if batch else None
    status     = batch["status"]     if batch else "active"

    status_icon = {"active": "▶️", "paused": "⏸"}.get(status, "▶️")
    channel_txt = f"`{channel_id}`" if channel_id else "_none_"

    text = (
        f"⚙️ *Manage Course {course_id}*\n\n"
        f"Channel: {channel_txt}\n"
        f"Status : {status_icon} {status}\n"
    )

    kb = [
        [InlineKeyboardButton("➕ Set Channel", callback_data=f"setchan:{course_id}"),
         InlineKeyboardButton("➖ Remove Channel", callback_data=f"rmchan:{course_id}")],
        [InlineKeyboardButton("⏸ Pause", callback_data=f"pause:{course_id}"),
         InlineKeyboardButton("▶️ Resume", callback_data=f"resume:{course_id}")],
        [InlineKeyboardButton("🔄 Restart (reset sent)", callback_data=f"restart_confirm:{course_id}")],
        [InlineKeyboardButton("« Back to Courses", callback_data="back_courses")],
    ]

    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode="Markdown")


async def cb_back_courses(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # Re-fetch and redisplay
    ctx.args = []
    fake_msg = query.message
    async with aiohttp.ClientSession() as session:
        try:
            data = await api_get(session, f"{API_BASE}/batches")
        except Exception as e:
            await query.edit_message_text(f"❌ {e}")
            return

    courses = []
    for layout in data.get("data", []):
        for item in layout.get("list", []):
            courses.append(item)

    keyboard = []
    for c in courses:
        label = f"{'🔥 ' if c.get('is_trending') else ''}[{c['id']}] {c['title'][:45]}"
        keyboard.append([InlineKeyboardButton(label, callback_data=f"course:{c['id']}")])

    await query.edit_message_text(
        "📚 *All Courses* – tap one to manage:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def cb_setchan(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    course_id = int(query.data.split(":")[1])
    ctx.user_data["awaiting_channel_for"] = course_id
    await query.edit_message_text(
        f"Send the *channel ID* (e.g. `-1001234567890`) to link to course {course_id}:",
        parse_mode="Markdown"
    )


async def cb_rmchan(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    course_id = int(query.data.split(":")[1])
    db.upsert_batch(course_id, channel_id=None)
    await query.edit_message_text(f"✅ Channel removed from course {course_id}.")


async def cb_pause(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    course_id = int(query.data.split(":")[1])
    db.set_status(course_id, "paused")
    await query.edit_message_text(f"⏸ Course {course_id} paused.")


async def cb_resume(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    course_id = int(query.data.split(":")[1])
    db.set_status(course_id, "active")
    await query.edit_message_text(f"▶️ Course {course_id} resumed.")


async def cb_restart_confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    course_id = int(query.data.split(":")[1])
    kb = [[
        InlineKeyboardButton("✅ Yes, reset all sent content", callback_data=f"restart:{course_id}"),
        InlineKeyboardButton("❌ Cancel", callback_data=f"course:{course_id}"),
    ]]
    await query.edit_message_text(
        f"⚠️ *Restart Course {course_id}?*\n\nThis will delete all stored sent-file records for this batch so everything gets re-posted.",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="Markdown"
    )


async def cb_restart(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    course_id = int(query.data.split(":")[1])
    db.reset_sent(course_id)
    await query.edit_message_text(f"🔄 Course {course_id} reset. All content will be re-posted on next check.")


async def msg_channel_input(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle freetext channel ID input from owner."""
    if update.effective_user.id != OWNER_ID:
        return
    course_id = ctx.user_data.get("awaiting_channel_for")
    if course_id is None:
        return
    text = update.message.text.strip()
    try:
        channel_id = int(text)
    except ValueError:
        await update.message.reply_text("❌ Invalid channel ID. Must be a number like `-1001234567890`.")
        return
    db.upsert_batch(course_id, channel_id=channel_id)
    ctx.user_data.pop("awaiting_channel_for", None)
    await update.message.reply_text(f"✅ Channel `{channel_id}` linked to course {course_id}.", parse_mode="Markdown")


# ═══════════════════════ CONTENT POLLER ════════════════

async def fetch_files_recursive(session: aiohttp.ClientSession, course_id: int, parent_id: int = 0) -> list:
    """Recursively fetch all file-type content from a course."""
    url = f"{API_BASE}/all-content?courseid={course_id}"
    if parent_id:
        url += f"&id={parent_id}"
    try:
        data = await api_get(session, url)
    except Exception as e:
        log.warning(f"Failed fetching content {course_id}/{parent_id}: {e}")
        return []

    files = []
    for item in data.get("data", []):
        if item["type"] == "folder":
            folder_id = item["entity_id"]
            sub = await fetch_files_recursive(session, course_id, folder_id)
            files.extend(sub)
        elif item["type"] == "file":
            files.append(item)
    return files


async def get_content_detail(session: aiohttp.ClientSession, content_id: int, course_id: int) -> dict | None:
    url = f"{API_BASE}/content-details?content_id={content_id}&courseid={course_id}"
    try:
        data = await api_get(session, url)
        return data.get("data")
    except Exception as e:
        log.warning(f"content-details failed {content_id}: {e}")
        return None


async def post_content_to_channel(app: Application, course_id: int, channel_id: int, detail: dict, title: str):
    """Send one content card to the Telegram channel."""
    file_url  = detail.get("file_url", "")
    file_type = detail.get("file_type")   # 1=pdf, 2=video
    thumb     = detail.get("thumbnail", "")
    duration  = detail.get("duration", "")

    if file_type == 2 or detail.get("video_type"):  # video / m3u8
        open_url = make_player_url(file_url)
        tag = "🎬 Video"
    else:  # pdf or other
        open_url = file_url
        tag = "📄 PDF"

    dur_txt = f" • {int(duration)//60}m {int(duration)%60}s" if duration else ""
    caption = f"{tag}{dur_txt}\n<b>{title}</b>"

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Open", url=open_url)]])

    try:
        if thumb:
            await app.bot.send_photo(
                chat_id=channel_id,
                photo=thumb,
                caption=caption,
                reply_markup=kb,
                parse_mode="HTML"
            )
        else:
            await app.bot.send_message(
                chat_id=channel_id,
                text=caption,
                reply_markup=kb,
                parse_mode="HTML"
            )
    except Exception as e:
        log.error(f"Failed posting to channel {channel_id}: {e}")


async def check_and_post(app: Application):
    """Periodic job: check each active batch and post new content."""
    batches = db.get_all_active_batches()
    if not batches:
        return

    async with aiohttp.ClientSession() as session:
        for batch in batches:
            course_id  = batch["course_id"]
            channel_id = batch["channel_id"]
            if not channel_id:
                continue

            log.info(f"Checking course {course_id} → channel {channel_id}")
            files = await fetch_files_recursive(session, course_id)

            for f in files:
                content_id = f["entity_id"]
                if db.is_sent(course_id, content_id):
                    continue

                detail = await get_content_detail(session, content_id, course_id)
                if not detail:
                    continue

                file_url = detail.get("file_url", "")
                if not file_url:
                    continue

                await post_content_to_channel(app, course_id, channel_id, detail, f["title"])
                db.mark_sent(course_id, content_id)
                await asyncio.sleep(1)   # gentle rate limiting


# ═══════════════════════ MAIN ══════════════════════════

def main():
    db.init()

    app = Application.builder().token(BOT_TOKEN).build()

    # Commands
    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("batches", cmd_batches))

    # Callbacks
    app.add_handler(CallbackQueryHandler(cb_course,          pattern=r"^course:\d+$"))
    app.add_handler(CallbackQueryHandler(cb_back_courses,    pattern=r"^back_courses$"))
    app.add_handler(CallbackQueryHandler(cb_setchan,         pattern=r"^setchan:\d+$"))
    app.add_handler(CallbackQueryHandler(cb_rmchan,          pattern=r"^rmchan:\d+$"))
    app.add_handler(CallbackQueryHandler(cb_pause,           pattern=r"^pause:\d+$"))
    app.add_handler(CallbackQueryHandler(cb_resume,          pattern=r"^resume:\d+$"))
    app.add_handler(CallbackQueryHandler(cb_restart_confirm, pattern=r"^restart_confirm:\d+$"))
    app.add_handler(CallbackQueryHandler(cb_restart,         pattern=r"^restart:\d+$"))

    # Free-text handler for channel ID input
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, msg_channel_input))

    # Periodic job
    interval_seconds = CHECK_INTERVAL_HOURS * 3600
    app.job_queue.run_repeating(
        lambda ctx: asyncio.create_task(check_and_post(app)),
        interval=interval_seconds,
        first=30   # first run 30s after start
    )

    log.info("Bot started.")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
