# bot.py
import asyncio
import os
import random
import re
import shutil
import time
import uuid
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

from pyrogram import Client, filters, enums, idle
from pyrogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    Chat,
)
from pyrogram.errors import MessageNotModified

from config import Config
from database import (
    get_or_create_user,
    is_banned,
    set_ban,
    count_users,
    get_all_users,
    register_temp_path,
    update_user_stats,
)
from utils.progress import progress_for_pyrogram, human_bytes
from utils.extractors import extract_archive, detect_encrypted
from utils.link_parser import (
    find_links_in_text,
    extract_links_from_folder,
    classify_link,
)
from utils.cleanup import cleanup_worker
from utils.media_tools import extract_audio, generate_thumbnail
from utils.http_downloader import download_file
from utils.m3u8_tools import get_m3u8_variants, download_m3u8_stream
from utils.gdrive import get_gdrive_direct_link


# ----------------- Pyrogram client -----------------

app = Client(
    "serena_unzip_bot",
    api_id=Config.API_ID,
    api_hash=Config.API_HASH,
    bot_token=Config.BOT_TOKEN,
    in_memory=True,
)

# in‑memory state
user_locks: Dict[int, asyncio.Lock] = {}
tasks: Dict[str, Dict[str, Any]] = {}        # unzip tasks & meta
pending_password: Dict[int, Dict[str, Any]] = {}
user_cancelled: Dict[int, bool] = {}
M3U8_TASKS: Dict[str, Dict[str, Any]] = {}    # quality select tasks

VIDEO_EXT_SET = {".mp4", ".mkv", ".mov", ".avi", ".webm", ".ts"}

EMOJI_LIST = [
    "🚀",
    "📦",
    "🎬",
    "🧩",
    "📄",
    "🔗",
    "🧪",
    "⚡",
    "💾",
    "🧰",
]

# premium + caption settings
premium_until: Dict[int, float] = {}  # user_id -> timestamp until premium
user_caption_settings: Dict[int, Dict[str, Any]] = {}  # user_id -> {base, counter, rfrom, rto, updated_at}
pending_settings_action: Dict[int, str] = {}  # user_id -> "caption" | "replace"

# thumbnail mode per user: 'original' or 'random'
user_thumb_mode: Dict[int, str] = {}  # user_id -> mode

# link sessions (for TXT + messages): (chat_id, msg_id) -> {links, content}
LINK_SESSIONS: Dict[Tuple[int, int], Dict[str, Any]] = {}


def get_lock(user_id: int) -> asyncio.Lock:
    if user_id not in user_locks:
        user_locks[user_id] = asyncio.Lock()
    return user_locks[user_id]


def is_owner(user_id: int) -> bool:
    return user_id in Config.OWNER_IDS


def is_video_path(path: str) -> bool:
    return Path(path).suffix.lower() in VIDEO_EXT_SET


def random_emoji() -> str:
    return random.choice(EMOJI_LIST)


def is_premium_user(user_id: int) -> bool:
    t = premium_until.get(user_id)
    if not t:
        return False
    if t < time.time():
        premium_until.pop(user_id, None)
        return False
    return True


def get_thumb_mode(user_id: int) -> str:
    return user_thumb_mode.get(user_id, "random")


# ----------------- logging helpers (per user, with topics if available) -----------------

log_chat_info: Optional[Chat] = None
log_is_forum: bool = False
user_log_topics: Dict[int, int] = {}  # user_id -> root_msg_id (topic root message id)


async def get_log_chat_info(client: Client) -> Tuple[Optional[Chat], bool]:
    """
    Returns (log_chat, is_forum)
    """
    global log_chat_info, log_is_forum
    if log_chat_info is not None:
        return log_chat_info, log_is_forum

    try:
        chat = await client.get_chat(Config.LOG_CHANNEL_ID)
        log_chat_info = chat
        log_is_forum = bool(getattr(chat, "is_forum", False))
    except Exception:
        log_chat_info = None
        log_is_forum = False

    return log_chat_info, log_is_forum


async def get_user_log_target(client: Client, user) -> Tuple[Optional[int], Optional[int]]:
    """
    Returns (chat_id, root_msg_id or None).
    If log chat is a forum, tries to create a topic per user and uses its root message.
    Uses reply_to_message_id for compatibility (no message_thread_id usage).
    """
    if not Config.LOG_CHANNEL_ID:
        return None, None

    chat_info, is_forum = await get_log_chat_info(client)
    if not chat_info:
        return None, None

    chat_id = chat_info.id

    if not is_forum:
        # normal group/channel
        return chat_id, None

    # forum group with topics
    if user.id in user_log_topics:
        return chat_id, user_log_topics[user.id]

    # try to create topic per user
    if not hasattr(client, "create_forum_topic"):
        # old pyrogram – topic API may not exist
        return chat_id, None

    root_msg_id = None
    name = f"{user.first_name or 'User'} | {user.id}"
    try:
        topic_msg = await client.create_forum_topic(chat_id, name=name)
        # topic_msg is a message pinned to that topic
        root_msg_id = getattr(topic_msg, "id", None) or getattr(topic_msg, "message_id", None)
    except Exception:
        root_msg_id = None

    if root_msg_id is None:
        # fallback: no topic support
        return chat_id, None

    user_log_topics[user.id] = root_msg_id

    # intro message as reply in that topic
    intro = (
        f"👤 <b>User</b>\n"
        f"• Name: <b>{user.first_name or ''}</b> (@{user.username or 'N/A'})\n"
        f"• ID: <code>{user.id}</code>\n"
        f"• First seen: <code>{time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())}</code>"
    )
    try:
        await client.send_message(chat_id, intro, reply_to_message_id=root_msg_id)
    except Exception:
        pass

    return chat_id, root_msg_id


async def log_user_input(client: Client, message: Message, context: str):
    if not Config.LOG_CHANNEL_ID:
        return
    user = message.from_user
    if not user:
        return

    chat_id, root_msg_id = await get_user_log_target(client, user)
    if not chat_id:
        return

    cap = (
        f"🔹 <b>INPUT</b>\n"
        f"• User: <b>{user.first_name or ''}</b> (@{user.username or 'N/A'})\n"
        f"• ID: <code>{user.id}</code>\n"
        f"• Context: <code>{context}</code>"
    )
    if message.caption:
        cap += f"\n\n{message.caption}"

    # text log
    try:
        if root_msg_id:
            await client.send_message(chat_id, cap, reply_to_message_id=root_msg_id)
        else:
            await client.send_message(chat_id, cap)
    except Exception:
        pass

    # media log by re-sending file_id
    try:
        if message.document:
            fid = message.document.file_id
            if root_msg_id:
                await client.send_document(chat_id, fid, caption=cap, reply_to_message_id=root_msg_id)
            else:
                await client.send_document(chat_id, fid, caption=cap)
        elif message.video:
            fid = message.video.file_id
            if root_msg_id:
                await client.send_video(chat_id, fid, caption=cap, reply_to_message_id=root_msg_id)
            else:
                await client.send_video(chat_id, fid, caption=cap)
        elif message.photo:
            fid = message.photo.file_id
            if root_msg_id:
                await client.send_photo(chat_id, fid, caption=cap, reply_to_message_id=root_msg_id)
            else:
                await client.send_photo(chat_id, fid, caption=cap)
        elif message.audio:
            fid = message.audio.file_id
            if root_msg_id:
                await client.send_audio(chat_id, fid, caption=cap, reply_to_message_id=root_msg_id)
            else:
                await client.send_audio(chat_id, fid, caption=cap)
    except Exception:
        pass


async def log_user_output(client: Client, user, msg: Message, context: str):
    if not Config.LOG_CHANNEL_ID or not user or not msg:
        return

    chat_id, root_msg_id = await get_user_log_target(client, user)
    if not chat_id:
        return

    cap = (
        f"✅ <b>OUTPUT</b>\n"
        f"• User: <b>{user.first_name or ''}</b> (@{user.username or 'N/A'})\n"
        f"• ID: <code>{user.id}</code>\n"
        f"• Context: <code>{context}</code>"
    )
    if msg.caption:
        cap += f"\n\n{msg.caption}"

    # text log
    try:
        if root_msg_id:
            await client.send_message(chat_id, cap, reply_to_message_id=root_msg_id)
        else:
            await client.send_message(chat_id, cap)
    except Exception:
        pass

    # media log by re-sending file_id from msg
    try:
        if msg.document:
            fid = msg.document.file_id
            if root_msg_id:
                await client.send_document(chat_id, fid, caption=cap, reply_to_message_id=root_msg_id)
            else:
                await client.send_document(chat_id, fid, caption=cap)
        elif msg.video:
            fid = msg.video.file_id
            if root_msg_id:
                await client.send_video(chat_id, fid, caption=cap, reply_to_message_id=root_msg_id)
            else:
                await client.send_video(chat_id, fid, caption=cap)
        elif msg.photo:
            fid = msg.photo.file_id
            if root_msg_id:
                await client.send_photo(chat_id, fid, caption=cap, reply_to_message_id=root_msg_id)
            else:
                await client.send_photo(chat_id, fid, caption=cap)
        elif msg.audio:
            fid = msg.audio.file_id
            if root_msg_id:
                await client.send_audio(chat_id, fid, caption=cap, reply_to_message_id=root_msg_id)
            else:
                await client.send_audio(chat_id, fid, caption=cap)
    except Exception:
        pass

  # ----------------- caption & thumbnail helpers -----------------


def get_caption_cfg(user_id: int) -> Optional[Dict[str, Any]]:
    cfg = user_caption_settings.get(user_id)
    if not cfg:
        return None

    # TTL: 1 day for non-premium users
    now = time.time()
    if not is_premium_user(user_id) and now - cfg.get("updated_at", 0) > 86400:
        user_caption_settings.pop(user_id, None)
        return None

    return cfg


def build_caption(user_id: int, default_caption: str) -> str:
    cfg = get_caption_cfg(user_id)
    if not cfg:
        return default_caption

    caption = default_caption

    # Numbered caption: 001 Title, 002 Title, ...
    base = cfg.get("base")
    if base:
        counter = cfg.get("counter", 0) + 1
        cfg["counter"] = counter
        caption = f"{counter:03d} {base}"

    # Replace words
    rfrom = cfg.get("rfrom")
    rto = cfg.get("rto")
    if rfrom and rto:
        caption = caption.replace(rfrom, rto)

    cfg["updated_at"] = time.time()
    user_caption_settings[user_id] = cfg
    return caption


async def choose_thumbnail(user_id: int, video_path: str) -> Optional[str]:
    """
    Choose thumbnail for a local video file based on user setting.
    'original'  -> frame from the very start (00:00:00.200)
    'random'    -> frame from a bit later (00:00:02)
    """
    mode = get_thumb_mode(user_id)
    time_pos = "00:00:00.200" if mode == "original" else "00:00:02"

    thumb_path = video_path + ".jpg"
    try:
        await generate_thumbnail(video_path, thumb_path, time_pos=time_pos)
        return thumb_path
    except Exception:
        return None


# ----------------- basic helpers -----------------


async def check_force_sub(client: Client, message: Message) -> bool:
    if not message.from_user:
        return True
    if not Config.FORCE_SUB_CHANNEL:
        return True
    try:
        member = await client.get_chat_member(
            Config.FORCE_SUB_CHANNEL, message.from_user.id
        )
        if member.status not in (
            enums.ChatMemberStatus.OWNER,
            enums.ChatMemberStatus.ADMINISTRATOR,
            enums.ChatMemberStatus.MEMBER,
        ):
            raise ValueError
        return True
    except Exception:
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "Join official channel",
                        url=f"https://t.me/{Config.FORCE_SUB_CHANNEL}",
                    )
                ],
                [InlineKeyboardButton("Try again", callback_data="retry_force_sub")],
            ]
        )
        try:
            await message.reply_text(
                "Yo fam, pehle official channel join karo phir wapas try karo 😎",
                reply_markup=kb,
            )
        except Exception:
            pass
        return False


def main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Join official channel",
                    url=f"https://t.me/{Config.FORCE_SUB_CHANNEL}",
                )
            ],
            [
                InlineKeyboardButton(
                    "Owner Contact",
                    url=f"https://t.me/{Config.OWNER_USERNAME}",
                )
            ],
        ]
    )


def settings_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📝 Add Caption", callback_data="settings:caption"),
                InlineKeyboardButton("🔤 Replace Words", callback_data="settings:replace"),
            ],
            [
                InlineKeyboardButton("📸 Original Thumb", callback_data="settings:thumb:original"),
                InlineKeyboardButton("🎲 Random Thumb", callback_data="settings:thumb:random"),
            ],
            [
                InlineKeyboardButton("⚙️ Reset Settings", callback_data="settings:reset"),
            ],
            [
                InlineKeyboardButton(
                    "Owner Contact", url=f"https://t.me/{Config.OWNER_USERNAME}"
                )
            ],
        ]
    )


def is_archive_file(name: str) -> bool:
    n = name.lower()
    return any(n.endswith(ext) for ext in (".zip", ".rar", ".7z", ".tar", ".gz", ".tgz", ".tar.gz"))


def is_video_file(name: str) -> bool:
    n = name.lower()
    return any(n.endswith(ext) for ext in VIDEO_EXT_SET)


def file_action_keyboard(msg: Message, is_archive: bool, is_video: bool) -> InlineKeyboardMarkup:
    chat_id = msg.chat.id
    msg_id = msg.id
    rows = []
    if is_archive:
        rows.append(
            [
                InlineKeyboardButton(
                    "📦 Unzip",
                    callback_data=f"unzip|{chat_id}|{msg_id}|nopass",
                ),
                InlineKeyboardButton(
                    "🔐 With Password",
                    callback_data=f"unzip|{chat_id}|{msg_id}|askpass",
                ),
            ]
        )
    if is_video:
        rows.append(
            [
                InlineKeyboardButton(
                    "🎧 Extract Audio",
                    callback_data=f"audio|{chat_id}|{msg_id}",
                ),
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                "Owner Contact", url=f"https://t.me/{Config.OWNER_USERNAME}"
            )
        ]
    )
    return InlineKeyboardMarkup(rows)


# ----------------- commands -----------------


@app.on_message(filters.command("start"))
async def start_cmd(client: Client, message: Message):
    if not message.from_user:
        return

    if await is_banned(message.from_user.id):
        return

    if not await check_force_sub(client, message):
        return

    await get_or_create_user(message.from_user.id)

    caption = (
        f"Hey {message.from_user.first_name or 'there'} 👋\n\n"
        f"Welcome to <b>{Config.BOT_NAME}</b>\n\n"
        "I’m your all‑in‑one archive & media assistant:\n"
        f"{random_emoji()} Unzip 20+ formats (ZIP/RAR/7Z/TAR, with passwords)\n"
        f"{random_emoji()} Extract audio from any video\n"
        f"{random_emoji()} Auto‑process TXT & links (direct, m3u8, GDrive)\n"
        f"{random_emoji()} Smart file listing: send single or all files\n\n"
        "Use <code>/help</code> to see full usage with examples."
    )

    if Config.START_PIC:
        await message.reply_photo(
            Config.START_PIC,
            caption=caption,
            reply_markup=main_keyboard(),
        )
    else:
        await message.reply_text(
            caption,
            reply_markup=main_keyboard(),
        )


@app.on_message(filters.command("help") & filters.private)
async def help_cmd(client: Client, message: Message):
    text = (
        "✨ <b>Serena Unzip – Help & Usage</b>\n\n"
        "🧩 <b>Basic</b>\n"
        "• <code>/start</code> – Welcome screen & quick intro.\n"
        "• <code>/help</code> – This help menu.\n"
        "• <code>/settings</code> – View current defaults & caption/thumbnail tools.\n"
        "• <code>/cancel</code> – Mark current task as cancelled.\n\n"
        "📦 <b>Archives (ZIP / RAR / 7Z / TAR…)</b>\n"
        "1) Send or forward any archive file.\n"
        "2) Tap:\n"
        "   • <b>📦 Unzip</b> – Normal extract.\n"
        "   • <b>🔐 With Password</b> – Bot asks for password, then extracts.\n"
        "3) After extract you get:\n"
        "   • Summary of videos / PDFs / APKs / TXT / m3u8 / others.\n"
        "   • Inline file list → tap any to get that single file.\n"
        "   • <b>Send ALL</b> – sends every file back (videos as playable media).\n\n"
        "🎬 <b>Videos & Audio</b>\n"
        "• Send any video → tap <b>🎧 Extract Audio</b>.\n"
        "  - Bot downloads, extracts audio via ffmpeg and sends it.\n"
        "• Extracted / downloaded videos are sent as real Telegram videos\n"
        "  with thumbnails generated from inside the video.\n"
        "  - /settings → choose <b>Original Thumb</b> or <b>Random Thumb</b> per user.\n\n"
        "🔗 <b>TXT / Links Power Mode</b>\n"
        "1) Send a message or <b>.txt file</b> containing links in DM.\n"
        "2) Bot shows:\n"
        "   • <b>Download all videos/files</b>\n"
        "   • <b>Cleaned TXT only</b> (just the links)\n"
        "   • <b>Skip</b>\n"
        "3) On <b>Download all</b>:\n"
        "   • Direct file links (mp4/mkv/zip/apk/xapk/audio/…) are downloaded\n"
        "     with progress bar (speed + ETA) and then sent.\n"
        "   • Google Drive links are converted to direct download when possible and\n"
        "     downloaded with their real filename (e.g. .mp4, .jpg, .pdf).\n"
        "   • m3u8 links: you get quality buttons (360p / 480p / 720p / Auto etc).\n"
        "     You choose quality → ffmpeg downloads → bot sends mp4.\n"
        "   • When link‑based downloading starts, the DM status message is pinned\n"
        "     and unpinned when completed.\n\n"
        "👥 <b>Groups & Channels</b>\n"
        "• Mention @bot or reply to the bot with a message containing links →\n"
        "  same link processing as in DM.\n\n"
        "🛠 <b>Admin Only</b>\n"
        "• <code>/status</code> – User stats & disk usage.\n"
        "• <code>/broadcast</code> – Reply and broadcast to all users.\n"
        "• <code>/premium &lt;id&gt; [days]</code> – Grant premium (caption TTL never expires).\n"
        "• <code>/ban &lt;id&gt;</code> / <code>/unban &lt;id&gt;</code> – Ban/unban.\n"
        "• <code>/clean</code> – Manual temp storage cleanup.\n\n"
        "<i>Tip:</i> Very large Google Drive files or protected links may fail due to Google,\n"
        "not the bot. Try smaller or public links for best results.\n"
    )
    await message.reply_text(text)


@app.on_message(filters.command("settings") & filters.private)
async def settings_cmd(client: Client, message: Message):
    cfg = get_caption_cfg(message.from_user.id)
    base = cfg.get("base") if cfg else None
    rfrom = cfg.get("rfrom") if cfg else None
    rto = cfg.get("rto") if cfg else None
    thumb_mode = get_thumb_mode(message.from_user.id)

    status_lines = []
    if base:
        status_lines.append(f"• Caption base: <code>{base}</code>")
    else:
        status_lines.append("• Caption base: <code>None</code>")

    if rfrom and rto:
        status_lines.append(f"• Replace: <code>{rfrom}</code> → <code>{rto}</code>")
    else:
        status_lines.append("• Replace: <code>None</code>")

    text = (
        f"{random_emoji()} <b>Current Settings</b>\n\n"
        "• Auto delete temp files: <b>30 minutes</b>\n"
        "• Default extract mode: <b>Full archive</b>\n"
        "• Language: <b>English</b>\n\n"
        "<b>Caption tools:</b>\n" + "\n".join(status_lines) +
        f"\n\n<b>Thumbnail mode:</b> <code>{thumb_mode}</code>\n"
        "\nUse the buttons below to tweak your caption & thumbnail style."
    )

    if Config.START_PIC:
        await message.reply_photo(
            Config.START_PIC,
            caption=text,
            reply_markup=settings_keyboard(),
        )
    else:
        await message.reply_text(text, reply_markup=settings_keyboard())


@app.on_message(filters.command("cancel") & (filters.private | filters.group | filters.channel))
async def cancel_cmd(client: Client, message: Message):
    if not message.from_user:
        return
    user_cancelled[message.from_user.id] = True
    await message.reply_text(
        "Gotchu. I’ll try to stop after the current step finishes 💨"
  )
# ----------------- admin commands -----------------


@app.on_message(filters.command(["status", "users"]) & filters.private)
async def status_cmd(client: Client, message: Message):
    if not message.from_user or not is_owner(message.from_user.id):
        return

    total, premium, banned = await count_users()

    total_b = used_b = free_b = 0
    try:
        st = shutil.disk_usage("/")
        total_b, used_b, free_b = st.total, st.used, st.free
    except Exception:
        pass

    txt = (
        "📊 <b>Bot Status</b>\n\n"
        f"Users: <b>{total}</b>\n"
        f"Premium: <b>{premium}</b>\n"
        f"Banned: <b>{banned}</b>\n\n"
        f"Disk total: <code>{human_bytes(total_b)}</code>\n"
        f"Disk used: <code>{human_bytes(used_b)}</code>\n"
        f"Disk free: <code>{human_bytes(free_b)}</code>\n"
    )
    await message.reply_text(txt)


@app.on_message(filters.command("broadcast") & filters.private)
async def broadcast_cmd(client: Client, message: Message):
    if not message.from_user or not is_owner(message.from_user.id):
        return

    if not message.reply_to_message:
        await message.reply_text("Reply to a message and use /broadcast.")
        return

    users = await get_all_users()
    sent = 0
    failed = 0
    for uid in users:
        try:
            await message.reply_to_message.copy(chat_id=uid)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1

    await message.reply_text(f"Broadcast done.\nSent: {sent}\nFailed: {failed}")


@app.on_message(filters.command("premium") & filters.private)
async def premium_cmd(client: Client, message: Message):
    if not message.from_user or not is_owner(message.from_user.id):
        return

    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.reply_text(
            "Usage:\n"
            "<code>/premium &lt;user_id&gt; [days]</code>\n"
            "Example:\n<code>/premium 123456789 10</code>"
        )
        return

    try:
        target_id = int(parts[1])
    except ValueError:
        await message.reply_text("User ID must be an integer.")
        return

    days = 10
    if len(parts) >= 3:
        try:
            days = int(parts[2])
        except ValueError:
            pass

    if days <= 0:
        days = 10

    premium_until[target_id] = time.time() + days * 86400
    await message.reply_text(
        f"User <code>{target_id}</code> is premium for <b>{days}</b> day(s).\n"
        f"Caption rules for them won’t auto‑reset during this time."
    )


@app.on_message(filters.command(["ban", "unban"]) & filters.private)
async def ban_cmd(client: Client, message: Message):
    if not message.from_user or not is_owner(message.from_user.id):
        return

    cmd = message.command[0].lower()
    target_id = None

    if message.reply_to_message:
        target_id = message.reply_to_message.from_user.id
    elif len(message.command) > 1:
        try:
            target_id = int(message.command[1])
        except ValueError:
            pass

    if not target_id:
        await message.reply_text("User id ya kisi user ke message pe reply karo.")
        return

    if cmd == "ban":
        await set_ban(target_id, True)
        await message.reply_text(f"User {target_id} banned.")
    else:
        await set_ban(target_id, False)
        await message.reply_text(f"User {target_id} unbanned.")


@app.on_message(filters.command("clean") & filters.private)
async def clean_cmd(client: Client, message: Message):
    if not message.from_user or not is_owner(message.from_user.id):
        return
    await message.reply_text(
        "Manual cleanup trigger ho gaya (background worker already run ho raha hai)."
    )


# ----------------- file handlers (private + group + channel) -----------------


@app.on_message(
    (filters.document | filters.video | filters.photo | filters.audio)
    & (filters.private | filters.group | filters.channel)
)
async def on_file(client: Client, message: Message):
    if not message.from_user:
        return
    user_id = message.from_user.id
    if await is_banned(user_id):
        return

    if not await check_force_sub(client, message):
        return

    chat_type = message.chat.type

    await get_or_create_user(user_id)

    # log input
    try:
        if message.document:
            ctx = f"document upload: {message.document.file_name}"
        elif message.video:
            ctx = f"video upload: {message.video.file_name}"
        elif message.photo:
            ctx = "photo upload"
        elif message.audio:
            ctx = f"audio upload: {message.audio.file_name}"
        else:
            ctx = "media upload"
        await log_user_input(client, message, ctx)
    except Exception:
        pass

    # TXT as link source (DM only)
    if (
        chat_type == enums.ChatType.PRIVATE
        and message.document
        and (message.document.file_name or "").lower().endswith(".txt")
    ):
        temp_root = Path(Config.TEMP_DIR) / str(user_id) / uuid.uuid4().hex
        temp_root.mkdir(parents=True, exist_ok=True)
        await register_temp_path(user_id, str(temp_root), Config.AUTO_DELETE_DEFAULT_MIN)

        status = await message.reply_text("Downloading TXT to parse links…")
        try:
            txt_path = await client.download_media(
                message.document,
                file_name=str(temp_root),
            )
        except Exception as e:
            await status.edit_text(f"TXT download failed:\n<code>{e}</code>")
            return

        try:
            content = Path(txt_path).read_text(encoding="utf-8", errors="ignore")
        except Exception:
            content = ""

        try:
            await status.delete()
        except Exception:
            pass

        await process_links_message(client, message, content)
        return

    media = message.document or message.video
    if not media:
        return
    file_name = media.file_name or "file"

    kb = file_action_keyboard(
        message,
        is_archive=is_archive_file(file_name),
        is_video=is_video_file(file_name),
    )

    await message.reply_text(
        f"Nice drop: <code>{file_name}</code>\nChoose what you wanna do 👇",
        reply_markup=kb,
    )

# ----------------- text / links handler (DM + Groups) -----------------


async def process_links_message(client: Client, message: Message, content: str):
    if not message.from_user:
        return

    links = find_links_in_text(content or "")
    if not links:
        await message.reply_text("No valid URLs found in this text.")
        return

    LINK_SESSIONS[(message.chat.id, message.id)] = {
        "links": links,
        "content": content or "",
    }

    try:
        await log_user_input(client, message, f"text links: {len(links)} urls")
    except Exception:
        pass

    chat_id = message.chat.id
    msg_id = message.id

    kb = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "⬇️ Download all videos/files",
                    callback_data=f"links|download_all|{chat_id}|{msg_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    "🧹 Cleaned TXT only",
                    callback_data=f"links|clean_txt|{chat_id}|{msg_id}",
                ),
                InlineKeyboardButton(
                    "Skip",
                    callback_data=f"links|skip|{chat_id}|{msg_id}",
                ),
            ],
        ]
    )
    await message.reply_text(
        f"Found <b>{len(links)}</b> urls.\nChoose what you wanna do 👇", reply_markup=kb
    )


@app.on_message((filters.text | filters.caption) & filters.private)
async def on_text(client: Client, message: Message):
    if not message.from_user:
        return
    user_id = message.from_user.id

    # Ignore commands
    if message.text and message.text.lstrip().startswith("/"):
        return

    if await is_banned(user_id):
        return

    # password reply?
    if user_id in pending_password:
        info = pending_password.pop(user_id)
        password = (message.text or message.caption or "").strip()
        await handle_unzip_from_password(client, message, info, password)
        return

    # settings reply?
    if user_id in pending_settings_action:
        action = pending_settings_action.pop(user_id)
        txt = (message.text or message.caption or "").strip()
        if action == "caption":
            if not txt:
                await message.reply_text("Caption can’t be empty. Try again with some text.")
            else:
                cfg = user_caption_settings.get(user_id) or {}
                cfg["base"] = txt
                cfg["counter"] = 0
                cfg["updated_at"] = time.time()
                user_caption_settings[user_id] = cfg
                await message.reply_text(
                    f"Bet. I’ll caption your videos like:\n"
                    f"<code>001 {txt}</code>\n"
                    f"<code>002 {txt}</code> etc. 🔥"
                )
        elif action == "replace":
            parts = [p.strip() for p in re.split(r"->|=>", txt, maxsplit=1)]
            if len(parts) != 2 or not parts[0]:
                await message.reply_text(
                    "Format wrong.\nSend like:\n<code>old_text -> new_text</code>"
                )
            else:
                old, new = parts
                cfg = user_caption_settings.get(user_id) or {}
                cfg["rfrom"] = old
                cfg["rto"] = new
                cfg["updated_at"] = time.time()
                user_caption_settings[user_id] = cfg
                await message.reply_text(
                    f"Gotchu. I’ll replace <code>{old}</code> with <code>{new}</code> in captions."
                )
        return

    if not await check_force_sub(client, message):
        return

    await get_or_create_user(user_id)

    content = (message.text or message.caption or "").strip()
    await process_links_message(client, message, content)


@app.on_message((filters.text | filters.caption) & (filters.group | filters.channel))
async def group_text_handler(client: Client, message: Message):
    if not message.from_user:
        return

    text = (message.text or message.caption or "") or ""
    if text.lstrip().startswith("/"):
        return

    # Mention or reply to bot only
    me = await client.get_me()
    mentioned = False

    if me.username and ("@" + me.username.lower()) in text.lower():
        mentioned = True

    if (
        message.reply_to_message
        and message.reply_to_message.from_user
        and message.reply_to_message.from_user.is_bot
        and message.reply_to_message.from_user.id == me.id
    ):
        mentioned = True

    if not mentioned:
        return

    if await is_banned(message.from_user.id):
        return

    if not await check_force_sub(client, message):
        return

    await get_or_create_user(message.from_user.id)

    content = text.strip()
    await process_links_message(client, message, content)


# ----------------- callback handlers -----------------


@app.on_callback_query()
async def callbacks(client: Client, cq: CallbackQuery):
    data = cq.data or ""
    if data == "retry_force_sub":
        await cq.message.delete()
        return

    # Settings actions
    if data.startswith("settings:"):
        if not cq.from_user:
            await cq.answer()
            return
        user_id = cq.from_user.id
        action = data.split(":", 1)[1]

        if action == "reset":
            user_caption_settings.pop(user_id, None)
            user_thumb_mode.pop(user_id, None)
            await cq.message.edit_text(
                "Your caption/replace/thumb settings are back to default 🤙",
                reply_markup=settings_keyboard(),
            )
            await cq.answer("Settings reset", show_alert=False)
            return

        if action == "caption":
            pending_settings_action[user_id] = "caption"
            await cq.message.reply_text(
                "Send me the base caption text.\n\n"
                "Example: <code>My Serena Pack</code>\n\n"
                "I’ll use it like:\n"
                "<code>001 My Serena Pack</code>\n"
                "<code>002 My Serena Pack</code> etc."
            )
            await cq.answer()
            return

        if action == "replace":
            pending_settings_action[user_id] = "replace"
            await cq.message.reply_text(
                "Send the replace rule in this format:\n"
                "<code>old_text -> new_text</code>\n\n"
                "Example:\n<code>kumari -> serena</code>\n"
                "I’ll replace <b>kumari</b> with <b>serena</b> in captions."
            )
            await cq.answer()
            return

        if action.startswith("thumb:"):
            mode = action.split(":", 1)[1]
            if mode in ("original", "random"):
                user_thumb_mode[user_id] = mode
                msg_txt = (
                    "📸 Original thumbnails enabled ✅"
                    if mode == "original"
                    else "🎲 Random thumbnails enabled ✅"
                )
                await cq.message.reply_text(msg_txt)
            await cq.answer()
            return

    # Unzip
    if data.startswith("unzip|"):
        try:
            _, chat_id, msg_id, mode = data.split("|", 3)
            original_msg = await client.get_messages(int(chat_id), int(msg_id))
        except Exception:
            await cq.answer("Original file nahi mila.", show_alert=True)
            return
        await handle_unzip_button(client, cq, original_msg, mode)
        return

    # Unzip cancel session
    if data.startswith("ucancel|"):
        _, task_id = data.split("|", 1)
        tasks.pop(task_id, None)
        try:
            await cq.message.edit_text("Unzip session cancelled ✅")
        except Exception:
            pass
        await cq.answer()
        return

    # Audio extract
    if data.startswith("audio|"):
        try:
            _, chat_id, msg_id = data.split("|", 2)
            original_msg = await client.get_messages(int(chat_id), int(msg_id))
        except Exception:
            await cq.answer("Original video nahi mila.", show_alert=True)
            return
        await handle_extract_audio(client, cq, original_msg)
        return

    # send all/single extracted
    if data.startswith("sendall|"):
        _, task_id = data.split("|", 1)
        await handle_send_all(client, cq, task_id)
        return

    if data.startswith("sendone|"):
        _, task_id, index = data.split("|", 2)
        await handle_send_one(client, cq, task_id, int(index))
        return

    # links actions
    if data.startswith("links|"):
        parts = data.split("|", 3)
        if len(parts) < 4:
            await cq.answer("Original message nahi mila.", show_alert=True)
            return
        _, action, chat_id, msg_id = parts
        try:
            original_msg = await client.get_messages(int(chat_id), int(msg_id))
        except Exception:
            await cq.answer("Original message nahi mila.", show_alert=True)
            return

        key = (original_msg.chat.id, original_msg.id)
        session = LINK_SESSIONS.get(key)
        content = session["content"] if session else (original_msg.text or original_msg.caption or "") or ""
        links = session["links"] if session else find_links_in_text(content)

        if action == "clean_txt":
            await cq.answer()
            txt = "\n".join(sorted(set(links))) or "No valid URLs found."
            await cq.message.edit_text("<b>Cleaned URLs:</b>\n\n" + txt[:4000])
        elif action == "download_all":
            await cq.answer()
            await handle_links_download_all(client, cq, original_msg)
        else:
            await cq.answer()
            await cq.message.edit_text("Skipped link processing.")
        return

    # m3u8 quality choice
    if data.startswith("m3q|"):
        try:
            _, task_id, idx_str = data.split("|", 2)
            index = int(idx_str)
        except Exception:
            await cq.answer("Invalid selection.", show_alert=True)
            return
        await handle_m3u8_quality_choice(client, cq, task_id, index)
        return

    await cq.answer()

# ----------------- unzip & audio -----------------


async def handle_unzip_button(
    client: Client, cq: CallbackQuery, original_msg: Message, mode: str
):
    if not cq.from_user:
        await cq.answer()
        return
    user_id = cq.from_user.id
    if await is_banned(user_id):
        await cq.answer("You are banned.", show_alert=True)
        return

    if not await check_force_sub(client, original_msg):
        await cq.answer()
        return

    doc = original_msg.document
    if not doc:
        await cq.answer("No document found.", show_alert=True)
        return

    file_name = doc.file_name or "archive"
    if not is_archive_file(file_name):
        await cq.answer("Ye archive file nahi lag rahi.", show_alert=True)
        return

    if mode == "askpass":
        pending_password[user_id] = {
            "chat_id": original_msg.chat.id,
            "msg_id": original_msg.id,
            "file_name": file_name,
        }
        await cq.message.reply_text(
            f"Send password for <code>{file_name}</code> (just text)."
        )
        await cq.answer()
        return

    await cq.answer()
    await run_unzip_task(client, original_msg, password=None)


async def handle_unzip_from_password(
    client: Client, msg: Message, info: Dict[str, Any], password: str
):
    chat_id = info["chat_id"]
    msg_id = info["msg_id"]
    original_msg = await client.get_messages(chat_id, msg_id)
    await msg.reply_text("Got the password, starting extraction…")
    await run_unzip_task(client, original_msg, password=password)


async def run_unzip_task(client: Client, msg: Message, password: Optional[str]):
    if not msg.from_user:
        return
    user_id = msg.from_user.id
    lock = get_lock(user_id)

    if lock.locked():
        await msg.reply_text(
            "Chill, ek task already running hai. Pehle usko finish hone do."
        )
        return

    async with lock:
        user_cancelled[user_id] = False
        doc = msg.document
        file_name = doc.file_name or "archive"
        size_bytes = doc.file_size or 0
        size_mb = size_bytes / (1024 * 1024)

        await get_or_create_user(user_id)

        temp_root = Path(Config.TEMP_DIR) / str(user_id) / uuid.uuid4().hex
        temp_root.mkdir(parents=True, exist_ok=True)

        await register_temp_path(user_id, str(temp_root), Config.AUTO_DELETE_DEFAULT_MIN)

        status_msg = await msg.reply_text("Downloading archive to server…")

        start = time.time()
        try:
            downloaded_path = await client.download_media(
                doc,
                file_name=str(temp_root),
                progress=progress_for_pyrogram,
                progress_args=(status_msg, start, file_name, "to my server"),
            )
        except Exception as e:
            await status_msg.edit_text(f"Download fail ho gaya:\n<code>{e}</code>")
            return

        if not downloaded_path:
            await status_msg.edit_text("Download hua nahi, file path missing hai.")
            return

        archive_path = downloaded_path

        try:
            await log_user_input(client, msg, f"archive: {file_name}")
        except Exception:
            pass

        if user_cancelled.get(user_id):
            await status_msg.edit_text("Task cancel kar diya ✅")
            return

        if not password and detect_encrypted(archive_path):
            await status_msg.edit_text(
                "Archive password protected lag rahi hai.\n"
                "Use 'With Password' button & try again."
            )
            return

        await status_msg.edit_text("Extraction shuru… Thoda sabr 😎")
        extract_dir = temp_root / "extracted"
        try:
            result = extract_archive(archive_path, str(extract_dir), password=password)
        except Exception as e:
            await status_msg.edit_text(f"Extract error:\n<code>{e}</code>")
            return

        if user_cancelled.get(user_id):
            await status_msg.edit_text(
                "Task cancel ho gaya mid‑way, output skip kar diya."
            )
            return

        stats = result["stats"]
        files = sorted(result["files"], key=lambda p: p.lower())

        links_map = extract_links_from_folder(str(extract_dir))

        task_id = uuid.uuid4().hex
        tasks[task_id] = {
            "type": "unzip",
            "user_id": user_id,
            "base_dir": str(extract_dir),
            "files": files,
            "archive_name": os.path.basename(archive_path),
        }

        summary = (
            f"<b>Extraction done ✅</b>\n\n"
            f"Archive: <code>{os.path.basename(archive_path)}</code>\n"
            f"Total files: {stats['total_files']}\n"
            f"Folders: {stats['folders']}\n"
            f"Videos: {stats['videos']} | PDFs: {stats['pdf']} | APK: {stats['apk']}\n"
            f"TXT: {stats['txt']} | M3U/M3U8: {stats['m3u']} | Others: {stats['others']}\n\n"
            f"Links inside archive:\n"
            f"• Direct: {len(links_map.get('direct', []))}\n"
            f"• m3u8: {len(links_map.get('m3u8', []))}\n"
            f"• GDrive: {len(links_map.get('gdrive', []))}\n"
            f"• Telegram: {len(links_map.get('telegram', []))}\n"
        )

        rows = []
        rows.append(
            [InlineKeyboardButton("❌ Cancel", callback_data=f"ucancel|{task_id}")]
        )
        rows.append(
            [InlineKeyboardButton("🚀 Send ALL files", callback_data=f"sendall|{task_id}")]
        )

        max_files_buttons = 25
        for idx, rel_path in enumerate(files[:max_files_buttons]):
            short = rel_path
            if len(short) > 40:
                short = "..." + short[-37:]
            rows.append(
                [InlineKeyboardButton(short, callback_data=f"sendone|{task_id}|{idx}")]
            )

        kb = InlineKeyboardMarkup(rows)

        await status_msg.edit_text(summary, reply_markup=kb)
        await update_user_stats(user_id, size_mb)


async def handle_send_all(client: Client, cq: CallbackQuery, task_id: str):
    info = tasks.get(task_id)
    if not info:
        await cq.answer("Task expired ya clean ho chuka hai.", show_alert=True)
        return

    if not cq.from_user:
        await cq.answer()
        return
    user = cq.from_user
    if user.id != info["user_id"]:
        await cq.answer("Ye tumhara task nahi hai.", show_alert=True)
        return

    base_dir = Path(info["base_dir"])
    files = info["files"]
    archive_name = info.get("archive_name", "archive")

    await cq.answer()
    await cq.message.edit_text(
        "Sending all extracted files… thoda time lag sakta hai."
    )

    chat_id = cq.message.chat.id
    reply_to = cq.message.id
    is_private = cq.message.chat.type == enums.ChatType.PRIVATE
    pinned = False

    if is_private:
        try:
            await client.pin_chat_message(chat_id, cq.message.id)
            pinned = True
        except Exception:
            pinned = False

    for rel in files:
        if user_cancelled.get(user.id):
            break

        full = base_dir / rel
        if not full.is_file():
            continue
        try:
            sent = None
            if is_video_path(rel):
                name = Path(rel).name
                base_caption = name
                caption = build_caption(user.id, base_caption)
                thumb_arg = await choose_thumbnail(user.id, str(full))

                status = await client.send_message(
                    chat_id,
                    f"Uploading: {name}",
                    reply_to_message_id=reply_to,
                )
                start_u = time.time()
                sent = await client.send_video(
                    chat_id,
                    str(full),
                    caption=caption,
                    thumb=thumb_arg,
                    progress=progress_for_pyrogram,
                    progress_args=(status, start_u, name, "to Telegram"),
                    reply_to_message_id=reply_to,
                )
                try:
                    await status.delete()
                except Exception:
                    pass
            else:
                status = await client.send_message(
                    chat_id,
                    f"Uploading: {rel}",
                    reply_to_message_id=reply_to,
                )
                start_u = time.time()
                sent = await client.send_document(
                    chat_id=chat_id,
                    document=str(full),
                    caption=rel,
                    progress=progress_for_pyrogram,
                    progress_args=(status, start_u, rel, "to Telegram"),
                    reply_to_message_id=reply_to,
                )
                try:
                    await status.delete()
                except Exception:
                    pass

            if sent:
                try:
                    await log_user_output(
                        client, user, sent, f"unzip send_all from {archive_name}"
                    )
                except Exception:
                    pass
        except Exception:
            pass
        await asyncio.sleep(0.5)

    if is_private and pinned:
        try:
            await client.unpin_chat_message(chat_id, cq.message.id)
        except Exception:
            pass

    await client.send_message(chat_id, "All extracted files sent ✅", reply_to_message_id=reply_to)


async def handle_send_one(
    client: Client, cq: CallbackQuery, task_id: str, index: int
):
    info = tasks.get(task_id)
    if not info:
        await cq.answer("Task expired ya clean ho chuka hai.", show_alert=True)
        return

    if not cq.from_user:
        await cq.answer()
        return
    user = cq.from_user
    if user.id != info["user_id"]:
        await cq.answer("Ye tumhara task nahi hai.", show_alert=True)
        return

    files = info["files"]
    if index < 0 or index >= len(files):
        await cq.answer("Invalid index.", show_alert=True)
        return

    await cq.answer()
    base_dir = Path(info["base_dir"])
    rel = files[index]
    full = base_dir / rel
    if not full.is_file():
        await cq.message.reply_text("File missing ho gayi lagti hai.")
        return

    chat_id = cq.message.chat.id
    reply_to = cq.message.id

    try:
        sent = None
        if is_video_path(rel):
            name = Path(rel).name
            base_caption = name
            caption = build_caption(user.id, base_caption)
            thumb_arg = await choose_thumbnail(user.id, str(full))

            status = await client.send_message(
                chat_id,
                f"Uploading: {name}",
                reply_to_message_id=reply_to,
            )
            start_u = time.time()
            sent = await client.send_video(
                chat_id,
                str(full),
                caption=caption,
                thumb=thumb_arg,
                progress=progress_for_pyrogram,
                progress_args=(status, start_u, name, "to Telegram"),
                reply_to_message_id=reply_to,
            )
            try:
                await status.delete()
            except Exception:
                pass
        else:
            status = await client.send_message(
                chat_id,
                f"Uploading: {rel}",
                reply_to_message_id=reply_to,
            )
            start_u = time.time()
            sent = await client.send_document(
                chat_id=chat_id,
                document=str(full),
                caption=rel,
                progress=progress_for_pyrogram,
                progress_args=(status, start_u, rel, "to Telegram"),
                reply_to_message_id=reply_to,
            )
            try:
                await status.delete()
            except Exception:
                pass

        if sent:
            try:
                await log_user_output(
                    client,
                    user,
                    sent,
                    f"unzip send_one from {info.get('archive_name','archive')}",
                )
            except Exception:
                pass
    except Exception:
        pass


async def handle_extract_audio(client: Client, cq: CallbackQuery, msg: Message):
    if not cq.from_user:
        await cq.answer()
        return
    user = cq.from_user
    user_id = user.id
    if await is_banned(user_id):
        await cq.answer("You are banned.", show_alert=True)
        return

    video = msg.video
    if not video:
        await cq.answer("Ye video nahi hai.", show_alert=True)
        return

    lock = get_lock(user_id)
    if lock.locked():
        await cq.answer(
            "Ek task already running hai, thoda wait karo.", show_alert=True
        )
        return

    await cq.answer()

    async with lock:
        file_name = video.file_name or "video"
        base_name = os.path.splitext(file_name)[0]
        temp_root = Path(Config.TEMP_DIR) / str(user_id) / uuid.uuid4().hex
        temp_root.mkdir(parents=True, exist_ok=True)

        await register_temp_path(
            user_id, str(temp_root), Config.AUTO_DELETE_DEFAULT_MIN
        )

        reply_to = cq.message.id
        status = await cq.message.reply_text(
            "Downloading video for audio extract…"
        )

        start = time.time()
        try:
            downloaded_path = await client.download_media(
                video,
                file_name=str(temp_root),
                progress=progress_for_pyrogram,
                progress_args=(status, start, file_name, "to my server"),
            )
        except Exception as e:
            await status.edit_text(f"Download fail:\n<code>{e}</code>")
            return

        if not downloaded_path:
            await status.edit_text("Download hua nahi, file path missing hai.")
            return

        video_path = downloaded_path
        audio_path = str(temp_root / f"{base_name}.m4a")
        try:
            await extract_audio(video_path, audio_path)
        except Exception as e:
            await status.edit_text(f"ffmpeg error:\n<code>{e}</code>")
            return

        await status.edit_text("Uploading audio to you…")
        try:
            start_u = time.time()
            sent = await client.send_document(
                chat_id=cq.message.chat.id,
                document=audio_path,
                caption=f"Extracted audio from {file_name}",
                progress=progress_for_pyrogram,
                progress_args=(status, start_u, f"{base_name}.m4a", "to Telegram"),
                reply_to_message_id=reply_to,
            )
            try:
                await status.delete()
            except Exception:
                pass
            try:
                await log_user_output(
                    client, user, sent, "audio extracted from video"
                )
            except Exception:
                pass
        except Exception:
            pass

# ----------------- links: download_all (direct + GDrive + m3u8) -----------------


async def handle_links_download_all(
    client: Client, cq: CallbackQuery, original_msg: Message
):
    key = (original_msg.chat.id, original_msg.id)
    session = LINK_SESSIONS.get(key)
    content = session["content"] if session else (original_msg.text or original_msg.caption or "") or ""
    all_links = session["links"] if session else find_links_in_text(content)
    if not all_links:
        await cq.message.edit_text("Koi URL nahi mila.")
        return

    # categorize
    cats: Dict[str, list] = {
        "direct": [],
        "m3u8": [],
        "gdrive": [],
        "telegram": [],
        "unknown": [],
    }
    for url in all_links:
        kind = classify_link(url)
        cats.setdefault(kind, [])
        cats[kind].append(url)

    direct_links = cats.get("direct", [])
    m3u8_links = cats.get("m3u8", [])
    gdrive_links = cats.get("gdrive", [])
    unknown_links = cats.get("unknown", [])

    candidate_direct = direct_links + unknown_links

    if not candidate_direct and not m3u8_links and not gdrive_links:
        await cq.message.edit_text(
            "Direct/m3u8/GDrive type supported links nahi mile.\n"
            "Telegram / unknown complex links ke liye auto-download off hai."
        )
        return

    if not cq.from_user:
        return
    user = cq.from_user
    user_id = user.id

    temp_root = Path(Config.TEMP_DIR) / str(user_id) / uuid.uuid4().hex
    temp_root.mkdir(parents=True, exist_ok=True)
    await register_temp_path(
        user_id, str(temp_root), Config.AUTO_DELETE_DEFAULT_MIN
    )

    first_text = (
        f"Direct: {len(direct_links)} | Unknown(as direct): {len(unknown_links)} | "
        f"GDrive: {len(gdrive_links)} | m3u8: {len(m3u8_links)}\n"
        "Downloading supported direct/GDrive files pehle…"
    )
    try:
        await cq.message.edit_text(first_text)
    except MessageNotModified:
        pass
    except Exception:
        pass

    ok = 0
    fail = 0
    chat_id = cq.message.chat.id
    reply_to = cq.message.id
    is_private = cq.message.chat.type == enums.ChatType.PRIVATE
    pinned = False

    if is_private:
        try:
            await client.pin_chat_message(chat_id, cq.message.id)
            pinned = True
        except Exception:
            pinned = False

    # direct + unknown as direct
    for url in candidate_direct:
        if user_cancelled.get(user_id):
            break

        base_raw = url.split("?", 1)[0].split("#", 1)[0]
        base_guess = base_raw.rsplit("/", 1)[-1] or f"file_{uuid.uuid4().hex}"
        dest_path = str(temp_root / base_guess)
        try:
            status = await client.send_message(
                chat_id,
                f"Downloading from link:\n{url}",
                reply_to_message_id=reply_to,
            )
            final_path = await download_file(
                url,
                dest_path,
                status_message=status,
                file_name=base_guess,
                direction="to my server",
            )
            basename = os.path.basename(final_path)
            await status.edit_text(f"Uploading to you:\n{basename}")
            if is_video_path(basename):
                base_caption = basename
                caption = build_caption(user_id, base_caption)
                thumb_arg = await choose_thumbnail(user_id, final_path)

                start_u = time.time()
                sent = await client.send_video(
                    chat_id,
                    final_path,
                    caption=caption,
                    thumb=thumb_arg,
                    progress=progress_for_pyrogram,
                    progress_args=(status, start_u, basename, "to Telegram"),
                    reply_to_message_id=reply_to,
                )
            else:
                start_u = time.time()
                sent = await client.send_document(
                    chat_id,
                    final_path,
                    caption=basename,
                    progress=progress_for_pyrogram,
                    progress_args=(status, start_u, basename, "to Telegram"),
                    reply_to_message_id=reply_to,
                )
            try:
                await status.delete()
            except Exception:
                pass
            ok += 1
            try:
                await log_user_output(
                    client, user, sent, f"direct/unknown link: {url}"
                )
            except Exception:
                pass
        except Exception:
            fail += 1
        await asyncio.sleep(0.5)

    # Google Drive
    for url in gdrive_links:
        if user_cancelled.get(user_id):
            break

        direct_url = get_gdrive_direct_link(url)
        if not direct_url:
            fail += 1
            continue
        base_raw = direct_url.split("?", 1)[0].split("#", 1)[0]
        base_guess = base_raw.rsplit("/", 1)[-1] or f"gdrive_{uuid.uuid4().hex}"
        dest_path = str(temp_root / base_guess)
        try:
            status = await client.send_message(
                chat_id,
                f"Downloading from GDrive:\n{url}",
                reply_to_message_id=reply_to,
            )
            final_path = await download_file(
                direct_url,
                dest_path,
                status_message=status,
                file_name=base_guess,
                direction="to my server",
            )
            basename = os.path.basename(final_path)
            await status.edit_text(f"Uploading to you:\n{basename}")
            if is_video_path(basename):
                base_caption = basename
                caption = build_caption(user_id, base_caption)
                thumb_arg = await choose_thumbnail(user_id, final_path)

                start_u = time.time()
                sent = await client.send_video(
                    chat_id,
                    final_path,
                    caption=caption,
                    thumb=thumb_arg,
                    progress=progress_for_pyrogram,
                    progress_args=(status, start_u, basename, "to Telegram"),
                    reply_to_message_id=reply_to,
                )
            else:
                start_u = time.time()
                sent = await client.send_document(
                    chat_id,
                    final_path,
                    caption=basename,
                    progress=progress_for_pyrogram,
                    progress_args=(status, start_u, basename, "to Telegram"),
                    reply_to_message_id=reply_to,
                )
            try:
                await status.delete()
            except Exception:
                pass
            ok += 1
            try:
                await log_user_output(
                    client, user, sent, f"GDrive link: {url}"
                )
            except Exception:
                pass
        except Exception:
            fail += 1
        await asyncio.sleep(0.5)

    # m3u8: quality menus
    for url in m3u8_links:
        if user_cancelled.get(user_id):
            break
        await offer_m3u8_quality_menu(client, cq, user_id, url, temp_root)

    txt = (
        f"Direct/GDrive download complete.\n"
        f"Success: {ok}\n"
        f"Failed: {fail}\n\n"
        f"m3u8 links ke liye quality choose karne ke buttons alag se bhej diye gaye hain."
    )
    try:
        await cq.message.edit_text(txt)
    except MessageNotModified:
        pass
    except Exception:
        pass

    if is_private and pinned:
        try:
            await client.unpin_chat_message(chat_id, cq.message.id)
        except Exception:
            pass
        await client.send_message(chat_id, "All link downloads finished ✅", reply_to_message_id=reply_to)


async def offer_m3u8_quality_menu(
    client: Client, cq: CallbackQuery, user_id: int, url: str, temp_root: Path
):
    chat_id = cq.message.chat.id
    reply_to = cq.message.id

    try:
        variants = await get_m3u8_variants(url)
    except Exception as e:
        await client.send_message(
            chat_id,
            f"m3u8 parse nahi ho paya:\n<code>{e}</code>",
            reply_to_message_id=reply_to,
        )
        return

    base_raw = url.split("?", 1)[0].split("#", 1)[0]
    base_name = base_raw.rsplit("/", 1)[-1] or "stream"
    if base_name.endswith(".m3u8"):
        base_name = base_name[:-6]

    if not variants:
        variants = [{"name": "Auto", "url": url}]

    task_id = uuid.uuid4().hex
    M3U8_TASKS[task_id] = {
        "user_id": user_id,
        "url": url,
        "variants": variants,
        "temp_root": str(temp_root),
        "base_name": base_name,
    }

    buttons = []
    for idx, v in enumerate(variants):
        buttons.append(
            [InlineKeyboardButton(v["name"], callback_data=f"m3q|{task_id}|{idx}")]
        )

    kb = InlineKeyboardMarkup(buttons)
    await client.send_message(
        chat_id,
        f"m3u8 stream mila:\n<code>{url}</code>\n\nQuality choose karo:",
        reply_markup=kb,
        reply_to_message_id=reply_to,
    )


async def handle_m3u8_quality_choice(
    client: Client, cq: CallbackQuery, task_id: str, index: int
):
    info = M3U8_TASKS.get(task_id)
    if not info:
        await cq.answer("Task expire ho gaya.", show_alert=True)
        return

    if not cq.from_user or cq.from_user.id != info["user_id"]:
        await cq.answer("Ye tumhara task nahi hai.", show_alert=True)
        return

    variants = info["variants"]
    if index < 0 or index >= len(variants):
        await cq.answer("Invalid selection.", show_alert=True)
        return

    v = variants[index]
    url = v["url"]
    name = v["name"]
    temp_root = Path(info["temp_root"])
    base_name = info["base_name"]

    chat_id = cq.message.chat.id
    user = cq.from_user
    user_id = user.id
    reply_to = cq.message.id

    await cq.answer()
    await cq.message.edit_text(f"Downloading {name} stream…")

    dest_path = str(temp_root / f"{base_name}_{name}.mp4")
    try:
        await download_m3u8_stream(url, dest_path)
    except Exception as e:
        await cq.message.edit_text(f"m3u8 download fail:\n<code>{e}</code>")
        M3U8_TASKS.pop(task_id, None)
        return

    base_caption = f"{base_name} [{name}]"
    caption = build_caption(user_id, base_caption)
    thumb_arg = await choose_thumbnail(user_id, dest_path)

    await cq.message.edit_text("Uploading m3u8 video to you…")
    start_u = time.time()
    sent = await client.send_video(
        chat_id,
        dest_path,
        caption=caption,
        thumb=thumb_arg,
        progress=progress_for_pyrogram,
        progress_args=(cq.message, start_u, base_caption, "to Telegram"),
        reply_to_message_id=reply_to,
    )
    try:
        await cq.message.delete()
    except Exception:
        pass
    try:
        await log_user_output(
            client, user, sent, f"m3u8 link: {url}"
        )
    except Exception:
        pass
    M3U8_TASKS.pop(task_id, None)


# ----------------- main (local run only; Render par server.py) -----------------


async def main():
    asyncio.create_task(cleanup_worker())
    await app.start()
    print("Serena Unzip bot started.")
    await idle()
    await app.stop()


if __name__ == "__main__":
    asyncio.run(main())

