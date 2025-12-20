# bot.py
import asyncio
import os
import random
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
    update_user_stats,
    set_ban,
    count_users,
    get_all_users,
    register_temp_path,
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
tasks: Dict[str, Dict[str, Any]] = {}        # unzip tasks
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


# ----------------- logging helpers (topics per user) -----------------

log_chat_info: Optional[Chat] = None
log_is_forum: bool = False
user_log_topics: Dict[int, int] = {}  # user_id -> message_thread_id


async def get_log_chat_info(client: Client) -> Tuple[Optional[Chat], bool]:
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
    Returns (chat_id, thread_id or None).
    If log chat is a forum, ensures a topic per user.
    """
    if not Config.LOG_CHANNEL_ID:
        return None, None

    chat_info, is_forum = await get_log_chat_info(client)
    if not chat_info:
        return None, None

    chat_id = chat_info.id

    if not is_forum:
        return chat_id, None

    # forum with topics
    if user.id in user_log_topics:
        return chat_id, user_log_topics[user.id]

    # create new topic
    name = f"{user.first_name or 'User'} | {user.id}"
    try:
        topic_msg = await client.create_forum_topic(chat_id, name=name)
        thread_id = topic_msg.message_thread_id
        user_log_topics[user.id] = thread_id
        intro = (
            f"👤 <b>User</b>\n"
            f"• Name: <b>{user.first_name or ''}</b> (@{user.username or 'N/A'})\n"
            f"• ID: <code>{user.id}</code>\n"
            f"• First seen: <code>{time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())}</code>"
        )
        await client.send_message(chat_id, intro, message_thread_id=thread_id)
        return chat_id, thread_id
    except Exception:
        # fallback: send in main chat
        return chat_id, None


async def log_user_input(client: Client, message: Message, context: str):
    if not Config.LOG_CHANNEL_ID:
        return
    user = message.from_user
    if not user:
        return
    chat_id, thread_id = await get_user_log_target(client, user)
    if not chat_id:
        return

    cap = f"🔹 <b>INPUT</b>: {context}"
    if message.caption:
        cap += f"\n\n{message.caption}"

    try:
        await message.copy(
            chat_id=chat_id,
            message_thread_id=thread_id,
            caption=cap,
        )
    except Exception:
        pass


async def log_user_output(client: Client, user, msg: Message, context: str):
    if not Config.LOG_CHANNEL_ID or not user:
        return
    chat_id, thread_id = await get_user_log_target(client, user)
    if not chat_id:
        return

    cap = f"✅ <b>OUTPUT</b>: {context}"
    if msg.caption:
        cap += f"\n\n{msg.caption}"

    try:
        await msg.copy(
            chat_id=chat_id,
            message_thread_id=thread_id,
            caption=cap,
        )
    except Exception:
        pass


# ----------------- helpers -----------------


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
        "• <code>/settings</code> – View current defaults.\n"
        "• <code>/cancel</code> – Mark current task as cancelled.\n\n"
        "📦 <b>Archives (ZIP / RAR / 7Z / TAR…)</b>\n"
        "1) Send or forward any archive file.\n"
        "2) Tap:\n"
        "   • <b>📦 Unzip</b> – Normal extract.\n"
        "   • <b>🔐 With Password</b> – Bot asks for password, then extracts.\n"
        "3) After extract you get:\n"
        "   • Summary of videos / PDFs / APKs / TXT / m3u8 / others.\n"
        "   • Inline list of files → tap any to get <b>only that file</b>.\n"
        "   • <b>Send ALL</b> – sends every file back (videos as playable media).\n\n"
        "🎬 <b>Videos & Audio</b>\n"
        "• Send any video → tap <b>🎧 Extract Audio</b>.\n"
        "  - Bot downloads, extracts audio via ffmpeg and sends it.\n"
        "• Extracted / downloaded videos are sent as real Telegram videos\n"
        "  with thumbnails generated from inside the video.\n\n"
        "🔗 <b>TXT / Links Power Mode</b>\n"
        "1) Send a message or <b>.txt file</b> containing links in DM.\n"
        "2) Bot detects URLs and shows:\n"
        "   • <b>Download all videos/files</b>\n"
        "   • <b>Cleaned TXT only</b> (just the links)\n"
        "   • <b>Skip</b>\n"
        "3) On <b>Download all</b>:\n"
        "   • Direct file links (mp4/mkv/zip/apk/xapk/audio/…) are downloaded\n"
        "     with progress bar (speed + ETA) and then sent.\n"
        "   • Google Drive links are converted to direct download if possible and\n"
        "     downloaded with their real filename (e.g. .mp4, .jpg, .pdf).\n"
        "   • m3u8 links: you get quality buttons (360p / 480p / 720p / Auto etc).\n"
        "     You choose quality → ffmpeg downloads → bot sends mp4.\n"
        "   • When link‑based downloading starts, the DM status message is pinned\n"
        "     and unpinned when completed.\n\n"
        "👥 <b>Groups & Channels</b>\n"
        "• Add bot with message/media permissions:\n"
        "  - Archives & videos get unzip / extract buttons.\n\n"
        "🛠 <b>Admin Only</b>\n"
        "• <code>/status</code> – User stats & disk usage.\n"
        "• <code>/broadcast</code> – Reply and broadcast to all users.\n"
        "• <code>/ban &lt;id&gt;</code> / <code>/unban &lt;id&gt;</code> – Ban/unban.\n"
        "• <code>/clean</code> – Manual temp storage cleanup.\n\n"
        "<i>Tip:</i> Very large Google Drive files or protected shares might fail\n"
        "due to Google limits, not the bot itself.\n"
    )
    await message.reply_text(text)


@app.on_message(filters.command("settings") & filters.private)
async def settings_cmd(client: Client, message: Message):
    text = (
        f"{random_emoji()} <b>Current Settings</b>\n\n"
        "• Auto delete temp files: <b>30 minutes</b>\n"
        "• Default extract mode: <b>Full archive</b>\n"
        "• Language: <b>English</b>\n\n"
        "Advanced per-user caption / replace rules can be added later.\n"
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
        "Aight, current task ko cancel mode pe daal diya. Next step se skip hoga 😉"
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

    user = await get_or_create_user(user_id)

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


# ----------------- text / links handler (DM only) -----------------


async def process_links_message(client: Client, message: Message, content: str):
    if not message.from_user:
        return
    user_id = message.from_user.id

    links = find_links_in_text(content or "")
    if not links:
        await message.reply_text(
            "No valid URLs found in this text."
        )
        return

    # log input text
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
        f"Found <b>{len(links)}</b> urls.\nChoose what to do:", reply_markup=kb
    )


@app.on_message((filters.text | filters.caption) & filters.private)
async def on_text(client: Client, message: Message):
    if not message.from_user:
        return
    # Agar command hai to ignore (broadcast etc.)
    if message.text and message.text.lstrip().startswith("/"):
        return

    if await is_banned(message.from_user.id):
        return

    # password reply?
    if message.from_user.id in pending_password:
        info = pending_password.pop(message.from_user.id)
        password = (message.text or message.caption or "").strip()
        await handle_unzip_from_password(client, message, info, password)
        return

    if not await check_force_sub(client, message):
        return

    await get_or_create_user(message.from_user.id)

    content = (message.text or message.caption or "").strip()
    await process_links_message(client, message, content)


# ----------------- callback handlers -----------------


@app.on_callback_query()
async def callbacks(client: Client, cq: CallbackQuery):
    data = cq.data or ""
    if data == "retry_force_sub":
        await cq.message.delete()
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

        if action == "clean_txt":
            await cq.answer()
            content = (original_msg.text or original_msg.caption or "") or ""
            links = find_links_in_text(content)
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

# ----------------- unzip flow -----------------


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
            f"Password bhejo for <code>{file_name}</code> (just text)."
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

        await register_temp_path(
            user_id, str(temp_root), Config.AUTO_DELETE_DEFAULT_MIN
        )

        status_msg = await msg.reply_text("Downloading archive to server…")

        start = time.time()
        try:
            downloaded_path = await client.download_media(
                doc,
                file_name=str(temp_root),  # dir; pyrogram set file name
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

        if user_cancelled.get(user_id):
            await status_msg.edit_text("Task cancel kar diya ✅")
            return

        if not password and detect_encrypted(archive_path):
            await status_msg.edit_text(
                "Archive password protected lag rahi hai.\n"
                "Niche wale 'With Password' button use karo & dobara try karo."
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
        files = sorted(result["files"], key=lambda p: p.lower())  # stable order

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
        max_files_buttons = 25
        for idx, rel_path in enumerate(files[:max_files_buttons]):
            short = rel_path
            if len(short) > 40:
                short = "..." + short[-37:]
            rows.append(
                [InlineKeyboardButton(short, callback_data=f"sendone|{task_id}|{idx}")]
            )

        rows.append(
            [
                InlineKeyboardButton(
                    "🚀 Send ALL files", callback_data=f"sendall|{task_id}"
                )
            ]
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
    is_private = cq.message.chat.type == enums.ChatType.PRIVATE
    pinned = False

    if is_private:
        try:
            await client.pin_chat_message(chat_id, cq.message.id)
            pinned = True
        except Exception:
            pinned = False

    for rel in files:
        full = base_dir / rel
        if not full.is_file():
            continue
        try:
            if is_video_path(rel):
                name = Path(rel).name
                caption = name
                thumb_path = str(full) + ".jpg"
                try:
                    await generate_thumbnail(str(full), thumb_path)
                    thumb_arg = thumb_path
                except Exception:
                    thumb_arg = None

                sent = await client.send_video(
                    chat_id,
                    str(full),
                    caption=caption,
                    thumb=thumb_arg,
                )
            else:
                sent = await client.send_document(
                    chat_id=chat_id,
                    document=str(full),
                    caption=rel,
                )
            # log output
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

    await client.send_message(chat_id, "All extracted files sent ✅")


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
    try:
        if is_video_path(rel):
            name = Path(rel).name
            caption = name
            thumb_path = str(full) + ".jpg"
            try:
                await generate_thumbnail(str(full), thumb_path)
                thumb_arg = thumb_path
            except Exception:
                thumb_arg = None

            sent = await client.send_video(
                chat_id,
                str(full),
                caption=caption,
                thumb=thumb_arg,
            )
        else:
            sent = await client.send_document(
                chat_id=chat_id,
                document=str(full),
                caption=rel,
            )
        try:
            await log_user_output(
                client, user, sent, f"unzip send_one from {info.get('archive_name','archive')}"
            )
        except Exception:
            pass
    except Exception:
        pass


# ----------------- extract audio from video -----------------


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

        await status.edit_text("Audio ready, sending…")
        try:
            sent = await client.send_document(
                chat_id=cq.message.chat.id,
                document=audio_path,
                caption=f"Extracted audio from {file_name}",
            )
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
    content = (original_msg.text or original_msg.caption or "") or ""
    all_links = find_links_in_text(content)
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

    # Pin progress message in DM
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
        base_raw = url.split("?", 1)[0].split("#", 1)[0]
        base_guess = base_raw.rsplit("/", 1)[-1] or f"file_{uuid.uuid4().hex}"
        dest_path = str(temp_root / base_guess)
        try:
            status = await client.send_message(
                chat_id,
                f"Downloading from link:\n{url}",
            )
            final_path = await download_file(
                url,
                dest_path,
                status_message=status,
                file_name=base_guess,
                direction="from web",
            )
            try:
                await status.delete()
            except Exception:
                pass

            basename = os.path.basename(final_path)
            if is_video_path(basename):
                caption = basename
                thumb_path = final_path + ".jpg"
                try:
                    await generate_thumbnail(final_path, thumb_path)
                    thumb_arg = thumb_path
                except Exception:
                    thumb_arg = None

                sent = await client.send_video(
                    chat_id, final_path, caption=caption, thumb=thumb_arg
                )
            else:
                sent = await client.send_document(chat_id, final_path, caption=basename)
            ok += 1
            try:
                await log_user_output(
                    client, user, sent, "download from direct/unknown link"
                )
            except Exception:
                pass
        except Exception:
            fail += 1
        await asyncio.sleep(0.5)

    # Google Drive
    for url in gdrive_links:
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
            )
            final_path = await download_file(
                direct_url,
                dest_path,
                status_message=status,
                file_name=base_guess,
                direction="from GDrive",
            )
            try:
                await status.delete()
            except Exception:
                pass

            basename = os.path.basename(final_path)
            if is_video_path(basename):
                caption = basename
                thumb_path = final_path + ".jpg"
                try:
                    await generate_thumbnail(final_path, thumb_path)
                    thumb_arg = thumb_path
                except Exception:
                    thumb_arg = None

                sent = await client.send_video(
                    chat_id, final_path, caption=caption, thumb=thumb_arg
                )
            else:
                sent = await client.send_document(chat_id, final_path, caption=basename)
            ok += 1
            try:
                await log_user_output(
                    client, user, sent, "download from Google Drive link"
                )
            except Exception:
                pass
        except Exception:
            fail += 1
        await asyncio.sleep(0.5)

    # m3u8: quality menus
    for url in m3u8_links:
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
        await client.send_message(chat_id, "All link downloads finished ✅")


async def offer_m3u8_quality_menu(
    client: Client, cq: CallbackQuery, user_id: int, url: str, temp_root: Path
):
    chat_id = cq.message.chat.id

    try:
        variants = await get_m3u8_variants(url)
    except Exception as e:
        await client.send_message(
            chat_id,
            f"m3u8 parse nahi ho paya:\n<code>{e}</code>"
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

    await cq.answer()
    await cq.message.edit_text(f"Downloading {name} stream…")

    dest_path = str(temp_root / f"{base_name}_{name}.mp4")
    try:
        await download_m3u8_stream(url, dest_path)
    except Exception as e:
        await cq.message.edit_text(f"m3u8 download fail:\n<code>{e}</code>")
        M3U8_TASKS.pop(task_id, None)
        return

    caption = f"{base_name} [{name}]"
    thumb_path = dest_path + ".jpg"
    try:
        await generate_thumbnail(dest_path, thumb_path)
        thumb_arg = thumb_path
    except Exception:
        thumb_arg = None

    sent = await client.send_video(
        chat_id,
        dest_path,
        caption=caption,
        thumb=thumb_arg,
    )
    M3U8_TASKS.pop(task_id, None)
    try:
        await client.delete_messages(chat_id, cq.message.id)
    except Exception:
        pass
    try:
        await log_user_output(
            client, user, sent, "download from m3u8 stream"
        )
    except Exception:
        pass


# ----------------- main (local run only; Render par server.py) -----------------


async def main():
    asyncio.create_task(cleanup_worker())
    await app.start()
    print("Serena Unzip bot started.")
    await idle()
    await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
