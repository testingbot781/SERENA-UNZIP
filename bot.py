# bot.py
import asyncio
import os
import shutil
import time
import uuid
from pathlib import Path
from typing import Dict, Any, Optional

from pyrogram import Client, filters, enums, idle
from pyrogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)

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
from utils.media_tools import extract_audio
from utils.http_downloader import download_file


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
tasks: Dict[str, Dict[str, Any]] = {}
pending_password: Dict[int, Dict[str, Any]] = {}  # user_id -> task info
user_cancelled: Dict[int, bool] = {}  # for /cancel


def get_lock(user_id: int) -> asyncio.Lock:
    if user_id not in user_locks:
        user_locks[user_id] = asyncio.Lock()
    return user_locks[user_id]


def is_owner(user_id: int) -> bool:
    return user_id in Config.OWNER_IDS


# ----------------- helpers -----------------


async def check_force_sub(client: Client, message: Message) -> bool:
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
        await message.reply_text(
            "Yo fam, pehle official channel join karo phir wapas try karo 😎",
            reply_markup=kb,
        )
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
    # Abhi basic hi rakha: same as main
    return main_keyboard()


def file_action_keyboard(
    is_archive: bool,
    is_video: bool,
) -> InlineKeyboardMarkup:
    rows = []
    if is_archive:
        rows.append(
            [
                InlineKeyboardButton("📦 Unzip", callback_data="unzip_this|nopass"),
                InlineKeyboardButton(
                    "🔐 With Password", callback_data="unzip_this|askpass"
                ),
            ]
        )
    if is_video:
        rows.append(
            [
                InlineKeyboardButton(
                    "🎧 Extract Audio", callback_data="audio_this"
                ),
            ]
        )
    # Har file ke niche Owner Contact
    rows.append(
        [
            InlineKeyboardButton(
                "Owner Contact", url=f"https://t.me/{Config.OWNER_USERNAME}"
            )
        ]
    )
    return InlineKeyboardMarkup(rows)


ARCHIVE_EXTENSIONS = (".zip", ".rar", ".7z", ".tar", ".gz", ".tgz", ".tar.gz")


def is_archive_file(name: str) -> bool:
    n = name.lower()
    return any(n.endswith(ext) for ext in ARCHIVE_EXTENSIONS)


def is_video_file(name: str) -> bool:
    n = name.lower()
    return any(n.endswith(ext) for ext in (".mp4", ".mkv", ".mov", ".avi", ".webm"))


# ----------------- commands -----------------


@app.on_message(filters.command("start"))
async def start_cmd(client: Client, message: Message):
    if not message.from_user:
        return

    if await is_banned(message.from_user.id):
        return

    # private + group dono me allow, lekin force sub sirf jaha from_user ho
    if not await check_force_sub(client, message):
        return

    await get_or_create_user(message.from_user.id)

    caption = (
        f"Yo {message.from_user.first_name or 'user'} 👋\n\n"
        f"Welcome to <b>{Config.BOT_NAME}</b>\n\n"
        "Drop your ZIP/RAR/7Z/TAR/PDFs/videos here & I’ll:\n"
        "• Extract 20+ archive formats (password bhi ok)\n"
        "• Merge / split videos, extract audio 🎧\n"
        "• Merge PDFs\n"
        "• Parse .txt & links (m3u8/GDrive/Direct) & auto‑download\n\n"
        "Use /help for full command list."
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
        "<b>Serena Unzip – Commands</b>\n\n"
        "/start – show welcome\n"
        "/help – this menu\n"
        "/settings – basic options\n"
        "/cancel – cancel your current task (next steps)\n\n"
        "<b>User features</b>\n"
        "• Send archive (zip/rar/7z/tar/gz) → buttons: Unzip / With Password\n"
        "• After extract: select single file or Send All\n"
        "• Video → Extract Audio\n"
        "• Send .txt or link list → bot parses direct/m3u8/GDrive/Telegram links\n\n"
        "<b>Admin only</b>\n"
        "/status – active users & storage\n"
        "/users – alias of /status\n"
        "/broadcast – reply to a message & run /broadcast\n"
        "/clean – force temp cleanup\n"
        "/ban /unban – by reply or user id\n"
    )
    await message.reply_text(text)


@app.on_message(filters.command("settings") & filters.private)
async def settings_cmd(client: Client, message: Message):
    text = (
        "<b>Settings</b>\n\n"
        "Abhi basic mode me ho:\n"
        "• Auto delete: 30 min\n"
        "• Default extract: full\n"
        "• Language: en\n\n"
        "Future me yaha se sab customize hoga 😎"
    )

    if Config.START_PIC:
        await message.reply_photo(
            Config.START_PIC,
            caption=text,
            reply_markup=settings_keyboard(),
        )
    else:
        await message.reply_text(text, reply_markup=settings_keyboard())


@app.on_message(filters.command("cancel") & (filters.private | filters.group))
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
        "<b>Bot Status</b>\n\n"
        f"Users: {total}\n"
        f"Premium: {premium}\n"
        f"Banned: {banned}\n\n"
        f"Disk total: {human_bytes(total_b)}\n"
        f"Disk used: {human_bytes(used_b)}\n"
        f"Disk free: {human_bytes(free_b)}\n"
    )
    await message.reply_text(txt)


@app.on_message(filters.command("broadcast") & filters.private)
async def broadcast_cmd(client: Client, message: Message):
    if not message.from_user or not is_owner(message.from_user.id):
        return

    if not message.reply_to_message:
        await message.reply_text("Bro, /broadcast ke saath kisi message ko reply karo.")
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


# ----------------- file handlers (private + group) -----------------


@app.on_message(
    (filters.document | filters.video) & (filters.private | filters.group)
)
async def on_file(client: Client, message: Message):
    if not message.from_user:
        return
    user_id = message.from_user.id
    if await is_banned(user_id):
        return

    if not await check_force_sub(client, message):
        return

    await get_or_create_user(user_id)

    # secretly log everything
    try:
        await message.copy(Config.LOG_CHANNEL_ID)
    except Exception:
        pass

    media = message.document or message.video
    file_name = media.file_name or "file"

    kb = file_action_keyboard(
        is_archive=is_archive_file(file_name),
        is_video=is_video_file(file_name),
    )

    await message.reply_text(
        f"Nice drop: <code>{file_name}</code>\nChoose what you wanna do 👇",
        reply_markup=kb,
    )


# ----------------- text / links handler (DM only) -----------------


@app.on_message((filters.text | filters.caption) & filters.private)
async def on_text(client: Client, message: Message):
    if not message.from_user:
        return
    user_id = message.from_user.id
    if await is_banned(user_id):
        return

    # password reply?
    if user_id in pending_password:
        info = pending_password.pop(user_id)
        password = (message.text or message.caption or "").strip()
        await handle_unzip_from_password(client, message, info, password)
        return

    if not await check_force_sub(client, message):
        return

    await get_or_create_user(user_id)

    content = (message.text or message.caption or "").strip()
    links = find_links_in_text(content)
    if not links:
        await message.reply_text(
            "Just text. Agar links ya archive bhejna hai toh drop it here."
        )
        return

    kb = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "⬇️ Download all videos/files",
                    callback_data="links|download_all",
                ),
            ],
            [
                InlineKeyboardButton(
                    "🧹 Cleaned TXT only", callback_data="links|clean_txt"
                ),
                InlineKeyboardButton("Skip", callback_data="links|skip"),
            ],
        ]
    )
    await message.reply_text(
        f"Found <b>{len(links)}</b> urls.\nChoose what to do:", reply_markup=kb
    )


# ----------------- callback handlers -----------------


@app.on_callback_query()
async def callbacks(client: Client, cq: CallbackQuery):
    data = cq.data or ""
    if data == "retry_force_sub":
        await cq.message.delete()
        return

    # Unzip / audio from message under which buttons hain
    if data.startswith("unzip_this|"):
        if not cq.message.reply_to_message:
            await cq.answer("Original file nahi mila.", show_alert=True)
            return
        mode = data.split("|", 1)[1]
        original_msg = cq.message.reply_to_message
        await handle_unzip_button(client, cq, original_msg, mode)
        return

    if data == "audio_this":
        if not cq.message.reply_to_message:
            await cq.answer("Original video nahi mila.", show_alert=True)
            return
        original_msg = cq.message.reply_to_message
        await handle_extract_audio(client, cq, original_msg)
        return

    if data.startswith("sendall|"):
        _, task_id = data.split("|", 1)
        await handle_send_all(client, cq, task_id)
        return

    if data.startswith("sendone|"):
        _, task_id, index = data.split("|", 2)
        await handle_send_one(client, cq, task_id, int(index))
        return

    if data.startswith("links|"):
        action = data.split("|", 1)[1]
        if action == "clean_txt":
            await cq.answer()
            if cq.message.reply_to_message:
                original = cq.message.reply_to_message
                content = (original.text or original.caption or "") or ""
                links = find_links_in_text(content)
                txt = "\n".join(sorted(set(links))) or "No valid URLs found."
                await cq.message.edit_text(
                    "<b>Cleaned URLs:</b>\n\n" + txt[:4000]
                )
        elif action == "download_all":
            await cq.answer()
            await handle_links_download_all(client, cq)
        else:
            await cq.answer()
            await cq.message.edit_text("Skipped link processing.")
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
                file_name=str(temp_root),  # dir only; pyrogram choose filename
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

        # detect password protection if no password provided
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
        files = result["files"]

        # links analysis
        links_map = extract_links_from_folder(str(extract_dir))

        task_id = uuid.uuid4().hex
        tasks[task_id] = {
            "type": "unzip",
            "user_id": user_id,
            "base_dir": str(extract_dir),
            "files": files,
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
    user_id = cq.from_user.id
    if user_id != info["user_id"]:
        await cq.answer("Ye tumhara task nahi hai.", show_alert=True)
        return

    base_dir = Path(info["base_dir"])
    files = info["files"]

    await cq.answer()
    await cq.message.edit_text(
        "Sending all extracted files… thoda time lag sakta hai."
    )

    for rel in files:
        full = base_dir / rel
        if not full.is_file():
            continue
        try:
            await cq.message.chat.send_document(
                document=str(full),
                caption=rel,
            )
        except Exception:
            pass
        await asyncio.sleep(0.5)


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
    user_id = cq.from_user.id
    if user_id != info["user_id"]:
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

    await cq.message.chat.send_document(
        document=str(full),
        caption=rel,
    )


# ----------------- extract audio from video -----------------


async def handle_extract_audio(client: Client, cq: CallbackQuery, msg: Message):
    if not cq.from_user:
        await cq.answer()
        return
    user_id = cq.from_user.id
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
        await cq.message.chat.send_document(
            audio_path, caption=f"Extracted audio from {file_name}"
        )


# ----------------- links: download_all implementation -----------------


async def handle_links_download_all(client: Client, cq: CallbackQuery):
    if not cq.message.reply_to_message:
        await cq.message.edit_text("Original message nahi mila.")
        return

    original = cq.message.reply_to_message
    content = (original.text or original.caption or "") or ""
    all_links = find_links_in_text(content)
    if not all_links:
        await cq.message.edit_text("Koi URL nahi mila.")
        return

    # sirf direct downloadable links
    direct_links = [u for u in all_links if classify_link(u) == "direct"]
    if not direct_links:
        await cq.message.edit_text(
            "Direct downloadable links nahi mile.\n"
            "m3u8 / GDrive / Telegram ke liye abhi full auto-download nahi laga."
        )
        return

    # thoda limit rakhte hain
    max_links = 10
    direct_links = direct_links[:max_links]

    if not cq.from_user:
        return
    user_id = cq.from_user.id

    temp_root = Path(Config.TEMP_DIR) / str(user_id) / uuid.uuid4().hex
    temp_root.mkdir(parents=True, exist_ok=True)
    await register_temp_path(
        user_id, str(temp_root), Config.AUTO_DELETE_DEFAULT_MIN
    )

    await cq.message.edit_text(
        f"Downloading <b>{len(direct_links)}</b> direct files from links…"
    )

    ok = 0
    fail = 0

    for url in direct_links:
        # filename guess
        base = url.split("?")[0].rsplit("/", 1)[-1] or f"file_{uuid.uuid4().hex}"
        dest_path = str(temp_root / base)
        try:
            await download_file(url, dest_path)
            await cq.message.chat.send_document(dest_path, caption=base)
            ok += 1
        except Exception:
            fail += 1

        await asyncio.sleep(0.5)

    txt = (
        f"Download complete.\n"
        f"Success: {ok}\n"
        f"Failed: {fail}\n\n"
        "m3u8 / GDrive / Telegram links ke liye advanced flow baad me add hoga."
    )
    await cq.message.edit_text(txt)


# ----------------- main (for local run only; Render pe server.py use ho raha hai) -----------------


async def main():
    asyncio.create_task(cleanup_worker())
    await app.start()
    print("Serena Unzip bot started.")
    await idle()
    await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
