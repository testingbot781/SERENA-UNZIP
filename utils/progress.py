import math
import time
from typing import Dict

from pyrogram.types import Message

from config import Config

_last_update: Dict[int, float] = {}  # msg_id -> timestamp


def human_bytes(size: int) -> str:
    # simple human readable
    if size == 0:
        return "0 B"
    power = 1024
    n = 0
    power_labels = ["B", "KB", "MB", "GB", "TB"]
    while size >= power and n < len(power_labels) - 1:
        size /= power
        n += 1
    return f"{size:.2f} {power_labels[n]}"


def human_time(seconds: int) -> str:
    if seconds < 0:
        seconds = 0
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


async def progress_for_pyrogram(
    current: int,
    total: int,
    message: Message,
    start_time: float,
    file_name: str,
    direction: str = "to my server",  # or "to Telegram"
):
    now = time.time()
    msg_id = message.id
    last = _last_update.get(msg_id, 0)
    if now - last < Config.PROGRESS_UPDATE_INTERVAL and current != total:
        return

    _last_update[msg_id] = now

    if total == 0:
        percent = 0
    else:
        percent = current * 100 / total

    elapsed = now - start_time
    speed = current / elapsed if elapsed > 0 else 0
    eta = int((total - current) / speed) if speed > 0 else 0

    filled_len = int(20 * percent / 100)
    bar = "●" * filled_len + "○" * (20 - filled_len)

    text = (
        "➵⋆🪐ᴛᴇᴄʜɴɪᴄᴀʟ_sᴇʀᴇɴᴀ𓂃\n\n"
        f"{file_name}\n"
        f"{direction}\n"
        f" [{bar}] \n"
        f"◌Progress😉:〘 {percent:.2f}% 〙\n"
        f"Done: 〘{human_bytes(current)} of {human_bytes(total)}〙\n"
        f"◌Speed🚀:〘 {human_bytes(int(speed))}/s 〙\n"
        f"◌Time Left⏳:〘 {human_time(eta)} 〙"
    )

    try:
        await message.edit_text(text)
    except Exception:
        # ignore edit errors (e.g., message deleted)
        pass

    if current == total and msg_id in _last_update:
        _last_update.pop(msg_id, None)
