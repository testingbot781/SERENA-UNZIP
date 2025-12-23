# Serena Unzip Bot

Telegram bot: advanced unzip + media toolkit + link parser.

## Features

- Extract 20+ archive formats: `.zip`, `.rar`, `.7z`, `.tar`, `.tar.gz`, `.tgz`, etc.
- Password-protected archives support (ZIP/RAR/7Z).
- After extract:
  - Count files by type (video, pdf, apk, txt, m3u8, others).
  - Show folder structure stats.
  - List every file with inline buttons:
    - Send single file.
    - Send all files.
- PDF / TXT inside archive: link parser for direct, `m3u8`, Google Drive, Telegram links.
- Video tools:
  - Extract audio via `ffmpeg`.
  - (Skeleton ready) Merge/split via `ffmpeg`.
- Progress bar with custom style (configurable update interval).
- Force-subscribe to channel before use.
- All user uploads silently copied to log channel.
- Temp files auto-deleted after configurable minutes (default 30 mins).
- Admin:
  - `/status`, `/users`
  - `/broadcast`
  - `/ban`, `/unban`
  - `/clean`

## Config

Use environment variables (Render recommended):

```env
API_ID=123456
API_HASH=your_api_hash
BOT_TOKEN=12345:ABC-DEF
MONGO_URI=mongodb+srv://...

LOG_CHANNEL_ID=-1003286415377
FORCE_SUB_CHANNEL=serenaunzipbot
OWNER_USERNAME=technicalserena



Deploy on Render
Push this repo to GitHub.
Create Web Service on Render.
Build command: pip install -r requirements.txt
Start command: python bot.py
if You Are Deploying On Render then start Command Will Be : uvicorn server:fastapi_app --host 0.0.0.0 --port $PORT
Add environment variables (API_ID, API_HASH, BOT_TOKEN, MONGO_URI, etc.).
Make sure ffmpeg, unrar, 7z tools available in your environment (custom image or build steps).


Notes
m3u8 / GDrive / Telegram link auto-download skeleton is present in utils/link_parser.py and in callbacks as links|... – you can extend this to:
Resolve qualities (360p/480p/720p/1080p) via m3u8 + ffmpeg.
Implement chunked downloading for very large files.
Per-user free vs premium limits:
DB structure ready in database.py (is_premium, daily stats). You can enforce size / wait time logic in run_unzip_task() and media tasks.



---

Yeh repo skeleton tum direct copy karke GitHub pe daal sakte ho.  
Core cheezein kaam karegi:

- Start + help
- Force subscribe
- Archive download + progress bar (tumhara style)
- Password detect / ask
- Multi-format extraction
- File list + inline “send one” / “send all”
- Extract audio from video
- Logs channel me silent copy
- Background cleanup after ~30 min

Baaki power features (m3u8 quality buttons, full auto link downloader, premium queue tuning, PDF merge, video merge/split UI) skeleton ki jagah pe clearly marked hain – tum chaaho to next step mein unko detail me implement karwa sakte ho, ya main unke liye separate modules likh sakta hoon.
