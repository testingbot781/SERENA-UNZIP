# server.py
import asyncio

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from bot import app as tg_app  # pyrogram Client
from utils.cleanup import cleanup_worker


fastapi_app = FastAPI(title="Serena Unzip Web Service")


@fastapi_app.on_event("startup")
async def on_startup():
    # start background cleanup worker
    asyncio.create_task(cleanup_worker())

    # start Telegram bot client
    await tg_app.start()
    print("Serena Unzip bot started (web service mode)")


@fastapi_app.on_event("shutdown")
async def on_shutdown():
    # stop Telegram bot client
    await tg_app.stop()
    print("Serena Unzip bot stopped")


@fastapi_app.get("/", response_class=PlainTextResponse)
async def root():
    return "Serena Unzip Bot is running ✅"
