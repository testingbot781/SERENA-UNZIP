import datetime
from typing import Optional, Dict, Any

from motor.motor_asyncio import AsyncIOMotorClient

from config import Config

client = AsyncIOMotorClient(Config.MONGO_URI)
db = client[Config.DB_NAME]

users_col = db["users"]
files_col = db["temp_files"]  # for cleanup scheduler


async def get_or_create_user(user_id: int) -> Dict[str, Any]:
    user = await users_col.find_one({"_id": user_id})
    if not user:
        user = {
            "_id": user_id,
            "is_premium": False,
            "is_banned": False,
            "settings": {
                "auto_delete_min": Config.AUTO_DELETE_DEFAULT_MIN,
                "lang": "en",
                "default_extract_mode": "full",  # full/single
                "preferred_output": "file",     # file/link
            },
            "stats": {
                "last_reset": datetime.date.today().isoformat(),
                "daily_tasks": 0,
                "daily_size_mb": 0.0,
                "last_task_ts": None,
            },
        }
        await users_col.insert_one(user)
    else:
        # reset daily stats if date changed
        today = datetime.date.today().isoformat()
        if user["stats"].get("last_reset") != today:
            await users_col.update_one(
                {"_id": user_id},
                {"$set": {
                    "stats.last_reset": today,
                    "stats.daily_tasks": 0,
                    "stats.daily_size_mb": 0.0
                }}
            )
            user["stats"]["last_reset"] = today
            user["stats"]["daily_tasks"] = 0
            user["stats"]["daily_size_mb"] = 0.0
    return user


async def update_user_stats(user_id: int, size_mb: float):
    await users_col.update_one(
        {"_id": user_id},
        {
            "$inc": {
                "stats.daily_tasks": 1,
                "stats.daily_size_mb": float(size_mb)
            },
            "$set": {
                "stats.last_task_ts": datetime.datetime.utcnow()
            }
        }
    )


async def set_premium(user_id: int, value: bool = True):
    await users_col.update_one(
        {"_id": user_id},
        {"$set": {"is_premium": value}},
        upsert=True
    )


async def set_ban(user_id: int, value: bool = True):
    await users_col.update_one(
        {"_id": user_id},
        {"$set": {"is_banned": value}},
        upsert=True
    )


async def is_banned(user_id: int) -> bool:
    u = await users_col.find_one({"_id": user_id}, {"is_banned": 1})
    return bool(u and u.get("is_banned"))


async def get_all_users():
    cursor = users_col.find({}, {"_id": 1})
    return [doc["_id"] async for doc in cursor]


async def count_users():
    total = await users_col.count_documents({})
    premium = await users_col.count_documents({"is_premium": True})
    banned = await users_col.count_documents({"is_banned": True})
    return total, premium, banned


async def register_temp_path(user_id: int, path: str, ttl_min: int):
    """Register temp file/dir for cleanup."""
    await files_col.insert_one({
        "user_id": user_id,
        "path": path,
        "created_at": datetime.datetime.utcnow(),
        "ttl_min": ttl_min
    })


async def get_expired_temp_paths(now: Optional[datetime.datetime] = None):
    if now is None:
        now = datetime.datetime.utcnow()
    cursor = files_col.find({})
    expired_ids = []
    expired_paths = []
    async for doc in cursor:
        created = doc["created_at"]
        ttl_min = doc["ttl_min"]
        if created + datetime.timedelta(minutes=ttl_min) <= now:
            expired_ids.append(doc["_id"])
            expired_paths.append(doc["path"])
    if expired_ids:
        await files_col.delete_many({"_id": {"$in": expired_ids}})
    return expired_paths
