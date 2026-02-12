import os
import asyncio
import re
import tempfile
import logging
import sys
import traceback
from datetime import datetime
from typing import List, Dict, Optional
from contextlib import asynccontextmanager

# Pyromod is required for client.ask
from pyromod import listen

from pyrogram import Client, filters, idle
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.enums import UserStatus, ParseMode
from pyrogram.errors import (
    FloodWait, PeerFlood, UserPrivacyRestricted,
    UserChannelsTooMuch, ChatAdminRequired, UserNotParticipant,
    UsernameNotOccupied, ChatIdInvalid, PeerIdInvalid, ApiIdInvalid,
    AccessTokenInvalid, PhoneNumberInvalid, SessionPasswordNeeded,
    AuthKeyUnregistered, UsernameInvalid
)
import motor.motor_asyncio
from motor.motor_asyncio import AsyncIOMotorClient

# --------------------------------------------------------------
# LOGGING CONFIGURATION (everything logged to both console and file)
# --------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('bot.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# --------------------------------------------------------------
# ENVIRONMENT VARIABLES (Hardcoded defaults as requested)
# --------------------------------------------------------------
API_ID = int(os.getenv("API_ID", "29113757"))
API_HASH = os.getenv("API_HASH", "4fb029c4a5d6beb7b6c8c0616c840939")
BOT_TOKEN = os.getenv("BOT_TOKEN", "8244250546:AAGcgXiYkBOLdmuBhZoc1t9OU0bi-g0tk04")
OWNER_ID = int(os.getenv("OWNER_ID", "6773435708"))
LOG_GROUP = int(os.getenv("LOG_GROUP", "-1002275616383"))
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb+srv://iamnobita1:nobitamusic1@cluster0.k08op.mongodb.net/?retryWrites=true&w=majority")
LIMIT_PER_ACCOUNT = int(os.getenv("LIMIT_PER_ACCOUNT", "45"))

# --------------------------------------------------------------
# MONGO DB SETUP WITH FALLBACK & RETRY
# --------------------------------------------------------------
mongo_client = None
db = None
sessions_col = None
admins_col = None

async def init_mongodb():
    """Initialize MongoDB connection with retry and fallback."""
    global mongo_client, db, sessions_col, admins_col
    max_retries = 3
    retry_delay = 2
    for attempt in range(max_retries):
        try:
            logger.info(f"MongoDB connection attempt {attempt+1}/{max_retries}")
            mongo_client = AsyncIOMotorClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
            # Ping to verify connection
            await mongo_client.admin.command('ping')
            db = mongo_client["member_adder_bot"]
            sessions_col = db["sessions"]
            admins_col = db["admins"]
            logger.info("✅ MongoDB connected successfully.")
            print("✅ MongoDB connected successfully.")
            return
        except Exception as e:
            logger.error(f"❌ MongoDB connection attempt {attempt+1} failed: {e}")
            print(f"❌ MongoDB connection attempt {attempt+1} failed: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay * (attempt + 1))
            else:
                logger.critical("⚠️  Failed to connect to MongoDB after all retries. Bot will run with LIMITED functionality (sessions/admins will NOT be persistent).")
                print("⚠️  WARNING: MongoDB unavailable. Sessions and admins will NOT be persistent!")
                mongo_client = None
                db = None
                sessions_col = None
                admins_col = None

# --------------------------------------------------------------
# PYROGRAM BOT CLIENT
# --------------------------------------------------------------
bot = Client(
    "member_adder_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    plugins=[]  # no external plugins
)

# --------------------------------------------------------------
# CRITICAL: Initialize pyromod listen to enable client.ask()
# --------------------------------------------------------------
listen(bot)

# --------------------------------------------------------------
# HELPER FUNCTIONS (with exhaustive error handling & logging)
# --------------------------------------------------------------

async def is_admin(user_id: int) -> bool:
    """Check if user is owner or in admin list. Logs everything."""
    try:
        if user_id == OWNER_ID:
            logger.info(f"Admin check for {user_id}: ✅ Owner")
            return True
        if not admins_col:
            logger.error(f"Admin check for {user_id}: ❌ Admins collection not available (MongoDB down).")
            return False
        admin = await admins_col.find_one({"user_id": user_id})
        if admin:
            logger.info(f"Admin check for {user_id}: ✅ Admin")
            return True
        else:
            logger.info(f"Admin check for {user_id}: ❌ Not admin")
            return False
    except Exception as e:
        logger.error(f"Admin check for {user_id}: ❌ Exception - {e}")
        return False

async def get_all_sessions() -> List[Dict]:
    """Retrieve all stored session strings."""
    if not sessions_col:
        logger.error("get_all_sessions: Sessions collection not available (MongoDB down).")
        return []
    try:
        cursor = sessions_col.find({})
        result = await cursor.to_list(length=None)
        logger.info(f"get_all_sessions: Retrieved {len(result)} sessions.")
        return result
    except Exception as e:
        logger.error(f"get_all_sessions: Error - {e}")
        return []

async def get_session_names() -> List[str]:
    """Return only names of sessions."""
    if not sessions_col:
        return []
    try:
        cursor = sessions_col.find({}, {"name": 1, "_id": 0})
        docs = await cursor.to_list(length=None)
        names = [doc["name"] for doc in docs]
        logger.info(f"get_session_names: {names}")
        return names
    except Exception as e:
        logger.error(f"get_session_names: Error - {e}")
        return []

async def add_session(name: str, session_string: str):
    """Insert or update a session by name."""
    if not sessions_col:
        logger.error(f"add_session({name}): Cannot add session - MongoDB not available.")
        return
    try:
        await sessions_col.update_one(
            {"name": name},
            {"$set": {"session_string": session_string}},
            upsert=True
        )
        logger.info(f"add_session({name}): ✅ Session added/updated.")
    except Exception as e:
        logger.error(f"add_session({name}): ❌ Error - {e}")

async def remove_session(name: str):
    """Delete a session by name."""
    if not sessions_col:
        logger.error(f"remove_session({name}): Cannot remove - MongoDB not available.")
        return
    try:
        await sessions_col.delete_one({"name": name})
        logger.info(f"remove_session({name}): ✅ Session removed.")
    except Exception as e:
        logger.error(f"remove_session({name}): ❌ Error - {e}")

async def remove_all_sessions():
    """Delete all sessions."""
    if not sessions_col:
        logger.error("remove_all_sessions: Cannot remove - MongoDB not available.")
        return
    try:
        await sessions_col.delete_many({})
        logger.info("remove_all_sessions: ✅ All sessions removed.")
    except Exception as e:
        logger.error(f"remove_all_sessions: ❌ Error - {e}")

async def get_session_string(name: str) -> Optional[str]:
    """Retrieve session string by name."""
    if not sessions_col:
        logger.error(f"get_session_string({name}): MongoDB not available.")
        return None
    try:
        doc = await sessions_col.find_one({"name": name})
        if doc:
            return doc["session_string"]
        return None
    except Exception as e:
        logger.error(f"get_session_string({name}): Error - {e}")
        return None

async def add_admin(user_id: int):
    """Add a user to admin list."""
    if not admins_col:
        logger.error(f"add_admin({user_id}): Cannot add - MongoDB not available.")
        return
    try:
        await admins_col.update_one(
            {"user_id": user_id},
            {"$set": {"user_id": user_id}},
            upsert=True
        )
        logger.info(f"add_admin({user_id}): ✅ Admin added.")
    except Exception as e:
        logger.error(f"add_admin({user_id}): ❌ Error - {e}")

async def remove_admin(user_id: int):
    """Remove a user from admin list."""
    if not admins_col:
        logger.error(f"remove_admin({user_id}): Cannot remove - MongoDB not available.")
        return
    try:
        await admins_col.delete_one({"user_id": user_id})
        logger.info(f"remove_admin({user_id}): ✅ Admin removed.")
    except Exception as e:
        logger.error(f"remove_admin({user_id}): ❌ Error - {e}")

async def get_all_admins() -> List[int]:
    """Retrieve all admin user IDs."""
    if not admins_col:
        logger.error("get_all_admins: MongoDB not available.")
        return []
    try:
        cursor = admins_col.find({}, {"user_id": 1, "_id": 0})
        docs = await cursor.to_list(length=None)
        admins = [doc["user_id"] for doc in docs]
        logger.info(f"get_all_admins: {admins}")
        return admins
    except Exception as e:
        logger.error(f"get_all_admins: Error - {e}")
        return []

@asynccontextmanager
async def user_client(session_string: str, name: str):
    """Context manager for a user client with robust error handling."""
    client = Client(name, api_id=API_ID, api_hash=API_HASH, session_string=session_string, in_memory=True)
    started = False
    try:
        logger.info(f"user_client({name}): Starting...")
        await client.start()
        started = True
        logger.info(f"user_client({name}): ✅ Started")
        yield client
    except (ApiIdInvalid, AccessTokenInvalid, PhoneNumberInvalid, SessionPasswordNeeded, AuthKeyUnregistered) as e:
        logger.error(f"user_client({name}): ❌ Auth error - {e}. Session string may be invalid.")
        raise
    except ConnectionError as e:
        logger.error(f"user_client({name}): ❌ Connection error - {e}")
        raise
    except Exception as e:
        logger.error(f"user_client({name}): ❌ Unexpected error - {e}\n{traceback.format_exc()}")
        raise
    finally:
        if started:
            try:
                await client.stop()
                logger.info(f"user_client({name}): ✅ Stopped")
            except Exception as e:
                logger.error(f"user_client({name}): ❌ Error stopping - {e}")

def parse_group_identifier(text: str) -> List[str]:
    """Parse group IDs/links from user input. Accepts comma/newline separation."""
    try:
        # Remove common prefixes
        text = re.sub(r"https?://t\.me/\+?", "", text)
        text = re.sub(r"https?://t\.me/", "", text)
        # Split by commas or newlines
        parts = re.split(r"[\n,]+", text)
        # Strip whitespace and filter out empty
        parsed = [p.strip() for p in parts if p.strip()]
        logger.info(f"parse_group_identifier: Input '{text[:50]}...' -> {parsed}")
        return parsed
    except Exception as e:
        logger.error(f"parse_group_identifier: Error - {e}")
        return []

async def scrape_members_from_group(client: Client, group: str) -> List[int]:
    """
    Scrape active members from a single group.
    Returns list of user IDs (active: ONLINE, RECENTLY, LAST_WEEK).
    """
    members = []
    try:
        logger.info(f"scrape_members: Starting group '{group}'")
        async for member in client.get_chat_members(group):
            user = member.user
            if user.is_bot or user.is_deleted:
                continue
            if user.status in [UserStatus.ONLINE, UserStatus.RECENTLY, UserStatus.LAST_WEEK]:
                members.append(user.id)
        logger.info(f"scrape_members: ✅ Group '{group}' - scraped {len(members)} members.")
    except FloodWait as e:
        logger.warning(f"scrape_members: FloodWait on '{group}' - {e.value}s")
        await asyncio.sleep(e.value)
        # Retry once
        try:
            async for member in client.get_chat_members(group):
                user = member.user
                if user.is_bot or user.is_deleted:
                    continue
                if user.status in [UserStatus.ONLINE, UserStatus.RECENTLY, UserStatus.LAST_WEEK]:
                    members.append(user.id)
            logger.info(f"scrape_members: ✅ Group '{group}' (after retry) - scraped {len(members)} members.")
        except Exception as e2:
            logger.error(f"scrape_members: ❌ Retry failed for '{group}': {e2}")
            raise
    except UsernameNotOccupied:
        logger.error(f"scrape_members: ❌ Username not occupied: '{group}'")
        raise
    except ChatAdminRequired:
        logger.error(f"scrape_members: ❌ Chat admin required (not enough permissions): '{group}'")
        raise
    except UserNotParticipant:
        logger.error(f"scrape_members: ❌ User not participant (account not in group): '{group}'")
        raise
    except ChatIdInvalid:
        logger.error(f"scrape_members: ❌ Invalid chat ID/username: '{group}'")
        raise
    except Exception as e:
        logger.error(f"scrape_members: ❌ Unexpected error on '{group}': {e}\n{traceback.format_exc()}")
        raise
    return members

async def add_members_to_group(
    target_group: str,
    user_ids: List[int],
    progress_message: Message = None,
) -> Dict[str, int]:
    """
    Add members using multiple sessions.
    Each session adds up to LIMIT_PER_ACCOUNT members.
    Returns dict: {"total_added": int, "per_account": {name: count}, "failed": int, "total_members": int}
    """
    sessions = await get_all_sessions()
    if not sessions:
        logger.error("add_members_to_group: No session strings available.")
        raise ValueError("No session strings available.")

    total_added = 0
    failed = 0
    per_account = {}
    member_index = 0
    total_members = len(user_ids)

    logger.info(f"add_members_to_group: Target={target_group}, total_members={total_members}, accounts={len(sessions)}")

    for sess in sessions:
        name = sess["name"]
        session_string = sess["session_string"]
        added_count = 0
        limit = LIMIT_PER_ACCOUNT

        try:
            async with user_client(session_string, f"adder_{name}") as acc:
                while added_count < limit and member_index < total_members:
                    uid = user_ids[member_index]
                    member_index += 1

                    try:
                        await acc.add_chat_members(target_group, uid)
                        added_count += 1
                        total_added += 1
                        logger.info(f"[{name}] ✅ Added {uid} to {target_group} ({added_count}/{limit})")
                        await asyncio.sleep(5)  # delay to avoid flood
                    except UserPrivacyRestricted:
                        failed += 1
                        logger.warning(f"[{name}] ⚠️ Privacy restricted for {uid}")
                    except FloodWait as e:
                        logger.warning(f"[{name}] ⚠️ FloodWait: {e.value}s")
                        await asyncio.sleep(e.value + 1)
                        member_index -= 1  # retry same member
                    except PeerFlood:
                        logger.error(f"[{name}] ❌ PeerFlood - account banned from adding. Stopping this account.")
                        break
                    except (ChatAdminRequired, UserNotParticipant) as e:
                        failed += 1
                        logger.error(f"[{name}] ❌ Permissions error adding {uid}: {e}")
                    except Exception as e:
                        failed += 1
                        logger.error(f"[{name}] ❌ Unexpected error adding {uid}: {e}")

                    # Update progress every 5 adds
                    if added_count % 5 == 0 and progress_message:
                        percent = int((member_index / total_members) * 100) if total_members else 0
                        bar = "▓" * (percent // 10) + "░" * (10 - (percent // 10))
                        text = f"**Adding members...**\n`[{bar}]` {percent}%\nAdded: {total_added} | Failed: {failed}"
                        try:
                            await progress_message.edit_text(text)
                        except Exception as e:
                            logger.warning(f"[{name}] ⚠️ Failed to update progress message: {e}")
        except Exception as e:
            logger.error(f"add_members_to_group: Session '{name}' failed completely: {e}")

        per_account[name] = added_count
        logger.info(f"add_members_to_group: Session '{name}' added {added_count} members.")

    result = {
        "total_added": total_added,
        "per_account": per_account,
        "failed": failed,
        "total_members": total_members
    }
    logger.info(f"add_members_to_group: ✅ Completed. Total added: {total_added}, Failed: {failed}")
    return result

async def send_log_file(client: Client, chat_id: int, user_ids: List[int], prefix: str):
    """Create a .txt file with user IDs and send it to log group."""
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("\n".join(str(uid) for uid in user_ids))
            tmp_path = f.name
        caption = f"{prefix} - Total {len(user_ids)} members"
        await client.send_document(chat_id, document=tmp_path, caption=caption)
        logger.info(f"send_log_file: ✅ Sent to {chat_id} - {caption}")
    except Exception as e:
        logger.error(f"send_log_file: ❌ Failed - {e}")
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
                logger.info(f"send_log_file: Deleted temp file {tmp_path}")
            except Exception as e:
                logger.error(f"send_log_file: ❌ Failed to delete temp file {tmp_path}: {e}")

# --------------------------------------------------------------
# HELP & START HANDLERS
# --------------------------------------------------------------

async def get_help_text(user_id: int) -> str:
    """Generate help message with all commands and descriptions."""
    is_owner = user_id == OWNER_ID
    is_administrator = await is_admin(user_id)

    text = "**🤖 Member Adder Bot Help**\n\n"
    text += "**Public Commands:**\n"
    text += "• /start - Start the bot\n"
    text += "• /help - Show this help message\n\n"

    text += "**👑 Owner Commands:**\n"
    text += "• /addadmin <user_id> - Add a user as admin\n"
    text += "• /rmadmin <user_id> - Remove an admin\n"
    if is_owner:
        text += "  _(You have owner access)_\n"
    text += "\n"

    text += "**🛠️ Admin Commands:**\n"
    text += "• /addstring <Name> <SessionString> - Add a user session\n"
    text += "• /rmstring <Name> - Remove a session by name\n"
    text += "• /liststring - List all session names\n"
    text += "• /getstring - Get all session strings (first 50 chars)\n"
    text += "• /rmallstrings - Remove all sessions\n"
    text += "• /listadmins - List all admins\n"
    text += "• /scrab - Scrape members from groups and add to target\n"
    text += "• /import - Import user IDs from .txt and add to target\n"
    if is_administrator:
        text += "  _(You have admin access)_\n"
    else:
        text += "  _(Admin only - you don't have access)_\n"

    text += "\n**📌 Note:**\n"
    text += "• All admin commands require you to be added as an admin by the owner.\n"
    text += "• The owner is always an admin and can manage other admins.\n"
    return text

# ---------- COMMAND HANDLERS (now work in both private and group chats) ----------
@bot.on_message(filters.command("start"))
async def start_command(client: Client, message: Message):
    try:
        logger.info(f"Command /start from {message.from_user.id} in chat {message.chat.id}")
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📘 Help", callback_data="help")],
            [InlineKeyboardButton("👤 Owner", url=f"tg://user?id={OWNER_ID}")]
        ])
        await message.reply(
            "✅ **Member Adder Bot is running.**\n"
            "Use /scrab or /import to add members (admin only).\n"
            "Click the button below for help.",
            reply_markup=keyboard
        )
    except Exception as e:
        logger.error(f"start_command: ❌ {e}\n{traceback.format_exc()}")

@bot.on_message(filters.command("help"))
async def help_command(client: Client, message: Message):
    try:
        logger.info(f"Command /help from {message.from_user.id} in chat {message.chat.id}")
        text = await get_help_text(message.from_user.id)
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🏠 Start", callback_data="start")]
        ])
        await message.reply(text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"help_command: ❌ {e}\n{traceback.format_exc()}")

@bot.on_callback_query()
async def callback_query_handler(client: Client, query: CallbackQuery):
    try:
        logger.info(f"Callback query from {query.from_user.id}: {query.data}")
        if query.data == "help":
            text = await get_help_text(query.from_user.id)
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🏠 Start", callback_data="start")]
            ])
            await query.message.edit_text(text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)
        elif query.data == "start":
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("📘 Help", callback_data="help")],
                [InlineKeyboardButton("👤 Owner", url=f"tg://user?id={OWNER_ID}")]
            ])
            await query.message.edit_text(
                "✅ **Member Adder Bot is running.**\n"
                "Use /scrab or /import to add members (admin only).\n"
                "Click the button below for help.",
                reply_markup=keyboard
            )
        await query.answer()
    except Exception as e:
        logger.error(f"callback_query_handler: ❌ {e}\n{traceback.format_exc()}")

# ---------- SESSION MANAGEMENT ----------
@bot.on_message(filters.command("addstring"))
async def addstring_command(client: Client, message: Message):
    try:
        logger.info(f"Command /addstring from {message.from_user.id} in chat {message.chat.id}")
        if not await is_admin(message.from_user.id):
            await message.reply("⛔ Admin only.")
            return
        parts = message.text.split(maxsplit=2)
        if len(parts) != 3:
            await message.reply("Usage: `/addstring Name SessionString`", parse_mode=ParseMode.MARKDOWN)
            return
        name = parts[1]
        sess_str = parts[2]
        await add_session(name, sess_str)
        await message.reply(f"✅ Session `{name}` added/updated.")
    except Exception as e:
        logger.error(f"addstring_command: ❌ {e}\n{traceback.format_exc()}")
        await message.reply("❌ An error occurred while adding session.")

@bot.on_message(filters.command("rmstring"))
async def rmstring_command(client: Client, message: Message):
    try:
        logger.info(f"Command /rmstring from {message.from_user.id} in chat {message.chat.id}")
        if not await is_admin(message.from_user.id):
            await message.reply("⛔ Admin only.")
            return
        parts = message.text.split(maxsplit=1)
        if len(parts) != 2:
            await message.reply("Usage: `/rmstring Name`")
            return
        name = parts[1]
        await remove_session(name)
        await message.reply(f"✅ Session `{name}` removed.")
    except Exception as e:
        logger.error(f"rmstring_command: ❌ {e}\n{traceback.format_exc()}")
        await message.reply("❌ An error occurred while removing session.")

@bot.on_message(filters.command("liststring"))
async def liststring_command(client: Client, message: Message):
    try:
        logger.info(f"Command /liststring from {message.from_user.id} in chat {message.chat.id}")
        if not await is_admin(message.from_user.id):
            await message.reply("⛔ Admin only.")
            return
        names = await get_session_names()
        if not names:
            await message.reply("No sessions stored.")
        else:
            await message.reply("**Stored session names:**\n" + "\n".join(f"• `{n}`" for n in names))
    except Exception as e:
        logger.error(f"liststring_command: ❌ {e}\n{traceback.format_exc()}")
        await message.reply("❌ An error occurred while listing sessions.")

@bot.on_message(filters.command("getstring"))
async def getstring_command(client: Client, message: Message):
    try:
        logger.info(f"Command /getstring from {message.from_user.id} in chat {message.chat.id}")
        if not await is_admin(message.from_user.id):
            await message.reply("⛔ Admin only.")
            return
        sessions = await get_all_sessions()
        if not sessions:
            await message.reply("No sessions stored.")
            return
        text = "**Name → Session** (first 50 chars):\n"
        for s in sessions:
            name = s["name"]
            sess = s["session_string"][:50] + "..."
            text += f"• `{name}`: `{sess}`\n"
        await message.reply(text)
    except Exception as e:
        logger.error(f"getstring_command: ❌ {e}\n{traceback.format_exc()}")
        await message.reply("❌ An error occurred while retrieving sessions.")

@bot.on_message(filters.command("rmallstrings"))
async def rmallstrings_command(client: Client, message: Message):
    try:
        logger.info(f"Command /rmallstrings from {message.from_user.id} in chat {message.chat.id}")
        if not await is_admin(message.from_user.id):
            await message.reply("⛔ Admin only.")
            return
        await remove_all_sessions()
        await message.reply("✅ All sessions removed.")
    except Exception as e:
        logger.error(f"rmallstrings_command: ❌ {e}\n{traceback.format_exc()}")
        await message.reply("❌ An error occurred while removing all sessions.")

# ---------- ADMIN MANAGEMENT (OWNER ONLY) ----------
@bot.on_message(filters.command("addadmin"))
async def addadmin_command(client: Client, message: Message):
    try:
        logger.info(f"Command /addadmin from {message.from_user.id} in chat {message.chat.id}")
        if message.from_user.id != OWNER_ID:
            await message.reply("⛔ Owner only.")
            return
        parts = message.text.split()
        if len(parts) != 2:
            await message.reply("Usage: `/addadmin user_id`")
            return
        try:
            uid = int(parts[1])
        except ValueError:
            await message.reply("Invalid user ID.")
            return
        await add_admin(uid)
        await message.reply(f"✅ User {uid} added as admin.")
    except Exception as e:
        logger.error(f"addadmin_command: ❌ {e}\n{traceback.format_exc()}")
        await message.reply("❌ An error occurred while adding admin.")

@bot.on_message(filters.command("rmadmin"))
async def rmadmin_command(client: Client, message: Message):
    try:
        logger.info(f"Command /rmadmin from {message.from_user.id} in chat {message.chat.id}")
        if message.from_user.id != OWNER_ID:
            await message.reply("⛔ Owner only.")
            return
        parts = message.text.split()
        if len(parts) != 2:
            await message.reply("Usage: `/rmadmin user_id`")
            return
        try:
            uid = int(parts[1])
        except ValueError:
            await message.reply("Invalid user ID.")
            return
        if uid == OWNER_ID:
            await message.reply("❌ Cannot remove owner.")
            return
        await remove_admin(uid)
        await message.reply(f"✅ User {uid} removed from admins.")
    except Exception as e:
        logger.error(f"rmadmin_command: ❌ {e}\n{traceback.format_exc()}")
        await message.reply("❌ An error occurred while removing admin.")

@bot.on_message(filters.command("listadmins"))
async def listadmins_command(client: Client, message: Message):
    try:
        logger.info(f"Command /listadmins from {message.from_user.id} in chat {message.chat.id}")
        if not await is_admin(message.from_user.id):
            await message.reply("⛔ Admin only.")
            return
        admins = await get_all_admins()
        text = "**Admin list:**\n"
        text += f"• Owner: {OWNER_ID} (you)\n" if message.from_user.id == OWNER_ID else f"• Owner: {OWNER_ID}\n"
        for uid in admins:
            try:
                user = await client.get_users(uid)
                mention = user.mention
            except:
                mention = f"`{uid}`"
            text += f"• {mention}\n"
        await message.reply(text)
    except Exception as e:
        logger.error(f"listadmins_command: ❌ {e}\n{traceback.format_exc()}")
        await message.reply("❌ An error occurred while listing admins.")

# ---------- SCRAPING & ADDING ----------
@bot.on_message(filters.command("scrab"))
async def scrab_command(client: Client, message: Message):
    try:
        logger.info(f"Command /scrab from {message.from_user.id} in chat {message.chat.id}")
        if not await is_admin(message.from_user.id):
            await message.reply("⛔ Admin only.")
            return

        # Step 1: ask for groups to scrape
        q = await message.reply("📥 Send the group IDs / usernames / invite links to scrape members from.\nSeparate multiple by new line or comma.")
        response = await client.ask(message.chat.id, filters=filters.text, timeout=120)
        if not response or not response.text:
            await message.reply("❌ No input received.")
            return

        group_list = parse_group_identifier(response.text)
        if not group_list:
            await message.reply("❌ No valid group identifiers.")
            return

        # Step 2: check if we have at least one session
        sessions = await get_all_sessions()
        if not sessions:
            await message.reply("❌ No session strings available. Add one with /addstring")
            return

        # Step 3: scrape members
        status_msg = await message.reply("🔄 Scraping members... This may take a while.")
        all_members = []
        failed_groups = []

        # Try each session until one works
        scraper_session = None
        for sess in sessions:
            try:
                async with user_client(sess["session_string"], f"scraper_{sess['name']}") as scraper:
                    scraper_session = scraper
                    logger.info(f"scrab: Using session {sess['name']} for scraping.")
                    break
            except Exception as e:
                logger.error(f"scrab: Failed to start scraper session {sess['name']}: {e}")
                continue
        if not scraper_session:
            await status_msg.edit_text("❌ Failed to start any user client for scraping.")
            return

        for group in group_list:
            try:
                members = await scrape_members_from_group(scraper_session, group)
                all_members.extend(members)
                await status_msg.edit_text(f"✅ Scraped {len(members)} from {group}\nTotal so far: {len(all_members)}")
                await asyncio.sleep(1)
            except Exception as e:
                failed_groups.append(f"{group} ({str(e)[:50]})")
                logger.error(f"scrab: Failed to scrape {group}: {e}")
                await status_msg.edit_text(f"⚠️ Failed on {group}\nContinuing...")

        all_members = list(set(all_members))  # remove duplicates
        total_scraped = len(all_members)

        # Send log file
        await send_log_file(client, LOG_GROUP, all_members, f"Scraped members - {len(group_list)} groups")

        # Step 4: ask for target group
        await status_msg.edit_text("📤 Now send the **target group ID/username** to add these members.")
        target_resp = await client.ask(message.chat.id, filters=filters.text, timeout=120)
        if not target_resp or not target_resp.text:
            await message.reply("❌ No target group received.")
            return
        target_group = target_resp.text.strip()

        # Step 5: add members
        progress = await message.reply("⏳ Starting to add members...")
        try:
            result = await add_members_to_group(target_group, all_members, progress_message=progress)
        except Exception as e:
            logger.error(f"scrab: Adding members failed: {e}")
            await progress.edit_text(f"❌ Adding failed: {e}")
            return

        # Step 6: result message
        per_account_lines = "\n".join([f"  • {name}: {count}" for name, count in result["per_account"].items()])
        result_text = (
            f"✅ **Adding completed**\n"
            f"**Target Group:** `{target_group}`\n"
            f"**Total scraped:** {result['total_members']}\n"
            f"**Total added:** {result['total_added']}\n"
            f"**Failed (privacy/error):** {result['failed']}\n\n"
            f"**Per account:**\n{per_account_lines}"
        )
        await progress.edit_text(result_text)

        # Send result to user and LOG_GROUP
        await client.send_message(message.from_user.id, result_text)
        await client.send_message(LOG_GROUP, result_text)
        logger.info(f"scrab_command: ✅ Completed for {message.from_user.id}")

    except asyncio.TimeoutError:
        logger.warning("scrab_command: Timeout.")
        await message.reply("❌ Timeout. Please start again.")
    except Exception as e:
        logger.error(f"scrab_command: ❌ Unhandled error - {e}\n{traceback.format_exc()}")
        await message.reply("❌ An unexpected error occurred. Check logs.")

@bot.on_message(filters.command("import"))
async def import_command(client: Client, message: Message):
    try:
        logger.info(f"Command /import from {message.from_user.id} in chat {message.chat.id}")
        if not await is_admin(message.from_user.id):
            await message.reply("⛔ Admin only.")
            return

        # Step 1: ask for .txt file
        q = await message.reply("📁 Send the `.txt` file containing user IDs (one per line).")
        response = await client.ask(message.chat.id, filters=filters.document, timeout=120)
        if not response or not response.document:
            await message.reply("❌ No file received.")
            return
        file = await response.download()
        try:
            with open(file, "r") as f:
                lines = f.readlines()
            user_ids = []
            for line in lines:
                line = line.strip()
                if line and line.isdigit():
                    user_ids.append(int(line))
            os.unlink(file)
        except Exception as e:
            logger.error(f"import_command: Error reading file: {e}")
            await message.reply(f"❌ Error reading file: {e}")
            return

        if not user_ids:
            await message.reply("❌ No valid user IDs found in file.")
            return

        # Step 2: ask for target group
        await message.reply(f"✅ Loaded {len(user_ids)} user IDs.\nNow send the **target group ID/username**.")
        target_resp = await client.ask(message.chat.id, filters=filters.text, timeout=120)
        if not target_resp or not target_resp.text:
            await message.reply("❌ No target group received.")
            return
        target_group = target_resp.text.strip()

        # Step 3: add members
        progress = await message.reply("⏳ Adding members...")
        try:
            result = await add_members_to_group(target_group, user_ids, progress_message=progress)
        except Exception as e:
            logger.error(f"import_command: Adding members failed: {e}")
            await progress.edit_text(f"❌ Adding failed: {e}")
            return

        # Step 4: result
        per_account_lines = "\n".join([f"  • {name}: {count}" for name, count in result["per_account"].items()])
        result_text = (
            f"✅ **Import & Add completed**\n"
            f"**Target Group:** `{target_group}`\n"
            f"**Total in file:** {result['total_members']}\n"
            f"**Total added:** {result['total_added']}\n"
            f"**Failed (privacy/error):** {result['failed']}\n\n"
            f"**Per account:**\n{per_account_lines}"
        )
        await progress.edit_text(result_text)

        await client.send_message(message.from_user.id, result_text)
        await client.send_message(LOG_GROUP, result_text)
        logger.info(f"import_command: ✅ Completed for {message.from_user.id}")

    except asyncio.TimeoutError:
        logger.warning("import_command: Timeout.")
        await message.reply("❌ Timeout. Please start again.")
    except Exception as e:
        logger.error(f"import_command: ❌ Unhandled error - {e}\n{traceback.format_exc()}")
        await message.reply("❌ An unexpected error occurred. Check logs.")

# --------------------------------------------------------------
# MAIN ENTRY POINT
# --------------------------------------------------------------
async def main():
    print("🚀 Starting Member Adder Bot...")
    logger.info("Initializing MongoDB...")
    await init_mongodb()

    # Optional: Verify LOG_GROUP is reachable
    try:
        await bot.send_chat_action(LOG_GROUP, "typing")
        logger.info(f"✅ LOG_GROUP {LOG_GROUP} is reachable.")
    except Exception as e:
        logger.error(f"❌ LOG_GROUP {LOG_GROUP} is NOT reachable: {e}. Bot will still run but logs may fail.")
        print(f"⚠️  Warning: Cannot send to LOG_GROUP {LOG_GROUP}. Check if bot is admin there.")

    print("🤖 Starting bot client...")
    logger.info("Starting bot client...")
    try:
        await bot.start()
        print("✅ Bot started successfully.")
        logger.info("Bot started successfully.")
    except (ApiIdInvalid, AccessTokenInvalid) as e:
        logger.critical(f"❌ Bot failed to start - invalid API credentials: {e}")
        print(f"❌ Bot failed to start: {e}")
        return
    except Exception as e:
        logger.critical(f"❌ Bot failed to start: {e}\n{traceback.format_exc()}")
        print(f"❌ Bot failed to start: {e}")
        return

    print("📡 Bot is now idle. Press Ctrl+C to stop.")
    logger.info("Bot is idle.")
    try:
        await idle()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt.")
        print("\n🛑 Shutting down...")
    except Exception as e:
        logger.error(f"Error during idle: {e}\n{traceback.format_exc()}")
    finally:
        try:
            await bot.stop()
            logger.info("Bot stopped.")
            print("✅ Bot stopped.")
        except Exception as e:
            logger.error(f"Error stopping bot: {e}\n{traceback.format_exc()}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Shutdown by user.")
    except Exception as e:
        logger.critical(f"❌ Unhandled exception in main: {e}\n{traceback.format_exc()}")
        print(f"❌ Fatal error: {e}")
