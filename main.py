import os
import asyncio
import re
import tempfile
import logging
import sys
from datetime import datetime
from typing import List, Dict, Optional
from contextlib import asynccontextmanager

# Pyromod is required for client.ask
from pyromod import listen

from pyrogram import Client, filters, idle
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.enums import UserStatus, ChatMemberStatus, ParseMode
from pyrogram.errors import (
    FloodWait, PeerFlood, UserPrivacyRestricted,
    UserChannelsTooMuch, ChatAdminRequired, UserNotParticipant,
    UsernameNotOccupied, ChatIdInvalid, PeerIdInvalid
)
import motor.motor_asyncio
from motor.motor_asyncio import AsyncIOMotorClient

# --------------------------------------------------------------
# Logging Configuration
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
# MongoDB Setup with fallback and retry
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
            mongo_client = AsyncIOMotorClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
            # Ping the database to verify connection
            await mongo_client.admin.command('ping')
            db = mongo_client["member_adder_bot"]
            sessions_col = db["sessions"]
            admins_col = db["admins"]
            logger.info("MongoDB connected successfully.")
            print("✅ MongoDB connected successfully.")
            return
        except Exception as e:
            logger.error(f"MongoDB connection attempt {attempt+1} failed: {e}")
            print(f"❌ MongoDB connection attempt {attempt+1} failed: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay * (attempt + 1))
            else:
                logger.critical("Failed to connect to MongoDB after all retries. Bot will run with limited functionality.")
                print("⚠️  WARNING: MongoDB unavailable. Sessions and admins will NOT be persistent!")
                mongo_client = None
                db = None
                sessions_col = None
                admins_col = None

# --------------------------------------------------------------
# Pyrogram Bot Client
# --------------------------------------------------------------
bot = Client(
    "member_adder_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    plugins=[]  # no external plugins
)

# --------------------------------------------------------------
# Helper Functions with Fallbacks and Logging
# --------------------------------------------------------------
async def is_admin(user_id: int) -> bool:
    """Check if user is owner or in admin list."""
    if user_id == OWNER_ID:
        logger.info(f"User {user_id} is owner, admin access granted.")
        return True
    if not admins_col:
        logger.error("Admin collection not available (MongoDB down).")
        return False
    try:
        admin = await admins_col.find_one({"user_id": user_id})
        if admin:
            logger.info(f"User {user_id} is admin.")
            return True
        else:
            logger.info(f"User {user_id} is not admin.")
            return False
    except Exception as e:
        logger.error(f"Error checking admin status for {user_id}: {e}")
        return False

async def get_all_sessions() -> List[Dict]:
    """Retrieve all stored session strings."""
    if not sessions_col:
        logger.error("Sessions collection not available (MongoDB down).")
        return []
    try:
        cursor = sessions_col.find({})
        result = await cursor.to_list(length=None)
        logger.info(f"Retrieved {len(result)} sessions from DB.")
        return result
    except Exception as e:
        logger.error(f"Error getting all sessions: {e}")
        return []

async def get_session_names() -> List[str]:
    """Return only names of sessions."""
    if not sessions_col:
        return []
    try:
        cursor = sessions_col.find({}, {"name": 1, "_id": 0})
        docs = await cursor.to_list(length=None)
        names = [doc["name"] for doc in docs]
        logger.info(f"Retrieved session names: {names}")
        return names
    except Exception as e:
        logger.error(f"Error getting session names: {e}")
        return []

async def add_session(name: str, session_string: str):
    """Insert or update a session by name."""
    if not sessions_col:
        logger.error("Cannot add session: MongoDB not available.")
        return
    try:
        await sessions_col.update_one(
            {"name": name},
            {"$set": {"session_string": session_string}},
            upsert=True
        )
        logger.info(f"Session '{name}' added/updated.")
    except Exception as e:
        logger.error(f"Error adding session '{name}': {e}")

async def remove_session(name: str):
    """Delete a session by name."""
    if not sessions_col:
        logger.error("Cannot remove session: MongoDB not available.")
        return
    try:
        await sessions_col.delete_one({"name": name})
        logger.info(f"Session '{name}' removed.")
    except Exception as e:
        logger.error(f"Error removing session '{name}': {e}")

async def remove_all_sessions():
    """Delete all sessions."""
    if not sessions_col:
        logger.error("Cannot remove all sessions: MongoDB not available.")
        return
    try:
        await sessions_col.delete_many({})
        logger.info("All sessions removed.")
    except Exception as e:
        logger.error(f"Error removing all sessions: {e}")

async def get_session_string(name: str) -> Optional[str]:
    """Retrieve session string by name."""
    if not sessions_col:
        logger.error("Cannot get session: MongoDB not available.")
        return None
    try:
        doc = await sessions_col.find_one({"name": name})
        if doc:
            return doc["session_string"]
        return None
    except Exception as e:
        logger.error(f"Error getting session '{name}': {e}")
        return None

async def add_admin(user_id: int):
    """Add a user to admin list."""
    if not admins_col:
        logger.error("Cannot add admin: MongoDB not available.")
        return
    try:
        await admins_col.update_one(
            {"user_id": user_id},
            {"$set": {"user_id": user_id}},
            upsert=True
        )
        logger.info(f"Admin {user_id} added.")
    except Exception as e:
        logger.error(f"Error adding admin {user_id}: {e}")

async def remove_admin(user_id: int):
    """Remove a user from admin list."""
    if not admins_col:
        logger.error("Cannot remove admin: MongoDB not available.")
        return
    try:
        await admins_col.delete_one({"user_id": user_id})
        logger.info(f"Admin {user_id} removed.")
    except Exception as e:
        logger.error(f"Error removing admin {user_id}: {e}")

async def get_all_admins() -> List[int]:
    """Retrieve all admin user IDs."""
    if not admins_col:
        logger.error("Cannot get admins: MongoDB not available.")
        return []
    try:
        cursor = admins_col.find({}, {"user_id": 1, "_id": 0})
        docs = await cursor.to_list(length=None)
        admins = [doc["user_id"] for doc in docs]
        logger.info(f"Retrieved admins: {admins}")
        return admins
    except Exception as e:
        logger.error(f"Error getting admins: {e}")
        return []

@asynccontextmanager
async def user_client(session_string: str, name: str):
    """Context manager for a user client from session string with error handling."""
    client = Client(name, api_id=API_ID, api_hash=API_HASH, session_string=session_string, in_memory=True)
    try:
        await client.start()
        logger.info(f"User client '{name}' started.")
        yield client
    except Exception as e:
        logger.error(f"Failed to start user client '{name}': {e}")
        raise
    finally:
        try:
            await client.stop()
            logger.info(f"User client '{name}' stopped.")
        except Exception as e:
            logger.error(f"Error stopping user client '{name}': {e}")

def parse_group_identifier(text: str) -> List[str]:
    """Parse group IDs/links from user input. Accepts comma/newline separation."""
    # Remove common prefixes
    text = re.sub(r"https?://t\.me/\+?", "", text)
    text = re.sub(r"https?://t\.me/", "", text)
    # Split by commas or newlines
    parts = re.split(r"[\n,]+", text)
    # Strip whitespace and filter out empty
    parsed = [p.strip() for p in parts if p.strip()]
    logger.info(f"Parsed group identifiers: {parsed}")
    return parsed

async def scrape_members_from_group(client: Client, group: str) -> List[int]:
    """
    Scrape active members from a single group.
    Returns list of user IDs (active: ONLINE, RECENTLY, LAST_WEEK).
    """
    members = []
    try:
        logger.info(f"Starting scraping from group: {group}")
        async for member in client.get_chat_members(group):
            user = member.user
            if user.is_bot or user.is_deleted:
                continue
            if user.status in [UserStatus.ONLINE, UserStatus.RECENTLY, UserStatus.LAST_WEEK]:
                members.append(user.id)
        logger.info(f"Scraped {len(members)} members from {group}")
    except FloodWait as e:
        logger.warning(f"FloodWait on {group}: {e.value} seconds")
        await asyncio.sleep(e.value)
        # Retry once
        try:
            async for member in client.get_chat_members(group):
                # same logic
                user = member.user
                if user.is_bot or user.is_deleted:
                    continue
                if user.status in [UserStatus.ONLINE, UserStatus.RECENTLY, UserStatus.LAST_WEEK]:
                    members.append(user.id)
            logger.info(f"Scraped {len(members)} members from {group} after retry")
        except Exception as e2:
            logger.error(f"Retry failed for {group}: {e2}")
            raise
    except Exception as e:
        logger.error(f"Scrape error on {group}: {e}")
        raise
    return members

async def add_members_to_group(
    target_group: str,
    user_ids: List[int],
    progress_message: Message = None,
    log_callback=None
) -> Dict[str, int]:
    """
    Add members using multiple sessions.
    Each session adds up to LIMIT_PER_ACCOUNT members.
    Returns dict: {"total_added": int, "per_account": {name: count}, "failed": int, "total_members": int}
    """
    sessions = await get_all_sessions()
    if not sessions:
        logger.error("No session strings available for adding members.")
        raise ValueError("No session strings available.")

    total_added = 0
    failed = 0
    per_account = {}
    member_index = 0
    total_members = len(user_ids)

    logger.info(f"Starting add process to {target_group} with {len(sessions)} sessions, {total_members} members.")

    # Distribute members across accounts
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
                        logger.info(f"[{name}] Added {uid} to {target_group} (added {added_count}/{limit})")
                        await asyncio.sleep(5)  # delay to avoid flood
                    except UserPrivacyRestricted:
                        failed += 1
                        logger.warning(f"[{name}] Privacy restricted for {uid}")
                    except FloodWait as e:
                        logger.warning(f"[{name}] FloodWait: {e.value} seconds")
                        await asyncio.sleep(e.value + 1)
                        # retry same member
                        member_index -= 1
                    except PeerFlood:
                        logger.error(f"[{name}] PeerFlood - account likely banned from adding. Stopping this account.")
                        break
                    except Exception as e:
                        failed += 1
                        logger.error(f"[{name}] Unexpected error adding {uid}: {e}")

                    # Update progress every 5 adds or when account finishes
                    if added_count % 5 == 0 and progress_message:
                        percent = int((member_index / total_members) * 100) if total_members else 0
                        bar = "▓" * (percent // 10) + "░" * (10 - (percent // 10))
                        text = f"**Adding members...**\n`[{bar}]` {percent}%\nAdded: {total_added} | Failed: {failed}"
                        try:
                            await progress_message.edit_text(text)
                        except Exception as e:
                            logger.warning(f"Failed to update progress message: {e}")
        except Exception as e:
            logger.error(f"Session '{name}' failed completely: {e}")
            # Continue with next session

        per_account[name] = added_count
        logger.info(f"Session '{name}' added {added_count} members.")

    result = {
        "total_added": total_added,
        "per_account": per_account,
        "failed": failed,
        "total_members": total_members
    }
    logger.info(f"Adding completed. Total added: {total_added}, Failed: {failed}")
    return result

async def send_log_file(client: Client, chat_id: int, user_ids: List[int], prefix: str):
    """Create a .txt file with user IDs and send it to log group."""
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("\n".join(str(uid) for uid in user_ids))
            tmp_path = f.name
        caption = f"{prefix} - Total {len(user_ids)} members"
        await client.send_document(chat_id, document=tmp_path, caption=caption)
        logger.info(f"Log file sent to {chat_id}: {prefix} with {len(user_ids)} members.")
        os.unlink(tmp_path)
    except Exception as e:
        logger.error(f"Failed to send log file: {e}")

# --------------------------------------------------------------
# Help & Start Handlers
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

@bot.on_message(filters.command("start") & filters.private)
async def start_command(client: Client, message: Message):
    try:
        logger.info(f"Start command from {message.from_user.id}")
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
        logger.error(f"Error in start_command: {e}")

@bot.on_message(filters.command("help") & filters.private)
async def help_command(client: Client, message: Message):
    try:
        logger.info(f"Help command from {message.from_user.id}")
        text = await get_help_text(message.from_user.id)
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🏠 Start", callback_data="start")]
        ])
        await message.reply(text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Error in help_command: {e}")

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
        logger.error(f"Error in callback_query_handler: {e}")

# ---------- Session Management ----------
@bot.on_message(filters.command("addstring") & filters.private)
async def addstring_command(client: Client, message: Message):
    try:
        logger.info(f"addstring command from {message.from_user.id}")
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
        logger.error(f"Error in addstring_command: {e}")
        await message.reply("❌ An error occurred while adding session.")

@bot.on_message(filters.command("rmstring") & filters.private)
async def rmstring_command(client: Client, message: Message):
    try:
        logger.info(f"rmstring command from {message.from_user.id}")
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
        logger.error(f"Error in rmstring_command: {e}")
        await message.reply("❌ An error occurred while removing session.")

@bot.on_message(filters.command("liststring") & filters.private)
async def liststring_command(client: Client, message: Message):
    try:
        logger.info(f"liststring command from {message.from_user.id}")
        if not await is_admin(message.from_user.id):
            await message.reply("⛔ Admin only.")
            return
        names = await get_session_names()
        if not names:
            await message.reply("No sessions stored.")
        else:
            await message.reply("**Stored session names:**\n" + "\n".join(f"• `{n}`" for n in names))
    except Exception as e:
        logger.error(f"Error in liststring_command: {e}")
        await message.reply("❌ An error occurred while listing sessions.")

@bot.on_message(filters.command("getstring") & filters.private)
async def getstring_command(client: Client, message: Message):
    try:
        logger.info(f"getstring command from {message.from_user.id}")
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
        logger.error(f"Error in getstring_command: {e}")
        await message.reply("❌ An error occurred while retrieving sessions.")

@bot.on_message(filters.command("rmallstrings") & filters.private)
async def rmallstrings_command(client: Client, message: Message):
    try:
        logger.info(f"rmallstrings command from {message.from_user.id}")
        if not await is_admin(message.from_user.id):
            await message.reply("⛔ Admin only.")
            return
        await remove_all_sessions()
        await message.reply("✅ All sessions removed.")
    except Exception as e:
        logger.error(f"Error in rmallstrings_command: {e}")
        await message.reply("❌ An error occurred while removing all sessions.")

# ---------- Admin Management (Owner Only) ----------
@bot.on_message(filters.command("addadmin") & filters.private)
async def addadmin_command(client: Client, message: Message):
    try:
        logger.info(f"addadmin command from {message.from_user.id}")
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
        logger.error(f"Error in addadmin_command: {e}")
        await message.reply("❌ An error occurred while adding admin.")

@bot.on_message(filters.command("rmadmin") & filters.private)
async def rmadmin_command(client: Client, message: Message):
    try:
        logger.info(f"rmadmin command from {message.from_user.id}")
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
        logger.error(f"Error in rmadmin_command: {e}")
        await message.reply("❌ An error occurred while removing admin.")

@bot.on_message(filters.command("listadmins") & filters.private)
async def listadmins_command(client: Client, message: Message):
    try:
        logger.info(f"listadmins command from {message.from_user.id}")
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
        logger.error(f"Error in listadmins_command: {e}")
        await message.reply("❌ An error occurred while listing admins.")

# ---------- Scraping & Adding ----------
@bot.on_message(filters.command("scrab") & filters.private)
async def scrab_command(client: Client, message: Message):
    try:
        logger.info(f"scrab command from {message.from_user.id}")
        if not await is_admin(message.from_user.id):
            await message.reply("⛔ Admin only.")
            return

        # Step 1: ask for groups to scrape
        q = await message.reply("📥 Send the group IDs / usernames / invite links to scrape members from.\nSeparate multiple by new line or comma.")
        response = await client.ask(message.chat.id, filters.text, timeout=120)
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

        # Use first session for scraping, with fallback to next sessions if fails
        scraper_session = None
        for sess in sessions:
            try:
                async with user_client(sess["session_string"], f"scraper_{sess['name']}") as scraper:
                    scraper_session = scraper
                    logger.info(f"Using session {sess['name']} for scraping.")
                    break
            except Exception as e:
                logger.error(f"Failed to start scraper session {sess['name']}: {e}")
                continue
        if not scraper_session:
            await status_msg.edit_text("❌ Failed to start any user client for scraping.")
            return

        for group in group_list:
            try:
                members = await scrape_members_from_group(scraper_session, group)
                all_members.extend(members)
                await status_msg.edit_text(f"✅ Scraped {len(members)} from {group}\nTotal so far: {len(all_members)}")
                await asyncio.sleep(1)  # small delay between groups
            except Exception as e:
                failed_groups.append(f"{group} ({str(e)[:50]})")
                logger.error(f"Failed to scrape {group}: {e}")
                await status_msg.edit_text(f"⚠️ Failed on {group}\nContinuing...")

        # remove duplicates
        all_members = list(set(all_members))
        total_scraped = len(all_members)

        # send log file to LOG_GROUP
        await send_log_file(client, LOG_GROUP, all_members, f"Scraped members - {len(group_list)} groups")

        # Step 4: ask for target group
        await status_msg.edit_text("📤 Now send the **target group ID/username** to add these members.")
        target_resp = await client.ask(message.chat.id, filters.text, timeout=120)
        if not target_resp or not target_resp.text:
            await message.reply("❌ No target group received.")
            return
        target_group = target_resp.text.strip()

        # Step 5: add members
        progress = await message.reply("⏳ Starting to add members...")
        try:
            result = await add_members_to_group(target_group, all_members, progress_message=progress)
        except Exception as e:
            logger.error(f"Adding members failed: {e}")
            await progress.edit_text(f"❌ Adding failed: {e}")
            return

        # Step 6: generate result message
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

        # send result to user's PM and LOG_GROUP
        await client.send_message(message.from_user.id, result_text)
        await client.send_message(LOG_GROUP, result_text)
        logger.info(f"Scrab command completed for {message.from_user.id}")

    except asyncio.TimeoutError:
        logger.warning("Timeout in scrab command.")
        await message.reply("❌ Timeout. Please start again.")
    except Exception as e:
        logger.error(f"Unhandled error in scrab_command: {e}")
        await message.reply("❌ An unexpected error occurred. Check logs.")

@bot.on_message(filters.command("import") & filters.private)
async def import_command(client: Client, message: Message):
    try:
        logger.info(f"import command from {message.from_user.id}")
        if not await is_admin(message.from_user.id):
            await message.reply("⛔ Admin only.")
            return

        # Step 1: ask for .txt file
        q = await message.reply("📁 Send the `.txt` file containing user IDs (one per line).")
        response = await client.ask(message.chat.id, filters.document, timeout=120)
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
            logger.error(f"Error reading file: {e}")
            await message.reply(f"❌ Error reading file: {e}")
            return

        if not user_ids:
            await message.reply("❌ No valid user IDs found in file.")
            return

        # Step 2: ask for target group
        await message.reply(f"✅ Loaded {len(user_ids)} user IDs.\nNow send the **target group ID/username**.")
        target_resp = await client.ask(message.chat.id, filters.text, timeout=120)
        if not target_resp or not target_resp.text:
            await message.reply("❌ No target group received.")
            return
        target_group = target_resp.text.strip()

        # Step 3: add members
        progress = await message.reply("⏳ Adding members...")
        try:
            result = await add_members_to_group(target_group, user_ids, progress_message=progress)
        except Exception as e:
            logger.error(f"Adding members failed: {e}")
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
        logger.info(f"Import command completed for {message.from_user.id}")

    except asyncio.TimeoutError:
        logger.warning("Timeout in import command.")
        await message.reply("❌ Timeout. Please start again.")
    except Exception as e:
        logger.error(f"Unhandled error in import_command: {e}")
        await message.reply("❌ An unexpected error occurred. Check logs.")

# --------------------------------------------------------------
# Main entry with error handling and logging
# --------------------------------------------------------------
async def main():
    print("🚀 Starting Member Adder Bot...")
    logger.info("Initializing MongoDB...")
    await init_mongodb()
    
    print("🤖 Starting bot client...")
    logger.info("Starting bot client...")
    try:
        await bot.start()
        print("✅ Bot started successfully.")
        logger.info("Bot started successfully.")
    except Exception as e:
        logger.critical(f"Failed to start bot: {e}")
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
        logger.error(f"Error during idle: {e}")
    finally:
        try:
            await bot.stop()
            logger.info("Bot stopped.")
            print("✅ Bot stopped.")
        except Exception as e:
            logger.error(f"Error stopping bot: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Shutdown by user.")
    except Exception as e:
        logger.critical(f"Unhandled exception in main: {e}", exc_info=True)
        print(f"❌ Fatal error: {e}")
