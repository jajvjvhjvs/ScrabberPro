import os
import asyncio
import re
import tempfile
import logging
import traceback
from datetime import datetime
from typing import List, Dict, Optional
from contextlib import asynccontextmanager

# --------------------------------------------------------------
# 1. LOGGING SETUP (Prints everything to console)
# --------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("MemberAdderBot")

# --------------------------------------------------------------
# 2. IMPORTS & PYROMOD SETUP
# --------------------------------------------------------------
try:
    # Pyromod is required for client.ask – we also need to call listen()
    from pyromod import listen
    from pyrogram import Client, filters, idle
    from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
    from pyrogram.enums import UserStatus, ParseMode
    from pyrogram.errors import (
        FloodWait, PeerFlood, UserPrivacyRestricted,
        UserChannelsTooMuch, ChatAdminRequired, UserNotParticipant,
        UsernameNotOccupied, ChatIdInvalid, PeerIdInvalid,
        SessionPasswordNeeded, AuthKeyUnregistered, UserDeactivated
    )
    import motor.motor_asyncio
except ImportError as e:
    logger.critical(f"Missing requirements: {e}")
    print("Please run: pip install pyrogram tgcrypto motor pyromod")
    exit(1)

# --------------------------------------------------------------
# 3. ENVIRONMENT VARIABLES
# --------------------------------------------------------------
def get_env(name, default):
    val = os.getenv(name, default)
    logger.info(f"Loaded ENV {name}: {val if name != 'API_HASH' and name != 'BOT_TOKEN' else '********'}")
    return val

API_ID = int(get_env("API_ID", "27367791"))
API_HASH = get_env("API_HASH", "e7847da62e1461e2c0e87301b0aad8c4")
BOT_TOKEN = get_env("BOT_TOKEN", "8244250546:AAGcgXiYkBOLdmuBhZoc1t9OU0bi-g0tk04")
OWNER_ID = int(get_env("OWNER_ID", "6773435708"))
LOG_GROUP = int(get_env("LOG_GROUP", "-1002275616383"))
MONGODB_URI = get_env("MONGODB_URI", "mongodb+srv://iamnobita1:nobitamusic1@cluster0.k08op.mongodb.net/?retryWrites=true&w=majority")
LIMIT_PER_ACCOUNT = int(get_env("LIMIT_PER_ACCOUNT", "45"))

# --------------------------------------------------------------
# 4. MONGODB SETUP (With Connection Check)
# --------------------------------------------------------------
try:
    mongo_client = motor.motor_asyncio.AsyncIOMotorClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
    db = mongo_client["member_adder_bot"]
    sessions_col = db["sessions"]
    admins_col = db["admins"]
    logger.info("MongoDB Client initialized.")
except Exception as e:
    logger.critical(f"Failed to initialize MongoDB: {e}")
    exit(1)

# --------------------------------------------------------------
# 5. PYROGRAM BOT CLIENT
# --------------------------------------------------------------
bot = Client(
    "member_adder_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    plugins=None  # No external plugins – simplifies startup
)

# --------------------------------------------------------------
# 6. CRITICAL: ACTIVATE PYROMOD LISTEN
# --------------------------------------------------------------
listen(bot)

# --------------------------------------------------------------
# 7. HELPER FUNCTIONS (With Fallbacks & Logs)
# --------------------------------------------------------------

async def check_mongo_connection():
    """Ping MongoDB to ensure connection is alive."""
    try:
        await mongo_client.admin.command('ping')
        logger.info("MongoDB connection successful.")
    except Exception as e:
        logger.critical(f"MongoDB Connection Failed: {e}")
        raise e

async def is_admin(user_id: int) -> bool:
    """Check if user is owner or in admin list."""
    try:
        if user_id == OWNER_ID:
            return True
        admin = await admins_col.find_one({"user_id": user_id})
        return admin is not None
    except Exception as e:
        logger.error(f"Error checking admin status for {user_id}: {e}")
        return False

async def get_all_sessions() -> List[Dict]:
    """Retrieve all stored session strings."""
    try:
        cursor = sessions_col.find({})
        sessions = await cursor.to_list(length=None)
        logger.info(f"Retrieved {len(sessions)} sessions from DB.")
        return sessions
    except Exception as e:
        logger.error(f"Error getting sessions: {e}")
        return []

async def get_session_names() -> List[str]:
    try:
        cursor = sessions_col.find({}, {"name": 1, "_id": 0})
        docs = await cursor.to_list(length=None)
        return [doc["name"] for doc in docs]
    except Exception as e:
        logger.error(f"Error getting session names: {e}")
        return []

async def add_session(name: str, session_string: str):
    try:
        await sessions_col.update_one(
            {"name": name},
            {"$set": {"session_string": session_string}},
            upsert=True
        )
        logger.info(f"Session '{name}' saved to DB.")
    except Exception as e:
        logger.error(f"Error adding session {name}: {e}")
        raise e

async def remove_session(name: str):
    try:
        await sessions_col.delete_one({"name": name})
        logger.info(f"Session '{name}' removed from DB.")
    except Exception as e:
        logger.error(f"Error removing session {name}: {e}")

async def remove_all_sessions():
    try:
        await sessions_col.delete_many({})
        logger.info("All sessions removed from DB.")
    except Exception as e:
        logger.error(f"Error removing all sessions: {e}")

async def add_admin(user_id: int):
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
    try:
        await admins_col.delete_one({"user_id": user_id})
        logger.info(f"Admin {user_id} removed.")
    except Exception as e:
        logger.error(f"Error removing admin {user_id}: {e}")

async def get_all_admins() -> List[int]:
    try:
        cursor = admins_col.find({}, {"user_id": 1, "_id": 0})
        docs = await cursor.to_list(length=None)
        return [doc["user_id"] for doc in docs]
    except Exception as e:
        logger.error(f"Error fetching admins: {e}")
        return []

@asynccontextmanager
async def user_client(session_string: str, name: str):
    """Safely create and yield a User Client with error handling."""
    client = Client(
        name=f"temp_{name}",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=session_string,
        in_memory=True,
        no_updates=True  # We don't need updates for adders/scrapers
    )
    try:
        logger.info(f"Starting user client: {name}")
        await client.start()
        yield client
    except (AuthKeyUnregistered, UserDeactivated, SessionPasswordNeeded) as e:
        logger.error(f"Session '{name}' is INVALID or REVOKED: {e}")
    except Exception as e:
        logger.error(f"Failed to start user client '{name}': {e}")
    finally:
        try:
            if client.is_connected:
                await client.stop()
                logger.info(f"Stopped user client: {name}")
        except Exception:
            pass

def parse_group_identifier(text: str) -> List[str]:
    """Robust parser for group links/IDs."""
    try:
        # Remove common prefixes
        text = re.sub(r"https?://t\.me/\+?", "", text)
        text = re.sub(r"https?://t\.me/", "", text)
        text = re.sub(r"@", "", text)
        # Split by commas or newlines or spaces
        parts = re.split(r"[\n,\s]+", text)
        result = [p.strip() for p in parts if p.strip()]
        logger.info(f"Parsed groups: {result}")
        return result
    except Exception as e:
        logger.error(f"Error parsing group input: {e}")
        return []

async def scrape_members_from_group(client: Client, group: str) -> List[int]:
    """Scrapes members safely."""
    members = []
    try:
        logger.info(f"Scraping group: {group}")
        async for member in client.get_chat_members(group):
            user = member.user
            if user.is_bot or user.is_deleted:
                continue
            # Logic: Online, Recently, or Last Week
            if user.status in [UserStatus.ONLINE, UserStatus.RECENTLY, UserStatus.LAST_WEEK]:
                members.append(user.id)
        logger.info(f"Successfully scraped {len(members)} from {group}")
    except ChatAdminRequired:
        logger.error(f"Cannot scrape {group}: Admin rights required (or private chat).")
    except UserNotParticipant:
        logger.error(f"Cannot scrape {group}: Userbot is not a participant.")
    except Exception as e:
        logger.error(f"Scrape Error for {group}: {e}")
    return members

async def add_members_to_group(
    target_group: str,
    user_ids: List[int],
    progress_message: Message = None
) -> Dict[str, int]:
    
    sessions = await get_all_sessions()
    if not sessions:
        logger.error("No sessions available for adding.")
        raise ValueError("No session strings available.")

    total_added = 0
    failed = 0
    per_account = {}
    member_index = 0
    total_members = len(user_ids)
    
    logger.info(f"Starting ADD process. Target: {target_group}. Total Members: {total_members}")

    for sess in sessions:
        if member_index >= total_members:
            break

        name = sess["name"]
        session_string = sess["session_string"]
        added_count = 0
        limit = LIMIT_PER_ACCOUNT

        logger.info(f"Switching to Account: {name}")

        async with user_client(session_string, f"adder_{name}") as acc:
            if not acc or not acc.is_connected:
                logger.warning(f"Skipping account {name} (failed to connect)")
                per_account[name] = "Auth Failed"
                continue

            # Join target group if not present (Optional, improves success rate)
            try:
                await acc.join_chat(target_group)
            except Exception as e:
                logger.warning(f"Account {name} could not join target group: {e}")

            while added_count < limit and member_index < total_members:
                uid = user_ids[member_index]
                member_index += 1

                try:
                    await acc.add_chat_members(target_group, uid)
                    added_count += 1
                    total_added += 1
                    logger.info(f"[{name}] Added {uid} | Total: {total_added}")
                    await asyncio.sleep(2) # Safe delay
                except UserPrivacyRestricted:
                    failed += 1
                    logger.debug(f"[{name}] Failed {uid}: Privacy")
                except UserChannelsTooMuch:
                    failed += 1
                    logger.debug(f"[{name}] Failed {uid}: Channels Too Much")
                except UserNotParticipant:
                     # Account kicked or not in group
                    logger.warning(f"[{name}] Not in group. Breaking account.")
                    break
                except FloodWait as e:
                    logger.warning(f"[{name}] FloodWait: {e.value}s. Sleeping...")
                    await asyncio.sleep(e.value + 1)
                    member_index -= 1 # Retry this user
                except PeerFlood:
                    logger.error(f"[{name}] PeerFlood (Banned from adding). Switching account.")
                    break
                except Exception as e:
                    failed += 1
                    logger.error(f"[{name}] Generic Error adding {uid}: {e}")

                # UI Update
                if added_count % 5 == 0 and progress_message:
                    try:
                        percent = int((member_index / total_members) * 100)
                        bar = "▓" * (percent // 10) + "░" * (10 - (percent // 10))
                        text = (f"**Adding members...**\n"
                                f"`[{bar}]` {percent}%\n"
                                f"Active Account: `{name}`\n"
                                f"Added: {total_added} | Failed: {failed}")
                        await progress_message.edit_text(text)
                    except Exception:
                        pass
        
        per_account[name] = added_count

    return {
        "total_added": total_added,
        "per_account": per_account,
        "failed": failed,
        "total_members": total_members
    }

async def send_log_file(client: Client, chat_id: int, user_ids: List[int], prefix: str):
    try:
        if not user_ids:
            return
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("\n".join(str(uid) for uid in user_ids))
            tmp_path = f.name
        caption = f"{prefix} - Total {len(user_ids)} members\nTime: {datetime.now()}"
        await client.send_document(chat_id, document=tmp_path, caption=caption)
        os.unlink(tmp_path)
        logger.info(f"Log file sent to {chat_id}")
    except Exception as e:
        logger.error(f"Failed to send log file: {e}")

# --------------------------------------------------------------
# 8. HELP TEXT
# --------------------------------------------------------------
async def get_help_text(user_id: int) -> str:
    try:
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
        return text
    except Exception as e:
        logger.error(f"Error generating help text: {e}")
        return "Error generating help."

# --------------------------------------------------------------
# 9. MESSAGE HANDLERS (Wrapped in try/except)
# --------------------------------------------------------------

@bot.on_message(filters.command("start") & filters.private)
async def start_command(client: Client, message: Message):
    try:
        logger.info(f"User {message.from_user.id} started bot.")
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
        logger.error(f"Error in callback: {e}")

# ---------- Session Management ----------

@bot.on_message(filters.command("addstring") & filters.private)
async def addstring_command(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            return await message.reply("⛔ Admin only.")
        
        parts = message.text.split(maxsplit=2)
        if len(parts) != 3:
            return await message.reply("Usage: `/addstring Name SessionString`", parse_mode=ParseMode.MARKDOWN)
        
        name = parts[1]
        sess_str = parts[2]
        await add_session(name, sess_str)
        await message.reply(f"✅ Session `{name}` added/updated.")
    except Exception as e:
        logger.error(f"Error in addstring: {e}")
        await message.reply(f"❌ Error: {e}")

@bot.on_message(filters.command("rmstring") & filters.private)
async def rmstring_command(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            return await message.reply("⛔ Admin only.")
        
        parts = message.text.split(maxsplit=1)
        if len(parts) != 2:
            return await message.reply("Usage: `/rmstring Name`")
        
        name = parts[1]
        await remove_session(name)
        await message.reply(f"✅ Session `{name}` removed.")
    except Exception as e:
        logger.error(f"Error in rmstring: {e}")
        await message.reply(f"❌ Error: {e}")

@bot.on_message(filters.command("liststring") & filters.private)
async def liststring_command(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            return await message.reply("⛔ Admin only.")
        
        names = await get_session_names()
        if not names:
            await message.reply("No sessions stored.")
        else:
            await message.reply("**Stored session names:**\n" + "\n".join(f"• `{n}`" for n in names))
    except Exception as e:
        logger.error(f"Error in liststring: {e}")
        await message.reply(f"❌ Error: {e}")

@bot.on_message(filters.command("getstring") & filters.private)
async def getstring_command(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            return await message.reply("⛔ Admin only.")
        
        sessions = await get_all_sessions()
        if not sessions:
            return await message.reply("No sessions stored.")
        
        text = "**Name → Session** (first 50 chars):\n"
        for s in sessions:
            name = s.get("name", "Unknown")
            sess = s.get("session_string", "")[:50] + "..."
            text += f"• `{name}`: `{sess}`\n"
        await message.reply(text)
    except Exception as e:
        logger.error(f"Error in getstring: {e}")
        await message.reply(f"❌ Error: {e}")

@bot.on_message(filters.command("rmallstrings") & filters.private)
async def rmallstrings_command(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            return await message.reply("⛔ Admin only.")
        
        await remove_all_sessions()
        await message.reply("✅ All sessions removed.")
    except Exception as e:
        logger.error(f"Error in rmallstrings: {e}")
        await message.reply(f"❌ Error: {e}")

# ---------- Admin Management (Owner Only) ----------

@bot.on_message(filters.command("addadmin") & filters.private)
async def addadmin_command(client: Client, message: Message):
    try:
        if message.from_user.id != OWNER_ID:
            return await message.reply("⛔ Owner only.")
        
        parts = message.text.split()
        if len(parts) != 2:
            return await message.reply("Usage: `/addadmin user_id`")
        
        try:
            uid = int(parts[1])
        except ValueError:
            return await message.reply("Invalid user ID.")
        
        await add_admin(uid)
        await message.reply(f"✅ User {uid} added as admin.")
    except Exception as e:
        logger.error(f"Error in addadmin: {e}")
        await message.reply(f"❌ Error: {e}")

@bot.on_message(filters.command("rmadmin") & filters.private)
async def rmadmin_command(client: Client, message: Message):
    try:
        if message.from_user.id != OWNER_ID:
            return await message.reply("⛔ Owner only.")
        
        parts = message.text.split()
        if len(parts) != 2:
            return await message.reply("Usage: `/rmadmin user_id`")
        
        try:
            uid = int(parts[1])
        except ValueError:
            return await message.reply("Invalid user ID.")
        
        if uid == OWNER_ID:
            return await message.reply("❌ Cannot remove owner.")
        
        await remove_admin(uid)
        await message.reply(f"✅ User {uid} removed from admins.")
    except Exception as e:
        logger.error(f"Error in rmadmin: {e}")
        await message.reply(f"❌ Error: {e}")

@bot.on_message(filters.command("listadmins") & filters.private)
async def listadmins_command(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            return await message.reply("⛔ Admin only.")
        
        admins = await get_all_admins()
        text = "**Admin list:**\n"
        text += f"• Owner: {OWNER_ID}\n"
        for uid in admins:
            text += f"• `{uid}`\n"
        await message.reply(text)
    except Exception as e:
        logger.error(f"Error in listadmins: {e}")
        await message.reply(f"❌ Error: {e}")

# ---------- Scraping & Adding ----------

@bot.on_message(filters.command("scrab") & filters.private)
async def scrab_command(client: Client, message: Message):
    try:
        # Check Admin
        if not await is_admin(message.from_user.id):
            return await message.reply("⛔ Admin only.")

        # Check Sessions
        sessions = await get_all_sessions()
        if not sessions:
            return await message.reply("❌ No session strings available. Add one with /addstring")

        # Step 1: Input Groups
        try:
            prompt = await message.reply("📥 Send the group IDs / usernames / invite links to scrape members from.\nSeparate multiple by new line or comma.")
            response = await client.ask(message.chat.id, filters=filters.text, timeout=120)
        except Exception as e:
            return await message.reply(f"❌ Input timed out or failed: {e}")

        if not response or not response.text:
            return await message.reply("❌ No input received.")

        group_list = parse_group_identifier(response.text)
        if not group_list:
            return await message.reply("❌ No valid group identifiers found.")

        # Step 2: Scrape
        status_msg = await message.reply("🔄 Scraping members... This may take a while.")
        all_members = []
        failed_groups = []

        # Use first valid session for scraping
        first_session = sessions[0]
        async with user_client(first_session["session_string"], "scraper_main") as scraper:
            if not scraper or not scraper.is_connected:
                return await status_msg.edit_text("❌ Failed to connect scraper client (Invalid Session?).")

            for group in group_list:
                try:
                    members = await scrape_members_from_group(scraper, group)
                    all_members.extend(members)
                    await status_msg.edit_text(f"✅ Scraped {len(members)} from `{group}`\nTotal so far: {len(all_members)}")
                except Exception as e:
                    logger.error(f"Scrape loop error for {group}: {e}")
                    failed_groups.append(f"{group}")
                    await status_msg.edit_text(f"⚠️ Failed to scrape `{group}`. Continuing...")

        all_members = list(set(all_members)) # Unique
        
        if not all_members:
            return await status_msg.edit_text("❌ Scraped 0 members. Check logs or group permissions.")

        # Log File
        await send_log_file(client, LOG_GROUP, all_members, f"Scraped members - {len(group_list)} groups")

        # Step 3: Target Group
        try:
            await status_msg.edit_text(f"✅ Total Scraped: {len(all_members)}\n📤 Now send the **target group ID/username**.")
            target_resp = await client.ask(message.chat.id, filters=filters.text, timeout=120)
        except Exception as e:
            return await message.reply(f"❌ Input timed out: {e}")

        if not target_resp or not target_resp.text:
            return await message.reply("❌ No target group received.")
        
        target_group = target_resp.text.strip()

        # Step 4: Add Members
        progress = await message.reply("⏳ Starting to add members...")
        try:
            result = await add_members_to_group(target_group, all_members, progress_message=progress)
        except Exception as e:
            logger.error(f"Critical error in adding loop: {e}")
            return await progress.edit_text(f"❌ Critical Error: {e}")

        # Step 5: Results
        per_account_lines = "\n".join([f"  • {name}: {count}" for name, count in result["per_account"].items()])
        result_text = (
            f"✅ **Adding completed**\n"
            f"**Target Group:** `{target_group}`\n"
            f"**Total scraped:** {result['total_members']}\n"
            f"**Total added:** {result['total_added']}\n"
            f"**Failed (privacy/error):** {result['failed']}\n\n"
            f"**Per account:**\n{per_account_lines}"
        )
        
        try:
            await progress.edit_text(result_text)
            await client.send_message(message.from_user.id, result_text)
            await client.send_message(LOG_GROUP, result_text)
        except Exception as e:
            logger.error(f"Error sending final report: {e}")

    except Exception as main_e:
        logger.error(f"Unhandled exception in scrab_command: {main_e}")
        logger.error(traceback.format_exc())
        await message.reply(f"❌ An internal error occurred: {main_e}")

@bot.on_message(filters.command("import") & filters.private)
async def import_command(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            return await message.reply("⛔ Admin only.")

        # Step 1: File Input
        try:
            q = await message.reply("📁 Send the `.txt` file containing user IDs (one per line).")
            response = await client.ask(message.chat.id, filters=filters.document, timeout=120)
        except Exception as e:
            return await message.reply(f"❌ Input timed out: {e}")

        if not response.document:
            return await message.reply("❌ No file received.")

        # Process File
        user_ids = []
        try:
            file = await response.download()
            with open(file, "r") as f:
                lines = f.readlines()
            for line in lines:
                line = line.strip()
                if line and line.isdigit():
                    user_ids.append(int(line))
            os.unlink(file)
        except Exception as e:
            return await message.reply(f"❌ Error reading file: {e}")

        if not user_ids:
            return await message.reply("❌ No valid user IDs found in file.")

        # Step 2: Target Group
        try:
            await message.reply(f"✅ Loaded {len(user_ids)} IDs.\nNow send the **target group ID/username**.")
            target_resp = await client.ask(message.chat.id, filters=filters.text, timeout=120)
        except Exception as e:
            return await message.reply(f"❌ Input timed out: {e}")

        if not target_resp.text:
            return await message.reply("❌ No target group received.")
        
        target_group = target_resp.text.strip()

        # Step 3: Add Members
        progress = await message.reply("⏳ Adding members...")
        try:
            result = await add_members_to_group(target_group, user_ids, progress_message=progress)
        except Exception as e:
            logger.error(f"Error in import adding loop: {e}")
            return await progress.edit_text(f"❌ Adding failed: {e}")

        # Step 4: Results
        per_account_lines = "\n".join([f"  • {name}: {count}" for name, count in result["per_account"].items()])
        result_text = (
            f"✅ **Import & Add completed**\n"
            f"**Target Group:** `{target_group}`\n"
            f"**Total in file:** {result['total_members']}\n"
            f"**Total added:** {result['total_added']}\n"
            f"**Failed:** {result['failed']}\n\n"
            f"**Per account:**\n{per_account_lines}"
        )
        
        await progress.edit_text(result_text)
        await client.send_message(message.from_user.id, result_text)
        await client.send_message(LOG_GROUP, result_text)

    except Exception as e:
        logger.error(f"Unhandled exception in import_command: {e}")
        logger.error(traceback.format_exc())
        await message.reply(f"❌ An internal error occurred: {e}")

# --------------------------------------------------------------
# 10. MAIN EXECUTION
# --------------------------------------------------------------
async def main():
    try:
        # 1. Check MongoDB
        await check_mongo_connection()
        
        # 2. Start Bot
        logger.info("Starting Bot Client...")
        await bot.start()
        
        me = await bot.get_me()
        logger.info(f"Bot Started as @{me.username} (ID: {me.id})")
        
        # 3. Idle
        await idle()
        
        # 4. Stop
        await bot.stop()
        logger.info("Bot stopped.")
        
    except Exception as e:
        logger.critical(f"FATAL ERROR IN MAIN: {e}")
        logger.critical(traceback.format_exc())

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot stopped by user.")
