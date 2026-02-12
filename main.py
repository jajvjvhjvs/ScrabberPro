import os
import asyncio
import re
import tempfile
import logging
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
    UsernameNotOccupied, ChatIdInvalid, PeerIdInvalid, SessionPasswordNeeded
)
import motor.motor_asyncio

# --------------------------------------------------------------
# LOGGING SETUP
# --------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("MemberAdder")

# --------------------------------------------------------------
# ENVIRONMENT VARIABLES
# --------------------------------------------------------------
try:
    API_ID = int(os.getenv("API_ID", "29113757"))
    API_HASH = os.getenv("API_HASH", "4fb029c4a5d6beb7b6c8c0616c840939")
    BOT_TOKEN = os.getenv("BOT_TOKEN", "8244250546:AAGcgXiYkBOLdmuBhZoc1t9OU0bi-g0tk04")
    OWNER_ID = int(os.getenv("OWNER_ID", "6773435708"))
    LOG_GROUP = int(os.getenv("LOG_GROUP", "-1002275616383"))
    MONGODB_URI = os.getenv("MONGODB_URI", "mongodb+srv://iamnobita1:nobitamusic1@cluster0.k08op.mongodb.net/?retryWrites=true&w=majority")
    LIMIT_PER_ACCOUNT = int(os.getenv("LIMIT_PER_ACCOUNT", "45"))
    logger.info("Environment variables loaded successfully.")
except Exception as e:
    logger.error(f"Error loading environment variables: {e}")
    exit(1)

# --------------------------------------------------------------
# MongoDB Setup
# --------------------------------------------------------------
try:
    mongo_client = motor.motor_asyncio.AsyncIOMotorClient(MONGODB_URI)
    db = mongo_client["member_adder_bot"]
    sessions_col = db["sessions"]
    admins_col = db["admins"]
    logger.info("MongoDB connection initialized.")
except Exception as e:
    logger.error(f"Failed to connect to MongoDB: {e}")
    exit(1)

# --------------------------------------------------------------
# Pyrogram Bot Client
# --------------------------------------------------------------
bot = Client(
    "member_adder_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# --------------------------------------------------------------
# Helper Functions
# --------------------------------------------------------------

async def is_admin(user_id: int) -> bool:
    try:
        if user_id == OWNER_ID:
            return True
        admin = await admins_col.find_one({"user_id": user_id})
        return admin is not None
    except Exception as e:
        logger.error(f"Error checking admin status: {e}")
        return False

async def get_all_sessions() -> List[Dict]:
    try:
        cursor = sessions_col.find({})
        return await cursor.to_list(length=None)
    except Exception as e:
        logger.error(f"Error retrieving sessions: {e}")
        return []

async def get_session_names() -> List[str]:
    try:
        cursor = sessions_col.find({}, {"name": 1, "_id": 0})
        docs = await cursor.to_list(length=None)
        return [doc["name"] for doc in docs]
    except Exception as e:
        logger.error(f"Error retrieving session names: {e}")
        return []

async def add_session(name: str, session_string: str):
    try:
        await sessions_col.update_one(
            {"name": name},
            {"$set": {"session_string": session_string}},
            upsert=True
        )
        logger.info(f"Session '{name}' added/updated in database.")
    except Exception as e:
        logger.error(f"Error adding session: {e}")

async def remove_session(name: str):
    try:
        await sessions_col.delete_one({"name": name})
        logger.info(f"Session '{name}' removed from database.")
    except Exception as e:
        logger.error(f"Error removing session: {e}")

async def remove_all_sessions():
    try:
        await sessions_col.delete_many({})
        logger.info("All sessions cleared from database.")
    except Exception as e:
        logger.error(f"Error clearing all sessions: {e}")

async def add_admin(user_id: int):
    try:
        await admins_col.update_one(
            {"user_id": user_id},
            {"$set": {"user_id": user_id}},
            upsert=True
        )
        logger.info(f"Admin {user_id} added.")
    except Exception as e:
        logger.error(f"Error adding admin: {e}")

async def remove_admin(user_id: int):
    try:
        await admins_col.delete_one({"user_id": user_id})
        logger.info(f"Admin {user_id} removed.")
    except Exception as e:
        logger.error(f"Error removing admin: {e}")

async def get_all_admins() -> List[int]:
    try:
        cursor = admins_col.find({}, {"user_id": 1, "_id": 0})
        docs = await cursor.to_list(length=None)
        return [doc["user_id"] for doc in docs]
    except Exception as e:
        logger.error(f"Error getting admin list: {e}")
        return []

@asynccontextmanager
async def user_client(session_string: str, name: str):
    client = Client(name, api_id=API_ID, api_hash=API_HASH, session_string=session_string, in_memory=True)
    try:
        logger.info(f"Attempting to start user client: {name}")
        await client.start()
        yield client
    except Exception as e:
        logger.error(f"Failed to start user client {name}: {e}")
        yield None
    finally:
        try:
            await client.stop()
        except:
            pass

def parse_group_identifier(text: str) -> List[str]:
    try:
        text = re.sub(r"https?://t\.me/\+?", "", text)
        text = re.sub(r"https?://t\.me/", "", text)
        parts = re.split(r"[\n,]+", text)
        return [p.strip() for p in parts if p.strip()]
    except Exception as e:
        logger.error(f"Parsing error: {e}")
        return []

async def scrape_members_from_group(client: Client, group: str) -> List[int]:
    members = []
    if not client:
        return []
    try:
        logger.info(f"Scraping from: {group}")
        async for member in client.get_chat_members(group):
            user = member.user
            if not user or user.is_bot or user.is_deleted:
                continue
            if user.status in [UserStatus.ONLINE, UserStatus.RECENTLY, UserStatus.LAST_WEEK]:
                members.append(user.id)
    except Exception as e:
        logger.error(f"[Scrape Error] {group}: {e}")
    return members

async def add_members_to_group(
    target_group: str,
    user_ids: List[int],
    progress_message: Message = None
) -> Dict:
    sessions = await get_all_sessions()
    if not sessions:
        logger.warning("No sessions found in DB for adding members.")
        raise ValueError("No session strings available.")

    total_added = 0
    failed = 0
    per_account = {}
    member_index = 0
    total_members = len(user_ids)

    for sess in sessions:
        name = sess["name"]
        session_string = sess["session_string"]
        added_count = 0
        
        async with user_client(session_string, f"adder_{name}") as acc:
            if not acc:
                logger.error(f"Skipping session {name} due to login failure.")
                per_account[name] = "Login Failed"
                continue

            while added_count < LIMIT_PER_ACCOUNT and member_index < total_members:
                uid = user_ids[member_index]
                member_index += 1

                try:
                    await acc.add_chat_members(target_group, uid)
                    added_count += 1
                    total_added += 1
                    await asyncio.sleep(2) # slightly faster delay
                except UserPrivacyRestricted:
                    failed += 1
                except FloodWait as e:
                    logger.info(f"FloodWait on {name}: {e.value}s")
                    await asyncio.sleep(e.value + 1)
                    member_index -= 1
                except (PeerFlood, ChatAdminRequired, UserChannelsTooMuch):
                    logger.warning(f"Session {name} limited or restricted. Moving to next.")
                    break
                except Exception as e:
                    logger.debug(f"Error adding {uid}: {e}")
                    failed += 1

                if added_count % 5 == 0 and progress_message:
                    try:
                        percent = int((member_index / total_members) * 100)
                        bar = "▓" * (percent // 10) + "░" * (10 - (percent // 10))
                        await progress_message.edit_text(
                            f"**Adding members...**\n`[{bar}]` {percent}%\nAdded: {total_added} | Failed: {failed}"
                        )
                    except:
                        pass

        per_account[name] = added_count
        if member_index >= total_members:
            break

    return {
        "total_added": total_added,
        "per_account": per_account,
        "failed": failed,
        "total_members": total_members
    }

async def send_log_file(client: Client, chat_id: int, user_ids: List[int], prefix: str):
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("\n".join(str(uid) for uid in user_ids))
            tmp_path = f.name
        caption = f"{prefix} - Total {len(user_ids)} members"
        await client.send_document(chat_id, document=tmp_path, caption=caption)
        os.unlink(tmp_path)
    except Exception as e:
        logger.error(f"Failed to send log file: {e}")

# --------------------------------------------------------------
# Help & Start Handlers
# --------------------------------------------------------------

async def get_help_text(user_id: int) -> str:
    is_administrator = await is_admin(user_id)
    text = "**🤖 Member Adder Bot Help**\n\n"
    text += "**Public Commands:**\n"
    text += "• /start - Start the bot\n"
    text += "• /help - Show this help message\n\n"
    text += "**👑 Owner/Admin Commands:**\n"
    text += "• /addstring - Add a session\n"
    text += "• /liststring - List sessions\n"
    text += "• /scrab - Start scraping process\n"
    text += "• /import - Add from .txt file\n\n"
    text += f"**Status:** {'✅ Admin' if is_administrator else '❌ User'}"
    return text

@bot.on_message(filters.command("start") & filters.private)
async def start_command(client: Client, message: Message):
    logger.info(f"Start command from {message.from_user.id}")
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📘 Help", callback_data="help")],
        [InlineKeyboardButton("👤 Owner", url=f"tg://user?id={OWNER_ID}")]
    ])
    await message.reply(
        "✅ **Member Adder Bot is online.**\n"
        "Use /scrab or /import to add members (admin only).",
        reply_markup=keyboard
    )

@bot.on_message(filters.command("help") & filters.private)
async def help_command(client: Client, message: Message):
    text = await get_help_text(message.from_user.id)
    await message.reply(text, parse_mode=ParseMode.MARKDOWN)

@bot.on_callback_query()
async def callback_query_handler(client: Client, query: CallbackQuery):
    try:
        if query.data == "help":
            text = await get_help_text(query.from_user.id)
            await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Start", callback_data="start")]]))
        elif query.data == "start":
            await query.message.edit_text("✅ **Member Adder Bot is online.**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📘 Help", callback_data="help")]]))
        await query.answer()
    except Exception as e:
        logger.error(f"Callback error: {e}")

# ---------- Session Management ----------

@bot.on_message(filters.command("addstring") & filters.private)
async def addstring_command(client: Client, message: Message):
    if not await is_admin(message.from_user.id):
        return await message.reply("⛔ Admin only.")
    
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        return await message.reply("Usage: `/addstring Name SessionString`", parse_mode=ParseMode.MARKDOWN)
    
    name, sess_str = parts[1], parts[2]
    await add_session(name, sess_str)
    await message.reply(f"✅ Session `{name}` added/updated.")

@bot.on_message(filters.command("liststring") & filters.private)
async def liststring_command(client: Client, message: Message):
    if not await is_admin(message.from_user.id):
        return await message.reply("⛔ Admin only.")
    names = await get_session_names()
    if not names:
        return await message.reply("No sessions stored.")
    await message.reply("**Stored sessions:**\n" + "\n".join(f"• `{n}`" for n in names))

# ---------- Scraping & Adding ----------

@bot.on_message(filters.command("scrab") & filters.private)
async def scrab_command(client: Client, message: Message):
    if not await is_admin(message.from_user.id):
        return await message.reply("⛔ Admin only.")

    try:
        # Step 1: Input Source
        await message.reply("📥 Send the group IDs/links to scrape members from.")
        response = await client.ask(message.chat.id, filters.text, timeout=120)
        group_list = parse_group_identifier(response.text)
        
        sessions = await get_all_sessions()
        if not sessions:
            return await message.reply("❌ No sessions found.")

        status_msg = await message.reply("🔄 Scraping members...")
        all_members = []

        async with user_client(sessions[0]["session_string"], "scraper") as scraper:
            if not scraper:
                return await status_msg.edit_text("❌ Scraper session invalid.")
            
            for group in group_list:
                mems = await scrape_members_from_group(scraper, group)
                all_members.extend(mems)
                await status_msg.edit_text(f"✅ Scraped {len(mems)} from {group}\nTotal: {len(set(all_members))}")

        all_members = list(set(all_members))
        if not all_members:
            return await status_msg.edit_text("❌ No active members found.")

        # Step 2: Target
        await message.reply("📤 Send the **target group ID/username**.")
        target_resp = await client.ask(message.chat.id, filters.text, timeout=120)
        target_group = target_resp.text.strip()

        # Step 3: Action
        progress = await message.reply("⏳ Starting to add...")
        result = await add_members_to_group(target_group, all_members, progress)
        
        summary = f"✅ **Done!**\nAdded: {result['total_added']}\nFailed: {result['failed']}"
        await progress.edit_text(summary)
        await client.send_message(LOG_GROUP, f"Add Summary for {target_group}:\n{summary}")

    except asyncio.TimeoutError:
        await message.reply("❌ Timeout: Process cancelled.")
    except Exception as e:
        logger.error(f"Scrab Error: {e}")
        await message.reply(f"❌ Error: {e}")

@bot.on_message(filters.command("import") & filters.private)
async def import_command(client: Client, message: Message):
    if not await is_admin(message.from_user.id):
        return await message.reply("⛔ Admin only.")

    try:
        await message.reply("📁 Send the `.txt` file with user IDs.")
        response = await client.ask(message.chat.id, filters.document, timeout=120)
        file_path = await response.download()
        
        user_ids = []
        with open(file_path, "r") as f:
            for line in f:
                if line.strip().isdigit():
                    user_ids.append(int(line.strip()))
        os.unlink(file_path)

        if not user_ids:
            return await message.reply("❌ No IDs found.")

        await message.reply(f"✅ Loaded {len(user_ids)} IDs. Send target group.")
        target_resp = await client.ask(message.chat.id, filters.text, timeout=120)
        
        progress = await message.reply("⏳ Adding...")
        result = await add_members_to_group(target_resp.text.strip(), user_ids, progress)
        await progress.edit_text(f"✅ Completed!\nAdded: {result['total_added']}")

    except Exception as e:
        logger.error(f"Import error: {e}")
        await message.reply(f"❌ Error: {e}")

# --------------------------------------------------------------
# Main entry
# --------------------------------------------------------------
async def main():
    logger.info("Starting bot...")
    try:
        await bot.start()
        bot_info = await bot.get_me()
        logger.info(f"Bot started as @{bot_info.username}")
        await idle()
    except Exception as e:
        logger.error(f"Critical error in main: {e}")
    finally:
        await bot.stop()
        logger.info("Bot stopped.")

if __name__ == "__main__":
    asyncio.run(main())
