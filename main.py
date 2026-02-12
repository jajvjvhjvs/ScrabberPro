import os
import asyncio
import re
import tempfile
import logging
import traceback
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
    UsernameNotOccupied, ChatIdInvalid, PeerIdInvalid,
    AuthKeyInvalid, SessionRevoked
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

def log_info(text: str):
    logger.info(text)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] INFO: {text}")

def log_error(text: str):
    logger.error(text)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] ERROR: {text}")

# --------------------------------------------------------------
# ENVIRONMENT VARIABLES
# --------------------------------------------------------------
API_ID = int(os.getenv("API_ID", "29113757"))
API_HASH = os.getenv("API_HASH", "4fb029c4a5d6beb7b6c8c0616c840939")
BOT_TOKEN = os.getenv("BOT_TOKEN", "8244250546:AAGcgXiYkBOLdmuBhZoc1t9OU0bi-g0tk04")
OWNER_ID = int(os.getenv("OWNER_ID", "6773435708"))
LOG_GROUP = int(os.getenv("LOG_GROUP", "-1002275616383"))
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb+srv://iamnobita1:nobitamusic1@cluster0.k08op.mongodb.net/?retryWrites=true&w=majority")
LIMIT_PER_ACCOUNT = int(os.getenv("LIMIT_PER_ACCOUNT", "45"))

# --------------------------------------------------------------
# MongoDB Setup
# --------------------------------------------------------------
try:
    mongo_client = motor.motor_asyncio.AsyncIOMotorClient(MONGODB_URI)
    db = mongo_client["member_adder_bot"]
    sessions_col = db["sessions"]
    admins_col = db["admins"]
    log_info("Successfully connected to MongoDB.")
except Exception as e:
    log_error(f"MongoDB Connection Error: {e}")

# --------------------------------------------------------------
# Pyrogram Bot Client
# --------------------------------------------------------------
bot = Client(
    "member_adder_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    plugins=[]
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
        log_error(f"Error checking admin status: {e}")
        return False

async def get_all_sessions() -> List[Dict]:
    try:
        cursor = sessions_col.find({})
        return await cursor.to_list(length=None)
    except Exception as e:
        log_error(f"Error fetching sessions: {e}")
        return []

async def get_session_names() -> List[str]:
    try:
        cursor = sessions_col.find({}, {"name": 1, "_id": 0})
        docs = await cursor.to_list(length=None)
        return [doc["name"] for doc in docs]
    except Exception as e:
        log_error(f"Error fetching session names: {e}")
        return []

async def add_session(name: str, session_string: str):
    try:
        await sessions_col.update_one(
            {"name": name},
            {"$set": {"session_string": session_string}},
            upsert=True
        )
        log_info(f"Session '{name}' updated in DB.")
    except Exception as e:
        log_error(f"Error adding session to DB: {e}")

async def remove_session(name: str):
    try:
        await sessions_col.delete_one({"name": name})
        log_info(f"Session '{name}' removed from DB.")
    except Exception as e:
        log_error(f"Error removing session from DB: {e}")

async def remove_all_sessions():
    try:
        await sessions_col.delete_many({})
        log_info("All sessions cleared from DB.")
    except Exception as e:
        log_error(f"Error clearing sessions: {e}")

async def add_admin(user_id: int):
    try:
        await admins_col.update_one(
            {"user_id": user_id},
            {"$set": {"user_id": user_id}},
            upsert=True
        )
    except Exception as e:
        log_error(f"Error adding admin: {e}")

async def remove_admin(user_id: int):
    try:
        await admins_col.delete_one({"user_id": user_id})
    except Exception as e:
        log_error(f"Error removing admin: {e}")

@asynccontextmanager
async def user_client(session_string: str, name: str):
    client = Client(name, api_id=API_ID, api_hash=API_HASH, session_string=session_string, in_memory=True)
    try:
        await client.start()
        yield client
    except (AuthKeyInvalid, SessionRevoked):
        log_error(f"Session '{name}' is invalid or expired. Skipping...")
        yield None
    except Exception as e:
        log_error(f"Could not start client '{name}': {e}")
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
        log_error(f"Parsing error: {e}")
        return []

async def scrape_members_from_group(client: Client, group: str) -> List[int]:
    members = []
    log_info(f"Attempting to scrape: {group}")
    try:
        async for member in client.get_chat_members(group):
            user = member.user
            if not user or user.is_bot or user.is_deleted:
                continue
            if user.status in [UserStatus.ONLINE, UserStatus.RECENTLY, UserStatus.LAST_WEEK]:
                members.append(user.id)
    except ChatAdminRequired:
        log_error(f"Admin privileges required to scrape {group}")
    except Exception as e:
        log_error(f"Scrape Error in {group}: {e}")
        raise
    return members

async def add_members_to_group(
    target_group: str,
    user_ids: List[int],
    progress_message: Message = None
) -> Dict:
    sessions = await get_all_sessions()
    if not sessions:
        log_error("Add process halted: No sessions found.")
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
        
        log_info(f"Starting add sequence with account: {name}")
        async with user_client(session_string, f"adder_{name}") as acc:
            if not acc:
                per_account[name] = "FAILED_TO_START"
                continue

            while added_count < LIMIT_PER_ACCOUNT and member_index < total_members:
                uid = user_ids[member_index]
                member_index += 1

                try:
                    await acc.add_chat_members(target_group, uid)
                    added_count += 1
                    total_added += 1
                    await asyncio.sleep(5) 
                except UserPrivacyRestricted:
                    failed += 1
                except FloodWait as e:
                    log_info(f"Account {name} FloodWait: Sleeping {e.value}s")
                    await asyncio.sleep(e.value + 1)
                    member_index -= 1 # retry same user
                except PeerFlood:
                    log_error(f"Account {name} PeerFlood: Stopping this account.")
                    break
                except UserChannelsTooMuch:
                    failed += 1
                except Exception as e:
                    log_error(f"Error adding {uid} via {name}: {e}")
                    failed += 1

                if member_index % 5 == 0 and progress_message:
                    percent = int((member_index / total_members) * 100)
                    bar = "▓" * (percent // 10) + "░" * (10 - (percent // 10))
                    text = f"**Adding members...**\n`[{bar}]` {percent}%\nAdded: {total_added} | Failed: {failed}"
                    try:
                        await progress_message.edit_text(text)
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
        log_error(f"Failed to send log file: {e}")

# --------------------------------------------------------------
# Help & Start Handlers
# --------------------------------------------------------------

async def get_help_text(user_id: int) -> str:
    is_administrator = await is_admin(user_id)
    text = "**🤖 Member Adder Bot Help**\n\n"
    text += "**Public Commands:**\n"
    text += "• /start - Start the bot\n"
    text += "• /help - Show help message\n\n"

    text += "**👑 Owner Commands:**\n"
    text += "• /addadmin <user_id>\n"
    text += "• /rmadmin <user_id>\n\n"

    text += "**🛠️ Admin Commands:**\n"
    text += "• /addstring <Name> <String>\n"
    text += "• /rmstring <Name>\n"
    text += "• /liststring\n"
    text += "• /getstring\n"
    text += "• /rmallstrings\n"
    text += "• /listadmins\n"
    text += "• /scrab - Scrape & Add\n"
    text += "• /import - Import .txt & Add\n"
    
    if not is_administrator:
        text += "\n⚠️ _You are not an admin._"
    return text

@bot.on_message(filters.command("start") & filters.private)
async def start_command(client: Client, message: Message):
    log_info(f"Start command from {message.from_user.id}")
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📘 Help", callback_data="help")],
        [InlineKeyboardButton("👤 Owner", url=f"tg://user?id={OWNER_ID}")]
    ])
    await message.reply("✅ **Bot is active.**\nUse /scrab or /import (Admins only).", reply_markup=keyboard)

@bot.on_message(filters.command("help") & filters.private)
async def help_command(client: Client, message: Message):
    text = await get_help_text(message.from_user.id)
    await message.reply(text, parse_mode=ParseMode.MARKDOWN)

@bot.on_callback_query()
async def callback_query_handler(client: Client, query: CallbackQuery):
    try:
        if query.data == "help":
            text = await get_help_text(query.from_user.id)
            await query.message.edit_text(text, parse_mode=ParseMode.MARKDOWN)
        elif query.data == "start":
            await query.message.edit_text("✅ **Bot is active.**")
        await query.answer()
    except Exception as e:
        log_error(f"Callback error: {e}")

# ---------- Session Management ----------
@bot.on_message(filters.command("addstring") & filters.private)
async def addstring_command(client: Client, message: Message):
    if not await is_admin(message.from_user.id):
        return await message.reply("⛔ Admin only.")
    parts = message.text.split(maxsplit=2)
    if len(parts) != 3:
        return await message.reply("Usage: `/addstring Name String`")
    await add_session(parts[1], parts[2])
    await message.reply(f"✅ Session `{parts[1]}` added.")

@bot.on_message(filters.command("rmstring") & filters.private)
async def rmstring_command(client: Client, message: Message):
    if not await is_admin(message.from_user.id):
        return await message.reply("⛔ Admin only.")
    parts = message.text.split(maxsplit=1)
    if len(parts) != 2:
        return await message.reply("Usage: `/rmstring Name`")
    await remove_session(parts[1])
    await message.reply(f"✅ Session `{parts[1]}` removed.")

@bot.on_message(filters.command("liststring") & filters.private)
async def liststring_command(client: Client, message: Message):
    if not await is_admin(message.from_user.id):
        return
    names = await get_session_names()
    if not names:
        return await message.reply("No sessions.")
    await message.reply("**Sessions:**\n" + "\n".join(f"• `{n}`" for n in names))

@bot.on_message(filters.command("getstring") & filters.private)
async def getstring_command(client: Client, message: Message):
    if not await is_admin(message.from_user.id):
        return
    sessions = await get_all_sessions()
    if not sessions:
        return await message.reply("No sessions.")
    text = "".join([f"• `{s['name']}`: `{s['session_string'][:30]}...`\n" for s in sessions])
    await message.reply(text)

@bot.on_message(filters.command("rmallstrings") & filters.private)
async def rmallstrings_command(client: Client, message: Message):
    if not await is_admin(message.from_user.id):
        return
    await remove_all_sessions()
    await message.reply("✅ All cleared.")

# ---------- Admin Management ----------
@bot.on_message(filters.command("addadmin") & filters.private)
async def addadmin_command(client: Client, message: Message):
    if message.from_user.id != OWNER_ID:
        return
    parts = message.text.split()
    if len(parts) == 2 and parts[1].isdigit():
        await add_admin(int(parts[1]))
        await message.reply(f"✅ Added {parts[1]}")

@bot.on_message(filters.command("rmadmin") & filters.private)
async def rmadmin_command(client: Client, message: Message):
    if message.from_user.id != OWNER_ID:
        return
    parts = message.text.split()
    if len(parts) == 2 and parts[1].isdigit():
        if int(parts[1]) == OWNER_ID:
            return await message.reply("Cannot remove owner.")
        await remove_admin(int(parts[1]))
        await message.reply(f"✅ Removed {parts[1]}")

# ---------- Scraping & Adding ----------
@bot.on_message(filters.command("scrab") & filters.private)
async def scrab_command(client: Client, message: Message):
    if not await is_admin(message.from_user.id):
        return await message.reply("⛔ Admin only.")

    try:
        q = await message.reply("📥 Send group links/IDs to scrape (comma or new line).")
        response = await client.ask(message.chat.id, filters.text, timeout=120)
        group_list = parse_group_identifier(response.text)
        
        sessions = await get_all_sessions()
        if not sessions:
            return await message.reply("❌ No sessions found.")

        status_msg = await message.reply("🔄 Scraping...")
        all_members = []
        
        async with user_client(sessions[0]["session_string"], "scraper") as scraper:
            if not scraper:
                return await status_msg.edit_text("❌ Primary scraper session failed.")
            for group in group_list:
                try:
                    mems = await scrape_members_from_group(scraper, group)
                    all_members.extend(mems)
                except Exception:
                    continue
        
        all_members = list(set(all_members))
        if not all_members:
            return await status_msg.edit_text("❌ No members found to add.")

        await send_log_file(client, LOG_GROUP, all_members, f"Scraped {len(group_list)} groups")

        await status_msg.edit_text("📤 Send the **target group** ID/username.")
        target_resp = await client.ask(message.chat.id, filters.text, timeout=120)
        target_group = target_resp.text.strip()

        progress = await message.reply("⏳ Starting...")
        result = await add_members_to_group(target_group, all_members, progress)
        
        res_text = f"✅ **Completed**\nTotal: {result['total_added']}\nFailed: {result['failed']}"
        await progress.edit_text(res_text)
        await client.send_message(LOG_GROUP, res_text)

    except asyncio.TimeoutError:
        await message.reply("❌ Request timed out.")
    except Exception as e:
        log_error(f"Scrab Error: {traceback.format_exc()}")
        await message.reply(f"❌ Error: {e}")

@bot.on_message(filters.command("import") & filters.private)
async def import_command(client: Client, message: Message):
    if not await is_admin(message.from_user.id):
        return await message.reply("⛔ Admin only.")

    try:
        await message.reply("📁 Send the `.txt` file.")
        response = await client.ask(message.chat.id, filters.document, timeout=120)
        file = await response.download()
        
        user_ids = []
        with open(file, "r") as f:
            for line in f:
                if line.strip().isdigit():
                    user_ids.append(int(line.strip()))
        os.unlink(file)

        if not user_ids:
            return await message.reply("❌ File is empty or invalid.")

        await message.reply(f"✅ Loaded {len(user_ids)} IDs.\nSend **target group**.")
        target_resp = await client.ask(message.chat.id, filters.text, timeout=120)
        
        progress = await message.reply("⏳ Adding...")
        result = await add_members_to_group(target_resp.text.strip(), user_ids, progress)
        
        res_text = f"✅ **Import Complete**\nAdded: {result['total_added']}"
        await progress.edit_text(res_text)
    except Exception as e:
        log_error(f"Import Error: {e}")
        await message.reply(f"❌ Error: {e}")

# --------------------------------------------------------------
# Main entry
# --------------------------------------------------------------
async def main():
    try:
        log_info("Starting Bot...")
        await bot.start()
        log_info("Bot is online.")
        await idle()
    except Exception as e:
        log_error(f"Startup failed: {e}")
    finally:
        await bot.stop()

if __name__ == "__main__":
    asyncio.run(main())
