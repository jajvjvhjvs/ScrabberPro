import os
import asyncio
import re
import tempfile
from datetime import datetime
from typing import List, Dict, Optional
from contextlib import asynccontextmanager

from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.enums import UserStatus, ChatMemberStatus, ParseMode
from pyrogram.errors import (
    FloodWait, PeerFlood, UserPrivacyRestricted,
    UserChannelsTooMuch, ChatAdminRequired, UserNotParticipant,
    UsernameNotOccupied, ChatIdInvalid, PeerIdInvalid
)
import motor.motor_asyncio

# --------------------------------------------------------------
# ENVIRONMENT VARIABLES (all must be set)
# --------------------------------------------------------------
API_ID = int(os.getenv("API_ID", "29113757"))
API_HASH = os.getenv("API_HASH", "4fb029c4a5d6beb7b6c8c0616c840939")
BOT_TOKEN = os.getenv("BOT_TOKEN", "8244250546:AAGcgXiYkBOLdmuBhZoc1t9OU0bi-g0tk04")
OWNER_ID = int(os.getenv("OWNER_ID", "6773435708"))
LOG_GROUP = int(os.getenv("LOG_GROUP", "-1002275616383"))          # chat id (with -100)
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb+srv://iamnobita1:nobitamusic1@cluster0.k08op.mongodb.net/?retryWrites=true&w=majority")
LIMIT_PER_ACCOUNT = int(os.getenv("LIMIT_PER_ACCOUNT", "45"))

# --------------------------------------------------------------
# MongoDB Setup
# --------------------------------------------------------------
mongo_client = motor.motor_asyncio.AsyncIOMotorClient(MONGODB_URI)
db = mongo_client["member_adder_bot"]
sessions_col = db["sessions"]          # {name: str, session_string: str}
admins_col = db["admins"]              # {user_id: int}

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
# Helper Functions
# --------------------------------------------------------------
async def is_admin(user_id: int) -> bool:
    """Check if user is owner or in admin list."""
    if user_id == OWNER_ID:
        return True
    admin = await admins_col.find_one({"user_id": user_id})
    return admin is not None

async def get_all_sessions() -> List[Dict]:
    """Retrieve all stored session strings."""
    cursor = sessions_col.find({})
    return await cursor.to_list(length=None)

async def get_session_names() -> List[str]:
    """Return only names of sessions."""
    cursor = sessions_col.find({}, {"name": 1, "_id": 0})
    docs = await cursor.to_list(length=None)
    return [doc["name"] for doc in docs]

async def add_session(name: str, session_string: str):
    """Insert or update a session by name."""
    await sessions_col.update_one(
        {"name": name},
        {"$set": {"session_string": session_string}},
        upsert=True
    )

async def remove_session(name: str):
    """Delete a session by name."""
    await sessions_col.delete_one({"name": name})

async def remove_all_sessions():
    """Delete all sessions."""
    await sessions_col.delete_many({})

async def get_session_string(name: str) -> Optional[str]:
    """Retrieve session string by name."""
    doc = await sessions_col.find_one({"name": name})
    return doc["session_string"] if doc else None

async def add_admin(user_id: int):
    """Add a user to admin list."""
    await admins_col.update_one(
        {"user_id": user_id},
        {"$set": {"user_id": user_id}},
        upsert=True
    )

async def remove_admin(user_id: int):
    """Remove a user from admin list."""
    await admins_col.delete_one({"user_id": user_id})

async def get_all_admins() -> List[int]:
    """Retrieve all admin user IDs."""
    cursor = admins_col.find({}, {"user_id": 1, "_id": 0})
    docs = await cursor.to_list(length=None)
    return [doc["user_id"] for doc in docs]

@asynccontextmanager
async def user_client(session_string: str, name: str):
    """Context manager for a user client from session string."""
    client = Client(name, api_id=API_ID, api_hash=API_HASH, session_string=session_string, in_memory=True)
    try:
        await client.start()
        yield client
    finally:
        await client.stop()

def parse_group_identifier(text: str) -> List[str]:
    """Parse group IDs/links from user input. Accepts comma/newline separation."""
    # Remove common prefixes
    text = re.sub(r"https?://t\.me/\+?", "", text)
    text = re.sub(r"https?://t\.me/", "", text)
    # Split by commas or newlines
    parts = re.split(r"[\n,]+", text)
    # Strip whitespace and filter out empty
    return [p.strip() for p in parts if p.strip()]

async def scrape_members_from_group(client: Client, group: str) -> List[int]:
    """
    Scrape active members from a single group.
    Returns list of user IDs (active: ONLINE, RECENTLY, LAST_WEEK).
    """
    members = []
    try:
        async for member in client.get_chat_members(group):
            user = member.user
            if user.is_bot or user.is_deleted:
                continue
            if user.status in [UserStatus.ONLINE, UserStatus.RECENTLY, UserStatus.LAST_WEEK]:
                members.append(user.id)
    except Exception as e:
        print(f"[Scrape Error] {group}: {e}")
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
    Returns dict: {"total_added": int, "per_account": {name: count}, "failed": int}
    """
    sessions = await get_all_sessions()
    if not sessions:
        raise ValueError("No session strings available.")

    total_added = 0
    failed = 0
    per_account = {}
    member_index = 0
    total_members = len(user_ids)

    # Distribute members across accounts
    for sess in sessions:
        name = sess["name"]
        session_string = sess["session_string"]
        added_count = 0
        limit = LIMIT_PER_ACCOUNT

        async with user_client(session_string, f"adder_{name}") as acc:
            while added_count < limit and member_index < total_members:
                uid = user_ids[member_index]
                member_index += 1

                try:
                    await acc.add_chat_members(target_group, uid)
                    added_count += 1
                    total_added += 1
                    await asyncio.sleep(5)  # delay to avoid flood
                except UserPrivacyRestricted:
                    failed += 1
                    # skip silently
                except FloodWait as e:
                    await asyncio.sleep(e.value + 1)
                    # retry same member
                    member_index -= 1
                except PeerFlood:
                    # account banned from adding, break this account
                    break
                except Exception:
                    failed += 1
                    # other errors, skip member

                # Update progress every 5 adds or when account finishes
                if added_count % 5 == 0 and progress_message:
                    percent = int((member_index / total_members) * 100)
                    bar = "▓" * (percent // 10) + "░" * (10 - (percent // 10))
                    text = f"**Adding members...**\n`[{bar}]` {percent}%\nAdded: {total_added} | Failed: {failed}"
                    try:
                        await progress_message.edit_text(text)
                    except:
                        pass

        per_account[name] = added_count

    return {
        "total_added": total_added,
        "per_account": per_account,
        "failed": failed,
        "total_members": total_members
    }

async def send_log_file(client: Client, chat_id: int, user_ids: List[int], prefix: str):
    """Create a .txt file with user IDs and send it to log group."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("\n".join(str(uid) for uid in user_ids))
        tmp_path = f.name
    caption = f"{prefix} - Total {len(user_ids)} members"
    await client.send_document(chat_id, document=tmp_path, caption=caption)
    os.unlink(tmp_path)

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

@bot.on_message(filters.command("help") & filters.private)
async def help_command(client: Client, message: Message):
    text = await get_help_text(message.from_user.id)
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🏠 Start", callback_data="start")]
    ])
    await message.reply(text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)

@bot.on_callback_query()
async def callback_query_handler(client: Client, query: CallbackQuery):
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

# ---------- Session Management ----------
@bot.on_message(filters.command("addstring") & filters.private)
async def addstring_command(client: Client, message: Message):
    if not await is_admin(message.from_user.id):
        return await message.reply("⛔ Admin only.")
    parts = message.text.split(maxsplit=2)
    if len(parts) != 3:
        return await message.reply("Usage: `/addstring Name SessionString`", parse_mode=ParseMode.MARKDOWN)
    name = parts[1]
    sess_str = parts[2]
    await add_session(name, sess_str)
    await message.reply(f"✅ Session `{name}` added/updated.")

@bot.on_message(filters.command("rmstring") & filters.private)
async def rmstring_command(client: Client, message: Message):
    if not await is_admin(message.from_user.id):
        return await message.reply("⛔ Admin only.")
    parts = message.text.split(maxsplit=1)
    if len(parts) != 2:
        return await message.reply("Usage: `/rmstring Name`")
    name = parts[1]
    await remove_session(name)
    await message.reply(f"✅ Session `{name}` removed.")

@bot.on_message(filters.command("liststring") & filters.private)
async def liststring_command(client: Client, message: Message):
    if not await is_admin(message.from_user.id):
        return await message.reply("⛔ Admin only.")
    names = await get_session_names()
    if not names:
        await message.reply("No sessions stored.")
    else:
        await message.reply("**Stored session names:**\n" + "\n".join(f"• `{n}`" for n in names))

@bot.on_message(filters.command("getstring") & filters.private)
async def getstring_command(client: Client, message: Message):
    if not await is_admin(message.from_user.id):
        return await message.reply("⛔ Admin only.")
    sessions = await get_all_sessions()
    if not sessions:
        return await message.reply("No sessions stored.")
    text = "**Name → Session** (first 50 chars):\n"
    for s in sessions:
        name = s["name"]
        sess = s["session_string"][:50] + "..."
        text += f"• `{name}`: `{sess}`\n"
    await message.reply(text)

@bot.on_message(filters.command("rmallstrings") & filters.private)
async def rmallstrings_command(client: Client, message: Message):
    if not await is_admin(message.from_user.id):
        return await message.reply("⛔ Admin only.")
    await remove_all_sessions()
    await message.reply("✅ All sessions removed.")

# ---------- Admin Management (Owner Only) ----------
@bot.on_message(filters.command("addadmin") & filters.private)
async def addadmin_command(client: Client, message: Message):
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

@bot.on_message(filters.command("rmadmin") & filters.private)
async def rmadmin_command(client: Client, message: Message):
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

@bot.on_message(filters.command("listadmins") & filters.private)
async def listadmins_command(client: Client, message: Message):
    if not await is_admin(message.from_user.id):
        return await message.reply("⛔ Admin only.")
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

# ---------- Scraping & Adding ----------
@bot.on_message(filters.command("scrab") & filters.private)
async def scrab_command(client: Client, message: Message):
    if not await is_admin(message.from_user.id):
        return await message.reply("⛔ Admin only.")

    # Step 1: ask for groups to scrape
    q = await message.reply("📥 Send the group IDs / usernames / invite links to scrape members from.\nSeparate multiple by new line or comma.")
    response = await client.ask(message.chat.id, filters.text, timeout=120)
    if not response.text:
        return await message.reply("❌ No input received.")

    group_list = parse_group_identifier(response.text)
    if not group_list:
        return await message.reply("❌ No valid group identifiers.")

    # Step 2: check if we have at least one session
    sessions = await get_all_sessions()
    if not sessions:
        return await message.reply("❌ No session strings available. Add one with /addstring")

    # Step 3: scrape members
    status_msg = await message.reply("🔄 Scraping members... This may take a while.")
    all_members = []
    failed_groups = []

    # Use first session for scraping
    first_session = sessions[0]
    async with user_client(first_session["session_string"], "scraper") as scraper:
        for group in group_list:
            try:
                members = await scrape_members_from_group(scraper, group)
                all_members.extend(members)
                await status_msg.edit_text(f"✅ Scraped {len(members)} from {group}\nTotal so far: {len(all_members)}")
            except Exception as e:
                failed_groups.append(f"{group} ({str(e)[:50]})")
                await status_msg.edit_text(f"⚠️ Failed on {group}\nContinuing...")

    # remove duplicates
    all_members = list(set(all_members))
    total_scraped = len(all_members)

    # send log file to LOG_GROUP
    await send_log_file(client, LOG_GROUP, all_members, f"Scraped members - {len(group_list)} groups")

    # Step 4: ask for target group
    await status_msg.edit_text("📤 Now send the **target group ID/username** to add these members.")
    target_resp = await client.ask(message.chat.id, filters.text, timeout=120)
    if not target_resp.text:
        return await message.reply("❌ No target group received.")
    target_group = target_resp.text.strip()

    # Step 5: add members
    progress = await message.reply("⏳ Starting to add members...")
    try:
        result = await add_members_to_group(target_group, all_members, progress_message=progress)
    except Exception as e:
        return await progress.edit_text(f"❌ Adding failed: {e}")

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

@bot.on_message(filters.command("import") & filters.private)
async def import_command(client: Client, message: Message):
    if not await is_admin(message.from_user.id):
        return await message.reply("⛔ Admin only.")

    # Step 1: ask for .txt file
    q = await message.reply("📁 Send the `.txt` file containing user IDs (one per line).")
    response = await client.ask(message.chat.id, filters.document, timeout=120)
    if not response.document:
        return await message.reply("❌ No file received.")
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
        return await message.reply(f"❌ Error reading file: {e}")

    if not user_ids:
        return await message.reply("❌ No valid user IDs found in file.")

    # Step 2: ask for target group
    await message.reply(f"✅ Loaded {len(user_ids)} user IDs.\nNow send the **target group ID/username**.")
    target_resp = await client.ask(message.chat.id, filters.text, timeout=120)
    if not target_resp.text:
        return await message.reply("❌ No target group received.")
    target_group = target_resp.text.strip()

    # Step 3: add members
    progress = await message.reply("⏳ Adding members...")
    try:
        result = await add_members_to_group(target_group, user_ids, progress_message=progress)
    except Exception as e:
        return await progress.edit_text(f"❌ Adding failed: {e}")

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

# --------------------------------------------------------------
# Main entry
# --------------------------------------------------------------
async def main():
    print("Bot started.")
    await bot.run()

if __name__ == "__main__":
    asyncio.run(main())
