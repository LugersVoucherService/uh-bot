import discord
from discord import app_commands
from discord.ext import commands, tasks
import os
import json
import io
import re
from datetime import datetime, timedelta
from typing import Optional

from config import Config
from utils import APIClient, format_duration

BOT_START_TIME = datetime.now()
LOGS_FILE = 'command_logs.json'
VOUCHES_FILE = 'vouches.json'

config = Config()

COMMAND_LOGS = []
try:
    if os.path.exists(LOGS_FILE):
        with open(LOGS_FILE, 'r', encoding='utf-8') as f:
            COMMAND_LOGS = json.load(f)
except Exception as e:
    print(f"Warning: Could not load command logs: {e}")

MAX_LOGS = 5000

VOUCHES = {}
VOUCH_INDEX = {}
MAX_VOUCH_LOGS = 2000
MAX_VOUCHES_PER_USER = 1000
VOUCH_CHANNEL_ID = 1459965449709031636
TRUSTED_ROLE_ID = 1459956749162385529
STAFF_ROLE_IDS = {
    1459956324577312839,  # Moderator
    1459956032100106412,  # Head of Moderation
    1459955690943680572,  # Server Manager
    1459955117435654374,  # Developer
    1459955038574088337   # Owner
}

def load_vouches():
    global VOUCHES
    try:
        if os.path.exists(VOUCHES_FILE):
            with open(VOUCHES_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, dict):
                    VOUCHES = data
        # Build message index
        VOUCH_INDEX.clear()
        for user_id, record in VOUCHES.items():
            for entry in record.get("entries", []):
                msg_id = entry.get("message_id")
                if msg_id:
                    VOUCH_INDEX[str(msg_id)] = int(user_id)
    except Exception as e:
        print(f"Warning: Could not load vouches: {e}")

def save_vouches():
    try:
        with open(VOUCHES_FILE, 'w', encoding='utf-8') as f:
            json.dump(VOUCHES, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Error saving vouches: {e}")

def get_vouch_count(user_id: int) -> int:
    entry = VOUCHES.get(str(user_id), {})
    return int(entry.get("count", 0))

async def update_trusted_role(member: discord.Member):
    if not member:
        return
    count = get_vouch_count(member.id)
    trusted_role = member.guild.get_role(TRUSTED_ROLE_ID)
    if not trusted_role:
        return
    if count >= 5:
        if trusted_role not in member.roles:
            try:
                await member.add_roles(trusted_role, reason="Reached 5 vouches")
            except Exception as e:
                print(f"[WARN] Failed to add trusted role: {e}")
    else:
        if trusted_role in member.roles:
            try:
                await member.remove_roles(trusted_role, reason="Vouches dropped below 5")
            except Exception as e:
                print(f"[WARN] Failed to remove trusted role: {e}")

def save_logs():
    try:
        with open(LOGS_FILE, 'w', encoding='utf-8') as f:
            json.dump(COMMAND_LOGS[-MAX_LOGS:], f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Error saving logs: {e}")

intents = discord.Intents.default()
intents.members = True
intents.message_content = True
intents.guilds = True

bot = commands.Bot(command_prefix='/', intents=intents)
api_client = None

BOT_NAME = "Unknown Hub"
BOT_COLOR = discord.Color.from_rgb(102, 126, 234)
BOT_THUMBNAIL = "https://cdn.discordapp.com/attachments/1455604385244512510/1461478559456690451/image.png?ex=696ab379&is=696961f9&hm=06eb9a840579481101b1e9db5a42412f5387925695e1493ac442c5a42da4b3e4&"

COMMAND_LOGS = []
MAX_LOGS = 5000 

@bot.event
async def on_ready():
    global api_client
    
    print("=" * 70)
    print(f"[BOT] Connected as: {bot.user}")
    print(f"[BOT] Guilds: {len(bot.guilds)}")
    print(f"[BOT] Target Guild: {config.GUILD_ID}")
    print(f"[BOT] Admin Role: {config.ADMIN_ROLE_ID}")
    print(f"[BOT] API Base: {config.API_BASE}")
    print("=" * 70)
    
    api_client = APIClient(config.API_BASE, config.BOT_SECRET)
    print("[BOT] API client initialized")
    
    try:
        guild = discord.Object(id=config.GUILD_ID)
        synced = await bot.tree.sync(guild=guild)
        
        print(f"[BOT] Synced {len(synced)} command(s):")
        for cmd in synced:
            print(f"[BOT]   - /{cmd.name}")
            
    except discord.Forbidden:
        print(f"[ERROR] No permission to sync commands in guild {config.GUILD_ID}")
    except discord.HTTPException as e:
        print(f"[ERROR] Failed to sync commands: {e}")
    except Exception as e:
        print(f"[ERROR] Error during sync: {e}")
    
    if not cleanup_expired_keys.is_running():
        cleanup_expired_keys.start()
    load_vouches()

async def check_admin(interaction: discord.Interaction) -> bool:
    if interaction.guild_id != config.GUILD_ID:
        embed = discord.Embed(
            title="Invalid Guild",
            description="This command only works in the configured server.",
            color=discord.Color.red()
        )
        embed.set_footer(text=BOT_NAME)
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return False
    
    user_roles = [role.id for role in interaction.user.roles]
    if config.ADMIN_ROLE_ID not in user_roles:
        embed = discord.Embed(
            title="Insufficient Permissions",
            description=f"This command requires admin role.",
            color=discord.Color.red()
        )
        embed.set_footer(text=BOT_NAME)
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return False
    return True

async def check_dev(interaction: discord.Interaction) -> bool:
    if interaction.guild_id != config.GUILD_ID:
        embed = discord.Embed(
            title="Invalid Guild",
            description="This command only works in the configured server.",
            color=discord.Color.red()
        )
        embed.set_footer(text=BOT_NAME)
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return False
    
    user_roles = [role.id for role in interaction.user.roles]
    if 1459955117435654374 not in user_roles:
        embed = discord.Embed(
            title="Insufficient Permissions",
            description=f"This command requires developer role.",
            color=discord.Color.red()
        )
        embed.set_footer(text=BOT_NAME)
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return False
    return True

async def check_owner(interaction: discord.Interaction) -> bool:
    if interaction.guild_id != config.GUILD_ID:
        embed = discord.Embed(
            title="Invalid Guild",
            description="This command only works in the configured server.",
            color=discord.Color.red()
        )
        embed.set_footer(text=BOT_NAME)
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return False
    
    user_roles = [role.id for role in interaction.user.roles]
    if 1459955038574088337 not in user_roles:
        embed = discord.Embed(
            title="Insufficient Permissions",
            description=f"This command requires owner role.",
            color=discord.Color.red()
        )
        embed.set_footer(text=BOT_NAME)
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return False
    return True

def parse_duration(duration_str: str) -> Optional[tuple[int, str]]:
    duration_map = {
        '12H': (12 * 3600, '12 hours'),
        '1D': (1 * 86400, '1 day'),
        '7D': (7 * 86400, '7 days'),
        '30D': (30 * 86400, '30 days'),
        '365D': (365 * 86400, '1 year'),
        'LIFE': (365 * 86400 * 5, '5 years'),
    }
    
    return duration_map.get(duration_str.upper())

def parse_blacklist_duration(duration_str: str) -> Optional[tuple[int, str]]:
    duration_map = {
        '15M': (15 * 60, '15 minutes'),
        '1H': (1 * 3600, '1 hour'),
        '6H': (6 * 3600, '6 hours'),
        '1D': (1 * 86400, '1 day'),
        '7D': (7 * 86400, '7 days'),
    }
    return duration_map.get(duration_str.upper())

@bot.tree.command(
    name='givekey',
    description='Give a license key to someone',
    guilds=[discord.Object(id=config.GUILD_ID)]
)
@app_commands.describe(
    user='The user to give the key to',
    duration='Duration: 12h, 1d, 7d, 30d, 365d, or LIFE'
)
@app_commands.choices(duration=[
    app_commands.Choice(name='12 Hours', value='12h'),
    app_commands.Choice(name='1 Day', value='1d'),
    app_commands.Choice(name='7 Days', value='7d'),
    app_commands.Choice(name='30 Days', value='30d'),
    app_commands.Choice(name='1 Year', value='365d'),
    app_commands.Choice(name='LIFE (5 Years)', value='LIFE'),
])
async def givekey(interaction: discord.Interaction, user: discord.User, duration: str):
    print(f"[CMD] /givekey invoked by {interaction.user.name} for {user.name} | duration={duration}")
    
    if not await check_admin(interaction):
        return
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        log_command('givekey', interaction.user.id, interaction.user.name, user.id, user.name, {'duration': duration})
        duration_str = str(duration).strip().upper()
        duration_result = parse_duration(duration_str)
        
        if not duration_result:
            print(f"[CMD] Invalid duration: {duration}")
            embed = discord.Embed(
                title="Invalid Duration",
                description="Valid options: 12h, 1d, 7d, 30d, 365d, or LIFE",
                color=discord.Color.red()
            )
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        
        duration_seconds, duration_human = duration_result
        print(f"[CMD] Duration: {duration_human} ({duration_seconds}s)")
        
        key_response = await api_client.create_key(
            duration_seconds=duration_seconds,
            discord_user_id=str(user.id)
        )
        
        if not key_response or not key_response.get('key'):
            print(f"[ERROR] Key creation failed for {user.name}")
            embed = discord.Embed(
                title="Key Creation Failed",
                description="Unable to create key. Check API connectivity.",
                color=discord.Color.red()
            )
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        
        new_key = key_response['key']
        expiry = key_response.get('expiry_timestamp')
        print(f"[OK] Key created: {new_key[:20]}... for {user.name}")
        
        dm_status = "Sent"
        try:
            embed = discord.Embed(
                title="License Key Issued",
                description=f"You have received a license key from {interaction.user.name}",
                color=BOT_COLOR,
                timestamp=datetime.now()
            )
            embed.add_field(name="Key", value=f"`{new_key}`", inline=False)
            embed.add_field(name="Duration", value=duration_human, inline=True)
            embed.add_field(name="Expires", value=f"<t:{int(datetime.fromisoformat(expiry).timestamp())}:R>", inline=True)
            embed.add_field(
                name="Instructions",
                value="1. Visit https://unknownhub.vercel.app/\n2. Enter your key\n3. Follow the setup steps",
                inline=False
            )
            embed.set_footer(text=f"{BOT_NAME} - Keep your key secure")
            
            await user.send(embed=embed)
        except discord.Forbidden:
            dm_status = "DMs Disabled"
            print(f"[WARN] DMs disabled for {user.name}")
        except Exception as e:
            dm_status = f"Failed: {str(e)[:30]}"
            print(f"[WARN] Error sending DM: {e}")
        
        embed = discord.Embed(
            title="Key Created Successfully",
            color=discord.Color.green(),
            timestamp=datetime.now()
        )
        embed.add_field(name="User", value=f"{user.mention} ({user.id})", inline=False)
        embed.add_field(name="Key", value=f"`{new_key}`", inline=False)
        embed.add_field(name="Duration", value=duration_human, inline=True)
        embed.add_field(name="Expires", value=f"<t:{int(datetime.fromisoformat(expiry).timestamp())}:R>", inline=True)
        embed.add_field(name="DM Status", value=dm_status, inline=True)
        embed.set_footer(text=BOT_NAME)
        
        await interaction.followup.send(embed=embed, ephemeral=True)
        
        audit_channel = bot.get_channel(config.AUDIT_CHANNEL_ID) if config.AUDIT_CHANNEL_ID else None
        if audit_channel:
            audit_embed = discord.Embed(
                title="Key Issued",
                color=BOT_COLOR,
                timestamp=datetime.now()
            )
            audit_embed.add_field(name="Issued By", value=interaction.user.mention, inline=False)
            audit_embed.add_field(name="User", value=f"{user.mention} ({user.id})", inline=False)
            audit_embed.add_field(name="Duration", value=duration_human, inline=True)
            audit_embed.add_field(name="Key (Masked)", value=f"`{new_key[:8]}...{new_key[-8:]}`", inline=True)
            audit_embed.set_footer(text=BOT_NAME)
            
            try:
                await audit_channel.send(embed=audit_embed)
            except Exception as e:
                print(f"[WARN] Audit log failed: {e}")
    
    except Exception as e:
        print(f"[ERROR] /givekey failed: {e}")
        embed = discord.Embed(
            title="Error",
            description=f"Command failed: {str(e)[:100]}",
            color=discord.Color.red()
        )
        embed.set_footer(text=BOT_NAME)
        try:
            await interaction.followup.send(embed=embed, ephemeral=True)
        except:
            pass

def log_command(command_name: str, executor_id: int, executor_name: str, target_user_id: int = None, target_user_name: str = None, details: dict = None):
    log_entry = {
        'timestamp': datetime.now().isoformat(),
        'command': command_name,
        'executor_id': executor_id,
        'executor_name': executor_name,
        'target_user_id': target_user_id,
        'target_user_name': target_user_name,
        'details': details or {}
    }
    COMMAND_LOGS.append(log_entry)
    if len(COMMAND_LOGS) > MAX_LOGS:
        COMMAND_LOGS.pop(0)
    save_logs()
    print(f"[LOG] {command_name} by {executor_name} on {target_user_name or 'N/A'}")

class LogPaginator:
    def __init__(self, items, page_size=5):
        self.items = items
        self.page_size = page_size
        self.total_pages = (len(items) + page_size - 1) // page_size
    
    def get_page(self, page_num):
        if page_num < 0 or page_num >= self.total_pages:
            return None
        start = page_num * self.page_size
        return self.items[start:start + self.page_size]

@bot.tree.command(name='suspendkey', description='Suspend a license key', guilds=[discord.Object(id=config.GUILD_ID)])
@app_commands.describe(key='The license key to suspend')
async def suspendkey(interaction: discord.Interaction, key: str):
    if not await check_admin(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    log_command('suspendkey', interaction.user.id, interaction.user.name, details={'key': key[:8]})
    try:
        response = await api_client.suspend_key(key)
        if response and response.get('success'):
            embed = discord.Embed(title="Success", description=f"Key suspended: {key[:8]}...", color=discord.Color.orange())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
            print(f"[OK] Key suspended: {key[:8]}...")
        else:
            embed = discord.Embed(title="Failed", description="Could not suspend key", color=discord.Color.red())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        print(f"[ERROR] suspendkey failed: {e}")
        embed = discord.Embed(title="Error", description=str(e)[:100], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='unsuspendkey', description='Unsuspend a license key', guilds=[discord.Object(id=config.GUILD_ID)])
@app_commands.describe(key='The license key to unsuspend')
async def unsuspendkey(interaction: discord.Interaction, key: str):
    if not await check_admin(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    log_command('unsuspendkey', interaction.user.id, interaction.user.name, details={'key': key[:8]})
    try:
        response = await api_client.unsuspend_key(key)
        if response and response.get('success'):
            embed = discord.Embed(title="Success", description=f"Key unsuspended: {key[:8]}...", color=discord.Color.green())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
            print(f"[OK] Key unsuspended: {key[:8]}...")
        else:
            embed = discord.Embed(title="Failed", description="Could not unsuspend key", color=discord.Color.red())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        print(f"[ERROR] unsuspendkey failed: {e}")
        embed = discord.Embed(title="Error", description=str(e)[:100], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='deletekey', description='Delete a license key', guilds=[discord.Object(id=config.GUILD_ID)])
@app_commands.describe(key='The license key to delete')
async def deletekey(interaction: discord.Interaction, key: str):
    if not await check_admin(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    log_command('deletekey', interaction.user.id, interaction.user.name, details={'key': key[:8]})
    try:
        response = await api_client.delete_key(key)
        if response and response.get('success'):
            embed = discord.Embed(title="Success", description=f"Key permanently deleted: {key[:8]}...", color=discord.Color.red())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
            print(f"[OK] Key deleted: {key[:8]}...")
        else:
            embed = discord.Embed(title="Failed", description="Could not delete key", color=discord.Color.red())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        print(f"[ERROR] deletekey failed: {e}")
        embed = discord.Embed(title="Error", description=str(e)[:100], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='clearkey', description='Reset HWID from a key', guilds=[discord.Object(id=config.GUILD_ID)])
@app_commands.describe(key='The license key to reset')
async def clearkey(interaction: discord.Interaction, key: str):
    if not await check_admin(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    log_command('clearkey', interaction.user.id, interaction.user.name, details={'key': key[:8]})
    try:
        response = await api_client.clear_key(key)
        if response and response.get('success'):
            embed = discord.Embed(title="Success", description=f"HWID cleared from {key[:8]}...\nKey can now be used on another device.", color=discord.Color.blue())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
            print(f"[OK] Key HWID cleared: {key[:8]}...")
        else:
            embed = discord.Embed(title="Failed", description="Could not reset key", color=discord.Color.red())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        print(f"[ERROR] clearkey failed: {e}")
        embed = discord.Embed(title="Error", description=str(e)[:100], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='blacklist', description='Manage redeem blacklist', guilds=[discord.Object(id=config.GUILD_ID)])
@app_commands.describe(
    action='add, remove, or list',
    user='User to act on (required for add/remove)',
    duration='Duration (15m, 1h, 6h, 1d, 7d) for add'
)
@app_commands.choices(action=[
    app_commands.Choice(name='Add', value='add'),
    app_commands.Choice(name='Remove', value='remove'),
    app_commands.Choice(name='List', value='list'),
])
@app_commands.choices(duration=[
    app_commands.Choice(name='15 minutes', value='15m'),
    app_commands.Choice(name='1 hour', value='1h'),
    app_commands.Choice(name='6 hours', value='6h'),
    app_commands.Choice(name='1 day', value='1d'),
    app_commands.Choice(name='7 days', value='7d'),
])
async def blacklist(interaction: discord.Interaction, action: app_commands.Choice[str], user: discord.User = None, duration: str = '1h'):
    if not await check_admin(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    action_val = action.value
    log_command('blacklist', interaction.user.id, interaction.user.name, target_user_id=user.id if user else None, target_user_name=user.name if user else None, details={'action': action_val})
    try:
        if action_val == 'list':
            resp = await api_client.manage_blacklist('list')
            entries = resp.get("blacklist", []) if resp else []
            embed = discord.Embed(title="Active Blacklist", color=BOT_COLOR)
            if not entries:
                embed.description = "No active blacklist entries."
            else:
                for entry in entries:
                    uid = entry.get("discord_user_id")
                    rem = int(entry.get("seconds_remaining", 0))
                    expires_at = entry.get("expires_at")
                    embed.add_field(
                        name=f"User {uid}",
                        value=f"Expires in {format_duration(rem)}",
                        inline=False
                    )
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        if not user:
            embed = discord.Embed(title="User Required", description="Specify a user for add/remove.", color=discord.Color.red())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        if action_val == 'remove':
            resp = await api_client.manage_blacklist('remove', str(user.id))
            ok = resp and resp.get("success")
            embed = discord.Embed(
                title="Blacklist Removed" if ok else "Remove Failed",
                description=f"{user.mention} removed from blacklist" if ok else "Could not remove",
                color=discord.Color.green() if ok else discord.Color.red()
            )
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # add
        dur = parse_blacklist_duration(duration) or parse_blacklist_duration('1h')
        dur_seconds, dur_human = dur
        resp = await api_client.manage_blacklist('add', str(user.id), dur_seconds)
        ok = resp and resp.get("success")
        expires_at = resp.get("expires_at") if resp else None
        expires_text = f"<t:{int(expires_at)}:R>" if expires_at else "unknown"
        embed = discord.Embed(
            title="User Blacklisted" if ok else "Blacklist Failed",
            description=f"{user.mention} blocked for {dur_human}\nExpires {expires_text}",
            color=discord.Color.orange() if ok else discord.Color.red()
        )
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        print(f"[ERROR] blacklist failed: {e}")
        embed = discord.Embed(title="Error", description=str(e)[:100], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='modifykey', description='Modify key owner/status/duration', guilds=[discord.Object(id=config.GUILD_ID)])
@app_commands.describe(
    key='Key to modify',
    new_owner='New Discord user to bind (optional)',
    duration='New duration (optional)',
    status='New status (optional)'
)
@app_commands.choices(duration=[
    app_commands.Choice(name='12 Hours', value='12h'),
    app_commands.Choice(name='1 Day', value='1d'),
    app_commands.Choice(name='7 Days', value='7d'),
    app_commands.Choice(name='30 Days', value='30d'),
    app_commands.Choice(name='1 Year', value='365d'),
    app_commands.Choice(name='LIFE (5 Years)', value='LIFE'),
])
@app_commands.choices(status=[
    app_commands.Choice(name='Pre-activated', value='pre-activated'),
    app_commands.Choice(name='Redeemed', value='redeemed'),
    app_commands.Choice(name='Activated', value='activated'),
    app_commands.Choice(name='Suspended', value='suspended'),
])
async def modifykey(interaction: discord.Interaction, key: str, new_owner: discord.User = None, duration: str = None, status: app_commands.Choice[str] = None):
    if not await check_admin(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    log_command('modifykey', interaction.user.id, interaction.user.name, details={'key': key[:8], 'new_owner': new_owner.id if new_owner else None, 'duration': duration, 'status': status.value if status else None})
    try:
        payload = {}
        if new_owner:
            payload['discord_user_id'] = str(new_owner.id)
        if duration:
            parsed = parse_duration(duration)
            if not parsed:
                embed = discord.Embed(title="Invalid Duration", description="Choose a supported duration.", color=discord.Color.red())
                embed.set_footer(text=BOT_NAME)
                await interaction.followup.send(embed=embed, ephemeral=True)
                return
            payload['duration_seconds'] = parsed[0]
        if status:
            payload['status'] = status.value

        if not payload:
            embed = discord.Embed(title="No Changes", description="Provide at least one field to modify.", color=discord.Color.orange())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        resp = await api_client.modify_key(key, payload)
        if resp and resp.get("success"):
            embed = discord.Embed(title="Key Updated", color=discord.Color.green(), timestamp=datetime.now())
            embed.add_field(name="Key", value=key[:8] + "...", inline=False)
            if new_owner:
                embed.add_field(name="New Owner", value=f"{new_owner.mention} ({new_owner.id})", inline=False)
            if duration:
                embed.add_field(name="Duration", value=parse_duration(duration)[1], inline=True)
            if status:
                embed.add_field(name="Status", value=status.value, inline=True)
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            embed = discord.Embed(title="Modify Failed", description=str(resp), color=discord.Color.red())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        print(f"[ERROR] modifykey failed: {e}")
        embed = discord.Embed(title="Error", description=str(e)[:100], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='mergekeys', description='Merge duration from source key into target key', guilds=[discord.Object(id=config.GUILD_ID)])
@app_commands.describe(source_key='Key to consume', target_key='Key to extend')
async def mergekeys(interaction: discord.Interaction, source_key: str, target_key: str):
    if not await check_admin(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    log_command('mergekeys', interaction.user.id, interaction.user.name, details={'source': source_key[:8], 'target': target_key[:8]})
    try:
        resp = await api_client.merge_keys(source_key, target_key)
        if resp and resp.get("success"):
            embed = discord.Embed(title="Keys Merged", color=BOT_COLOR, timestamp=datetime.now())
            embed.add_field(name="Source", value=source_key[:8] + "...", inline=True)
            embed.add_field(name="Target", value=target_key[:8] + "...", inline=True)
            embed.add_field(name="New Duration", value=str(resp.get("target_duration_seconds")), inline=False)
            embed.add_field(name="New Expiry", value=str(resp.get("target_expiry_timestamp")), inline=False)
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            embed = discord.Embed(title="Merge Failed", description=str(resp), color=discord.Color.red())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        print(f"[ERROR] mergekeys failed: {e}")
        embed = discord.Embed(title="Error", description=str(e)[:100], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='bulkgenerate', description='Generate multiple keys of a duration (no owner)', guilds=[discord.Object(id=config.GUILD_ID)])
@app_commands.describe(
    duration='Duration: 12h, 1d, 7d, 30d, 365d, LIFE',
    count='How many keys to generate (max 200)'
)
@app_commands.choices(duration=[
    app_commands.Choice(name='12 Hours', value='12h'),
    app_commands.Choice(name='1 Day', value='1d'),
    app_commands.Choice(name='7 Days', value='7d'),
    app_commands.Choice(name='30 Days', value='30d'),
    app_commands.Choice(name='1 Year', value='365d'),
    app_commands.Choice(name='LIFE (5 Years)', value='LIFE'),
])
async def bulkgenerate(interaction: discord.Interaction, duration: str, count: int = 10):
    if not await check_admin(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    log_command('bulkgenerate', interaction.user.id, interaction.user.name, details={'duration': duration, 'count': count})
    try:
        parsed = parse_duration(duration)
        if not parsed:
            embed = discord.Embed(title="Invalid Duration", description="Use 12h, 1d, 7d, 30d, 365d, or LIFE.", color=discord.Color.red())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        duration_seconds, duration_human = parsed
        count = max(1, min(count, 200))
        keys = []
        failures = 0
        for _ in range(count):
            resp = await api_client.create_key(duration_seconds=duration_seconds, discord_user_id=None)
            if resp and resp.get('key'):
                keys.append(resp['key'])
            else:
                failures += 1
        if not keys:
            embed = discord.Embed(title="Generation Failed", description="No keys were created. Check API connectivity.", color=discord.Color.red())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        file_content = "\n".join(keys)
        file = discord.File(fp=io.BytesIO(file_content.encode('utf-8')), filename=f"keys_{duration.lower()}_{len(keys)}.txt")
        desc = f"Generated **{len(keys)}** key(s) for {duration_human}."
        if failures:
            desc += f" Failed: {failures}"
        embed = discord.Embed(title="Bulk Keys Generated", description=desc, color=BOT_COLOR)
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, file=file, ephemeral=True)
    except Exception as e:
        print(f"[ERROR] bulkgenerate failed: {e}")
        embed = discord.Embed(title="Error", description=str(e)[:120], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='pruneexpired', description='Delete expired keys only', guilds=[discord.Object(id=config.GUILD_ID)])
async def pruneexpired(interaction: discord.Interaction):
    if not await check_admin(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    log_command('pruneexpired', interaction.user.id, interaction.user.name)
    try:
        response = await api_client.prune_expired_keys()
        if response and response.get('success'):
            scanned = response.get('accounts_scanned', 0)
            deleted = response.get('keys_deleted', 0)
            embed = discord.Embed(
                title="Expired Keys Pruned",
                description=f"Accounts scanned: **{scanned}**\nKeys deleted: **{deleted}**",
                color=discord.Color.green()
            )
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
            print(f"[OK] Pruned expired keys: {deleted} deleted")
        else:
            embed = discord.Embed(title="Failed", description="Could not prune expired keys.", color=discord.Color.red())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        print(f"[ERROR] pruneexpired failed: {e}")
        embed = discord.Embed(title="Error", description=str(e)[:100], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='setsetting', description='Enable or disable a setting', guilds=[discord.Object(id=config.GUILD_ID)])
@app_commands.describe(
    setting='Setting to update',
    enabled='Enable or disable'
)
@app_commands.choices(setting=[
    app_commands.Choice(name='session_tokens_enabled', value='session_tokens_enabled'),
    app_commands.Choice(name='allow_shitty_unchwid', value='allow_shitty_unchwid'),
    app_commands.Choice(name='script_cache_enabled', value='script_cache_enabled'),
    app_commands.Choice(name='enforce_roblox_ua', value='enforce_roblox_ua'),
])
async def setsetting(interaction: discord.Interaction, setting: app_commands.Choice[str], enabled: bool):
    if not await check_admin(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    log_command('setsetting', interaction.user.id, interaction.user.name, details={'setting': setting.value, 'enabled': enabled})
    try:
        payload = {setting.value: enabled}
        response = await api_client.update_settings(payload)
        if response and response.get('success'):
            embed = discord.Embed(
                title="Setting Updated",
                description=f"`{setting.value}` set to **{enabled}**",
                color=discord.Color.green()
            )
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            embed = discord.Embed(title="Failed", description="Could not update settings.", color=discord.Color.red())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        print(f"[ERROR] setsetting failed: {e}")
        embed = discord.Embed(title="Error", description=str(e)[:100], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='setloader', description='Update loader-related settings', guilds=[discord.Object(id=config.GUILD_ID)])
@app_commands.describe(
    script_cache_ttl_seconds='30-3600 seconds',
    max_active_keys_per_account='1-50',
    hwid_reset_cooldown_hours='1-168 hours'
)
async def setloader(interaction: discord.Interaction, script_cache_ttl_seconds: int = None, max_active_keys_per_account: int = None, hwid_reset_cooldown_hours: float = None):
    if not await check_admin(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    payload = {}
    if script_cache_ttl_seconds is not None:
        payload["script_cache_ttl_seconds"] = script_cache_ttl_seconds
    if max_active_keys_per_account is not None:
        payload["max_active_keys_per_account"] = max_active_keys_per_account
    if hwid_reset_cooldown_hours is not None:
        payload["hwid_reset_cooldown_hours"] = hwid_reset_cooldown_hours
    if not payload:
        embed = discord.Embed(title="No Changes", description="Provide at least one setting to update.", color=discord.Color.orange())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)
        return
    try:
        resp = await api_client.update_settings(payload)
        if resp and resp.get("success"):
            pretty = "\n".join(f"{k}: {v}" for k, v in resp.get("settings", {}).items())
            embed = discord.Embed(title="Loader Settings Updated", description=pretty, color=discord.Color.green())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            embed = discord.Embed(title="Update Failed", description=str(resp), color=discord.Color.red())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        print(f"[ERROR] setloader failed: {e}")
        embed = discord.Embed(title="Error", description=str(e)[:100], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='keyinfo', description='View info about a specific key', guilds=[discord.Object(id=config.GUILD_ID)])
@app_commands.describe(key='The license key to look up')
async def keyinfo(interaction: discord.Interaction, key: str):
    if not await check_admin(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    log_command('keyinfo', interaction.user.id, interaction.user.name, details={'key': key[:8]})
    try:
        info = await api_client.key_info(key)
        if not info or info.get('error'):
            embed = discord.Embed(
                title="Key Not Found",
                description=info.get('error', 'Unable to retrieve key info'),
                color=discord.Color.red()
            )
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        
        embed = discord.Embed(
            title="Key Information",
            description=f"Key: {key[:8]}...{key[-8:]}",
            color=BOT_COLOR,
            timestamp=datetime.now()
        )
        embed.add_field(name="Status", value=info.get("status", "Unknown"), inline=True)
        embed.add_field(name="HWID Set", value="Yes" if info.get("hwid_set") else "No", inline=True)
        embed.add_field(name="Reset Count", value=str(info.get("reset_count", 0)), inline=True)
        created_at = info.get("created_at")
        expires_at = info.get("activation_expires_at") or info.get("expiry_timestamp")
        created_text = f"<t:{int(datetime.fromisoformat(created_at).timestamp())}:f>" if created_at else "Unknown"
        expires_text = f"<t:{int(datetime.fromisoformat(expires_at).timestamp())}:R>" if expires_at else "Unknown"
        embed.add_field(name="Created", value=created_text, inline=False)
        embed.add_field(name="Expires", value=expires_text, inline=False)
        if info.get("discord_user_id"):
            embed.add_field(name="Discord User ID", value=info.get("discord_user_id"), inline=True)
        if info.get("email"):
            embed.add_field(name="Email", value=info.get("email"), inline=True)
        if info.get("location"):
            loc = info["location"]
            location_text = f"{loc.get('city', 'Unknown')}, {loc.get('region', 'Unknown')}, {loc.get('country', 'Unknown')}"
            embed.add_field(name="Location", value=location_text, inline=False)
        embed.add_field(
            name="Signature History",
            value=str(info.get("signature_history_count", 0)),
            inline=True
        )
        embed.set_thumbnail(url=BOT_THUMBNAIL)
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        print(f"[ERROR] keyinfo failed: {e}")
        embed = discord.Embed(title="Error", description=str(e)[:100], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='keystats', description='View key statistics', guilds=[discord.Object(id=config.GUILD_ID)])
async def keystats(interaction: discord.Interaction):
    if not await check_dev(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    try:
        stats = await api_client.key_stats()
        if not stats or stats.get('error'):
            embed = discord.Embed(
                title="Stats Unavailable",
                description=stats.get('error', 'Unable to retrieve stats'),
                color=discord.Color.red()
            )
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        embed = discord.Embed(title="Key Statistics", color=BOT_COLOR, timestamp=datetime.now())
        embed.add_field(name="Total Keys", value=str(stats.get("total_keys", 0)), inline=True)
        embed.add_field(name="Latest Modified", value=stats.get("latest_modified", "Unknown"), inline=True)
        embed.set_thumbnail(url=BOT_THUMBNAIL)
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        print(f"[ERROR] keystats failed: {e}")
        embed = discord.Embed(title="Error", description=str(e)[:100], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='apistatus', description='Check API status', guilds=[discord.Object(id=config.GUILD_ID)])
async def apistatus(interaction: discord.Interaction):
    if not await check_dev(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    try:
        health = await api_client.health()
        status_text = health.get("status", "unknown") if health else "unknown"
        color = discord.Color.green() if status_text == "ok" else discord.Color.red()
        embed = discord.Embed(title="API Status", color=color, timestamp=datetime.now())
        embed.add_field(name="Status", value=status_text, inline=True)
        embed.add_field(name="API Base", value=config.API_BASE, inline=False)
        embed.set_thumbnail(url=BOT_THUMBNAIL)
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        print(f"[ERROR] apistatus failed: {e}")
        embed = discord.Embed(title="Error", description=str(e)[:100], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='apisettings', description='View API settings', guilds=[discord.Object(id=config.GUILD_ID)])
async def apisettings(interaction: discord.Interaction):
    if not await check_dev(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    try:
        settings = await api_client.get_session_tokens()
        enabled = settings.get("session_tokens_enabled", False) if settings else False
        embed = discord.Embed(title="API Settings", color=BOT_COLOR, timestamp=datetime.now())
        embed.add_field(name="Session Tokens", value="Enabled" if enabled else "Disabled", inline=True)
        embed.set_thumbnail(url=BOT_THUMBNAIL)
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        print(f"[ERROR] apisettings failed: {e}")
        embed = discord.Embed(title="Error", description=str(e)[:100], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='vouchstats', description='View vouch stats for a user', guilds=[discord.Object(id=config.GUILD_ID)])
@app_commands.describe(user='User to check')
async def vouchstats(interaction: discord.Interaction, user: discord.User):
    if not await check_dev(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    try:
        count = get_vouch_count(user.id)
        embed = discord.Embed(title="Vouch Stats", color=BOT_COLOR, timestamp=datetime.now())
        embed.add_field(name="User", value=f"{user.mention} ({user.id})", inline=False)
        embed.add_field(name="Vouches", value=str(count), inline=True)
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        print(f"[ERROR] vouchstats failed: {e}")
        embed = discord.Embed(title="Error", description=str(e)[:100], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='topvouches', description='Leaderboard for vouches', guilds=[discord.Object(id=config.GUILD_ID)])
async def topvouches(interaction: discord.Interaction):
    if not await check_dev(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    try:
        sorted_vouches = sorted(VOUCHES.items(), key=lambda item: item[1].get("count", 0), reverse=True)
        top = sorted_vouches[:10]
        if not top:
            embed = discord.Embed(title="Top Vouches", description="No vouches yet.", color=BOT_COLOR)
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        embed = discord.Embed(title="Top Vouches", color=BOT_COLOR, timestamp=datetime.now())
        for idx, (user_id, record) in enumerate(top, start=1):
            count = record.get("count", 0)
            member = interaction.guild.get_member(int(user_id))
            name = member.mention if member else f"<@{user_id}>"
            embed.add_field(name=f"#{idx} {name}", value=f"{count} vouches", inline=False)
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        print(f"[ERROR] topvouches failed: {e}")
        embed = discord.Embed(title="Error", description=str(e)[:100], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='uploadscript', description='Upload obfuscated script to API', guilds=[discord.Object(id=config.GUILD_ID)])
@app_commands.describe(attachment='Lua file to upload')
async def uploadscript(interaction: discord.Interaction, attachment: discord.Attachment):
    if not await check_owner(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    log_command('uploadscript', interaction.user.id, interaction.user.name)
    try:
        if not attachment:
            embed = discord.Embed(title="Missing File", description="Attach a .lua file.", color=discord.Color.red())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        allowed_ext = ('.lua', '.luau', '.txt')
        if not attachment.filename.lower().endswith(allowed_ext):
            embed = discord.Embed(title="Invalid File", description="Only .lua, .luau, or .txt files are allowed.", color=discord.Color.red())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        file_bytes = await attachment.read()
        script_text = file_bytes.decode('utf-8', errors='ignore')
        response = await api_client.upload_script(script_text, attachment.filename)
        if response and response.get('success'):
            embed = discord.Embed(title="Script Uploaded", color=discord.Color.green(), timestamp=datetime.now())
            embed.add_field(name="Size", value=f"{response.get('size', 0)} bytes", inline=True)
            embed.add_field(name="File", value=attachment.filename, inline=True)
            embed.set_thumbnail(url=BOT_THUMBNAIL)
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            embed = discord.Embed(title="Upload Failed", description=str(response), color=discord.Color.red())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        print(f"[ERROR] uploadscript failed: {e}")
        embed = discord.Embed(title="Error", description=str(e)[:100], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='updatescript', description='Update/overwrite a script in API storage', guilds=[discord.Object(id=config.GUILD_ID)])
@app_commands.describe(attachment='Lua file to upload', filename='Filename to save (optional, defaults to attachment name)')
async def updatescript(interaction: discord.Interaction, attachment: discord.Attachment, filename: str = None):
    if not await check_owner(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    log_command('updatescript', interaction.user.id, interaction.user.name)
    try:
        if not attachment:
            embed = discord.Embed(title="Missing File", description="Attach a .lua file.", color=discord.Color.red())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        target_name = filename.strip() if filename else attachment.filename
        allowed_ext = ('.lua', '.luau', '.txt')
        if not target_name.lower().endswith(allowed_ext):
            embed = discord.Embed(title="Invalid File", description="Only .lua, .luau, or .txt files are allowed.", color=discord.Color.red())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        file_bytes = await attachment.read()
        script_text = file_bytes.decode('utf-8', errors='ignore')
        response = await api_client.upload_script(script_text, target_name)
        if response and response.get('success'):
            embed = discord.Embed(title="Script Updated", color=discord.Color.green(), timestamp=datetime.now())
            embed.add_field(name="File", value=target_name, inline=True)
            embed.add_field(name="Size", value=f"{response.get('size', 0)} bytes", inline=True)
            embed.set_thumbnail(url=BOT_THUMBNAIL)
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            embed = discord.Embed(title="Update Failed", description=str(response), color=discord.Color.red())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        print(f"[ERROR] updatescript failed: {e}")
        embed = discord.Embed(title="Error", description=str(e)[:100], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='removescript', description='Remove a script from API storage', guilds=[discord.Object(id=config.GUILD_ID)])
@app_commands.describe(filename='Filename to delete (e.g., main.lua)')
async def removescript(interaction: discord.Interaction, filename: str):
    if not await check_owner(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    log_command('removescript', interaction.user.id, interaction.user.name, details={'file': filename})
    try:
        resp = await api_client.delete_script(filename)
        if resp and resp.get("success"):
            embed = discord.Embed(title="Script Removed", color=discord.Color.orange(), timestamp=datetime.now())
            embed.add_field(name="File", value=filename, inline=False)
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            embed = discord.Embed(title="Remove Failed", description=str(resp), color=discord.Color.red())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        print(f"[ERROR] removescript failed: {e}")
        embed = discord.Embed(title="Error", description=str(e)[:100], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='listscripts', description='List stored scripts', guilds=[discord.Object(id=config.GUILD_ID)])
async def listscripts(interaction: discord.Interaction):
    if not await check_owner(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    log_command('listscripts', interaction.user.id, interaction.user.name)
    try:
        resp = await api_client.list_scripts()
        scripts = resp.get("scripts", []) if resp else []
        if not scripts:
            embed = discord.Embed(title="Scripts", description="No scripts found.", color=BOT_COLOR)
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        scripts = sorted(scripts, key=lambda s: s.get("name", ""))
        page_size = 10
        total_pages = (len(scripts) + page_size - 1) // page_size

        def build_embed(pg: int) -> discord.Embed:
            start = pg * page_size
            chunk = scripts[start:start + page_size]
            embed = discord.Embed(
                title="Scripts",
                description=f"Page {pg+1}/{total_pages} • Total: {len(scripts)}",
                color=BOT_COLOR
            )
            for s in chunk:
                name = s.get("name")
                size = s.get("size", 0)
                lm = s.get("last_modified")
                lm_text = f"<t:{int(datetime.fromisoformat(lm).timestamp())}:R>" if lm else "unknown"
                embed.add_field(name=name, value=f"Size: {size} bytes\nUpdated: {lm_text}", inline=False)
            embed.set_footer(text=BOT_NAME)
            return embed

        class ScriptPager(discord.ui.View):
            def __init__(self):
                super().__init__(timeout=60)
                self.pg = 0

            async def update(self, interaction: discord.Interaction):
                await interaction.response.edit_message(embed=build_embed(self.pg), view=self)

            @discord.ui.button(label="⏮️ First", style=discord.ButtonStyle.secondary)
            async def first(self, interaction: discord.Interaction, button: discord.ui.Button):
                self.pg = 0
                await self.update(interaction)

            @discord.ui.button(label="◀️ Prev", style=discord.ButtonStyle.secondary)
            async def prev(self, interaction: discord.Interaction, button: discord.ui.Button):
                if self.pg > 0:
                    self.pg -= 1
                await self.update(interaction)

            @discord.ui.button(label="▶️ Next", style=discord.ButtonStyle.secondary)
            async def next(self, interaction: discord.Interaction, button: discord.ui.Button):
                if self.pg < total_pages - 1:
                    self.pg += 1
                await self.update(interaction)

            @discord.ui.button(label="⏭️ Last", style=discord.ButtonStyle.secondary)
            async def last(self, interaction: discord.Interaction, button: discord.ui.Button):
                self.pg = total_pages - 1
                await self.update(interaction)

        view = ScriptPager()
        await interaction.followup.send(embed=build_embed(0), view=view, ephemeral=True)
    except Exception as e:
        print(f"[ERROR] listscripts failed: {e}")
        embed = discord.Embed(title="Error", description=str(e)[:100], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)
@bot.tree.command(name='enable', description='Enable a feature flag', guilds=[discord.Object(id=config.GUILD_ID)])
@app_commands.describe(feature='Feature name (session-tokens)')
async def enable_feature(interaction: discord.Interaction, feature: str):
    if not await check_owner(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    log_command('enable', interaction.user.id, interaction.user.name, details={'feature': feature})
    try:
        if feature.lower() != 'session-tokens':
            embed = discord.Embed(title="Unknown Feature", description="Supported: session-tokens", color=discord.Color.red())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        response = await api_client.set_session_tokens(True)
        if response and response.get('success'):
            embed = discord.Embed(title="Session Tokens Enabled", color=discord.Color.green(), timestamp=datetime.now())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            embed = discord.Embed(title="Enable Failed", description=str(response), color=discord.Color.red())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        print(f"[ERROR] enable failed: {e}")
        embed = discord.Embed(title="Error", description=str(e)[:100], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='disable', description='Disable a feature flag', guilds=[discord.Object(id=config.GUILD_ID)])
@app_commands.describe(feature='Feature name (session-tokens)')
async def disable_feature(interaction: discord.Interaction, feature: str):
    if not await check_owner(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    log_command('disable', interaction.user.id, interaction.user.name, details={'feature': feature})
    try:
        if feature.lower() != 'session-tokens':
            embed = discord.Embed(title="Unknown Feature", description="Supported: session-tokens", color=discord.Color.red())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        response = await api_client.set_session_tokens(False)
        if response and response.get('success'):
            embed = discord.Embed(title="Session Tokens Disabled", color=discord.Color.orange(), timestamp=datetime.now())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            embed = discord.Embed(title="Disable Failed", description=str(response), color=discord.Color.red())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        print(f"[ERROR] disable failed: {e}")
        embed = discord.Embed(title="Error", description=str(e)[:100], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    if message.guild is None:
        await bot.process_commands(message)
        return
    if message.channel.id == VOUCH_CHANNEL_ID:
        content = message.content.strip()
        if content.lower().startswith("+vouch"):
            match = re.match(r"^\+vouch\s+(?:staff:\s*)?(<@!?\d+>|\d+)\s+(.+)$", content, re.IGNORECASE)
            if match:
                target_raw = match.group(1)
                reason = match.group(2).strip()
                if len(reason) >= 3:
                    target_id = None
                    if target_raw.startswith("<@"):
                        target_id = int(re.sub(r"[^\d]", "", target_raw))
                    else:
                        try:
                            target_id = int(target_raw)
                        except ValueError:
                            target_id = None
                    if target_id:
                        guild = message.guild
                        target_member = guild.get_member(target_id)
                        if target_member and any(role.id in STAFF_ROLE_IDS for role in target_member.roles):
                            user_key = str(target_id)
                            entry = {
                                "by": message.author.id,
                                "target": target_id,
                                "reason": reason[:200],
                                "timestamp": datetime.now().isoformat(),
                                "message_id": message.id
                            }
                            if user_key not in VOUCHES:
                                VOUCHES[user_key] = {"count": 0, "entries": []}
                            if VOUCHES[user_key]["count"] >= MAX_VOUCHES_PER_USER:
                                try:
                                    await message.add_reaction("❌")
                                except:
                                    pass
                                await message.reply(f"Max vouches reached for <@{target_id}> (limit {MAX_VOUCHES_PER_USER}).", mention_author=False)
                                await bot.process_commands(message)
                                return
                            if not any(e.get("message_id") == message.id for e in VOUCHES[user_key]["entries"]):
                                VOUCHES[user_key]["entries"].append(entry)
                                VOUCH_INDEX[str(message.id)] = target_id
                                VOUCHES[user_key]["count"] = len(VOUCHES[user_key]["entries"])
                                if len(VOUCHES[user_key]["entries"]) > MAX_VOUCH_LOGS:
                                    VOUCHES[user_key]["entries"] = VOUCHES[user_key]["entries"][-MAX_VOUCH_LOGS:]
                                    VOUCHES[user_key]["count"] = len(VOUCHES[user_key]["entries"])
                                save_vouches()

                                await update_trusted_role(target_member)
                                try:
                                    await message.add_reaction("❤️")
                                except Exception as e:
                                    print(f"[WARN] Failed to add reaction: {e}")
    await bot.process_commands(message)

@bot.event
async def on_message_delete(message: discord.Message):
    if message.guild is None:
        return
    if message.channel.id != VOUCH_CHANNEL_ID:
        return
    # Remove vouch entry tied to deleted message
    removed_any = False
    target_id = VOUCH_INDEX.pop(str(message.id), None)
    if target_id:
        record = VOUCHES.get(str(target_id), {})
        entries = record.get("entries", [])
        new_entries = [e for e in entries if e.get("message_id") != message.id]
        record["entries"] = new_entries
        record["count"] = len(new_entries)
        VOUCHES[str(target_id)] = record
        removed_any = True
        member = message.guild.get_member(int(target_id))
        if member:
            await update_trusted_role(member)
    else:
        for user_id, record in list(VOUCHES.items()):
            entries = record.get("entries", [])
            new_entries = [e for e in entries if e.get("message_id") != message.id]
            if len(new_entries) != len(entries):
                record["entries"] = new_entries
                record["count"] = len(new_entries)
                VOUCHES[user_id] = record
                removed_any = True
                member = message.guild.get_member(int(user_id))
                if member:
                    await update_trusted_role(member)
                break
    if removed_any:
        save_vouches()

@bot.tree.command(name='getbotuptime', description='View bot uptime and status', guilds=[discord.Object(id=config.GUILD_ID)])
async def getbotuptime(interaction: discord.Interaction):
    if not await check_dev(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    
    uptime = datetime.now() - BOT_START_TIME
    days = uptime.days
    hours, remainder = divmod(uptime.seconds, 3600)
    minutes = remainder // 60
    
    embed = discord.Embed(title="Bot Status", color=discord.Color.green())
    embed.add_field(name="Status", value="Online", inline=True)
    embed.add_field(name="Uptime", value=f"{days}d {hours}h {minutes}m", inline=True)
    embed.add_field(name="Guilds", value=len(bot.guilds), inline=True)
    embed.add_field(name="Users", value=len(bot.users), inline=True)
    embed.add_field(name="Logged Commands", value=len(COMMAND_LOGS), inline=True)
    embed.add_field(name="Started", value=f"<t:{int(BOT_START_TIME.timestamp())}:f>", inline=False)
    embed.set_footer(text=BOT_NAME)
    await interaction.followup.send(embed=embed, ephemeral=True)
    print(f"[OK] Bot status viewed by {interaction.user.name}")

@bot.tree.command(name='botstats', description='View bot stats and health', guilds=[discord.Object(id=config.GUILD_ID)])
async def botstats(interaction: discord.Interaction):
    if not await check_dev(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    try:
        uptime = datetime.now() - BOT_START_TIME
        days = uptime.days
        hours, remainder = divmod(uptime.seconds, 3600)
        minutes = remainder // 60
        log_full = len(COMMAND_LOGS) >= MAX_LOGS
        log_note = "MAX LOGS REACHED (auto-pruning)" if log_full else f"{len(COMMAND_LOGS)}/{MAX_LOGS}"

        embed = discord.Embed(title="Bot Stats", color=BOT_COLOR, timestamp=datetime.now())
        embed.add_field(name="Status", value="Online", inline=True)
        embed.add_field(name="Uptime", value=f"{days}d {hours}h {minutes}m", inline=True)
        embed.add_field(name="Guilds", value=len(bot.guilds), inline=True)
        embed.add_field(name="Users", value=len(bot.users), inline=True)
        embed.add_field(name="Command Logs", value=log_note, inline=False)
        embed.add_field(name="Vouch Targets", value=str(len(VOUCHES)), inline=True)
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        print(f"[ERROR] botstats failed: {e}")
        embed = discord.Embed(title="Error", description=str(e)[:100], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='viewkeys', description='View keys from storage (paginated)', guilds=[discord.Object(id=config.GUILD_ID)])
async def viewkeys(interaction: discord.Interaction):
    if not await check_dev(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    try:
        page_size = 50
        cache_pages = []
        tokens = []

        async def fetch_page(token: str = None):
            resp = await api_client.list_keys(page_size=page_size, continuation_token=token)
            return resp or {}

        first = await fetch_page()
        if not first.get("keys"):
            embed = discord.Embed(title="Keys", description="No keys found in storage.", color=BOT_COLOR)
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        cache_pages.append(first)
        tokens.append(first.get("next_continuation_token"))

        def build_embed(idx: int) -> discord.Embed:
            page = cache_pages[idx]
            keys = page.get("keys", [])
            total_known = sum(len(p.get("keys", [])) for p in cache_pages)
            desc = f"Page {idx+1} • Showing {len(keys)} • Cached total seen: {total_known}"
            embed = discord.Embed(title="Keys in Storage", description=desc, color=BOT_COLOR)
            for k in keys:
                name = k.get("key")
                lm = k.get("last_modified")
                size = k.get("size")
                lm_txt = f"<t:{int(datetime.fromisoformat(lm).timestamp())}:R>" if lm else "unknown"
                embed.add_field(name=name, value=f"Size: {size} bytes\nUpdated: {lm_txt}", inline=False)
            embed.set_footer(text=BOT_NAME)
            return embed

        class R2KeyPager(discord.ui.View):
            def __init__(self):
                super().__init__(timeout=120)
                self.idx = 0

            async def update(self, interaction: discord.Interaction):
                await interaction.response.edit_message(embed=build_embed(self.idx), view=self)

            @discord.ui.button(label="◀️ Prev", style=discord.ButtonStyle.secondary)
            async def prev(self, interaction: discord.Interaction, button: discord.ui.Button):
                if self.idx > 0:
                    self.idx -= 1
                await self.update(interaction)

            @discord.ui.button(label="▶️ Next", style=discord.ButtonStyle.secondary)
            async def next(self, interaction: discord.Interaction, button: discord.ui.Button):
                # fetch next page if needed
                if self.idx == len(cache_pages) - 1:
                    next_token = tokens[self.idx] if self.idx < len(tokens) else None
                    if next_token:
                        page = await fetch_page(next_token)
                        cache_pages.append(page)
                        tokens.append(page.get("next_continuation_token"))
                        # if empty, don't advance
                        if not page.get("keys"):
                            await self.update(interaction)
                            return
                if self.idx < len(cache_pages) - 1:
                    self.idx += 1
                await self.update(interaction)

            @discord.ui.button(label="⏭️ Last", style=discord.ButtonStyle.secondary)
            async def last(self, interaction: discord.Interaction, button: discord.ui.Button):
                # fetch until no continuation or limit of 20 pages to avoid spam
                while tokens and tokens[-1] and len(cache_pages) < 20:
                    page = await fetch_page(tokens[-1])
                    cache_pages.append(page)
                    tokens.append(page.get("next_continuation_token"))
                    if not page.get("keys"):
                        break
                    if not page.get("next_continuation_token"):
                        break
                self.idx = len(cache_pages) - 1
                await self.update(interaction)

            @discord.ui.button(label="⏮️ First", style=discord.ButtonStyle.secondary)
            async def first(self, interaction: discord.Interaction, button: discord.ui.Button):
                self.idx = 0
                await self.update(interaction)

        view = R2KeyPager()
        await interaction.followup.send(embed=build_embed(0), view=view, ephemeral=True)
    except Exception as e:
        print(f"[ERROR] viewkeys failed: {e}")
        embed = discord.Embed(title="Error", description=str(e)[:100], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='modlogs', description='View all command logs', guilds=[discord.Object(id=config.GUILD_ID)])
@app_commands.describe(page='Page number')
async def modlogs(interaction: discord.Interaction, page: int = 1):
    if not await check_owner(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    
    try:
        if not COMMAND_LOGS:
            embed = discord.Embed(title="Command Logs", description="No commands executed yet", color=discord.Color.greyple())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        
        reversed_logs = list(reversed(COMMAND_LOGS))
        paginator = LogPaginator(reversed_logs, page_size=5)
        
        if page < 1 or page > paginator.total_pages:
            embed = discord.Embed(title="Invalid Page", description=f"Pages: 1-{paginator.total_pages}", color=discord.Color.red())
            embed.set_footer(text=BOT_NAME)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        
        page_logs = paginator.get_page(page - 1)
        embed = discord.Embed(title="Command Logs", description=f"Page {page}/{paginator.total_pages} | Total: {len(COMMAND_LOGS)}", color=BOT_COLOR)
        
        for log in page_logs:
            timestamp = log['timestamp'][:16]
            executor = log['executor_name']
            target = log['target_user_name'] or 'N/A'
            details_str = ' '.join(f"{k}={v}" for k, v in log['details'].items()) if log['details'] else 'N/A'
            embed.add_field(name=f"{log['command']} at {timestamp}", value=f"By: {executor}\nTarget: {target}\nDetails: {details_str}", inline=False)
        
        embed.set_footer(text=f"{BOT_NAME} | Page {page}/{paginator.total_pages}")
        await interaction.followup.send(embed=embed, ephemeral=True)
        print(f"[OK] Mod logs viewed by {interaction.user.name} - page {page}")
    except Exception as e:
        print(f"[ERROR] modlogs failed: {e}")
        embed = discord.Embed(title="Error", description=str(e)[:100], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await interaction.followup.send(embed=embed, ephemeral=True)

@tasks.loop(hours=1)
async def cleanup_expired_keys():
    try:
        print("[TASK] Running cleanup task...")
        if api_client:
            response = await api_client.prune_expired_keys()
            if response and response.get('success'):
                deleted = response.get('keys_deleted', 0)
                print(f"[TASK] Pruned expired keys: {deleted} deleted")
    except Exception as e:
        print(f"[ERROR] Cleanup task failed: {e}")

@cleanup_expired_keys.before_loop
async def before_cleanup():
    await bot.wait_until_ready()

@bot.event
async def on_command_error(ctx, error):
    print(f"[ERROR] Command error in {ctx.command}: {error}")
    try:
        embed = discord.Embed(title="Error", description=str(error)[:200], color=discord.Color.red())
        embed.set_footer(text=BOT_NAME)
        await ctx.send(embed=embed)
    except:
        pass

@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    print(f"[ERROR] App command error: {error}")
    error_msg = str(error)[:200] if str(error) else "Unknown error"
    embed = discord.Embed(title="Error", description=error_msg, color=discord.Color.red())
    embed.set_footer(text=BOT_NAME)
    try:
        if interaction.response.is_done():
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, ephemeral=True)
    except:
        pass


def main():
    token = config.BOT_TOKEN
    
    if not token:
        print("[FATAL] BOT_TOKEN not found in environment")
        exit(1)
    
    try:
        print(f"[BOT] Starting bot...")
        bot.run(token)
    except KeyboardInterrupt:
        print("[BOT] Shutdown requested")
        save_logs()
    except Exception as e:
        print(f"[FATAL] Bot error: {e}")
        save_logs()
        exit(1)

if __name__ == '__main__':
    main()
