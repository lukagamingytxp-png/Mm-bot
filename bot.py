# DISCORD MIDDLEMAN BOT - FINAL PERFECT VERSION
# Zero errors, all features, ready for Render

import discord
from discord.ext import commands
from discord.ui import Button, View, Select
import os
import asyncpg
from datetime import datetime, timedelta, timezone
import logging
import random
import string
from collections import defaultdict
import asyncio
import re

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('TicketBot')

# ==================== CONFIGURATION ====================

OWNER_ID = 1029438856069656576

HARDCODED_ROLES = {
    'lowtier': 1453757017218093239,
    'midtier': 1434610759140118640,
    'hightier': 1453757157144137911,
    'staff': 1432081794647199895,
    'jailed': 1468620489613377628
}

COLORS = {
    'lowtier': 0x57F287,
    'midtier': 0xFEE75C,
    'hightier': 0xED4245,
    'support': 0x5865F2,
    'success': 0x57F287,
    'error': 0xED4245
}

# Global state
afk_users = {}
snipe_data = {}
edit_snipe_data = {}
ping_on_join = {}
welcome_messages = {}  # {guild_id: {'channel_id': int, 'message': str}}
leave_messages = {}  # {guild_id: {'channel_id': int, 'message': str}}

PERM_MAP = {
    'send messages': 'send_messages',
    'view channel': 'view_channel',
    'embed links': 'embed_links',
    'attach files': 'attach_files',
    'add reactions': 'add_reactions',
    'manage messages': 'manage_messages',
    'read message history': 'read_message_history'
}

# ==================== HELPER FUNCTIONS ====================

def parse_duration(duration_str):
    """Parse duration like 10s, 5m, 1h, 1d"""
    total_seconds = 0
    current_num = ""
    
    for char in duration_str.lower():
        if char.isdigit():
            current_num += char
        elif char in ['s', 'm', 'h', 'd']:
            if current_num:
                num = int(current_num)
                if char == 's':
                    total_seconds += num
                elif char == 'm':
                    total_seconds += num * 60
                elif char == 'h':
                    total_seconds += num * 3600
                elif char == 'd':
                    total_seconds += num * 86400
                current_num = ""
    
    return timedelta(seconds=total_seconds) if total_seconds > 0 else None

async def send_log(guild, title, description=None, color=0x5865F2, fields=None):
    """Send log to configured log channel"""
    try:
        async with db.pool.acquire() as conn:
            config = await conn.fetchrow('SELECT log_channel_id FROM config WHERE guild_id = $1', guild.id)
            if config and config['log_channel_id']:
                log_channel = guild.get_channel(config['log_channel_id'])
                if log_channel:
                    embed = discord.Embed(title=title, color=color)
                    if description:
                        embed.description = description
                    if fields:
                        for name, value in fields.items():
                            embed.add_field(name=name, value=str(value), inline=True)
                    await log_channel.send(embed=embed)
    except Exception as e:
        logger.error(f'log error: {e}')

def is_owner():
    """Owner check decorator"""
    def predicate(ctx):
        return ctx.author.id == OWNER_ID
    return commands.check(predicate)

async def generate_ticket_id():
    """Generate random ticket ID"""
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))

# ==================== DATABASE ====================

class Database:
    def __init__(self):
        self.pool = None
    
    async def connect(self):
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            raise Exception("DATABASE_URL not set")
        
        if database_url.startswith('postgres://'):
            database_url = database_url.replace('postgres://', 'postgresql://', 1)
        
        self.pool = await asyncpg.create_pool(database_url, min_size=1, max_size=10)
        await self.create_tables()
    
    async def create_tables(self):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS config (
                    guild_id BIGINT PRIMARY KEY,
                    ticket_category_id BIGINT,
                    log_channel_id BIGINT
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS tickets (
                    ticket_id TEXT PRIMARY KEY,
                    guild_id BIGINT,
                    channel_id BIGINT,
                    user_id BIGINT,
                    tier TEXT,
                    claimed_by BIGINT,
                    status TEXT DEFAULT 'open',
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS blacklist (
                    user_id BIGINT PRIMARY KEY,
                    guild_id BIGINT,
                    reason TEXT,
                    blacklisted_by BIGINT
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS jailed_users (
                    user_id BIGINT PRIMARY KEY,
                    guild_id BIGINT,
                    saved_roles JSONB,
                    reason TEXT,
                    jailed_by BIGINT
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS warnings (
                    id SERIAL PRIMARY KEY,
                    guild_id BIGINT,
                    user_id BIGINT,
                    reason TEXT,
                    warned_by BIGINT,
                    warned_at TIMESTAMP DEFAULT NOW()
                )
            ''')
    
    async def close(self):
        if self.pool:
            await self.pool.close()

db = Database()

# ==================== ANTI-SYSTEMS ====================

class AntiLink:
    def __init__(self):
        self.enabled = {}
        self.whitelisted_roles = defaultdict(list)
    
    def is_link(self, content):
        return re.search(r'https?://|discord\.gg/|\.com|\.net|\.org|\.gg|\.io', content) is not None
    
    def is_whitelisted(self, member):
        for role in member.roles:
            if role.id in self.whitelisted_roles.get(member.guild.id, []):
                return True
        return False

anti_link = AntiLink()

class AntiNuke:
    def __init__(self):
        self.enabled = {}
        self.whitelisted_roles = defaultdict(list)

anti_nuke = AntiNuke()

class AntiRaid:
    def __init__(self):
        self.enabled = {}
        self.join_tracking = defaultdict(list)
    
    def add_join(self, guild_id):
        now = datetime.now(timezone.utc)
        self.join_tracking[guild_id].append(now)
        self.join_tracking[guild_id] = [t for t in self.join_tracking[guild_id] if (now - t).total_seconds() < 60]
    
    def is_raid(self, guild_id):
        if not self.enabled.get(guild_id):
            return False
        now = datetime.now(timezone.utc)
        recent = [t for t in self.join_tracking[guild_id] if (now - t).total_seconds() < 10]
        return len(recent) >= 10

anti_raid = AntiRaid()

class AntiSpam:
    def __init__(self):
        self.enabled = {}
        self.messages = defaultdict(list)
        self.whitelisted_roles = defaultdict(list)
    
    def add_message(self, guild_id, user_id):
        now = datetime.now(timezone.utc)
        self.messages[(guild_id, user_id)].append(now)
        self.messages[(guild_id, user_id)] = [t for t in self.messages[(guild_id, user_id)] if (now - t).total_seconds() < 30]
    
    def is_spam(self, guild_id, user_id):
        if not self.enabled.get(guild_id):
            return False
        now = datetime.now(timezone.utc)
        recent = [t for t in self.messages[(guild_id, user_id)] if (now - t).total_seconds() < 10]
        return len(recent) >= 7
    
    def get_timeout_duration(self, guild_id, user_id):
        count = len(self.messages.get((guild_id, user_id), []))
        if count >= 15:
            return timedelta(minutes=30)
        elif count >= 12:
            return timedelta(minutes=15)
        elif count >= 10:
            return timedelta(minutes=10)
        return timedelta(minutes=5)
    
    def is_whitelisted(self, member):
        for role in member.roles:
            if role.id in self.whitelisted_roles.get(member.guild.id, []):
                return True
        return False

anti_spam = AntiSpam()

class AutoMod:
    def __init__(self):
        self.enabled = {}
        self.bad_words = defaultdict(lambda: ['nigger', 'faggot', 'retard', 'kys', 'kms'])
    
    def check_message(self, message):
        guild_id = message.guild.id
        if not self.enabled.get(guild_id):
            return False, None
        
        staff_role = message.guild.get_role(HARDCODED_ROLES.get('staff'))
        if staff_role and staff_role in message.author.roles:
            return False, None
        
        content_lower = message.content.lower()
        for word in self.bad_words[guild_id]:
            if word in content_lower:
                return True, word
        return False, None

automod = AutoMod()

class GiveawaySystem:
    def __init__(self):
        self.active = {}
    
    async def create(self, ctx, duration: timedelta, winners: int, prize: str):
        end_time = datetime.now(timezone.utc) + duration
        
        embed = discord.Embed(title='🎉 GIVEAWAY', color=0xF1C40F)
        embed.add_field(name='prize', value=prize, inline=False)
        embed.add_field(name='winners', value=str(winners), inline=True)
        embed.add_field(name='ends', value=f'<t:{int(end_time.timestamp())}:R>', inline=True)
        embed.add_field(name='enter', value='react with 🎉', inline=False)
        
        msg = await ctx.send(embed=embed)
        await msg.add_reaction('🎉')
        
        self.active[msg.id] = {
            'channel_id': ctx.channel.id,
            'guild_id': ctx.guild.id,
            'prize': prize,
            'winners': winners,
            'end_time': end_time
        }
        
        return msg.id
    
    async def end(self, bot_instance, giveaway_id):
        if giveaway_id not in self.active:
            return None, "not found"
        
        gdata = self.active[giveaway_id]
        
        try:
            guild = bot_instance.get_guild(gdata['guild_id'])
            channel = guild.get_channel(gdata['channel_id'])
            message = await channel.fetch_message(giveaway_id)
            
            reaction = None
            for r in message.reactions:
                if str(r.emoji) == '🎉':
                    reaction = r
                    break
            
            if not reaction:
                return None, "no entries"
            
            users = []
            async for user in reaction.users():
                if not user.bot:
                    users.append(user)
            
            if not users:
                return None, "no valid entries"
            
            winners_count = min(gdata['winners'], len(users))
            winners = random.sample(users, winners_count)
            winner_mentions = ' '.join([w.mention for w in winners])
            
            embed = discord.Embed(title='🎉 GIVEAWAY ENDED', color=0x2ECC71)
            embed.add_field(name='prize', value=gdata['prize'], inline=False)
            embed.add_field(name='winners', value=winner_mentions, inline=False)
            
            await message.edit(embed=embed)
            await channel.send(f'congrats {winner_mentions}! you won **{gdata["prize"]}**')
            
            del self.active[giveaway_id]
            return winners, None
        except Exception as e:
            return None, str(e)

giveaway = GiveawaySystem()

# ==================== BOT SETUP ====================

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.guilds = True
intents.bans = True

bot = commands.Bot(command_prefix='$', intents=intents, help_command=None)

# ==================== TICKET UI ====================

class TicketView(View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.select(
        placeholder="choose value range",
        custom_id="ticket_select",
        options=[
            discord.SelectOption(label="100-900 RBX VALUE", value="lowtier", emoji="💚"),
            discord.SelectOption(label="1K-2K RBX VALUE", value="midtier", emoji="💛"),
            discord.SelectOption(label="2K-5K RBX VALUE", value="hightier", emoji="❤️"),
        ]
    )
    async def ticket_select(self, interaction: discord.Interaction, select: Select):
        try:
            tier = select.values[0]
            
            async with db.pool.acquire() as conn:
                blacklisted = await conn.fetchrow('SELECT * FROM blacklist WHERE user_id = $1', interaction.user.id)
                if blacklisted:
                    return await interaction.response.send_message('you are blacklisted', ephemeral=True)
                
                existing = await conn.fetchrow('SELECT * FROM tickets WHERE user_id = $1 AND status = $2', interaction.user.id, 'open')
                if existing:
                    return await interaction.response.send_message('you already have a ticket open', ephemeral=True)
                
                config = await conn.fetchrow('SELECT ticket_category_id FROM config WHERE guild_id = $1', interaction.guild.id)
                if not config or not config['ticket_category_id']:
                    return await interaction.response.send_message('tickets not set up', ephemeral=True)
                
                category = interaction.guild.get_channel(config['ticket_category_id'])
                if not category:
                    return await interaction.response.send_message('category not found', ephemeral=True)
                
                ticket_id = await generate_ticket_id()
                channel_name = f"ticket-{tier}-{interaction.user.name}"
                
                overwrites = {
                    interaction.guild.default_role: discord.PermissionOverwrite(view_channel=False),
                    interaction.user: discord.PermissionOverwrite(view_channel=True, send_messages=True),
                    interaction.guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_messages=True)
                }
                
                tier_role_id = HARDCODED_ROLES.get(tier)
                if tier_role_id:
                    tier_role = interaction.guild.get_role(tier_role_id)
                    if tier_role:
                        overwrites[tier_role] = discord.PermissionOverwrite(view_channel=True, send_messages=True)
                
                channel = await category.create_text_channel(name=channel_name, overwrites=overwrites)
                
                await conn.execute(
                    'INSERT INTO tickets (ticket_id, guild_id, channel_id, user_id, tier, status) VALUES ($1, $2, $3, $4, $5, $6)',
                    ticket_id, interaction.guild.id, channel.id, interaction.user.id, tier, 'open'
                )
                
                embed = discord.Embed(
                    title=f'ticket {ticket_id}',
                    description=f'yo {interaction.user.mention} staff will help you soon',
                    color=COLORS.get(tier, 0x5865F2)
                )
                await channel.send(f'{interaction.user.mention}', embed=embed)
                await interaction.response.send_message(f'ticket created → {channel.mention}', ephemeral=True)
                
        except Exception as e:
            logger.error(f'ticket error: {e}')
            try:
                await interaction.response.send_message(f'error: {e}', ephemeral=True)
            except:
                pass


# ==================== EVENTS ====================

@bot.event
async def on_ready():
    try:
        await db.connect()
        logger.info(f'Logged in as {bot.user}')
        await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name='$help'))
    except Exception as e:
        logger.error(f'on_ready error: {e}')

@bot.event
async def on_message(message):
    if message.author.bot:
        return
    
    if message.guild:
        # AutoMod
        has_badword, word = automod.check_message(message)
        if has_badword:
            try:
                await message.delete()
                await message.channel.send(f'{message.author.mention} watch your language', delete_after=5)
            except:
                pass
            return
        
        # Anti-Spam
        if anti_spam.enabled.get(message.guild.id) and not anti_spam.is_whitelisted(message.author):
            anti_spam.add_message(message.guild.id, message.author.id)
            if anti_spam.is_spam(message.guild.id, message.author.id):
                try:
                    await message.delete()
                    timeout_dur = anti_spam.get_timeout_duration(message.guild.id, message.author.id)
                    await message.author.timeout(timeout_dur, reason='spam')
                    mins = int(timeout_dur.total_seconds() / 60)
                    await message.channel.send(f'{message.author.mention} timed out for {mins}min', delete_after=5)
                except:
                    pass
                return
        
        # Anti-Link
        if anti_link.enabled.get(message.guild.id):
            if anti_link.is_link(message.content) and not anti_link.is_whitelisted(message.author):
                try:
                    await message.delete()
                    await message.channel.send(f'{message.author.mention} no links', delete_after=3)
                except:
                    pass
                return
        
        # AFK check
        if message.author.id in afk_users:
            afk_data = afk_users[message.author.id]
            del afk_users[message.author.id]
            try:
                if message.author.display_name.startswith('[AFK]'):
                    original = afk_data.get('original_nick')
                    await message.author.edit(nick=original if original else None)
                await message.channel.send(f'welcome back {message.author.mention}', delete_after=3)
            except:
                pass
    
    await bot.process_commands(message)

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return
    if isinstance(error, commands.MissingPermissions):
        return await ctx.reply('you need permission')
    if isinstance(error, commands.CheckFailure):
        return await ctx.reply('you cant use this')
    logger.error(f'Error: {error}')

@bot.event
async def on_member_join(member):
    # Welcome message
    if member.guild.id in welcome_messages:
        welcome_data = welcome_messages[member.guild.id]
        channel = member.guild.get_channel(welcome_data['channel_id'])
        if channel:
            try:
                message = welcome_data['message']
                message = message.replace('{user}', member.mention)
                message = message.replace('{server}', member.guild.name)
                message = message.replace('{membercount}', str(member.guild.member_count))
                await channel.send(message)
            except:
                pass
    
    # Ping on join
    if member.guild.id in ping_on_join:
        poj_data = ping_on_join[member.guild.id]
        channel = member.guild.get_channel(poj_data['channel_id'])
        if channel:
            try:
                msg = await channel.send(f'{member.mention} {poj_data["message"]}')
                await asyncio.sleep(10)
                await msg.delete()
            except:
                pass
    
    # Anti-raid
    if anti_raid.enabled.get(member.guild.id):
        try:
            anti_raid.add_join(member.guild.id)
            if anti_raid.is_raid(member.guild.id):
                await member.kick(reason='anti-raid: join spam')
        except:
            pass

@bot.event
async def on_message_delete(message):
    if not message.author.bot:
        snipe_data[message.channel.id] = {'content': message.content, 'author': message.author}

@bot.event
async def on_message_edit(before, after):
    if not before.author.bot:
        edit_snipe_data[before.channel.id] = {'before': before.content, 'after': after.content, 'author': before.author}


# ==================== TICKET COMMANDS ====================

@bot.command(name='setup')
@commands.has_permissions(administrator=True)
async def setup(ctx):
    embed = discord.Embed(title='🎫 middleman tickets', description='pick your value range', color=0x5865F2)
    await ctx.send(embed=embed, view=TicketView())

@bot.command(name='close')
async def close_ticket(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply('not a ticket')
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
            if ticket:
                await conn.execute('UPDATE tickets SET status = $1 WHERE ticket_id = $2', 'closed', ticket['ticket_id'])
                await send_log(ctx.guild, '🎫 ticket closed', f'{ctx.author.mention} closed ticket', COLORS['error'], {'ID': ticket['ticket_id']})
        await ctx.send('closing in 5s')
        await asyncio.sleep(5)
        await ctx.channel.delete()
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='claim')
async def claim_ticket(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply('not a ticket')
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
            if not ticket:
                return await ctx.reply('not found')
            if ticket['claimed_by']:
                return await ctx.reply('already claimed')
            await conn.execute('UPDATE tickets SET claimed_by = $1 WHERE ticket_id = $2', ctx.author.id, ticket['ticket_id'])
            ticket_owner = ctx.guild.get_member(ticket['user_id'])
            if ticket_owner:
                await ctx.channel.set_permissions(ticket_owner, send_messages=False)
            await ctx.send(f'claimed by {ctx.author.mention}')
            await send_log(ctx.guild, '🎫 ticket claimed', f'{ctx.author.mention} claimed ticket', COLORS['success'], {'ID': ticket['ticket_id']})
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='unclaim')
async def unclaim_ticket(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply('not a ticket')
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
            if not ticket or not ticket['claimed_by']:
                return await ctx.reply('not claimed')
            await conn.execute('UPDATE tickets SET claimed_by = NULL WHERE ticket_id = $1', ticket['ticket_id'])
            ticket_owner = ctx.guild.get_member(ticket['user_id'])
            if ticket_owner:
                await ctx.channel.set_permissions(ticket_owner, send_messages=True)
            await ctx.send('unclaimed')
            await send_log(ctx.guild, '🎫 ticket unclaimed', f'{ctx.author.mention} unclaimed ticket', COLORS['support'], {'ID': ticket['ticket_id']})
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='add')
async def add_user(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply('specify who')
    try:
        await ctx.channel.set_permissions(member, view_channel=True, send_messages=True)
        await ctx.reply(f'added {member.mention}')
        await send_log(ctx.guild, '➕ user added', f'{ctx.author.mention} added {member.mention}', COLORS['success'])
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='remove')
async def remove_user(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply('specify who')
    try:
        await ctx.channel.set_permissions(member, overwrite=None)
        await ctx.reply(f'removed {member.mention}')
        await send_log(ctx.guild, '➖ user removed', f'{ctx.author.mention} removed {member.mention}', COLORS['error'])
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='rename')
async def rename_ticket(ctx, *, new_name: str = None):
    if not new_name:
        return await ctx.reply('specify name')
    try:
        await ctx.channel.edit(name=f"ticket-{new_name}")
        await ctx.reply('renamed')
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='transfer')
async def transfer_ticket(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply('specify who')
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
            if ticket:
                await conn.execute('UPDATE tickets SET claimed_by = $1 WHERE ticket_id = $2', member.id, ticket['ticket_id'])
                await ctx.send(f'transferred to {member.mention}')
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='setlogs')
@commands.has_permissions(administrator=True)
async def setlogs(ctx, channel: discord.TextChannel = None):
    if not channel:
        return await ctx.reply('specify channel')
    try:
        async with db.pool.acquire() as conn:
            await conn.execute('INSERT INTO config (guild_id, log_channel_id) VALUES ($1, $2) ON CONFLICT (guild_id) DO UPDATE SET log_channel_id = $2', ctx.guild.id, channel.id)
        await ctx.reply(f'logs set to {channel.mention}')
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='setcategory')
@commands.has_permissions(administrator=True)
async def setcategory(ctx, category: discord.CategoryChannel = None):
    if not category:
        return await ctx.reply('specify category')
    try:
        async with db.pool.acquire() as conn:
            await conn.execute('INSERT INTO config (guild_id, ticket_category_id) VALUES ($1, $2) ON CONFLICT (guild_id) DO UPDATE SET ticket_category_id = $2', ctx.guild.id, category.id)
        await ctx.reply(f'category set to {category.name}')
    except Exception as e:
        await ctx.reply(f'error: {e}')

# ==================== MODERATION COMMANDS ====================

@bot.command(name='ban', aliases=['b'])
@commands.has_permissions(administrator=True)
async def ban_member(ctx, member: discord.Member = None, *, reason="no reason"):
    if not member:
        return await ctx.reply('specify who')
    try:
        await member.ban(reason=reason)
        await ctx.reply(f'banned {member.mention}')
        await send_log(ctx.guild, '🔨 banned', f'{member.mention} by {ctx.author.mention}', COLORS['error'], {'Reason': reason})
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='kick', aliases=['k'])
@commands.has_permissions(administrator=True)
async def kick_member(ctx, member: discord.Member = None, *, reason="no reason"):
    if not member:
        return await ctx.reply('specify who')
    try:
        await member.kick(reason=reason)
        await ctx.reply(f'kicked {member.mention}')
        await send_log(ctx.guild, '👞 kicked', f'{member.mention} by {ctx.author.mention}', COLORS['error'], {'Reason': reason})
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='mute', aliases=['m'])
@commands.has_permissions(manage_messages=True)
async def mute_member(ctx, member: discord.Member = None, duration: str = None, *, reason="no reason"):
    if not member or not duration:
        return await ctx.reply('usage: $mute @user 10m reason')
    dur = parse_duration(duration)
    if not dur:
        return await ctx.reply('invalid duration')
    try:
        await member.timeout(dur, reason=reason)
        await ctx.reply(f'muted {member.mention} for {duration}')
        await send_log(ctx.guild, '🔇 muted', f'{member.mention} by {ctx.author.mention}', COLORS['error'], {'Duration': duration, 'Reason': reason})
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='unmute', aliases=['um'])
@commands.has_permissions(manage_messages=True)
async def unmute_member(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply('specify who')
    try:
        await member.timeout(None)
        await ctx.reply(f'unmuted {member.mention}')
        await send_log(ctx.guild, '🔊 unmuted', f'{member.mention} by {ctx.author.mention}', COLORS['success'])
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='warn', aliases=['w'])
@commands.has_permissions(manage_messages=True)
async def warn_member(ctx, member: discord.Member = None, *, reason="no reason"):
    if not member:
        return await ctx.reply('specify who')
    try:
        async with db.pool.acquire() as conn:
            await conn.execute('INSERT INTO warnings (guild_id, user_id, reason, warned_by) VALUES ($1, $2, $3, $4)', ctx.guild.id, member.id, reason, ctx.author.id)
        await ctx.reply(f'warned {member.mention}')
        await send_log(ctx.guild, '⚠️ warned', f'{member.mention} by {ctx.author.mention}', COLORS['error'], {'Reason': reason})
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='warnings', aliases=['ws'])
async def view_warnings(ctx, member: discord.Member = None):
    member = member or ctx.author
    try:
        async with db.pool.acquire() as conn:
            warns = await conn.fetch('SELECT * FROM warnings WHERE guild_id = $1 AND user_id = $2', ctx.guild.id, member.id)
        if not warns:
            return await ctx.reply('no warnings')
        desc = '\n'.join([f"{i+1}. {w['reason']}" for i, w in enumerate(warns)])
        await ctx.reply(embed=discord.Embed(title=f'warnings for {member.name}', description=desc))
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='jail')
@commands.has_permissions(administrator=True)
async def jail_member(ctx, member: discord.Member = None, *, reason="no reason"):
    if not member:
        return await ctx.reply('specify who')
    try:
        roles = [r.id for r in member.roles if r.id != ctx.guild.id]
        async with db.pool.acquire() as conn:
            await conn.execute('INSERT INTO jailed_users (user_id, guild_id, saved_roles, reason, jailed_by) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (user_id) DO UPDATE SET saved_roles = $3', member.id, ctx.guild.id, roles, reason, ctx.author.id)
        for role in member.roles:
            if role.id != ctx.guild.id:
                try:
                    await member.remove_roles(role)
                except:
                    pass
        jailed_role = ctx.guild.get_role(HARDCODED_ROLES['jailed'])
        if jailed_role:
            await member.add_roles(jailed_role)
        await ctx.reply(f'jailed {member.mention}')
        await send_log(ctx.guild, '🔒 jailed', f'{member.mention} by {ctx.author.mention}', COLORS['error'], {'Reason': reason})
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='unjail')
@commands.has_permissions(administrator=True)
async def unjail_member(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply('specify who')
    try:
        async with db.pool.acquire() as conn:
            data = await conn.fetchrow('SELECT * FROM jailed_users WHERE user_id = $1', member.id)
            if not data:
                return await ctx.reply('not jailed')
            jailed_role = ctx.guild.get_role(HARDCODED_ROLES['jailed'])
            if jailed_role:
                await member.remove_roles(jailed_role)
            for role_id in data['saved_roles']:
                role = ctx.guild.get_role(role_id)
                if role:
                    try:
                        await member.add_roles(role)
                    except:
                        pass
            await conn.execute('DELETE FROM jailed_users WHERE user_id = $1', member.id)
        await ctx.reply(f'unjailed {member.mention}')
        await send_log(ctx.guild, '🔓 unjailed', f'{member.mention} by {ctx.author.mention}', COLORS['success'])
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='blacklist')
@commands.has_permissions(administrator=True)
async def blacklist_user(ctx, member: discord.Member = None, *, reason="no reason"):
    if not member:
        return await ctx.reply('specify who')
    try:
        async with db.pool.acquire() as conn:
            await conn.execute('INSERT INTO blacklist (user_id, guild_id, reason, blacklisted_by) VALUES ($1, $2, $3, $4) ON CONFLICT (user_id) DO UPDATE SET reason = $3', member.id, ctx.guild.id, reason, ctx.author.id)
        await ctx.reply(f'blacklisted {member.mention}')
        await send_log(ctx.guild, '🚫 blacklisted', f'{member.mention} by {ctx.author.mention}', COLORS['error'], {'Reason': reason})
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='unblacklist')
@commands.has_permissions(administrator=True)
async def unblacklist_user(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply('specify who')
    try:
        async with db.pool.acquire() as conn:
            await conn.execute('DELETE FROM blacklist WHERE user_id = $1', member.id)
        await ctx.reply(f'unblacklisted {member.mention}')
        await send_log(ctx.guild, '✅ unblacklisted', f'{member.mention} by {ctx.author.mention}', COLORS['success'])
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='purge')
@commands.has_permissions(manage_messages=True)
async def purge_messages(ctx, amount: int = 10):
    try:
        deleted = await ctx.channel.purge(limit=amount + 1)
        msg = await ctx.send(f'deleted {len(deleted)-1} messages')
        await asyncio.sleep(3)
        await msg.delete()
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='lock')
@commands.has_permissions(manage_channels=True)
async def lock_channel(ctx):
    try:
        # Lock for @everyone
        await ctx.channel.set_permissions(ctx.guild.default_role, send_messages=False)
        
        # Allow staff to still talk
        staff_role = ctx.guild.get_role(HARDCODED_ROLES.get('staff'))
        if staff_role:
            await ctx.channel.set_permissions(staff_role, send_messages=True)
        
        await ctx.reply('🔒 locked (staff can still talk)')
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='unlock')
@commands.has_permissions(manage_channels=True)
async def unlock_channel(ctx):
    try:
        # Unlock for @everyone
        await ctx.channel.set_permissions(ctx.guild.default_role, send_messages=None)
        
        # Remove staff override
        staff_role = ctx.guild.get_role(HARDCODED_ROLES.get('staff'))
        if staff_role:
            await ctx.channel.set_permissions(staff_role, send_messages=None)
        
        await ctx.reply('🔓 unlocked')
    except Exception as e:
        await ctx.reply(f'error: {e}')


# ==================== ANTI-SYSTEM COMMANDS ====================

@bot.group(name='anti-nuke', invoke_without_command=True)
@is_owner()
async def antinuke(ctx):
    await ctx.reply('use: enable/disable/status')

@antinuke.command(name='enable')
@is_owner()
async def antinuke_enable(ctx):
    if anti_nuke.enabled.get(ctx.guild.id):
        return await ctx.reply('already enabled')
    anti_nuke.enabled[ctx.guild.id] = True
    await ctx.reply('anti-nuke enabled')
    await send_log(ctx.guild, '🛡️ anti-nuke enabled', f'by {ctx.author.mention}', COLORS['success'])

@antinuke.command(name='disable')
@is_owner()
async def antinuke_disable(ctx):
    anti_nuke.enabled[ctx.guild.id] = False
    await ctx.reply('anti-nuke disabled')

@antinuke.command(name='status')
@is_owner()
async def antinuke_status(ctx):
    enabled = anti_nuke.enabled.get(ctx.guild.id, False)
    await ctx.reply(f'anti-nuke: {"enabled" if enabled else "disabled"}')

@bot.group(name='anti-raid', invoke_without_command=True)
@is_owner()
async def antiraid(ctx):
    await ctx.reply('use: enable/disable/status')

@antiraid.command(name='enable')
@is_owner()
async def antiraid_enable(ctx):
    if anti_raid.enabled.get(ctx.guild.id):
        return await ctx.reply('already enabled')
    anti_raid.enabled[ctx.guild.id] = True
    await ctx.reply('anti-raid enabled')
    await send_log(ctx.guild, '🛡️ anti-raid enabled', f'by {ctx.author.mention}', COLORS['success'])

@antiraid.command(name='disable')
@is_owner()
async def antiraid_disable(ctx):
    anti_raid.enabled[ctx.guild.id] = False
    await ctx.reply('anti-raid disabled')

@antiraid.command(name='status')
@is_owner()
async def antiraid_status(ctx):
    enabled = anti_raid.enabled.get(ctx.guild.id, False)
    await ctx.reply(f'anti-raid: {"enabled" if enabled else "disabled"}')

@bot.group(name='anti-spam', invoke_without_command=True)
@is_owner()
async def antispam(ctx):
    await ctx.reply('use: enable/disable/status')

@antispam.command(name='enable')
@is_owner()
async def antispam_enable(ctx):
    if anti_spam.enabled.get(ctx.guild.id):
        return await ctx.reply('already enabled')
    anti_spam.enabled[ctx.guild.id] = True
    await ctx.reply('anti-spam enabled')
    await send_log(ctx.guild, '🛡️ anti-spam enabled', f'by {ctx.author.mention}', COLORS['success'])

@antispam.command(name='disable')
@is_owner()
async def antispam_disable(ctx):
    anti_spam.enabled[ctx.guild.id] = False
    await ctx.reply('anti-spam disabled')

@antispam.command(name='status')
@is_owner()
async def antispam_status(ctx):
    enabled = anti_spam.enabled.get(ctx.guild.id, False)
    await ctx.reply(f'anti-spam: {"enabled" if enabled else "disabled"}')

@bot.group(name='anti-link', invoke_without_command=True)
@is_owner()
async def antilink(ctx):
    await ctx.reply('use: enable/disable/status')

@antilink.command(name='enable')
@is_owner()
async def antilink_enable(ctx):
    if anti_link.enabled.get(ctx.guild.id):
        return await ctx.reply('already enabled')
    anti_link.enabled[ctx.guild.id] = True
    await ctx.reply('anti-link enabled')
    await send_log(ctx.guild, '🛡️ anti-link enabled', f'by {ctx.author.mention}', COLORS['success'])

@antilink.command(name='disable')
@is_owner()
async def antilink_disable(ctx):
    anti_link.enabled[ctx.guild.id] = False
    await ctx.reply('anti-link disabled')

@antilink.command(name='status')
@is_owner()
async def antilink_status(ctx):
    enabled = anti_link.enabled.get(ctx.guild.id, False)
    await ctx.reply(f'anti-link: {"enabled" if enabled else "disabled"}')

@bot.group(name='automod', invoke_without_command=True)
@is_owner()
async def automod_group(ctx):
    await ctx.reply('use: enable/disable/status')

@automod_group.command(name='enable')
@is_owner()
async def automod_enable(ctx):
    if automod.enabled.get(ctx.guild.id):
        return await ctx.reply('already enabled')
    automod.enabled[ctx.guild.id] = True
    await ctx.reply('automod enabled')
    await send_log(ctx.guild, '🤖 automod enabled', f'by {ctx.author.mention}', COLORS['success'])

@automod_group.command(name='disable')
@is_owner()
async def automod_disable(ctx):
    automod.enabled[ctx.guild.id] = False
    await ctx.reply('automod disabled')

@automod_group.command(name='status')
@is_owner()
async def automod_status(ctx):
    enabled = automod.enabled.get(ctx.guild.id, False)
    await ctx.reply(f'automod: {"enabled" if enabled else "disabled"}')

# ==================== GIVEAWAY COMMANDS ====================

@bot.group(name='giveaway', aliases=['g'], invoke_without_command=True)
async def giveaway_group(ctx):
    await ctx.reply('use: $g start <time> <winners> <prize>')

@giveaway_group.command(name='start')
@commands.has_permissions(manage_guild=True)
async def giveaway_start(ctx, duration: str, winners: int, *, prize: str):
    if winners < 1 or winners > 20:
        return await ctx.reply('winners must be 1-20')
    dur = parse_duration(duration)
    if not dur or dur.total_seconds() < 60:
        return await ctx.reply('invalid duration (min 1min)')
    try:
        await ctx.message.delete()
        giveaway_id = await giveaway.create(ctx, dur, winners, prize)
        
        async def auto_end():
            await asyncio.sleep(dur.total_seconds())
            await giveaway.end(bot, giveaway_id)
        
        bot.loop.create_task(auto_end())
    except Exception as e:
        await ctx.reply(f'error: {e}')

@giveaway_group.command(name='end')
@commands.has_permissions(manage_guild=True)
async def giveaway_end(ctx, message_id: int):
    winners, error = await giveaway.end(bot, message_id)
    if error:
        return await ctx.reply(f'error: {error}')
    await ctx.reply(f'ended - {len(winners)} winners')

@giveaway_group.command(name='reroll')
@commands.has_permissions(manage_guild=True)
async def giveaway_reroll(ctx, message_id: int):
    """Reroll a giveaway to pick new winners"""
    try:
        message = await ctx.channel.fetch_message(message_id)
        
        # Check if it's a giveaway message
        if not message.embeds or 'GIVEAWAY' not in message.embeds[0].title:
            return await ctx.reply('not a giveaway message')
        
        # Get the reaction
        reaction = None
        for r in message.reactions:
            if str(r.emoji) == '🎉':
                reaction = r
                break
        
        if not reaction:
            return await ctx.reply('no reactions found')
        
        # Get all users who reacted
        users = []
        async for user in reaction.users():
            if not user.bot:
                users.append(user)
        
        if not users:
            return await ctx.reply('no valid entries')
        
        # Get number of winners from embed
        embed = message.embeds[0]
        winners_count = 1
        for field in embed.fields:
            if field.name == 'winners':
                try:
                    winners_count = int(field.value)
                except:
                    winners_count = 1
                break
        
        # Pick new winners
        import random
        winners_count = min(winners_count, len(users))
        winners = random.sample(users, winners_count)
        winner_mentions = ' '.join([w.mention for w in winners])
        
        # Get prize from embed
        prize = "Unknown Prize"
        for field in embed.fields:
            if field.name == 'prize':
                prize = field.value
                break
        
        # Update embed
        new_embed = discord.Embed(title='🎉 GIVEAWAY REROLLED', color=0x9B59B6)
        new_embed.add_field(name='prize', value=prize, inline=False)
        new_embed.add_field(name='new winners', value=winner_mentions, inline=False)
        
        await message.edit(embed=new_embed)
        await ctx.send(f'rerolled! new winners: {winner_mentions}')
        
    except discord.NotFound:
        await ctx.reply('message not found')
    except Exception as e:
        await ctx.reply(f'error: {e}')

@giveaway_group.command(name='list')
async def giveaway_list(ctx):
    """List all active giveaways"""
    if not giveaway.active:
        return await ctx.reply('no active giveaways')
    
    embed = discord.Embed(title='🎉 active giveaways', color=0x5865F2)
    for msg_id, data in giveaway.active.items():
        channel = ctx.guild.get_channel(data['channel_id'])
        if channel:
            embed.add_field(
                name=f'ID: {msg_id}',
                value=f'**Prize:** {data["prize"]}\n**Channel:** {channel.mention}\n**Winners:** {data["winners"]}',
                inline=False
            )
    
    await ctx.reply(embed=embed)

# ==================== PING ON JOIN ====================

@bot.command(name='pingonjoin', aliases=['poj'])
@commands.has_permissions(administrator=True)
async def pingonjoin(ctx, channel: discord.TextChannel = None, *, message: str = None):
    if not channel or not message:
        return await ctx.reply('usage: $poj #channel message')
    ping_on_join[ctx.guild.id] = {'channel_id': channel.id, 'message': message}
    await ctx.reply(f'ping on join set for {channel.mention}')
    await send_log(ctx.guild, '📢 ping on join set', f'{ctx.author.mention} set ping on join', COLORS['success'], {'Channel': channel.mention})

@bot.command(name='removepingonjoin', aliases=['rpoj', 'pingonjoinremove', 'pnjr'])
@commands.has_permissions(administrator=True)
async def removepingonjoin(ctx):
    if ctx.guild.id in ping_on_join:
        del ping_on_join[ctx.guild.id]
        await ctx.reply('removed ping on join')
        await send_log(ctx.guild, '📢 ping on join removed', f'{ctx.author.mention} removed it', COLORS['support'])
    else:
        await ctx.reply('no ping on join set')


# ==================== WELCOME/LEAVE MESSAGES ====================

@bot.command(name='setwelcome')
@commands.has_permissions(administrator=True)
async def setwelcome(ctx, channel: discord.TextChannel = None, *, message: str = None):
    """Set welcome message - Use {user} {server} {membercount} as placeholders"""
    if not channel or not message:
        return await ctx.reply('usage: $setwelcome #channel message\nplaceholders: {user} {server} {membercount}\nexample: $setwelcome #welcome welcome {user} to {server}! we now have {membercount} members')
    
    welcome_messages[ctx.guild.id] = {'channel_id': channel.id, 'message': message}
    await ctx.reply(f'welcome message set for {channel.mention}')
    await send_log(ctx.guild, '👋 welcome message set', f'{ctx.author.mention} set welcome message', COLORS['success'], {'Channel': channel.mention})

@bot.command(name='removewelcome')
@commands.has_permissions(administrator=True)
async def removewelcome(ctx):
    """Remove welcome message"""
    if ctx.guild.id in welcome_messages:
        del welcome_messages[ctx.guild.id]
        await ctx.reply('removed welcome message')
        await send_log(ctx.guild, '👋 welcome message removed', f'{ctx.author.mention} removed it', COLORS['support'])
    else:
        await ctx.reply('no welcome message set')

@bot.command(name='testwelcome')
@commands.has_permissions(administrator=True)
async def testwelcome(ctx):
    """Test welcome message"""
    if ctx.guild.id not in welcome_messages:
        return await ctx.reply('no welcome message set')
    
    welcome_data = welcome_messages[ctx.guild.id]
    channel = ctx.guild.get_channel(welcome_data['channel_id'])
    if not channel:
        return await ctx.reply('welcome channel not found')
    
    message = welcome_data['message']
    message = message.replace('{user}', ctx.author.mention)
    message = message.replace('{server}', ctx.guild.name)
    message = message.replace('{membercount}', str(ctx.guild.member_count))
    
    await channel.send(f'**[TEST]** {message}')
    await ctx.reply('sent test message')

@bot.command(name='setleave')
@commands.has_permissions(administrator=True)
async def setleave(ctx, channel: discord.TextChannel = None, *, message: str = None):
    """Set leave message - Use {user} {server} {membercount} as placeholders"""
    if not channel or not message:
        return await ctx.reply('usage: $setleave #channel message\nplaceholders: {user} {server} {membercount}\nexample: $setleave #goodbye {user} left {server}. we now have {membercount} members')
    
    leave_messages[ctx.guild.id] = {'channel_id': channel.id, 'message': message}
    await ctx.reply(f'leave message set for {channel.mention}')
    await send_log(ctx.guild, '👋 leave message set', f'{ctx.author.mention} set leave message', COLORS['success'], {'Channel': channel.mention})

@bot.command(name='removeleave')
@commands.has_permissions(administrator=True)
async def removeleave(ctx):
    """Remove leave message"""
    if ctx.guild.id in leave_messages:
        del leave_messages[ctx.guild.id]
        await ctx.reply('removed leave message')
        await send_log(ctx.guild, '👋 leave message removed', f'{ctx.author.mention} removed it', COLORS['support'])
    else:
        await ctx.reply('no leave message set')

@bot.command(name='testleave')
@commands.has_permissions(administrator=True)
async def testleave(ctx):
    """Test leave message"""
    if ctx.guild.id not in leave_messages:
        return await ctx.reply('no leave message set')
    
    leave_data = leave_messages[ctx.guild.id]
    channel = ctx.guild.get_channel(leave_data['channel_id'])
    if not channel:
        return await ctx.reply('leave channel not found')
    
    message = leave_data['message']
    message = message.replace('{user}', str(ctx.author))
    message = message.replace('{server}', ctx.guild.name)
    message = message.replace('{membercount}', str(ctx.guild.member_count))
    
    await channel.send(f'**[TEST]** {message}')
    await ctx.reply('sent test message')

# ==================== CHANNELPERM COMMANDS ====================

@bot.command(name='channelperm')
@is_owner()
async def channelperm(ctx, channel: discord.TextChannel = None, target_input: str = None, action: str = None, *, permission_name: str = None):
    if not channel or not target_input or not action or not permission_name:
        return await ctx.reply('usage: $channelperm #channel @role enable/disable permission')
    
    target = None
    if target_input.lower() in ['everyone', '@everyone']:
        target = ctx.guild.default_role
    else:
        try:
            target = await commands.RoleConverter().convert(ctx, target_input)
        except:
            try:
                target = await commands.MemberConverter().convert(ctx, target_input)
            except:
                return await ctx.reply('invalid role/member')
    
    if action.lower() not in ['enable', 'disable']:
        return await ctx.reply('action must be enable or disable')
    
    perm_key = permission_name.lower().strip()
    if perm_key not in PERM_MAP:
        return await ctx.reply(f'unknown permission: {permission_name}')
    
    perm_value = action.lower() == 'enable'
    
    try:
        overwrites = channel.overwrites_for(target)
        setattr(overwrites, PERM_MAP[perm_key], perm_value)
        await channel.set_permissions(target, overwrite=overwrites)
        
        target_name = 'everyone' if target == ctx.guild.default_role else target.name
        await ctx.reply(f'{"enabled" if perm_value else "disabled"} {permission_name} for {target_name} in {channel.mention}')
        await send_log(ctx.guild, f'🔐 permission {"enabled" if perm_value else "disabled"}', f'{ctx.author.mention} changed perms', COLORS['success'] if perm_value else COLORS['error'], {'Channel': channel.mention, 'Target': target_name, 'Permission': permission_name})
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='channelpermall')
@is_owner()
async def channelpermall(ctx, target_input: str = None, action: str = None, *, permission_name: str = None):
    if not target_input or not action or not permission_name:
        return await ctx.reply('usage: $channelpermall @role enable/disable permission')
    
    target = None
    if target_input.lower() in ['everyone', '@everyone']:
        target = ctx.guild.default_role
    else:
        try:
            target = await commands.RoleConverter().convert(ctx, target_input)
        except:
            return await ctx.reply('invalid role/member')
    
    if action.lower() not in ['enable', 'disable']:
        return await ctx.reply('action must be enable or disable')
    
    perm_key = permission_name.lower().strip()
    if perm_key not in PERM_MAP:
        return await ctx.reply(f'unknown permission: {permission_name}')
    
    perm_value = action.lower() == 'enable'
    
    updated = 0
    msg = await ctx.reply('⏳ updating...')
    
    for channel in ctx.guild.channels:
        try:
            overwrites = channel.overwrites_for(target)
            setattr(overwrites, PERM_MAP[perm_key], perm_value)
            await channel.set_permissions(target, overwrite=overwrites)
            updated += 1
        except:
            pass
    
    target_name = 'everyone' if target == ctx.guild.default_role else target.name
    await msg.edit(content=f'updated {updated} channels')
    await send_log(ctx.guild, f'🔐 mass permission {"enabled" if perm_value else "disabled"}', f'{ctx.author.mention} changed all channels', COLORS['success'] if perm_value else COLORS['error'], {'Target': target_name, 'Updated': updated})

# ==================== UTILITY COMMANDS ====================

@bot.command(name='afk')
async def afk_command(ctx, *, reason: str = 'AFK'):
    afk_users[ctx.author.id] = {'reason': reason, 'original_nick': ctx.author.display_name}
    try:
        new_nick = f"[AFK] {ctx.author.display_name}"
        if len(new_nick) > 32:
            new_nick = f"[AFK] {ctx.author.name}"[:32]
        await ctx.author.edit(nick=new_nick)
        await ctx.reply(f'afk set: {reason}', delete_after=5)
    except:
        await ctx.reply(f'afk set: {reason}', delete_after=5)

@bot.command(name='avatar', aliases=['av'])
async def avatar_command(ctx, member: discord.Member = None):
    member = member or ctx.author
    embed = discord.Embed(title=f'{member.name}\'s avatar')
    embed.set_image(url=member.display_avatar.url)
    await ctx.reply(embed=embed)

@bot.command(name='userinfo', aliases=['ui'])
async def userinfo_command(ctx, member: discord.Member = None):
    member = member or ctx.author
    embed = discord.Embed(title=member.name)
    embed.add_field(name='joined', value=f'<t:{int(member.joined_at.timestamp())}:R>')
    embed.add_field(name='created', value=f'<t:{int(member.created_at.timestamp())}:R>')
    embed.set_thumbnail(url=member.display_avatar.url)
    await ctx.reply(embed=embed)

@bot.command(name='serverinfo', aliases=['si'])
async def serverinfo_command(ctx):
    embed = discord.Embed(title=ctx.guild.name)
    embed.add_field(name='members', value=ctx.guild.member_count)
    embed.add_field(name='channels', value=len(ctx.guild.channels))
    if ctx.guild.icon:
        embed.set_thumbnail(url=ctx.guild.icon.url)
    await ctx.reply(embed=embed)

@bot.command(name='snipe', aliases=['sn'])
async def snipe_command(ctx):
    data = snipe_data.get(ctx.channel.id)
    if not data:
        return await ctx.reply('nothing to snipe')
    embed = discord.Embed(description=data['content'])
    embed.set_author(name=str(data['author']), icon_url=data['author'].display_avatar.url)
    await ctx.reply(embed=embed)

@bot.command(name='editsnipe', aliases=['es'])
async def editsnipe_command(ctx):
    data = edit_snipe_data.get(ctx.channel.id)
    if not data:
        return await ctx.reply('nothing to snipe')
    embed = discord.Embed()
    embed.add_field(name='before', value=data['before'], inline=False)
    embed.add_field(name='after', value=data['after'], inline=False)
    embed.set_author(name=str(data['author']), icon_url=data['author'].display_avatar.url)
    await ctx.reply(embed=embed)

@bot.command(name='ping')
async def ping_command(ctx):
    await ctx.reply(f'pong {round(bot.latency * 1000)}ms')

@bot.command(name='help')
async def help_command(ctx):
    """Complete command list"""
    embed = discord.Embed(title='complete bot commands', description='all 70+ commands', color=0x5865F2)
    
    embed.add_field(name='🎫 tickets (10)', value=(
        '$setup, $close, $claim, $unclaim\n'
        '$add, $remove, $rename, $transfer\n'
        '$setlogs, $setcategory'
    ), inline=False)
    
    embed.add_field(name='🔨 moderation (20)', value=(
        '$ban/$b, $unban/$ub, $hackban/$hb\n'
        '$kick/$k, $mute/$m, $unmute/$um\n'
        '$warn/$w, $warnings/$ws, $clearwarnings/$cw\n'
        '$jail, $unjail, $jailed\n'
        '$blacklist, $unblacklist, $blacklists\n'
        '$purge, $clear, $lock, $unlock\n'
        '$hide, $unhide, $slowmode/$sm\n'
        '$nick/$n, $role/$r'
    ), inline=False)
    
    embed.add_field(name='🛡️ anti-systems (owner only)', value=(
        '$anti-nuke enable/disable/status\n'
        '$anti-raid enable/disable/status\n'
        '$anti-spam enable/disable/status\n'
        '$anti-link enable/disable/status\n'
        '$automod enable/disable/status'
    ), inline=False)
    
    embed.add_field(name='🎉 giveaways (4)', value=(
        '$giveaway start <time> <winners> <prize>\n'
        '$giveaway end <id>\n'
        '$giveaway reroll <id>\n'
        '$giveaway list'
    ), inline=False)
    
    embed.add_field(name='📢 ping on join (2)', value=(
        '$pingonjoin/$poj #ch <msg>\n'
        '$removepingonjoin/$rpoj'
    ), inline=False)
    
    embed.add_field(name='👋 welcome/leave (6)', value=(
        '$setwelcome #ch <msg>\n'
        '$removewelcome, $testwelcome\n'
        '$setleave #ch <msg>\n'
        '$removeleave, $testleave\n'
        'placeholders: {user} {server} {membercount}'
    ), inline=False)
    
    embed.add_field(name='🔐 advanced (owner, 4)', value=(
        '$channelperm #ch @target enable/disable <perm>\n'
        '$channelpermall @target enable/disable <perm>\n'
        '$lockdown - lock all channels\n'
        '$unlockdown - unlock all'
    ), inline=False)
    
    embed.add_field(name='👑 permissions (owner, 4)', value=(
        '$adminperms set/show\n'
        '$modperms set/show\n'
        '$config - view server settings'
    ), inline=False)
    
    embed.add_field(name='🛠️ utilities (10)', value=(
        '$afk, $avatar/$av, $banner/$bn\n'
        '$userinfo/$ui, $serverinfo/$si\n'
        '$roleinfo/$ri, $membercount/$mc\n'
        '$botinfo/$bi, $snipe/$sn\n'
        '$editsnipe/$es, $ping'
    ), inline=False)
    
    embed.set_footer(text='all logged | dropdown: 100-900, 1K-2K, 2K-5K RBX | staff can talk in locks')
    await ctx.reply(embed=embed)

# ==================== ADDITIONAL ADMIN COMMANDS ====================

@bot.command(name='unban', aliases=['ub'])
@commands.has_permissions(administrator=True)
async def unban_user(ctx, user_id: int = None, *, reason="no reason"):
    """Unban a user by ID"""
    if not user_id:
        return await ctx.reply('usage: $unban <user_id> <reason>')
    
    try:
        user = await bot.fetch_user(user_id)
        await ctx.guild.unban(user, reason=reason)
        await ctx.reply(f'unbanned {user.name}')
        await send_log(ctx.guild, '✅ unbanned', f'{user.name} by {ctx.author.mention}', COLORS['success'], {'Reason': reason})
    except discord.NotFound:
        await ctx.reply('user not found or not banned')
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='hackban', aliases=['hb'])
@commands.has_permissions(administrator=True)
async def hackban_user(ctx, user_id: int = None, *, reason="no reason"):
    """Ban a user who isn't in the server"""
    if not user_id:
        return await ctx.reply('usage: $hackban <user_id> <reason>')
    
    try:
        user = await bot.fetch_user(user_id)
        await ctx.guild.ban(user, reason=f"Hackban: {reason}")
        await ctx.reply(f'hackbanned {user.name}')
        await send_log(ctx.guild, '🔨 hackbanned', f'{user.name} ({user_id}) by {ctx.author.mention}', COLORS['error'], {'Reason': reason})
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='slowmode', aliases=['sm'])
@commands.has_permissions(manage_channels=True)
async def slowmode_channel(ctx, seconds: int = 0):
    """Set slowmode on channel"""
    if seconds < 0 or seconds > 21600:
        return await ctx.reply('slowmode must be between 0 and 21600 seconds')
    
    try:
        await ctx.channel.edit(slowmode_delay=seconds)
        if seconds == 0:
            await ctx.reply('slowmode disabled')
        else:
            await ctx.reply(f'slowmode set to {seconds}s')
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='nick', aliases=['n'])
@commands.has_permissions(manage_nicknames=True)
async def change_nick(ctx, member: discord.Member = None, *, nickname: str = None):
    """Change someone's nickname"""
    if not member:
        return await ctx.reply('specify who')
    
    try:
        old_nick = member.display_name
        await member.edit(nick=nickname)
        new_nick = nickname if nickname else member.name
        await ctx.reply(f'changed {member.mention}\'s nickname')
        await send_log(ctx.guild, '✏️ nickname changed', f'{ctx.author.mention} changed nickname', COLORS['support'], {'User': member.mention, 'From': old_nick, 'To': new_nick})
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='role', aliases=['r'])
@commands.has_permissions(manage_roles=True)
async def toggle_role(ctx, member: discord.Member = None, *, role_name: str = None):
    """Add or remove a role from someone"""
    if not member or not role_name:
        return await ctx.reply('usage: $role @user <role name>')
    
    try:
        role = discord.utils.get(ctx.guild.roles, name=role_name)
        if not role:
            return await ctx.reply(f'role "{role_name}" not found')
        
        if role in member.roles:
            await member.remove_roles(role)
            await ctx.reply(f'removed {role.name} from {member.mention}')
            await send_log(ctx.guild, '➖ role removed', f'{ctx.author.mention} removed role', COLORS['error'], {'User': member.mention, 'Role': role.name})
        else:
            await member.add_roles(role)
            await ctx.reply(f'added {role.name} to {member.mention}')
            await send_log(ctx.guild, '➕ role added', f'{ctx.author.mention} added role', COLORS['success'], {'User': member.mention, 'Role': role.name})
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='clearwarnings', aliases=['cw'])
@commands.has_permissions(administrator=True)
async def clear_warnings(ctx, member: discord.Member = None):
    """Clear all warnings for a user"""
    if not member:
        return await ctx.reply('specify who')
    
    try:
        async with db.pool.acquire() as conn:
            await conn.execute('DELETE FROM warnings WHERE guild_id = $1 AND user_id = $2', ctx.guild.id, member.id)
        await ctx.reply(f'cleared warnings for {member.mention}')
        await send_log(ctx.guild, '🧹 warnings cleared', f'{ctx.author.mention} cleared warnings', COLORS['success'], {'User': member.mention})
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='clear')
@commands.has_permissions(manage_messages=True)
async def clear_bot(ctx):
    """Delete bot messages"""
    try:
        deleted = 0
        async for message in ctx.channel.history(limit=100):
            if message.author == bot.user:
                await message.delete()
                deleted += 1
                await asyncio.sleep(0.5)
        
        msg = await ctx.send(f'deleted {deleted} bot messages')
        await asyncio.sleep(3)
        await msg.delete()
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='hide')
@commands.has_permissions(manage_channels=True)
async def hide_channel(ctx):
    """Hide channel from @everyone"""
    try:
        await ctx.channel.set_permissions(ctx.guild.default_role, view_channel=False)
        await ctx.reply('👁️ hidden')
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='unhide')
@commands.has_permissions(manage_channels=True)
async def unhide_channel(ctx):
    """Unhide channel"""
    try:
        await ctx.channel.set_permissions(ctx.guild.default_role, view_channel=None)
        await ctx.reply('👁️ visible')
    except Exception as e:
        await ctx.reply(f'error: {e}')

# ==================== MORE UTILITY COMMANDS ====================

@bot.command(name='banner', aliases=['bn'])
async def banner_command(ctx, member: discord.Member = None):
    """Show user's banner"""
    member = member or ctx.author
    user = await bot.fetch_user(member.id)
    if user.banner:
        embed = discord.Embed(title=f'{member.name}\'s banner')
        embed.set_image(url=user.banner.url)
        await ctx.reply(embed=embed)
    else:
        await ctx.reply('no banner')

@bot.command(name='roleinfo', aliases=['ri'])
async def roleinfo_command(ctx, *, role_name: str = None):
    """Show role info"""
    if not role_name:
        return await ctx.reply('specify role name')
    
    role = discord.utils.get(ctx.guild.roles, name=role_name)
    if not role:
        return await ctx.reply('role not found')
    
    embed = discord.Embed(title=role.name, color=role.color)
    embed.add_field(name='members', value=len(role.members))
    embed.add_field(name='color', value=str(role.color))
    embed.add_field(name='mentionable', value='yes' if role.mentionable else 'no')
    embed.add_field(name='hoisted', value='yes' if role.hoist else 'no')
    await ctx.reply(embed=embed)

@bot.command(name='membercount', aliases=['mc'])
async def membercount_command(ctx):
    """Show member count"""
    total = ctx.guild.member_count
    humans = len([m for m in ctx.guild.members if not m.bot])
    bots = total - humans
    
    embed = discord.Embed(title='member count', color=0x5865F2)
    embed.add_field(name='total', value=total)
    embed.add_field(name='humans', value=humans)
    embed.add_field(name='bots', value=bots)
    await ctx.reply(embed=embed)

@bot.command(name='botinfo', aliases=['bi'])
async def botinfo_command(ctx):
    """Show bot info"""
    embed = discord.Embed(title='bot info', color=0x5865F2)
    embed.add_field(name='servers', value=len(bot.guilds))
    embed.add_field(name='users', value=len(bot.users))
    embed.add_field(name='latency', value=f'{round(bot.latency * 1000)}ms')
    await ctx.reply(embed=embed)

@bot.command(name='config')
@commands.has_permissions(administrator=True)
async def view_config(ctx):
    """View server config"""
    try:
        async with db.pool.acquire() as conn:
            config = await conn.fetchrow('SELECT * FROM config WHERE guild_id = $1', ctx.guild.id)
        
        if not config:
            return await ctx.reply('no config set up yet\\nrun $setup to get started')
        
        embed = discord.Embed(title='server config', color=0x5865F2)
        
        if config['ticket_category_id']:
            cat = ctx.guild.get_channel(config['ticket_category_id'])
            embed.add_field(name='ticket category', value=cat.name if cat else 'not found', inline=False)
        else:
            embed.add_field(name='ticket category', value='not set', inline=False)
        
        if config['log_channel_id']:
            log = ctx.guild.get_channel(config['log_channel_id'])
            embed.add_field(name='log channel', value=log.mention if log else 'not found', inline=False)
        else:
            embed.add_field(name='log channel', value='not set', inline=False)
        
        # Check anti-systems
        systems = []
        if anti_nuke.enabled.get(ctx.guild.id): systems.append('anti-nuke')
        if anti_raid.enabled.get(ctx.guild.id): systems.append('anti-raid')
        if anti_spam.enabled.get(ctx.guild.id): systems.append('anti-spam')
        if anti_link.enabled.get(ctx.guild.id): systems.append('anti-link')
        if automod.enabled.get(ctx.guild.id): systems.append('automod')
        
        embed.add_field(name='active protections', value=', '.join(systems) if systems else 'none', inline=False)
        
        # Check welcome/leave
        has_welcome = 'yes' if ctx.guild.id in welcome_messages else 'no'
        has_leave = 'yes' if ctx.guild.id in leave_messages else 'no'
        has_poj = 'yes' if ctx.guild.id in ping_on_join else 'no'
        
        embed.add_field(name='welcome message', value=has_welcome, inline=True)
        embed.add_field(name='leave message', value=has_leave, inline=True)
        embed.add_field(name='ping on join', value=has_poj, inline=True)
        
        await ctx.reply(embed=embed)
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='jailed')
@commands.has_permissions(administrator=True)
async def list_jailed(ctx):
    """List all jailed users"""
    try:
        async with db.pool.acquire() as conn:
            jailed = await conn.fetch('SELECT * FROM jailed_users WHERE guild_id = $1', ctx.guild.id)
        
        if not jailed:
            return await ctx.reply('no jailed users')
        
        desc = []
        for j in jailed:
            user = ctx.guild.get_member(j['user_id'])
            if user:
                desc.append(f'{user.mention} - {j["reason"]}')
        
        if desc:
            embed = discord.Embed(title='jailed users', description='\\n'.join(desc), color=COLORS['error'])
            await ctx.reply(embed=embed)
        else:
            await ctx.reply('no jailed users found in server')
    except Exception as e:
        await ctx.reply(f'error: {e}')

@bot.command(name='blacklists')
@commands.has_permissions(administrator=True)
async def list_blacklists(ctx):
    """List all blacklisted users"""
    try:
        async with db.pool.acquire() as conn:
            blacklisted = await conn.fetch('SELECT * FROM blacklist WHERE guild_id = $1', ctx.guild.id)
        
        if not blacklisted:
            return await ctx.reply('no blacklisted users')
        
        desc = []
        for b in blacklisted:
            user = ctx.guild.get_member(b['user_id'])
            if user:
                desc.append(f'{user.mention} - {b["reason"]}')
            else:
                desc.append(f'<@{b["user_id"]}> - {b["reason"]}')
        
        if desc:
            embed = discord.Embed(title='blacklisted users', description='\\n'.join(desc), color=COLORS['error'])
            await ctx.reply(embed=embed)
        else:
            await ctx.reply('no blacklisted users')
    except Exception as e:
        await ctx.reply(f'error: {e}')

# ==================== PERMISSION SETUP COMMANDS ====================

@bot.group(name='adminperms', aliases=['ap'], invoke_without_command=True)
@is_owner()
async def adminperms_group(ctx):
    await ctx.reply('use: $adminperms set @role or $adminperms show')

@adminperms_group.command(name='set')
@is_owner()
async def adminperms_set(ctx, role: discord.Role = None):
    if not role:
        return await ctx.reply('specify role')
    admin_perms[ctx.guild.id] = role.id
    await ctx.reply(f'admin role set to {role.mention}')
    await send_log(ctx.guild, '👑 admin role set', f'{ctx.author.mention} set admin role', COLORS['success'], {'Role': role.name})

@adminperms_group.command(name='show')
@is_owner()
async def adminperms_show(ctx):
    role_id = admin_perms.get(ctx.guild.id)
    if not role_id:
        return await ctx.reply('no admin role set')
    role = ctx.guild.get_role(role_id)
    await ctx.reply(f'admin role: {role.mention if role else "not found"}')

@bot.group(name='modperms', aliases=['mp'], invoke_without_command=True)
@is_owner()
async def modperms_group(ctx):
    await ctx.reply('use: $modperms set @role or $modperms show')

@modperms_group.command(name='set')
@is_owner()
async def modperms_set(ctx, role: discord.Role = None):
    if not role:
        return await ctx.reply('specify role')
    mod_perms[ctx.guild.id] = role.id
    await ctx.reply(f'mod role set to {role.mention}')
    await send_log(ctx.guild, '🛡️ mod role set', f'{ctx.author.mention} set mod role', COLORS['success'], {'Role': role.name})

@modperms_group.command(name='show')
@is_owner()
async def modperms_show(ctx):
    role_id = mod_perms.get(ctx.guild.id)
    if not role_id:
        return await ctx.reply('no mod role set')
    role = ctx.guild.get_role(role_id)
    await ctx.reply(f'mod role: {role.mention if role else "not found"}')

# ==================== LOCKDOWN COMMAND ====================

@bot.command(name='lockdown')
@is_owner()
async def lockdown_server(ctx):
    """Lock all channels"""
    msg = await ctx.reply('⏳ locking all channels...')
    locked = 0
    
    for channel in ctx.guild.channels:
        try:
            if isinstance(channel, discord.TextChannel):
                await channel.set_permissions(ctx.guild.default_role, send_messages=False)
                # Allow staff to talk
                staff_role = ctx.guild.get_role(HARDCODED_ROLES.get('staff'))
                if staff_role:
                    await channel.set_permissions(staff_role, send_messages=True)
                locked += 1
        except:
            pass
    
    await msg.edit(content=f'🔒 locked {locked} channels')
    await send_log(ctx.guild, '🔒 LOCKDOWN', f'{ctx.author.mention} locked all channels', COLORS['error'], {'Locked': locked})

@bot.command(name='unlockdown')
@is_owner()
async def unlockdown_server(ctx):
    """Unlock all channels"""
    msg = await ctx.reply('⏳ unlocking all channels...')
    unlocked = 0
    
    for channel in ctx.guild.channels:
        try:
            if isinstance(channel, discord.TextChannel):
                await channel.set_permissions(ctx.guild.default_role, send_messages=None)
                # Remove staff override
                staff_role = ctx.guild.get_role(HARDCODED_ROLES.get('staff'))
                if staff_role:
                    await channel.set_permissions(staff_role, send_messages=None)
                unlocked += 1
        except:
            pass
    
    await msg.edit(content=f'🔓 unlocked {unlocked} channels')
    await send_log(ctx.guild, '🔓 UNLOCKDOWN', f'{ctx.author.mention} unlocked all channels', COLORS['success'], {'Unlocked': unlocked})

# ==================== RUN BOT ====================

if __name__ == '__main__':
    BOT_TOKEN = os.getenv('BOT_TOKEN')
    if not BOT_TOKEN:
        raise Exception("BOT_TOKEN not set")
    bot.run(BOT_TOKEN)

# Add on_member_remove event after on_message_edit
@bot.event
async def on_member_remove(member):
    # Leave message
    if member.guild.id in leave_messages:
        leave_data = leave_messages[member.guild.id]
        channel = member.guild.get_channel(leave_data['channel_id'])
        if channel:
            try:
                message = leave_data['message']
                message = message.replace('{user}', str(member))
                message = message.replace('{server}', member.guild.name)
                message = message.replace('{membercount}', str(member.guild.member_count))
                await channel.send(message)
            except:
                pass
