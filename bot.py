import discord
from discord.ext import commands
from discord.ui import Button, View, Select, Modal, TextInput
import os
import asyncpg
from datetime import datetime, timedelta, timezone
from aiohttp import web
import logging
import random
import string
from typing import Optional, Dict
import asyncio
from collections import defaultdict
import json
import io
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('TicketBot')

OWNER_ID = 1029438856069656576

HARDCODED_ROLES = {
    'lowtier': 1453757017218093239,
    'midtier': 1434610759140118640,
    'hightier': 1453757157144137911,
    'staff': 1432081794647199895,
    'jailed': 1468620489613377628
}

HARDCODED_CHANNELS = {
    'proof': 1472695529883435091
}

COLORS = {
    'lowtier': 0x57F287,
    'midtier': 0xFEE75C,
    'hightier': 0xED4245,
    'support': 0x5865F2,
    'success': 0x57F287,
    'error': 0xED4245
}

# In-memory stores
afk_users = {}
snipe_data = {}
edit_snipe_data = {}
mod_perms = {}
admin_perms = {}
bot_start_time = datetime.now(timezone.utc)

class RateLimiter:
    def __init__(self):
        self.cooldowns = defaultdict(float)
    def check_cooldown(self, user_id: int, command: str, cooldown: int = 3) -> bool:
        key = f"{user_id}:{command}"
        now = datetime.now(timezone.utc).timestamp()
        if key in self.cooldowns and now - self.cooldowns[key] < cooldown:
            return False
        self.cooldowns[key] = now
        return True

rate_limiter = RateLimiter()


# ALL ANTI-PROTECTION SYSTEMS
# Anti-Nuke, Anti-Raid, Anti-Spam, Anti-Link, Anti-React

from collections import defaultdict
from datetime import datetime, timedelta, timezone

# ANTI-LINK - Blocks all links unless whitelisted
class AntiLink:
    def __init__(self):
        self.enabled = {}
        self.whitelisted_roles = defaultdict(list)
        self.whitelisted_urls = defaultdict(list)
    
    def is_link(self, content):
        url_pattern = re.compile(r'https?://|discord\.gg/|\.com|\.net|\.org|\.gg|\.io')
        return url_pattern.search(content) is not None
    
    def is_url_whitelisted(self, guild_id, content):
        for url in self.whitelisted_urls.get(guild_id, []):
            if url.lower() in content.lower():
                return True
        return False
    
    def is_whitelisted(self, member):
        for role in member.roles:
            if role.id in self.whitelisted_roles.get(member.guild.id, []):
                return True
        return False

anti_link = AntiLink()

# ANTI-NUKE - Mass actions, unauthorized bots/webhooks  
class AntiNuke:
    def __init__(self):
        self.enabled = {}
        self.whitelisted_roles = defaultdict(list)
        self.channel_deletes = defaultdict(list)
        self.bans = defaultdict(list)
        self.kicks = defaultdict(list)
        self.role_deletes = defaultdict(list)
        self.channel_creates = defaultdict(list)
        self.bot_start_time = datetime.now(timezone.utc)
    
    def is_recent(self, timestamp):
        now = datetime.now(timezone.utc)
        if timestamp.tzinfo:
            timestamp = timestamp.replace(tzinfo=None)
        return (now.replace(tzinfo=None) - timestamp).total_seconds() < 3
    
    def is_after_restart(self, timestamp):
        if timestamp.tzinfo:
            timestamp = timestamp.replace(tzinfo=None)
        bot_time = self.bot_start_time.replace(tzinfo=None)
        return timestamp > bot_time
    
    def add_action(self, guild_id, user_id, action_type):
        now = datetime.now(timezone.utc)
        storage = getattr(self, action_type)
        storage[(guild_id, user_id)].append(now)
        storage[(guild_id, user_id)] = [t for t in storage[(guild_id, user_id)] 
                                        if (now - t).total_seconds() < 10]
    
    def check_nuke(self, guild_id, user_id):
        if not self.enabled.get(guild_id): return False
        counts = {
            'channel_deletes': len(self.channel_deletes.get((guild_id, user_id), [])),
            'bans': len(self.bans.get((guild_id, user_id), [])),
            'kicks': len(self.kicks.get((guild_id, user_id), [])),
            'role_deletes': len(self.role_deletes.get((guild_id, user_id), [])),
            'channel_creates': len(self.channel_creates.get((guild_id, user_id), []))
        }
        return (counts['channel_deletes'] >= 3 or counts['bans'] >= 3 or 
                counts['kicks'] >= 3 or counts['role_deletes'] >= 3 or 
                counts['channel_creates'] >= 5)
    
    def is_whitelisted(self, member):
        if not member: return False
        for role in member.roles:
            if role.id in self.whitelisted_roles.get(member.guild.id, []):
                return True
        return False


    def is_recent_and_matches(self, entry, target_id):
        """Triple check: recent, after restart, target matches"""
        now = datetime.now(timezone.utc)
        entry_time = entry.created_at.replace(tzinfo=None) if entry.created_at.tzinfo else entry.created_at
        bot_time = self.bot_start_time.replace(tzinfo=None) if self.bot_start_time.tzinfo else self.bot_start_time
        now_naive = now.replace(tzinfo=None)
        
        time_diff = (now_naive - entry_time).total_seconds()
        if time_diff > 3:
            return False
        if entry_time < bot_time:
            return False
        if hasattr(entry.target, 'id') and entry.target.id != target_id:
            return False
        return True

anti_nuke = AntiNuke()

# ANTI-RAID - Join spam, new accounts, no avatar
class AntiRaid:
    def __init__(self):
        self.enabled = {}
        self.join_tracking = defaultdict(list)
        self.whitelisted_roles = defaultdict(list)
        self.settings = defaultdict(lambda: {
            'max_joins': 10,
            'time_window': 10,
            'min_account_age': 7,
            'require_avatar': True,
            'action': 'kick'
        })
    
    def add_join(self, guild_id, user_id):
        now = datetime.now(timezone.utc)
        self.join_tracking[guild_id].append((user_id, now))
        self.join_tracking[guild_id] = [(uid, t) for uid, t in self.join_tracking[guild_id] 
                                        if (now - t).total_seconds() < 60]
    
    def is_raid(self, guild_id):
        if not self.enabled.get(guild_id): return False
        settings = self.settings[guild_id]
        now = datetime.now(timezone.utc)
        recent = [(uid, t) for uid, t in self.join_tracking[guild_id] 
                 if (now - t).total_seconds() < settings['time_window']]
        return len(recent) >= settings['max_joins']
    
    def check_member(self, member):
        guild_id = member.guild.id
        if not self.enabled.get(guild_id): return False, None
        settings = self.settings[guild_id]
        reasons = []
        account_age = (datetime.now(timezone.utc) - member.created_at.replace(tzinfo=None)).days
        if account_age < settings['min_account_age']:
            reasons.append(f"account {account_age} days old")
        if settings['require_avatar'] and member.avatar is None:
            reasons.append("no avatar")
        return len(reasons) > 0, reasons
    
    def is_whitelisted(self, member):
        for role in member.roles:
            if role.id in self.whitelisted_roles.get(member.guild.id, []):
                return True
        return False

anti_raid = AntiRaid()

# ANTI-SPAM - Message spam with escalating timeouts
class AntiSpam:
    def __init__(self):
        self.enabled = {}
        self.messages = defaultdict(list)
        self.whitelisted_roles = defaultdict(list)
        self.settings = defaultdict(lambda: {
            'max_messages': 7,
            'time_window': 10,
            'max_mentions': 5,
            'max_emojis': 10,
            'action': 'timeout'
        })
    
    def add_message(self, guild_id, user_id):
        now = datetime.now(timezone.utc)
        self.messages[(guild_id, user_id)].append(now)
        self.messages[(guild_id, user_id)] = [t for t in self.messages[(guild_id, user_id)] 
                                              if (now - t).total_seconds() < 30]
    
    def is_spam(self, guild_id, user_id):
        if not self.enabled.get(guild_id): return False
        settings = self.settings[guild_id]
        now = datetime.now(timezone.utc)
        recent = [t for t in self.messages[(guild_id, user_id)] 
                 if (now - t).total_seconds() < settings['time_window']]
        return len(recent) >= settings['max_messages']
    
    def get_timeout_duration(self, guild_id, user_id):
        count = len(self.messages.get((guild_id, user_id), []))
        if count >= 15: return timedelta(minutes=30)
        elif count >= 12: return timedelta(minutes=15)
        elif count >= 10: return timedelta(minutes=10)
        return timedelta(minutes=5)
    
    def check_message(self, message):
        guild_id = message.guild.id
        if not self.enabled.get(guild_id): return False, None
        settings = self.settings[guild_id]
        reasons = []
        mention_count = len(message.mentions) + len(message.role_mentions)
        if mention_count > settings['max_mentions']:
            reasons.append(f"{mention_count} mentions")
        return len(reasons) > 0, reasons
    
    def is_whitelisted(self, member):
        for role in member.roles:
            if role.id in self.whitelisted_roles.get(member.guild.id, []):
                return True
        return False

anti_spam = AntiSpam()

# ANTI-REACT - Reaction spam detection
class AntiReact:
    def __init__(self):
        self.enabled = {}
        self.reactions = defaultdict(list)
        self.whitelisted_roles = defaultdict(list)
        self.settings = defaultdict(lambda: {
            'max_reacts': 10,
            'time_window': 10,
            'action': 'warn'
        })
    
    def add_reaction(self, guild_id, user_id, message_id):
        now = datetime.now(timezone.utc)
        self.reactions[(guild_id, user_id)].append((now, message_id))
        self.reactions[(guild_id, user_id)] = [(t, mid) for t, mid in self.reactions[(guild_id, user_id)] 
                                                if (now - t).total_seconds() < 30]
    
    def is_react_spam(self, guild_id, user_id):
        if not self.enabled.get(guild_id): return False
        settings = self.settings[guild_id]
        now = datetime.now(timezone.utc)
        recent = [(t, mid) for t, mid in self.reactions[(guild_id, user_id)] 
                 if (now - t).total_seconds() < settings['time_window']]
        unique_messages = len(set(mid for _, mid in recent))
        return len(recent) >= settings['max_reacts'] and unique_messages >= 5
    
    def get_recent_messages(self, guild_id, user_id):
        settings = self.settings[guild_id]
        now = datetime.now(timezone.utc)
        recent = [(t, mid) for t, mid in self.reactions[(guild_id, user_id)] 
                 if (now - t).total_seconds() < settings['time_window']]
        return list(set(mid for _, mid in recent))
    
    def is_whitelisted(self, member):
        for role in member.roles:
            if role.id in self.whitelisted_roles.get(member.guild.id, []):
                return True
        return False

anti_react = AntiReact()


class AutoMod:
    def __init__(self):
        self.enabled = {}
        self.bad_words = defaultdict(lambda: [
            'nigger', 'nigga', 'faggot', 'fag', 'retard', 'retarded',
            'cunt', 'kys', 'kms', 'rape', 'molest', 'pedo', 'pedophile',
            'tranny', 'dyke', 'chink', 'spic', 'wetback', 'gook',
            'beaner', 'cracker', 'honkey'
        ])
        self.staff_bypass = True  # Staff can say anything
    
    def check_message(self, message):
        """Check if message contains bad words"""
        guild_id = message.guild.id
        if not self.enabled.get(guild_id):
            return False, None
        
        content_lower = message.content.lower()
        
        # Check if staff (bypass)
        if self.staff_bypass:
            staff_role = message.guild.get_role(HARDCODED_ROLES.get('staff'))
            if staff_role and staff_role in message.author.roles:
                return False, None
            # Check admin perms
            if message.author.guild_permissions.administrator:
                return False, None
        
        # Check for bad words
        for word in self.bad_words[guild_id]:
            if word in content_lower:
                return True, word
        
        return False, None
    
    def add_word(self, guild_id, word):
        """Add custom bad word"""
        word = word.lower()
        if word not in self.bad_words[guild_id]:
            self.bad_words[guild_id].append(word)
            return True
        return False
    
    def remove_word(self, guild_id, word):
        """Remove bad word"""
        word = word.lower()
        if word in self.bad_words[guild_id]:
            self.bad_words[guild_id].remove(word)
            return True
        return False


automod = AutoMod()

# LOCKDOWN SYSTEM
class Lockdown:
    def __init__(self):
        self.locked_channels = defaultdict(list)
    
    def is_locked(self, guild_id):
        return len(self.locked_channels.get(guild_id, [])) > 0

lockdown = Lockdown()


# GIVEAWAY SYSTEM - React to enter
# Add this after the lockdown class

class GiveawaySystem:
    def __init__(self):
        self.active_giveaways = {}  # message_id: giveaway_data
    
    async def create_giveaway(self, ctx, duration: timedelta, winners: int, prize: str):
        """Create a giveaway"""
        end_time = datetime.now(timezone.utc) + duration
        
        embed = discord.Embed(title='🎉 GIVEAWAY 🎉', color=0xF1C40F)
        embed.add_field(name='Prize', value=prize, inline=False)
        embed.add_field(name='Winners', value=str(winners), inline=True)
        embed.add_field(name='Ends', value=f'<t:{int(end_time.timestamp())}:R>', inline=True)
        embed.add_field(name='How to Enter', value='React with 🎉 to enter!', inline=False)
        embed.set_footer(text=f'Hosted by {ctx.author.display_name}')
        
        msg = await ctx.send(embed=embed)
        await msg.add_reaction('🎉')
        
        self.active_giveaways[msg.id] = {
            'message_id': msg.id,
            'channel_id': ctx.channel.id,
            'guild_id': ctx.guild.id,
            'prize': prize,
            'winners': winners,
            'host': ctx.author.id,
            'end_time': end_time,
            'ended': False
        }
        
        return msg.id
    
    async def end_giveaway(self, bot, giveaway_id, reroll=False):
        """End a giveaway and pick winners"""
        if giveaway_id not in self.active_giveaways:
            return None, "Giveaway not found"
        
        gdata = self.active_giveaways[giveaway_id]
        
        try:
            guild = bot.get_guild(gdata['guild_id'])
            channel = guild.get_channel(gdata['channel_id'])
            message = await channel.fetch_message(gdata['message_id'])
            
            # Get all users who reacted with 🎉
            reaction = None
            for r in message.reactions:
                if str(r.emoji) == '🎉':
                    reaction = r
                    break
            
            if not reaction:
                return None, "No one entered"
            
            # Get all users (exclude bots)
            users = []
            async for user in reaction.users():
                if not user.bot:
                    users.append(user)
            
            if not users:
                return None, "No valid entries"
            
            # Pick random winners
            import random
            winners_count = min(gdata['winners'], len(users))
            winners = random.sample(users, winners_count)
            
            # Announce winners
            winner_mentions = ' '.join([w.mention for w in winners])
            
            embed = discord.Embed(title='🎉 GIVEAWAY ENDED 🎉', color=0x2ECC71)
            embed.add_field(name='Prize', value=gdata['prize'], inline=False)
            embed.add_field(name='Winners', value=winner_mentions, inline=False)
            embed.set_footer(text=f"Hosted by {guild.get_member(gdata['host']).display_name}")
            
            await message.edit(embed=embed)
            await channel.send(f'🎉 Congratulations {winner_mentions}! You won **{gdata["prize"]}**!')
            
            if not reroll:
                gdata['ended'] = True
            
            return winners, None
            
        except Exception as e:
            return None, str(e)

giveaway = GiveawaySystem()

# GIVEAWAY COMMANDS

bot = commands.Bot(command_prefix='$', intents=intents, help_command=None)

async def generate_ticket_id():
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))

async def handle_health(request):
    return web.Response(text='OK', status=200)

async def start_web_server():
    app = web.Application()
    app.router.add_get('/health', handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv('PORT', 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f'Web server started on port {port}')

def is_owner():
    async def predicate(ctx):
        return ctx.author.id == OWNER_ID
    return commands.check(predicate)

def has_admin_perms():
    async def predicate(ctx):
        if ctx.author.id == OWNER_ID: return True
        if ctx.author.guild_permissions.administrator: return True
        admin_role_id = admin_perms.get(ctx.guild.id)
        if not admin_role_id: return False
        admin_role = ctx.guild.get_role(admin_role_id)
        if not admin_role: return False
        for role in ctx.author.roles:
            if role >= admin_role:
                return True
        return False
    return commands.check(predicate)

def has_mod_perms():
    async def predicate(ctx):
        if ctx.author.id == OWNER_ID: return True
        if ctx.author.guild_permissions.administrator: return True
        admin_role_id = admin_perms.get(ctx.guild.id)
        if admin_role_id:
            admin_role = ctx.guild.get_role(admin_role_id)
            if admin_role:
                for role in ctx.author.roles:
                    if role >= admin_role:
                        return True
        mod_role_id = mod_perms.get(ctx.guild.id)
        if not mod_role_id: return False
        mod_role = ctx.guild.get_role(mod_role_id)
        if not mod_role: return False
        for role in ctx.author.roles:
            if role >= mod_role:
                return True
        return False
    return commands.check(predicate)

def parse_duration(duration_str):
    units = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}
    match = re.match(r'^(\d+)([smhd])$', duration_str.lower())
    if not match:
        return None
    amount, unit = int(match.group(1)), match.group(2)
    return timedelta(seconds=amount * units[unit])


async def send_log(guild, title, description=None, color=0x5865F2, fields=None, user=None):
    """Send log to log channel"""
    try:
        async with db.pool.acquire() as conn:
            config = await conn.fetchrow('SELECT log_channel_id FROM config WHERE guild_id = $1', guild.id)
            if config and config['log_channel_id']:
                log_channel = guild.get_channel(config['log_channel_id'])
                if log_channel:
                    embed = discord.Embed(title=title, color=color, timestamp=datetime.now(timezone.utc))
                    if description:
                        embed.description = description
                    if user:
                        embed.set_author(name=str(user), icon_url=user.display_avatar.url)
                    if fields:
                        for name, value in fields.items():
                            embed.add_field(name=name, value=str(value), inline=True)
                    await log_channel.send(embed=embed)
    except Exception as e:
        logger.error(f'log error: {e}')

class MiddlemanModal(Modal, title='Middleman Request'):
    def __init__(self, tier):
        super().__init__()
        self.tier = tier
        self.trader = TextInput(label='Trading with', placeholder='@username or ID', required=True)
        self.giving = TextInput(label='You give', placeholder='e.g., 1 garam', style=discord.TextStyle.paragraph, required=True)
        self.receiving = TextInput(label='You receive', placeholder='e.g., 296 Robux', style=discord.TextStyle.paragraph, required=True)
        self.tip = TextInput(label='Tip (optional)', placeholder='Optional', required=False)
        self.add_item(self.trader)
        self.add_item(self.giving)
        self.add_item(self.receiving)
        self.add_item(self.tip)
    async def on_submit(self, interaction):
        if not rate_limiter.check_cooldown(interaction.user.id, 'ticket', 10):
            return await interaction.response.send_message('⏱️ Wait 10 seconds', ephemeral=True)
        await interaction.response.defer(ephemeral=True)
        try:
            guild = interaction.guild
            user = interaction.user
            async with db.pool.acquire() as conn:
                blacklist = await conn.fetchrow('SELECT * FROM blacklist WHERE user_id = $1 AND guild_id = $2', user.id, guild.id)
                if blacklist:
                    return await interaction.followup.send('❌ You are blacklisted', ephemeral=True)
                config = await conn.fetchrow('SELECT * FROM config WHERE guild_id = $1', guild.id)
                if not config or not config['ticket_category_id']:
                    return await interaction.followup.send('❌ Not configured. Ask admin to run `$setcategory`', ephemeral=True)
            category = guild.get_channel(config['ticket_category_id'])
            if not category:
                return await interaction.followup.send('❌ Category not found', ephemeral=True)
            ticket_id = await generate_ticket_id()
            tier_names_short = {'lowtier': '0-150m', 'midtier': '150m-500m', 'hightier': '500m+'}
            tier_short = tier_names_short.get(self.tier, self.tier)
            channel_name = f'ticket-{tier_short}-{user.name}'
            overwrites = {
                guild.default_role: discord.PermissionOverwrite(read_messages=False),
                user: discord.PermissionOverwrite(read_messages=True, send_messages=True),
                guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True, manage_channels=True)
            }
            role_id = HARDCODED_ROLES.get(self.tier)
            if role_id:
                role = guild.get_role(role_id)
                if role:
                    overwrites[role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
            channel = await category.create_text_channel(name=channel_name, overwrites=overwrites)
            trade_details = {'trader': self.trader.value, 'giving': self.giving.value, 'receiving': self.receiving.value, 'tip': self.tip.value or 'None'}
            async with db.pool.acquire() as conn:
                await conn.execute('INSERT INTO tickets (ticket_id, guild_id, channel_id, user_id, ticket_type, tier, trade_details) VALUES ($1, $2, $3, $4, $5, $6, $7)', ticket_id, guild.id, channel.id, user.id, 'middleman', self.tier, json.dumps(trade_details))
            tier_names = {'lowtier': '0-150M Middleman', 'midtier': '150-500M Middleman', 'hightier': '500M+ Middleman'}
            embed = discord.Embed(color=COLORS.get(self.tier))
            embed.set_author(name=user.display_name, icon_url=user.display_avatar.url)
            embed.title = '⚖️ Middleman Request'
            embed.description = f'**Tier:** {tier_names.get(self.tier)}\n\nA middleman will claim this shortly.\n\n📋 **Guidelines:**\n• Be patient and respectful\n• Provide all necessary info\n• Don\'t spam or ping staff\n• Wait for staff to claim'
            embed.add_field(name='Trading With', value=trade_details['trader'], inline=False)
            embed.add_field(name='Giving', value=trade_details['giving'], inline=True)
            embed.add_field(name='Receiving', value=trade_details['receiving'], inline=True)
            if trade_details['tip'] != 'None':
                embed.add_field(name='Tip', value=trade_details['tip'], inline=False)
            embed.set_footer(text=f'ID: {ticket_id}')
            view = TicketControlView()
            ping_msg = user.mention
            if role_id:
                tier_role = guild.get_role(role_id)
                if tier_role:
                    ping_msg += f" {tier_role.mention}"
            await channel.send(content=ping_msg, embed=embed, view=view)
            if config and config.get('log_channel_id'):
                log_channel = guild.get_channel(config['log_channel_id'])
                if log_channel:
                    log_embed = discord.Embed(title='✅ Ticket Opened', color=COLORS['success'])
                    log_embed.add_field(name='ID', value=f"`{ticket_id}`", inline=True)
                    log_embed.add_field(name='User', value=user.mention, inline=True)
                    log_embed.add_field(name='Tier', value=tier_names.get(self.tier), inline=True)
                    await log_channel.send(embed=log_embed)
            success_embed = discord.Embed(title='✅ Ticket Created', description=f'{channel.mention}', color=COLORS['success'])
            await interaction.followup.send(embed=success_embed, ephemeral=True)
        except Exception as e:
            logger.error(f'Error creating ticket: {e}')
            await interaction.followup.send(f'❌ Error: {str(e)}', ephemeral=True)

class MiddlemanTierSelect(Select):
    def __init__(self):
        options = [
            discord.SelectOption(
                label='0-150M Middleman',
                value='lowtier',
                emoji='🟢',
                description='For trades up to 150M value'
            ),
            discord.SelectOption(
                label='150-500M Middleman',
                value='midtier',
                emoji='🟡',
                description='For trades between 150M-500M value'
            ),
            discord.SelectOption(
                label='500M+ Middleman',
                value='hightier',
                emoji='🔴',
                description='For trades above 500M value'
            )
        ]
        super().__init__(placeholder='Select trade value', options=options)
    async def callback(self, interaction):
        modal = MiddlemanModal(self.values[0])
        await interaction.response.send_modal(modal)

class TicketPanelView(View):
    def __init__(self):
        super().__init__(timeout=None)
    @discord.ui.button(label='Support', style=discord.ButtonStyle.primary, emoji='🎫', custom_id='support_btn')
    async def support_button(self, interaction, button):
        if not rate_limiter.check_cooldown(interaction.user.id, 'ticket', 10):
            return await interaction.response.send_message('⏱️ Wait 10 seconds', ephemeral=True)
        await interaction.response.defer(ephemeral=True)
        try:
            guild = interaction.guild
            user = interaction.user
            async with db.pool.acquire() as conn:
                blacklist = await conn.fetchrow('SELECT * FROM blacklist WHERE user_id = $1 AND guild_id = $2', user.id, guild.id)
                if blacklist:
                    return await interaction.followup.send('❌ You are blacklisted', ephemeral=True)
                config = await conn.fetchrow('SELECT * FROM config WHERE guild_id = $1', guild.id)
                if not config or not config['ticket_category_id']:
                    return await interaction.followup.send('❌ Not configured', ephemeral=True)
            category = guild.get_channel(config['ticket_category_id'])
            ticket_id = await generate_ticket_id()
            channel_name = f'ticket-support-{user.name}'
            overwrites = {
                guild.default_role: discord.PermissionOverwrite(read_messages=False),
                user: discord.PermissionOverwrite(read_messages=True, send_messages=True),
                guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True, manage_channels=True)
            }
            staff_role = guild.get_role(HARDCODED_ROLES['staff'])
            if staff_role:
                overwrites[staff_role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
            channel = await category.create_text_channel(name=channel_name, overwrites=overwrites)
            async with db.pool.acquire() as conn:
                await conn.execute('INSERT INTO tickets (ticket_id, guild_id, channel_id, user_id, ticket_type, tier) VALUES ($1, $2, $3, $4, $5, $6)', ticket_id, guild.id, channel.id, user.id, 'support', 'support')
            embed = discord.Embed(title='🎫 Support Ticket', description='Staff will assist you shortly', color=COLORS['support'])
            embed.set_author(name=user.display_name, icon_url=user.display_avatar.url)
            embed.set_footer(text=f'ID: {ticket_id}')
            view = TicketControlView()
            ping_msg = user.mention
            if staff_role:
                ping_msg += f" {staff_role.mention}"
            await channel.send(content=ping_msg, embed=embed, view=view)
            success_embed = discord.Embed(title='✅ Ticket Created', description=f'{channel.mention}', color=COLORS['success'])
            await interaction.followup.send(embed=success_embed, ephemeral=True)
        except Exception as e:
            logger.error(f'Error: {e}')
            await interaction.followup.send(f'❌ Error: {str(e)}', ephemeral=True)
    @discord.ui.button(label='Middleman', style=discord.ButtonStyle.success, emoji='⚖️', custom_id='middleman_btn')
    async def middleman_button(self, interaction, button):
        embed = discord.Embed(title='⚖️ Select Trade Value', description='Choose your trade tier below', color=COLORS['support'])
        view = View(timeout=300)
        view.add_item(MiddlemanTierSelect())
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

class TicketControlView(View):
    def __init__(self):
        super().__init__(timeout=None)
    @discord.ui.button(label='Claim', style=discord.ButtonStyle.green, custom_id='claim_ticket', emoji='✋')
    async def claim_button(self, interaction, button):
        if not rate_limiter.check_cooldown(interaction.user.id, 'claim', 2):
            return await interaction.response.send_message('⏱️ Wait', ephemeral=True)
        try:
            async with db.pool.acquire() as conn:
                ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', interaction.channel.id)
                if not ticket:
                    return await interaction.response.send_message('❌ Not a ticket', ephemeral=True)
                if ticket['claimed_by']:
                    return await interaction.response.send_message('❌ Already claimed', ephemeral=True)
                await conn.execute('UPDATE tickets SET claimed_by = $1, status = $2 WHERE ticket_id = $3', interaction.user.id, 'claimed', ticket['ticket_id'])
            embed = discord.Embed(title='✋ Claimed', description=f'By {interaction.user.mention}', color=COLORS['success'])
            await interaction.response.send_message(embed=embed)
        except Exception as e:
            logger.error(f'Claim error: {e}')
            await interaction.response.send_message(f'❌ Error: {str(e)}', ephemeral=True)
    @discord.ui.button(label='Unclaim', style=discord.ButtonStyle.gray, custom_id='unclaim_ticket', emoji='↩️')
    async def unclaim_button(self, interaction, button):
        if not rate_limiter.check_cooldown(interaction.user.id, 'unclaim', 2):
            return await interaction.response.send_message('⏱️ Wait', ephemeral=True)
        try:
            async with db.pool.acquire() as conn:
                ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', interaction.channel.id)
                if not ticket:
                    return await interaction.response.send_message('❌ Not a ticket', ephemeral=True)
                if not ticket['claimed_by']:
                    return await interaction.response.send_message('❌ Not claimed', ephemeral=True)
                if ticket['claimed_by'] != interaction.user.id:
                    return await interaction.response.send_message('❌ Only claimer can unclaim', ephemeral=True)
                await conn.execute('UPDATE tickets SET claimed_by = NULL, status = $1 WHERE ticket_id = $2', 'open', ticket['ticket_id'])
            embed = discord.Embed(title='↩️ Unclaimed', description='Ticket is now available', color=COLORS['support'])
            await interaction.response.send_message(embed=embed)
        except Exception as e:
            logger.error(f'Unclaim error: {e}')
            await interaction.response.send_message(f'❌ Error: {str(e)}', ephemeral=True)
# ==================== TICKET COMMANDS ====================

@bot.command(name='close')
async def close_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply('❌ Not a ticket channel')
    if not rate_limiter.check_cooldown(ctx.author.id, 'close', 3):
        return await ctx.reply('⏱️ Wait 3 seconds')
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
            if not ticket:
                return await ctx.reply('❌ Ticket not found')
            if not ticket['claimed_by']:
                return await ctx.reply('❌ Must be claimed first')
            # Check permissions
            has_permission = False
            if ctx.author.id == ticket['claimed_by']:
                has_permission = True
            # Check if they have the right role
            if ticket['ticket_type'] == 'support':
                staff_role = ctx.guild.get_role(HARDCODED_ROLES['staff'])
                if staff_role and staff_role in ctx.author.roles:
                    has_permission = True
            elif ticket['ticket_type'] == 'middleman':
                if ticket.get('tier') and ticket['tier'] in ['lowtier', 'midtier', 'hightier']:
                    tier_role = ctx.guild.get_role(HARDCODED_ROLES.get(ticket['tier']))
                    if tier_role and tier_role in ctx.author.roles:
                        has_permission = True
            # Owner can always close
            if ctx.author.id == OWNER_ID:
                has_permission = True
            if not has_permission:
                return await ctx.reply('❌ You don\'t have permission to close this ticket')
            await conn.execute('UPDATE tickets SET status = $1 WHERE ticket_id = $2', 'closed', ticket['ticket_id'])
        embed = discord.Embed(title='🔒 Closing Ticket', description=f'Closed by {ctx.author.mention}', color=COLORS['error'])
        await ctx.send(embed=embed)
        config = await db.pool.fetchrow('SELECT * FROM config WHERE guild_id = $1', ctx.guild.id)
        if config and config.get('log_channel_id'):
            log_channel = ctx.guild.get_channel(config['log_channel_id'])
            if log_channel:
                opener = ctx.guild.get_member(ticket['user_id'])
                claimer = ctx.guild.get_member(ticket['claimed_by'])
                transcript = f"TRANSCRIPT\n{'='*50}\nID: {ticket['ticket_id']}\nOpened: {opener.name if opener else 'Unknown'}\nClaimed: {claimer.name if claimer else 'Unknown'}\nClosed: {ctx.author.name}\n{'='*50}\n\n"
                messages = []
                async for msg in ctx.channel.history(limit=100, oldest_first=True):
                    content = msg.content if msg.content else '[No content]'
                    messages.append(f"{msg.author.name}: {content}")
                transcript += '\n'.join(messages)
                file = discord.File(fp=io.BytesIO(transcript.encode('utf-8')), filename=f"transcript-{ticket['ticket_id']}.txt")
                log_embed = discord.Embed(title='🔒 Ticket Closed', color=COLORS['error'])
                log_embed.add_field(name='ID', value=ticket['ticket_id'], inline=True)
                await log_channel.send(embed=log_embed, file=file)
        await asyncio.sleep(0.5)
        await ctx.channel.delete()
    except Exception as e:
        logger.error(f'Close error: {e}')
        await ctx.reply(f'❌ Error: {str(e)}')

@bot.command(name='claim')
async def claim_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply('❌ Not a ticket')
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
            if not ticket: return await ctx.reply('❌ Not found')
            if ticket['claimed_by']: return await ctx.reply('❌ Already claimed')
            # Check permissions based on ticket type
            has_permission = False
            if ticket['ticket_type'] == 'support':
                # Support tickets - need staff role
                staff_role = ctx.guild.get_role(HARDCODED_ROLES['staff'])
                if staff_role and staff_role in ctx.author.roles:
                    has_permission = True
            elif ticket['ticket_type'] == 'middleman':
                # Middleman tickets - need appropriate tier role
                if ticket.get('tier') and ticket['tier'] in ['lowtier', 'midtier', 'hightier']:
                    tier_role = ctx.guild.get_role(HARDCODED_ROLES.get(ticket['tier']))
                    if tier_role and tier_role in ctx.author.roles:
                        has_permission = True
            # Owner can always claim
            if ctx.author.id == OWNER_ID:
                has_permission = True
            if not has_permission:
                return await ctx.reply('❌ You don\'t have the required role to claim this ticket')
            await conn.execute('UPDATE tickets SET claimed_by = $1, status = $2 WHERE ticket_id = $3', ctx.author.id, 'claimed', ticket['ticket_id'])
        embed = discord.Embed(title='✋ Ticket Claimed', description=f'By {ctx.author.mention}', color=COLORS['success'])
        await ctx.send(embed=embed)
    except Exception as e:
        await ctx.reply(f'❌ Error: {str(e)}')

@bot.command(name='unclaim')
async def unclaim_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply('❌ Not a ticket')
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
            if not ticket: return await ctx.reply('❌ Not found')
            if not ticket['claimed_by']: return await ctx.reply('❌ Not claimed')
            # Check permissions
            has_permission = False
            if ctx.author.id == ticket['claimed_by']:
                has_permission = True
            # Check if they have the right role
            if ticket['ticket_type'] == 'support':
                staff_role = ctx.guild.get_role(HARDCODED_ROLES['staff'])
                if staff_role and staff_role in ctx.author.roles:
                    has_permission = True
            elif ticket['ticket_type'] == 'middleman':
                if ticket.get('tier') and ticket['tier'] in ['lowtier', 'midtier', 'hightier']:
                    tier_role = ctx.guild.get_role(HARDCODED_ROLES.get(ticket['tier']))
                    if tier_role and tier_role in ctx.author.roles:
                        has_permission = True
            # Owner can always unclaim
            if ctx.author.id == OWNER_ID:
                has_permission = True
            if not has_permission:
                return await ctx.reply('❌ You don\'t have permission to unclaim this ticket')
            await conn.execute('UPDATE tickets SET claimed_by = NULL, status = $1 WHERE ticket_id = $2', 'open', ticket['ticket_id'])
        embed = discord.Embed(title='↩️ Ticket Unclaimed', description='Ticket is now available', color=COLORS['support'])
        await ctx.send(embed=embed)
    except Exception as e:
        await ctx.reply(f'❌ Error: {str(e)}')

@bot.command(name='add')
async def add_cmd(ctx, member: discord.Member = None):
    if not member: return await ctx.reply('❌ Missing user\n\nExample: `$add @John`')
    if not ctx.channel.name.startswith('ticket-'): return await ctx.reply('❌ Not a ticket')
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
            if not ticket: return await ctx.reply('❌ Not found')
            if not ticket['claimed_by']: return await ctx.reply('❌ Must be claimed first')
            # Check permissions
            has_permission = False
            if ctx.author.id == ticket['claimed_by']:
                has_permission = True
            # Check if they have the right role
            if ticket['ticket_type'] == 'support':
                staff_role = ctx.guild.get_role(HARDCODED_ROLES['staff'])
                if staff_role and staff_role in ctx.author.roles:
                    has_permission = True
            elif ticket['ticket_type'] == 'middleman':
                if ticket.get('tier') and ticket['tier'] in ['lowtier', 'midtier', 'hightier']:
                    tier_role = ctx.guild.get_role(HARDCODED_ROLES.get(ticket['tier']))
                    if tier_role and tier_role in ctx.author.roles:
                        has_permission = True
            # Owner can always add
            if ctx.author.id == OWNER_ID:
                has_permission = True
            if not has_permission:
                return await ctx.reply('❌ You don\'t have permission to add users to this ticket')
        await ctx.channel.set_permissions(member, read_messages=True, send_messages=True)
        embed = discord.Embed(title='✅ User Added', description=f'{member.mention} has been added to the ticket', color=COLORS['success'])
        await ctx.reply(embed=embed)
    except Exception as e:
        await ctx.reply(f'❌ Error: {str(e)}')

@bot.command(name='remove')
async def remove_cmd(ctx, member: discord.Member = None):
    if not member: return await ctx.reply('❌ Missing user\n\nExample: `$remove @John`')
    if not ctx.channel.name.startswith('ticket-'): return await ctx.reply('❌ Not a ticket')
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
            if not ticket: return await ctx.reply('❌ Not found')
            if not ticket['claimed_by']: return await ctx.reply('❌ Must be claimed first')
            # Check permissions
            has_permission = False
            if ctx.author.id == ticket['claimed_by']:
                has_permission = True
            # Check if they have the right role
            if ticket['ticket_type'] == 'support':
                staff_role = ctx.guild.get_role(HARDCODED_ROLES['staff'])
                if staff_role and staff_role in ctx.author.roles:
                    has_permission = True
            elif ticket['ticket_type'] == 'middleman':
                if ticket.get('tier') and ticket['tier'] in ['lowtier', 'midtier', 'hightier']:
                    tier_role = ctx.guild.get_role(HARDCODED_ROLES.get(ticket['tier']))
                    if tier_role and tier_role in ctx.author.roles:
                        has_permission = True
            # Owner can always remove
            if ctx.author.id == OWNER_ID:
                has_permission = True
            if not has_permission:
                return await ctx.reply('❌ You don\'t have permission to remove users from this ticket')
        await ctx.channel.set_permissions(member, overwrite=None)
        embed = discord.Embed(title='❌ User Removed', description=f'{member.mention} has been removed from the ticket', color=COLORS['error'])
        await ctx.reply(embed=embed)
    except Exception as e:
        await ctx.reply(f'❌ Error: {str(e)}')

@bot.command(name='rename')
async def rename_cmd(ctx, *, new_name: str = None):
    if not new_name: return await ctx.reply('❌ Missing name\n\nExample: `$rename urgent`')
    if not ctx.channel.name.startswith('ticket-'): return await ctx.reply('❌ Not a ticket')
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
            if not ticket: return await ctx.reply('❌ Not found')
            # Check permissions
            has_permission = False
            if ticket.get('claimed_by') and ctx.author.id == ticket['claimed_by']:
                has_permission = True
            # Check if they have the right role
            if ticket['ticket_type'] == 'support':
                staff_role = ctx.guild.get_role(HARDCODED_ROLES['staff'])
                if staff_role and staff_role in ctx.author.roles:
                    has_permission = True
            elif ticket['ticket_type'] == 'middleman':
                if ticket.get('tier') and ticket['tier'] in ['lowtier', 'midtier', 'hightier']:
                    tier_role = ctx.guild.get_role(HARDCODED_ROLES.get(ticket['tier']))
                    if tier_role and tier_role in ctx.author.roles:
                        has_permission = True
            # Owner can always rename
            if ctx.author.id == OWNER_ID:
                has_permission = True
            if not has_permission:
                return await ctx.reply('❌ You don\'t have permission to rename this ticket')
        await ctx.channel.edit(name=f"ticket-{new_name}")
        embed = discord.Embed(title='✏️ Ticket Renamed', description=f'Now: `ticket-{new_name}`', color=COLORS['support'])
        await ctx.reply(embed=embed)
    except Exception as e:
        await ctx.reply(f'❌ Error: {str(e)}')

@bot.command(name='transfer')
async def transfer_cmd(ctx, member: discord.Member = None):
    if not member: return await ctx.reply('❌ Missing user\n\nExample: `$transfer @John`')
    if not ctx.channel.name.startswith('ticket-'): return await ctx.reply('❌ Not a ticket')
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
            if not ticket: return await ctx.reply('❌ Not found')
            if not ticket['claimed_by']: return await ctx.reply('❌ Must be claimed first')
            # Check permissions
            has_permission = False
            if ctx.author.id == ticket['claimed_by']:
                has_permission = True
            # Check if they have the right role
            if ticket['ticket_type'] == 'support':
                staff_role = ctx.guild.get_role(HARDCODED_ROLES['staff'])
                if staff_role and staff_role in ctx.author.roles:
                    has_permission = True
            elif ticket['ticket_type'] == 'middleman':
                if ticket.get('tier') and ticket['tier'] in ['lowtier', 'midtier', 'hightier']:
                    tier_role = ctx.guild.get_role(HARDCODED_ROLES.get(ticket['tier']))
                    if tier_role and tier_role in ctx.author.roles:
                        has_permission = True
            # Owner can always transfer
            if ctx.author.id == OWNER_ID:
                has_permission = True
            if not has_permission:
                return await ctx.reply('❌ You don\'t have permission to transfer this ticket')
            # Check if target has the right role
            target_has_permission = False
            if ticket['ticket_type'] == 'support':
                staff_role = ctx.guild.get_role(HARDCODED_ROLES['staff'])
                if staff_role and staff_role in member.roles:
                    target_has_permission = True
            elif ticket['ticket_type'] == 'middleman':
                if ticket.get('tier') and ticket['tier'] in ['lowtier', 'midtier', 'hightier']:
                    tier_role = ctx.guild.get_role(HARDCODED_ROLES.get(ticket['tier']))
                    if tier_role and tier_role in member.roles:
                        target_has_permission = True
            if not target_has_permission:
                return await ctx.reply(f'❌ {member.mention} doesn\'t have the required role to handle this ticket')
            # Remove old claimer's permissions (unless they're the owner)
            old_claimer = ctx.guild.get_member(ticket['claimed_by'])
            if old_claimer and old_claimer.id != OWNER_ID:
                await ctx.channel.set_permissions(old_claimer, send_messages=False, read_messages=True)
            # Transfer the ticket
            await conn.execute('UPDATE tickets SET claimed_by = $1 WHERE ticket_id = $2', member.id, ticket['ticket_id'])
        embed = discord.Embed(title='🔄 Ticket Transferred', color=COLORS['support'])
        embed.add_field(name='From', value=ctx.author.mention, inline=True)
        embed.add_field(name='To', value=member.mention, inline=True)
        await ctx.send(f'{member.mention}', embed=embed)
    except Exception as e:
        await ctx.reply(f'❌ Error: {str(e)}')

@bot.command(name='proof')
async def proof_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply('❌ Only usable in tickets')
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
            if not ticket: return await ctx.reply('❌ Not found')
        proof_channel = ctx.guild.get_channel(HARDCODED_CHANNELS['proof'])
        if not proof_channel: return await ctx.reply('❌ Proof channel not found')
        opener = ctx.guild.get_member(ticket['user_id'])
        embed = discord.Embed(title='✅ Trade Completed', color=COLORS['success'])
        embed.add_field(name='Middleman', value=ctx.author.mention, inline=True)
        if ticket.get('tier'):
            tier_names = {'lowtier': '0-150M', 'midtier': '150-500M', 'hightier': '500M+'}
            embed.add_field(name='Tier', value=tier_names.get(ticket['tier']), inline=True)
        embed.add_field(name='Requester', value=opener.mention if opener else 'Unknown', inline=True)
        if ticket.get('trade_details'):
            try:
                details = json.loads(ticket['trade_details']) if isinstance(ticket['trade_details'], str) else ticket['trade_details']
                embed.add_field(name='Trader', value=details.get('trader', 'Unknown'), inline=False)
                embed.add_field(name='Giving', value=details.get('giving', 'N/A'), inline=True)
                embed.add_field(name='Receiving', value=details.get('receiving', 'N/A'), inline=True)
                if details.get('tip') and details['tip'] != 'None':
                    embed.add_field(name='Tip', value=details['tip'], inline=False)
            except: pass
        embed.set_footer(text=f'ID: {ticket["ticket_id"]}')
        await proof_channel.send(embed=embed)
        await ctx.reply(embed=discord.Embed(title='✅ Proof Sent', description=f'Posted to {proof_channel.mention}', color=COLORS['success']))
    except Exception as e:
        await ctx.reply(f'❌ Error: {str(e)}')

# ==================== SETUP COMMANDS ====================

@bot.command(name='setup')
@commands.has_permissions(administrator=True)
async def setup_cmd(ctx):
    embed = discord.Embed(
        title='🎟️ Ticket Center | Support & Middleman',
        description=(
            "🛠️ **Support**\n"
            "• General support\n"
            "• Claiming giveaway or event prizes\n"
            "• Partnership requests\n\n"
            "⚖️ **Middleman**\n"
            "• Secure & verified trading\n"
            "• Trusted middleman services\n"
            "• Trades protected by trusted middlemen"
        ),
        color=COLORS['support']
    )
    await ctx.send(embed=embed, view=TicketPanelView())
    try: await ctx.message.delete()
    except: pass

@bot.command(name='setcategory')
@commands.has_permissions(administrator=True)
async def setcategory_cmd(ctx, category: discord.CategoryChannel = None):
    if not category: return await ctx.reply('❌ Missing category\n\nExample: `$setcategory #tickets`')
    async with db.pool.acquire() as conn:
        await conn.execute('INSERT INTO config (guild_id, ticket_category_id) VALUES ($1, $2) ON CONFLICT (guild_id) DO UPDATE SET ticket_category_id = $2', ctx.guild.id, category.id)
    await ctx.reply(embed=discord.Embed(title='✅ Category Set', description=f'{category.mention}', color=COLORS['success']))

@bot.command(name='setlogs')
@commands.has_permissions(administrator=True)
async def setlogs_cmd(ctx, channel: discord.TextChannel = None):
    if not channel: return await ctx.reply('❌ Missing channel\n\nExample: `$setlogs #logs`')
    async with db.pool.acquire() as conn:
        await conn.execute('INSERT INTO config (guild_id, log_channel_id) VALUES ($1, $2) ON CONFLICT (guild_id) DO UPDATE SET log_channel_id = $2', ctx.guild.id, channel.id)
    await ctx.reply(embed=discord.Embed(title='✅ Logs Set', description=f'{channel.mention}', color=COLORS['success']))

@bot.command(name='config')
@commands.has_permissions(administrator=True)
async def config_cmd(ctx):
    async with db.pool.acquire() as conn:
        config = await conn.fetchrow('SELECT * FROM config WHERE guild_id = $1', ctx.guild.id)
    embed = discord.Embed(title='⚙️ Bot Configuration', color=COLORS['support'])
    if config:
        category = ctx.guild.get_channel(config.get('ticket_category_id')) if config.get('ticket_category_id') else None
        log_channel = ctx.guild.get_channel(config.get('log_channel_id')) if config.get('log_channel_id') else None
        embed.add_field(name='Ticket Category', value=category.mention if category else '❌ Not set', inline=True)
        embed.add_field(name='Log Channel', value=log_channel.mention if log_channel else '❌ Not set', inline=True)
    proof_channel = ctx.guild.get_channel(HARDCODED_CHANNELS['proof'])
    embed.add_field(name='Proof Channel', value=proof_channel.mention if proof_channel else '❌ Not found', inline=True)
    admin_role = ctx.guild.get_role(admin_perms.get(ctx.guild.id)) if admin_perms.get(ctx.guild.id) else None
    mod_role = ctx.guild.get_role(mod_perms.get(ctx.guild.id)) if mod_perms.get(ctx.guild.id) else None
    embed.add_field(name='Admin Role', value=admin_role.mention if admin_role else '❌ Not set', inline=True)
    embed.add_field(name='Mod Role', value=mod_role.mention if mod_role else '❌ Not set', inline=True)
    await ctx.reply(embed=embed)

# ==================== JAIL COMMANDS ====================

@bot.command(name='jail')
@commands.has_permissions(administrator=True)
async def jail_cmd(ctx, member: discord.Member = None, *, reason: str = "No reason"):
    if not member: return await ctx.reply('❌ Missing user\n\nExample: `$jail @User Scamming`')
    if member.bot: return await ctx.reply('❌ Cannot jail bots')
    if member.id == ctx.author.id: return await ctx.reply('❌ Cannot jail yourself')
    try:
        async with db.pool.acquire() as conn:
            existing = await conn.fetchrow('SELECT * FROM jailed_users WHERE user_id = $1', member.id)
            if existing: return await ctx.reply('❌ User already jailed')
        role_ids = [role.id for role in member.roles if role.id != ctx.guild.id]
        for role in member.roles:
            if role.id != ctx.guild.id:
                try: await member.remove_roles(role)
                except: pass
        jailed_role = ctx.guild.get_role(HARDCODED_ROLES['jailed'])
        if jailed_role: await member.add_roles(jailed_role)
        async with db.pool.acquire() as conn:
            await conn.execute('INSERT INTO jailed_users (user_id, guild_id, saved_roles, reason, jailed_by) VALUES ($1, $2, $3, $4, $5)', member.id, ctx.guild.id, json.dumps(role_ids), reason, ctx.author.id)
        embed = discord.Embed(title='🚔 User Jailed', color=COLORS['error'])
        embed.add_field(name='User', value=member.mention, inline=True)
        embed.add_field(name='Reason', value=reason, inline=True)
        embed.add_field(name='Roles Saved', value=f'{len(role_ids)} roles', inline=True)
        await ctx.reply(embed=embed)
    except Exception as e:
        await ctx.reply(f'❌ Error: {str(e)}')

@bot.command(name='unjail')
@commands.has_permissions(administrator=True)
async def unjail_cmd(ctx, member: discord.Member = None):
    if not member: return await ctx.reply('❌ Missing user\n\nExample: `$unjail @User`')
    try:
        async with db.pool.acquire() as conn:
            jailed_data = await conn.fetchrow('SELECT * FROM jailed_users WHERE user_id = $1', member.id)
            if not jailed_data: return await ctx.reply('❌ User not jailed')
        jailed_role = ctx.guild.get_role(HARDCODED_ROLES['jailed'])
        if jailed_role and jailed_role in member.roles:
            await member.remove_roles(jailed_role)
        saved_roles = json.loads(jailed_data['saved_roles']) if isinstance(jailed_data['saved_roles'], str) else jailed_data['saved_roles']
        restored = 0
        for role_id in saved_roles:
            role = ctx.guild.get_role(role_id)
            if role:
                try: await member.add_roles(role); restored += 1
                except: pass
        async with db.pool.acquire() as conn:
            await conn.execute('DELETE FROM jailed_users WHERE user_id = $1', member.id)
        embed = discord.Embed(title='✅ User Unjailed', color=COLORS['success'])
        embed.add_field(name='User', value=member.mention, inline=True)
        embed.add_field(name='Roles Restored', value=f'{restored}/{len(saved_roles)}', inline=True)
        await ctx.reply(embed=embed)
    except Exception as e:
        await ctx.reply(f'❌ Error: {str(e)}')

@bot.command(name='jailed')
@commands.has_permissions(administrator=True)
async def jailed_cmd(ctx):
    async with db.pool.acquire() as conn:
        rows = await conn.fetch('SELECT * FROM jailed_users WHERE guild_id = $1', ctx.guild.id)
    if not rows: return await ctx.reply('✅ No jailed users')
    embed = discord.Embed(title='🚔 Jailed Users', color=COLORS['error'])
    for row in rows:
        user = ctx.guild.get_member(row['user_id'])
        embed.add_field(name=user.mention if user else f"ID: {row['user_id']}", value=f"Reason: {row['reason']}", inline=False)
    await ctx.reply(embed=embed)

# ==================== BLACKLIST COMMANDS ====================

@bot.command(name='blacklist')
@commands.has_permissions(administrator=True)
async def blacklist_cmd(ctx, member: discord.Member = None, *, reason: str = "No reason"):
    if not member: return await ctx.reply('❌ Missing user\n\nExample: `$blacklist @Scammer Fraud`')
    async with db.pool.acquire() as conn:
        await conn.execute('INSERT INTO blacklist (user_id, guild_id, reason, blacklisted_by) VALUES ($1, $2, $3, $4) ON CONFLICT (user_id) DO UPDATE SET reason = $3, blacklisted_by = $4', member.id, ctx.guild.id, reason, ctx.author.id)
    embed = discord.Embed(title='🚫 User Blacklisted', color=COLORS['error'])
    embed.add_field(name='User', value=member.mention, inline=True)
    embed.add_field(name='Reason', value=reason, inline=True)
    await ctx.reply(embed=embed)

@bot.command(name='unblacklist')
@commands.has_permissions(administrator=True)
async def unblacklist_cmd(ctx, member: discord.Member = None):
    if not member: return await ctx.reply('❌ Missing user\n\nExample: `$unblacklist @John`')
    async with db.pool.acquire() as conn:
        await conn.execute('DELETE FROM blacklist WHERE user_id = $1', member.id)
    await ctx.reply(embed=discord.Embed(title='✅ Unblacklisted', description=f'{member.mention} can now create tickets', color=COLORS['success']))

@bot.command(name='blacklists')
@commands.has_permissions(administrator=True)
async def blacklists_cmd(ctx):
    async with db.pool.acquire() as conn:
        rows = await conn.fetch('SELECT * FROM blacklist WHERE guild_id = $1', ctx.guild.id)
    if not rows: return await ctx.reply('✅ No blacklisted users')
    embed = discord.Embed(title='🚫 Blacklisted Users', color=COLORS['error'])
    for row in rows:
        user = ctx.guild.get_member(row['user_id'])
        embed.add_field(name=user.mention if user else f"ID: {row['user_id']}", value=f"Reason: {row['reason']}", inline=False)
    await ctx.reply(embed=embed)

@bot.command(name='clear')
@commands.has_permissions(manage_messages=True)
async def clear_cmd(ctx):
    deleted = 0
    async for message in ctx.channel.history(limit=100):
        if message.author == bot.user:
            try: await message.delete(); deleted += 1; await asyncio.sleep(0.5)
            except: pass
    msg = await ctx.send(embed=discord.Embed(title='🧹 Cleared', description=f'Deleted {deleted} bot messages', color=COLORS['success']))
    await asyncio.sleep(3)
    await msg.delete()
    try: await ctx.message.delete()
    except: pass

@bot.command(name='purge')
@has_admin_perms()
async def purge_cmd(ctx, amount: int = None, member: discord.Member = None):
    if not amount: return await ctx.reply('❌ Missing amount\n\nExamples:\n`$purge 10` - Delete 10 messages\n`$purge 50 @User` - Delete 50 messages from user')
    if amount < 1 or amount > 100: return await ctx.reply('❌ Amount must be between 1 and 100')
    try:
        await ctx.message.delete()
    except: pass
    
    deleted = 0
    if member:
        # Delete messages from specific user
        def check(m):
            return m.author.id == member.id
        deleted_msgs = await ctx.channel.purge(limit=amount, check=check)
        deleted = len(deleted_msgs)
        msg = await ctx.send(embed=discord.Embed(title='🧹 Purged', description=f'Deleted **{deleted}** messages from {member.mention}', color=COLORS['success']))
    else:
        # Delete all messages
        deleted_msgs = await ctx.channel.purge(limit=amount)
        deleted = len(deleted_msgs)
        msg = await ctx.send(embed=discord.Embed(title='🧹 Purged', description=f'Deleted **{deleted}** messages', color=COLORS['success']))
    
    await asyncio.sleep(3)
    try: await msg.delete()
    except: pass

# ==================== BAN COMMANDS ====================

@bot.command(name='ban', aliases=['b'])
@has_admin_perms()
async def ban_cmd(ctx, member: discord.Member = None, *, reason: str = "No reason"):
    if not member: return await ctx.reply('❌ Missing user\n\nExample: `$ban @User Scamming` or `$b @User`')
    if member.id == ctx.author.id: return await ctx.reply('❌ You cannot ban yourself')
    if member.top_role >= ctx.author.top_role and ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ Cannot ban someone with a higher or equal role')
    if member.top_role >= ctx.guild.me.top_role:
        return await ctx.reply('❌ My role is too low to ban that user')
    try:
        try:
            await member.send(embed=discord.Embed(title=f'🔨 Banned from {ctx.guild.name}', description=f'**Reason:** {reason}\n**Banned by:** {ctx.author}', color=COLORS['error']))
        except: pass
        await member.ban(reason=f'{reason} | Banned by {ctx.author}', delete_message_days=0)
        embed = discord.Embed(title='🔨 User Banned', color=COLORS['error'])
        embed.add_field(name='User', value=f'{member} ({member.id})', inline=True)
        embed.add_field(name='Reason', value=reason, inline=True)
        embed.add_field(name='Banned By', value=ctx.author.mention, inline=True)
        await ctx.reply(embed=embed)
    except Exception as e:
        await ctx.reply(f'❌ Error: {str(e)}')

@bot.command(name='unban', aliases=['ub'])
@has_admin_perms()
async def unban_cmd(ctx, user_id: int = None, *, reason: str = "No reason"):
    if not user_id: return await ctx.reply('❌ Missing user ID\n\nExample: `$unban 123456789` or `$ub 123456789`')
    try:
        user = await bot.fetch_user(user_id)
        await ctx.guild.unban(user, reason=f'{reason} | Unbanned by {ctx.author}')
        embed = discord.Embed(title='✅ User Unbanned', color=COLORS['success'])
        embed.add_field(name='User', value=f'{user} ({user.id})', inline=True)
        embed.add_field(name='Reason', value=reason, inline=True)
        await ctx.reply(embed=embed)
    except discord.NotFound:
        await ctx.reply('❌ User not found or not banned')
    except Exception as e:
        await ctx.reply(f'❌ Error: {str(e)}')

@bot.command(name='hackban', aliases=['hb'])
@has_admin_perms()
async def hackban_cmd(ctx, user_id: int = None, *, reason: str = "No reason"):
    if not user_id: return await ctx.reply('❌ Missing user ID\n\nExample: `$hackban 123456789 Known scammer` or `$hb 123456789`')
    try:
        user = await bot.fetch_user(user_id)
        await ctx.guild.ban(user, reason=f'{reason} | Hackbanned by {ctx.author}', delete_message_days=0)
        embed = discord.Embed(title='🔨 User Hackbanned', color=COLORS['error'])
        embed.add_field(name='User', value=f'{user} ({user.id})', inline=True)
        embed.add_field(name='Reason', value=reason, inline=True)
        embed.add_field(name='Banned By', value=ctx.author.mention, inline=True)
        await ctx.reply(embed=embed)
    except discord.NotFound:
        await ctx.reply('❌ User not found')
    except Exception as e:
        await ctx.reply(f'❌ Error: {str(e)}')

@bot.command(name='unhackban', aliases=['uhb'])
@has_admin_perms()
async def unhackban_cmd(ctx, user_id: int = None):
    if not user_id: return await ctx.reply('❌ Missing user ID\n\nExample: `$unhackban 123456789` or `$uhb 123456789`')
    try:
        user = await bot.fetch_user(user_id)
        await ctx.guild.unban(user, reason=f'Unhackbanned by {ctx.author}')
        embed = discord.Embed(title='✅ User Unhackbanned', color=COLORS['success'])
        embed.add_field(name='User', value=f'{user} ({user.id})', inline=True)
        await ctx.reply(embed=embed)
    except discord.NotFound:
        await ctx.reply('❌ User not found or not banned')
    except Exception as e:
        await ctx.reply(f'❌ Error: {str(e)}')

@bot.command(name='kick', aliases=['k'])
@has_admin_perms()
async def kick_cmd(ctx, member: discord.Member = None, *, reason: str = "No reason"):
    if not member: return await ctx.reply('❌ Missing user\n\nExample: `$kick @User Breaking rules` or `$k @User`')
    if member.id == ctx.author.id: return await ctx.reply('❌ You cannot kick yourself')
    if member.top_role >= ctx.author.top_role and ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ Cannot kick someone with a higher or equal role')
    if member.top_role >= ctx.guild.me.top_role:
        return await ctx.reply('❌ My role is too low to kick that user')
    try:
        try:
            await member.send(embed=discord.Embed(title=f'👢 Kicked from {ctx.guild.name}', description=f'**Reason:** {reason}\n**Kicked by:** {ctx.author}', color=COLORS['error']))
        except: pass
        await member.kick(reason=f'{reason} | Kicked by {ctx.author}')
        embed = discord.Embed(title='👢 User Kicked', color=COLORS['error'])
        embed.add_field(name='User', value=f'{member} ({member.id})', inline=True)
        embed.add_field(name='Reason', value=reason, inline=True)
        embed.add_field(name='Kicked By', value=ctx.author.mention, inline=True)
        await ctx.reply(embed=embed)
    except Exception as e:
        await ctx.reply(f'❌ Error: {str(e)}')

# ==================== MUTE/WARN COMMANDS ====================

@bot.command(name='mute', aliases=['m'])
@has_mod_perms()
async def mute_cmd(ctx, member: discord.Member = None, duration: str = None, *, reason: str = "No reason"):
    if not member: return await ctx.reply('❌ Missing user\n\nExample: `$mute @User 10m Spamming` or `$m @User 1h`')
    if not duration: return await ctx.reply('❌ Missing duration\n\nExamples: `10s` `5m` `1h` `1d`')
    if member.id == ctx.author.id: return await ctx.reply('❌ Cannot mute yourself')
    if member.top_role >= ctx.author.top_role and ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ Cannot mute someone with a higher or equal role')
    delta = parse_duration(duration)
    if not delta: return await ctx.reply('❌ Invalid duration format\n\nExamples: `10s` `5m` `1h` `1d`')
    try:
        await member.timeout(delta, reason=f'{reason} | Muted by {ctx.author}')
        embed = discord.Embed(title='🔇 User Muted', color=COLORS['error'])
        embed.add_field(name='User', value=member.mention, inline=True)
        embed.add_field(name='Duration', value=duration, inline=True)
        embed.add_field(name='Reason', value=reason, inline=True)
        embed.add_field(name='Muted By', value=ctx.author.mention, inline=True)
        await ctx.reply(embed=embed)
    except Exception as e:
        await ctx.reply(f'❌ Error: {str(e)}')

@bot.command(name='unmute', aliases=['um'])
@has_mod_perms()
async def unmute_cmd(ctx, member: discord.Member = None):
    if not member: return await ctx.reply('❌ Missing user\n\nExample: `$unmute @User` or `$um @User`')
    try:
        await member.timeout(None, reason=f'Unmuted by {ctx.author}')
        embed = discord.Embed(title='🔊 User Unmuted', description=f'{member.mention} has been unmuted', color=COLORS['success'])
        await ctx.reply(embed=embed)
    except Exception as e:
        await ctx.reply(f'❌ Error: {str(e)}')

@bot.command(name='warn', aliases=['w'])
@has_mod_perms()
async def warn_cmd(ctx, member: discord.Member = None, *, reason: str = "No reason"):
    if not member: return await ctx.reply('❌ Missing user\n\nExample: `$warn @User Swearing` or `$w @User`')
    if member.bot: return await ctx.reply('❌ Cannot warn bots')
    try:
        async with db.pool.acquire() as conn:
            await conn.execute('INSERT INTO warnings (guild_id, user_id, reason, warned_by) VALUES ($1, $2, $3, $4)', ctx.guild.id, member.id, reason, ctx.author.id)
            count = await conn.fetchval('SELECT COUNT(*) FROM warnings WHERE guild_id = $1 AND user_id = $2', ctx.guild.id, member.id)
        try:
            await member.send(embed=discord.Embed(title=f'⚠️ Warning in {ctx.guild.name}', description=f'**Reason:** {reason}\n**Warned by:** {ctx.author}\n**Total warnings:** {count}', color=COLORS['error']))
        except: pass
        embed = discord.Embed(title='⚠️ User Warned', color=COLORS['error'])
        embed.add_field(name='User', value=member.mention, inline=True)
        embed.add_field(name='Reason', value=reason, inline=True)
        embed.add_field(name='Total Warnings', value=str(count), inline=True)
        embed.add_field(name='Warned By', value=ctx.author.mention, inline=True)
        await ctx.reply(embed=embed)
    except Exception as e:
        await ctx.reply(f'❌ Error: {str(e)}')

@bot.command(name='warnings', aliases=['ws'])
@has_mod_perms()
async def warnings_cmd(ctx, member: discord.Member = None):
    if not member: return await ctx.reply('❌ Missing user\n\nExample: `$warnings @User` or `$ws @User`')
    async with db.pool.acquire() as conn:
        rows = await conn.fetch('SELECT * FROM warnings WHERE guild_id = $1 AND user_id = $2 ORDER BY warned_at DESC', ctx.guild.id, member.id)
    if not rows: return await ctx.reply(f'✅ {member.mention} has no warnings')
    embed = discord.Embed(title=f'⚠️ Warnings for {member.display_name}', color=COLORS['error'])
    embed.set_thumbnail(url=member.display_avatar.url)
    for i, row in enumerate(rows[:10], 1):
        warner = ctx.guild.get_member(row['warned_by'])
        embed.add_field(name=f'#{i}', value=f"**Reason:** {row['reason']}\n**By:** {warner.mention if warner else 'Unknown'}", inline=False)
    embed.set_footer(text=f'Total: {len(rows)} warning(s)')
    await ctx.reply(embed=embed)

@bot.command(name='clearwarnings', aliases=['cw'])
@has_mod_perms()
async def clearwarnings_cmd(ctx, member: discord.Member = None):
    if not member: return await ctx.reply('❌ Missing user\n\nExample: `$clearwarnings @User` or `$cw @User`')
    async with db.pool.acquire() as conn:
        count = await conn.fetchval('SELECT COUNT(*) FROM warnings WHERE guild_id = $1 AND user_id = $2', ctx.guild.id, member.id)
        await conn.execute('DELETE FROM warnings WHERE guild_id = $1 AND user_id = $2', ctx.guild.id, member.id)
    embed = discord.Embed(title='✅ Warnings Cleared', description=f'Cleared **{count}** warning(s) for {member.mention}', color=COLORS['success'])
    await ctx.reply(embed=embed)

# ==================== ROLE COMMAND ====================

@bot.command(name='role', aliases=['r'])
@has_admin_perms()
async def role_cmd(ctx, member: discord.Member = None, *, role_name: str = None):
    if not member or not role_name:
        return await ctx.reply('❌ Missing arguments\n\nExample: `$role @User owner` or `$r @User mod`\n*(No ping needed for role name)*')
    role = discord.utils.find(lambda r: r.name.lower() == role_name.lower(), ctx.guild.roles)
    if not role: return await ctx.reply(f'❌ Role `{role_name}` not found')
    if role >= ctx.author.top_role and ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ Cannot give/remove a role higher than or equal to your own')
    if role >= ctx.guild.me.top_role:
        return await ctx.reply('❌ That role is higher than my role')
    if role in member.roles:
        if role == member.top_role and ctx.author.id != OWNER_ID:
            return await ctx.reply("❌ Cannot remove someone's highest role")
        await member.remove_roles(role)
        embed = discord.Embed(title='➖ Role Removed', color=COLORS['error'])
        embed.add_field(name='User', value=member.mention, inline=True)
        embed.add_field(name='Role', value=role.mention, inline=True)
    else:
        await member.add_roles(role)
        embed = discord.Embed(title='➕ Role Added', color=COLORS['success'])
        embed.add_field(name='User', value=member.mention, inline=True)
        embed.add_field(name='Role', value=role.mention, inline=True)
    await ctx.reply(embed=embed)
# ==================== CHANNEL MANAGEMENT ====================

@bot.command(name='slowmode', aliases=['sm'])
@has_admin_perms()
async def slowmode_cmd(ctx, seconds: int = None):
    if seconds is None: return await ctx.reply('❌ Missing seconds\n\nExample: `$slowmode 5` or `$sm 0` to disable')
    if seconds < 0 or seconds > 21600: return await ctx.reply('❌ Must be between 0 and 21600 seconds')
    await ctx.channel.edit(slowmode_delay=seconds)
    if seconds == 0:
        embed = discord.Embed(title='✅ Slowmode Disabled', description=f'Slowmode removed in {ctx.channel.mention}', color=COLORS['success'])
    else:
        embed = discord.Embed(title='🐢 Slowmode Set', description=f'Set to **{seconds}s** in {ctx.channel.mention}', color=COLORS['support'])
    await ctx.reply(embed=embed)

@bot.command(name='lock', aliases=['lk'])
@has_admin_perms()
async def lock_cmd(ctx):
    # Lock for @everyone
    await ctx.channel.set_permissions(ctx.guild.default_role, send_messages=False)
    # Allow staff role to talk
    staff_role = ctx.guild.get_role(HARDCODED_ROLES['staff'])
    if staff_role:
        await ctx.channel.set_permissions(staff_role, send_messages=True)
    # Allow mod role to talk
    mod_role_id = mod_perms.get(ctx.guild.id)
    if mod_role_id:
        mod_role = ctx.guild.get_role(mod_role_id)
        if mod_role:
            await ctx.channel.set_permissions(mod_role, send_messages=True)
    # Allow admin role to talk
    admin_role_id = admin_perms.get(ctx.guild.id)
    if admin_role_id:
        admin_role = ctx.guild.get_role(admin_role_id)
        if admin_role:
            await ctx.channel.set_permissions(admin_role, send_messages=True)
    embed = discord.Embed(title='🔒 Channel Locked', description=f'{ctx.channel.mention} has been locked', color=COLORS['error'])
    await ctx.send(embed=embed)

@bot.command(name='unlock', aliases=['ulk'])
@has_admin_perms()
async def unlock_cmd(ctx):
    await ctx.channel.set_permissions(ctx.guild.default_role, send_messages=None)
    embed = discord.Embed(title='🔓 Channel Unlocked', description=f'{ctx.channel.mention} has been unlocked', color=COLORS['success'])
    await ctx.send(embed=embed)

@bot.command(name='hide', aliases=['hd'])
@has_admin_perms()
async def hide_cmd(ctx):
    await ctx.channel.set_permissions(ctx.guild.default_role, view_channel=False)
    embed = discord.Embed(title='👁️ Channel Hidden', description=f'{ctx.channel.mention} is now hidden', color=COLORS['error'])
    await ctx.send(embed=embed)

@bot.command(name='unhide', aliases=['uhd'])
@has_admin_perms()
async def unhide_cmd(ctx):
    await ctx.channel.set_permissions(ctx.guild.default_role, view_channel=None)
    embed = discord.Embed(title='👁️ Channel Visible', description=f'{ctx.channel.mention} is now visible', color=COLORS['success'])
    await ctx.send(embed=embed)

@bot.command(name='nick', aliases=['n'])
@has_mod_perms()
async def nick_cmd(ctx, member: discord.Member = None, *, nickname: str = None):
    if not member: return await ctx.reply('❌ Missing user\n\nExample: `$nick @User CoolName` or `$n @User reset` to reset')
    try:
        if nickname and nickname.lower() == 'reset':
            await member.edit(nick=None)
            embed = discord.Embed(title='✏️ Nickname Reset', description=f'{member.mention}\'s nickname has been reset', color=COLORS['success'])
        else:
            await member.edit(nick=nickname)
            embed = discord.Embed(title='✏️ Nickname Changed', color=COLORS['success'])
            embed.add_field(name='User', value=member.mention, inline=True)
            embed.add_field(name='Nickname', value=nickname or 'None', inline=True)
        await ctx.reply(embed=embed)
    except Exception as e:
        await ctx.reply(f'❌ Error: {str(e)}')

# ==================== WHITELIST COMMANDS ====================

@bot.command(name='whitelist')
@is_owner()
async def whitelist_cmd(ctx, protection: str = None, *, target: str = None):
    if not protection or not target:
        return await ctx.reply('❌ Missing arguments\n\n**Usage:**\n`$whitelist anti-link @Role/@Member`\n`$whitelist anti-spam @Role/@Member`\n`$whitelist anti-nuke @Role/@Member`')
    protection = protection.lower()
    role = None
    member = None
    try:
        role = await commands.RoleConverter().convert(ctx, target)
    except:
        try:
            member = await commands.MemberConverter().convert(ctx, target)
        except:
            return await ctx.reply('❌ Invalid role/member')
    target_obj = role or member
    target_id = target_obj.id
    target_type = 'Role' if role else 'Member'
    if protection == 'anti-link':
        lst = anti_link.whitelisted_roles if role else anti_link.whitelisted_users
        if target_id in lst[ctx.guild.id]: return await ctx.reply(f'❌ Already whitelisted for anti-link')
        lst[ctx.guild.id].append(target_id)
        embed = discord.Embed(title='✅ Anti-Link Whitelist Added', description=f'{target_obj.mention} ({target_type}) can now post links', color=COLORS['success'])
    elif protection == 'anti-spam':
        lst = anti_spam.whitelisted_roles if role else anti_spam.whitelisted_users
        if target_id in lst[ctx.guild.id]: return await ctx.reply(f'❌ Already whitelisted for anti-spam')
        lst[ctx.guild.id].append(target_id)
        embed = discord.Embed(title='✅ Anti-Spam Whitelist Added', description=f'{target_obj.mention} ({target_type}) is exempt from spam detection', color=COLORS['success'])
    elif protection == 'anti-nuke':
        lst = anti_nuke.whitelisted_roles if role else anti_nuke.whitelisted_users
        if target_id in lst[ctx.guild.id]: return await ctx.reply(f'❌ Already whitelisted for anti-nuke')
        lst[ctx.guild.id].append(target_id)
        embed = discord.Embed(title='✅ Anti-Nuke Whitelist Added', description=f'{target_obj.mention} ({target_type}) can now add bots/integrations', color=COLORS['success'])
    else:
        return await ctx.reply('❌ Invalid protection\n\nOptions: `anti-link` `anti-spam` `anti-nuke`')
    await ctx.reply(embed=embed)

@bot.command(name='unwhitelist')
@is_owner()
async def unwhitelist_cmd(ctx, protection: str = None, *, target: str = None):
    if not protection or not target:
        return await ctx.reply('❌ Missing arguments\n\n**Usage:**\n`$unwhitelist anti-link @Role/@Member`\n`$unwhitelist anti-spam @Role/@Member`\n`$unwhitelist anti-nuke @Role/@Member`')
    protection = protection.lower()
    role = None
    member = None
    try:
        role = await commands.RoleConverter().convert(ctx, target)
    except:
        try:
            member = await commands.MemberConverter().convert(ctx, target)
        except:
            return await ctx.reply('❌ Invalid role/member')
    target_obj = role or member
    target_id = target_obj.id
    removed = False
    if protection == 'anti-link':
        lst = anti_link.whitelisted_roles if role else anti_link.whitelisted_users
        if target_id in lst[ctx.guild.id]: lst[ctx.guild.id].remove(target_id); removed = True
    elif protection == 'anti-spam':
        lst = anti_spam.whitelisted_roles if role else anti_spam.whitelisted_users
        if target_id in lst[ctx.guild.id]: lst[ctx.guild.id].remove(target_id); removed = True
    elif protection == 'anti-nuke':
        lst = anti_nuke.whitelisted_roles if role else anti_nuke.whitelisted_users
        if target_id in lst[ctx.guild.id]: lst[ctx.guild.id].remove(target_id); removed = True
    else:
        return await ctx.reply('❌ Invalid protection')
    if removed:
        await ctx.reply(embed=discord.Embed(title='✅ Whitelist Removed', description=f'{target_obj.mention} removed from **{protection}** whitelist', color=COLORS['success']))
    else:
        await ctx.reply(f'❌ {target_obj.mention} was not whitelisted for {protection}')

@bot.command(name='whitelisted')
@is_owner()
async def whitelisted_cmd(ctx, protection: str = None):
    if not protection:
        return await ctx.reply('❌ Missing protection\n\nOptions: `anti-link` `anti-spam` `anti-nuke`')
    protection = protection.lower()
    if protection == 'anti-link':
        users = anti_link.whitelisted_users.get(ctx.guild.id, [])
        roles = anti_link.whitelisted_roles.get(ctx.guild.id, [])
        title = '🔗 Anti-Link Whitelist'
    elif protection == 'anti-spam':
        users = anti_spam.whitelisted_users.get(ctx.guild.id, [])
        roles = anti_spam.whitelisted_roles.get(ctx.guild.id, [])
        title = '💬 Anti-Spam Whitelist'
    elif protection == 'anti-nuke':
        users = anti_nuke.whitelisted_users.get(ctx.guild.id, [])
        roles = anti_nuke.whitelisted_roles.get(ctx.guild.id, [])
        title = '🛡️ Anti-Nuke Whitelist'
    else:
        return await ctx.reply('❌ Invalid protection')
    if not users and not roles: return await ctx.reply(f'No whitelisted roles or members for **{protection}**')
    embed = discord.Embed(title=title, color=COLORS['support'])
    if roles:
        role_text = '\n'.join([ctx.guild.get_role(r).mention for r in roles if ctx.guild.get_role(r)])
        if role_text: embed.add_field(name='Roles', value=role_text, inline=False)
    if users:
        user_text = '\n'.join([ctx.guild.get_member(u).mention for u in users if ctx.guild.get_member(u)])
        if user_text: embed.add_field(name='Members', value=user_text, inline=False)
    await ctx.reply(embed=embed)

# ==================== ANTI PROTECTION COMMANDS ====================

@bot.command(name='lockdown')
@is_owner()
async def lockdown_cmd(ctx):
    if lockdown.is_locked(ctx.guild.id): return await ctx.reply('❌ Already locked')
    locked = 0
    staff_role = ctx.guild.get_role(HARDCODED_ROLES['staff'])
    mod_role_id = mod_perms.get(ctx.guild.id)
    mod_role = ctx.guild.get_role(mod_role_id) if mod_role_id else None
    admin_role_id = admin_perms.get(ctx.guild.id)
    admin_role = ctx.guild.get_role(admin_role_id) if admin_role_id else None
    for channel in ctx.guild.text_channels:
        try:
            await channel.set_permissions(ctx.guild.default_role, send_messages=False)
            if staff_role:
                await channel.set_permissions(staff_role, send_messages=True)
            if mod_role:
                await channel.set_permissions(mod_role, send_messages=True)
            if admin_role:
                await channel.set_permissions(admin_role, send_messages=True)
            lockdown.locked_channels[ctx.guild.id].append(channel.id)
            locked += 1
        except: pass
    embed = discord.Embed(title='🔒 Server Locked Down', description=f'**{locked} channels** locked\nStaff/Mod/Admin roles can still talk\nUse `$unlockdown` to restore', color=COLORS['error'])
    await ctx.reply(embed=embed)

@bot.command(name='unlockdown')
@is_owner()
async def unlockdown_cmd(ctx):
    if not lockdown.is_locked(ctx.guild.id): return await ctx.reply('❌ Not locked')
    unlocked = 0
    for channel_id in lockdown.locked_channels[ctx.guild.id]:
        channel = ctx.guild.get_channel(channel_id)
        if channel:
            await channel.set_permissions(ctx.guild.default_role, send_messages=None)
            unlocked += 1
    lockdown.locked_channels[ctx.guild.id] = []
    embed = discord.Embed(title='🔓 Server Unlocked', description=f'**{unlocked} channels** unlocked', color=COLORS['success'])
    await ctx.reply(embed=embed)

# ==================== CHANNELPERM COMMANDS ====================

PERM_MAP = {
    'send messages': 'send_messages', 'read messages': 'read_messages',
    'view channel': 'view_channel', 'embed links': 'embed_links',
    'attach files': 'attach_files', 'add reactions': 'add_reactions',
    'use external emojis': 'use_external_emojis', 'use external stickers': 'use_external_stickers',
    'mention everyone': 'mention_everyone', 'manage messages': 'manage_messages',
    'read message history': 'read_message_history', 'send tts messages': 'send_tts_messages',
    'use application commands': 'use_application_commands', 'manage threads': 'manage_threads',
    'create public threads': 'create_public_threads', 'create private threads': 'create_private_threads',
    'send messages in threads': 'send_messages_in_threads', 'connect': 'connect',
    'speak': 'speak', 'stream': 'stream', 'use voice activation': 'use_voice_activation',
    'priority speaker': 'priority_speaker', 'mute members': 'mute_members',
    'deafen members': 'deafen_members', 'move members': 'move_members'
}

@bot.command(name='channelperm')
@is_owner()
async def channelperm_cmd(ctx, channel: discord.TextChannel = None, target_input: str = None, action: str = None, *, permission_name: str = None):
    if not channel or not target_input or not action or not permission_name:
        return await ctx.reply('❌ Missing arguments\n\nExample: `$channelperm #general @Members disable embed links`')
    target = None
    if target_input.lower() in ['everyone', '@everyone']:
        target = ctx.guild.default_role
    else:
        try: target = await commands.RoleConverter().convert(ctx, target_input)
        except:
            try: target = await commands.MemberConverter().convert(ctx, target_input)
            except: return await ctx.reply('❌ Invalid role/member\n\nUse: `@Role`, `@Member`, or `everyone`')
    if action.lower() not in ['enable', 'disable']:
        return await ctx.reply('❌ Action must be `enable` or `disable`')
    perm_key = permission_name.lower().strip()
    if perm_key not in PERM_MAP:
        return await ctx.reply(f'❌ Unknown permission: `{permission_name}`\n\nCommon: `send messages`, `embed links`, `attach files`, `view channel`')
    perm_value = action.lower() == 'enable'
    try:
        overwrites = channel.overwrites_for(target)
        setattr(overwrites, PERM_MAP[perm_key], perm_value)
        await channel.set_permissions(target, overwrite=overwrites)
        target_name = 'everyone' if target == ctx.guild.default_role else target.name
        embed = discord.Embed(title=f'{"✅ Enabled" if perm_value else "🚫 Disabled"} Permission', color=COLORS['success'] if perm_value else COLORS['error'])
        embed.add_field(name='Channel', value=channel.mention, inline=True)
        embed.add_field(name='Target', value=f'`{target_name}`', inline=True)
        embed.add_field(name='Permission', value=f'`{permission_name}`', inline=True)
        await ctx.reply(embed=embed)
    except Exception as e:
        await ctx.reply(f'❌ Error: {str(e)}')

@bot.command(name='channelpermall')
@is_owner()
async def channelpermall_cmd(ctx, target_input: str = None, action: str = None, *, permission_name: str = None):
    if not target_input or not action or not permission_name:
        return await ctx.reply('❌ Missing arguments\n\nExample: `$channelpermall everyone disable send messages`')
    target = None
    if target_input.lower() in ['everyone', '@everyone']:
        target = ctx.guild.default_role
    else:
        try: target = await commands.RoleConverter().convert(ctx, target_input)
        except:
            try: target = await commands.MemberConverter().convert(ctx, target_input)
            except: return await ctx.reply('❌ Invalid role/member')
    if action.lower() not in ['enable', 'disable']:
        return await ctx.reply('❌ Action must be `enable` or `disable`')
    perm_key = permission_name.lower().strip()
    if perm_key not in PERM_MAP:
        return await ctx.reply(f'❌ Unknown permission: `{permission_name}`')
    perm_value = action.lower() == 'enable'
    updated = 0
    failed = 0
    msg = await ctx.reply(f'⏳ Updating all channels...')
    for channel in ctx.guild.channels:
        try:
            overwrites = channel.overwrites_for(target)
            setattr(overwrites, PERM_MAP[perm_key], perm_value)
            await channel.set_permissions(target, overwrite=overwrites)
            updated += 1
        except: failed += 1
    target_name = 'everyone' if target == ctx.guild.default_role else target.name
    embed = discord.Embed(title=f'{"✅ Enabled" if perm_value else "🚫 Disabled"} Permission', color=COLORS['success'] if perm_value else COLORS['error'])
    embed.add_field(name='Target', value=f'`{target_name}`', inline=True)
    embed.add_field(name='Permission', value=f'`{permission_name}`', inline=True)
    embed.add_field(name='Updated', value=f'{updated}/{updated+failed} channels', inline=True)
    await msg.edit(content=None, embed=embed)

# ==================== PERM SETUP COMMANDS ====================

@bot.group(name='adminperms', aliases=['ap'], invoke_without_command=True)
@is_owner()
async def adminperms_group(ctx):
    await ctx.reply('Usage: `$adminperms set @role` or `$adminperms show`')

@adminperms_group.command(name='set')
@is_owner()
async def adminperms_set(ctx, role: discord.Role = None):
    if not role: return await ctx.reply('❌ Missing role\n\nExample: `$adminperms set @Admin`')
    admin_perms[ctx.guild.id] = role.id
    embed = discord.Embed(title='✅ Admin Role Set', color=COLORS['success'])
    embed.description = f'{role.mention} and any role above it can now use:\n`ban` `unban` `hackban` `unhackban` `kick` `role` `slowmode` `lock` `unlock` `hide` `unhide` `warn` `mute`'
    await ctx.reply(embed=embed)

@adminperms_group.command(name='show')
@is_owner()
async def adminperms_show(ctx):
    role_id = admin_perms.get(ctx.guild.id)
    if not role_id: return await ctx.reply('❌ No admin role set\n\nUse `$adminperms set @role`')
    role = ctx.guild.get_role(role_id)
    embed = discord.Embed(title='⚙️ Admin Role', description=f'Currently set to: {role.mention if role else "❌ Role not found"}', color=COLORS['support'])
    await ctx.reply(embed=embed)

@bot.group(name='modperms', aliases=['mp'], invoke_without_command=True)
@is_owner()
async def modperms_group(ctx):
    await ctx.reply('Usage: `$modperms set @role` or `$modperms show`')

@modperms_group.command(name='set')
@is_owner()
async def modperms_set(ctx, role: discord.Role = None):
    if not role: return await ctx.reply('❌ Missing role\n\nExample: `$modperms set @Mod`')
    mod_perms[ctx.guild.id] = role.id
    embed = discord.Embed(title='✅ Mod Role Set', color=COLORS['success'])
    embed.description = f'{role.mention} and any role above it can now use:\n`mute` `unmute` `warn` `warnings` `clearwarnings` `nick`'
    await ctx.reply(embed=embed)

@modperms_group.command(name='show')
@is_owner()
async def modperms_show(ctx):
    role_id = mod_perms.get(ctx.guild.id)
    if not role_id: return await ctx.reply('❌ No mod role set\n\nUse `$modperms set @role`')
    role = ctx.guild.get_role(role_id)
    embed = discord.Embed(title='⚙️ Mod Role', description=f'Currently set to: {role.mention if role else "❌ Role not found"}', color=COLORS['support'])
    await ctx.reply(embed=embed)
# ==================== UTILITY COMMANDS ====================

@bot.command(name='afk')
async def afk_cmd(ctx, *, reason: str = 'AFK'):
    afk_users[ctx.author.id] = {'reason': reason, 'time': datetime.now(timezone.utc), 'original_nick': ctx.author.display_name}
    try:
        new_nick = f"[AFK] {ctx.author.display_name}"
        if len(new_nick) > 32:
            new_nick = f"[AFK] {ctx.author.name}"[:32]
        await ctx.author.edit(nick=new_nick, reason='AFK')
        await ctx.reply(f'✅ AFK set: {reason}', delete_after=5)
    except:
        await ctx.reply(f'✅ AFK set: {reason}', delete_after=5)

@bot.command(name='afkoff')
async def afkoff_cmd(ctx):
    if ctx.author.id in afk_users:
        original = afk_users[ctx.author.id].get('original_nick')
        del afk_users[ctx.author.id]
        try:
            if ctx.author.display_name.startswith('[AFK]'):
                await ctx.author.edit(nick=original if original else None, reason='AFK removed')
            await ctx.reply('✅ Welcome back!', delete_after=5)
        except:
            await ctx.reply('✅ Welcome back!', delete_after=5)
    else:
        await ctx.reply('❌ not afk', delete_after=5)

@bot.command(name='snipe', aliases=['sn'])
async def snipe_cmd(ctx):
    data = snipe_data.get(ctx.channel.id)
    if not data:
        return await ctx.reply(embed=discord.Embed(title='🔍 Nothing to Snipe', description='No recently deleted messages', color=COLORS['support']))
    embed = discord.Embed(title='🔍 Sniped Message', description=data['content'], color=COLORS['support'])
    embed.set_author(name=data['author'], icon_url=data['avatar'])
    await ctx.reply(embed=embed)

@bot.command(name='editsnipe', aliases=['es'])
async def editsnipe_cmd(ctx):
    data = edit_snipe_data.get(ctx.channel.id)
    if not data:
        return await ctx.reply(embed=discord.Embed(title='🔍 Nothing to Snipe', description='No recently edited messages', color=COLORS['support']))
    embed = discord.Embed(title='✏️ Edit Sniped', color=COLORS['support'])
    embed.add_field(name='Before', value=data['before'], inline=False)
    embed.add_field(name='After', value=data['after'], inline=False)
    embed.set_author(name=data['author'], icon_url=data['avatar'])
    await ctx.reply(embed=embed)

@bot.command(name='avatar', aliases=['av'])
async def avatar_cmd(ctx, member: discord.Member = None):
    member = member or ctx.author
    embed = discord.Embed(title=f'🖼️ {member.display_name}\'s Avatar', color=COLORS['support'])
    embed.set_image(url=member.display_avatar.url)
    embed.set_footer(text=f'Requested by {ctx.author.display_name}')
    await ctx.reply(embed=embed)

@bot.command(name='banner', aliases=['bn'])
async def banner_cmd(ctx, member: discord.Member = None):
    member = member or ctx.author
    user = await bot.fetch_user(member.id)
    if not user.banner:
        return await ctx.reply(embed=discord.Embed(title='❌ No Banner', description=f'{member.mention} has no banner', color=COLORS['error']))
    embed = discord.Embed(title=f'🖼️ {member.display_name}\'s Banner', color=COLORS['support'])
    embed.set_image(url=user.banner.url)
    await ctx.reply(embed=embed)

@bot.command(name='userinfo', aliases=['ui'])
async def userinfo_cmd(ctx, member: discord.Member = None):
    member = member or ctx.author
    now = datetime.now(timezone.utc)
    created = member.created_at.strftime('%b %d, %Y')
    joined = member.joined_at.strftime('%b %d, %Y') if member.joined_at else 'Unknown'
    acc_age = (now - member.created_at.replace(tzinfo=None)).days
    join_age = (now - member.joined_at.replace(tzinfo=None)).days if member.joined_at else 0
    roles = [r.mention for r in member.roles if r.id != ctx.guild.id]
    roles_str = ' '.join(roles[-10:]) if roles else 'None'
    embed = discord.Embed(title=f'👤 {member.display_name}', color=member.color if member.color.value else COLORS['support'])
    embed.set_thumbnail(url=member.display_avatar.url)
    embed.add_field(name='Username', value=str(member), inline=True)
    embed.add_field(name='ID', value=str(member.id), inline=True)
    embed.add_field(name='Bot', value='Yes' if member.bot else 'No', inline=True)
    embed.add_field(name='Account Created', value=f'{created}\n{acc_age} days ago', inline=True)
    embed.add_field(name='Joined Server', value=f'{joined}\n{join_age} days ago', inline=True)
    embed.add_field(name='Highest Role', value=member.top_role.mention, inline=True)
    embed.add_field(name=f'Roles ({len(roles)})', value=roles_str, inline=False)
    await ctx.reply(embed=embed)

@bot.command(name='serverinfo', aliases=['si'])
async def serverinfo_cmd(ctx):
    guild = ctx.guild
    created = guild.created_at.strftime('%b %d, %Y')
    bots = sum(1 for m in guild.members if m.bot)
    humans = guild.member_count - bots
    embed = discord.Embed(title=f'{guild.name}', color=COLORS['support'])
    if guild.icon: embed.set_thumbnail(url=guild.icon.url)
    embed.add_field(name='Owner', value=guild.owner.mention if guild.owner else 'Unknown', inline=True)
    embed.add_field(name='Server ID', value=str(guild.id), inline=True)
    embed.add_field(name='Created', value=created, inline=True)
    embed.add_field(name='Members', value=f'{guild.member_count} total\n{humans} humans • {bots} bots', inline=True)
    embed.add_field(name='Channels', value=f'{len(guild.text_channels)} text\n{len(guild.voice_channels)} voice', inline=True)
    embed.add_field(name='Roles', value=str(len(guild.roles)), inline=True)
    embed.add_field(name='Boost Level', value=f'Level {guild.premium_tier} ({guild.premium_subscription_count} boosts)', inline=True)
    embed.add_field(name='Emojis', value=f'{len(guild.emojis)}/{guild.emoji_limit}', inline=True)
    await ctx.reply(embed=embed)

@bot.command(name='roleinfo', aliases=['ri'])
async def roleinfo_cmd(ctx, role: discord.Role = None):
    if not role: return await ctx.reply('❌ Missing role\n\nExample: `$roleinfo @Mod`')
    members_with_role = len([m for m in ctx.guild.members if role in m.roles])
    created = role.created_at.strftime('%b %d, %Y')
    embed = discord.Embed(title=f'{role.name}', color=role.color if role.color.value else COLORS['support'])
    embed.add_field(name='ID', value=str(role.id), inline=True)
    embed.add_field(name='Color', value=str(role.color), inline=True)
    embed.add_field(name='Created', value=created, inline=True)
    embed.add_field(name='Members', value=str(members_with_role), inline=True)
    embed.add_field(name='Position', value=str(role.position), inline=True)
    embed.add_field(name='Managed', value='Yes' if role.managed else 'No', inline=True)
    embed.add_field(name='Mentionable', value='Yes' if role.mentionable else 'No', inline=True)
    embed.add_field(name='Hoisted', value='Yes' if role.hoist else 'No', inline=True)
    await ctx.reply(embed=embed)

@bot.command(name='membercount', aliases=['mc'])
async def membercount_cmd(ctx):
    bots = sum(1 for m in ctx.guild.members if m.bot)
    humans = ctx.guild.member_count - bots
    embed = discord.Embed(title=f'{ctx.guild.name} Members', color=COLORS['support'])
    embed.add_field(name='Total', value=str(ctx.guild.member_count), inline=True)
    embed.add_field(name='Humans', value=str(humans), inline=True)
    embed.add_field(name='Bots', value=str(bots), inline=True)
    await ctx.reply(embed=embed)

@bot.command(name='botinfo', aliases=['bi'])
async def botinfo_cmd(ctx):
    now = datetime.now(timezone.utc)
    uptime = now - bot_start_time
    hours, remainder = divmod(int(uptime.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    latency = round(bot.latency * 1000)
    embed = discord.Embed(title='Bot Info', color=COLORS['support'])
    embed.set_thumbnail(url=bot.user.display_avatar.url)
    embed.add_field(name='Ping', value=f'{latency}ms', inline=True)
    embed.add_field(name='Uptime', value=f'{hours}h {minutes}m {seconds}s', inline=True)
    embed.add_field(name='Servers', value=str(len(bot.guilds)), inline=True)
    embed.add_field(name='Users', value=str(len(set(bot.get_all_members()))), inline=True)
    await ctx.reply(embed=embed)

@bot.command(name='ping')
async def ping_cmd(ctx):
    latency = round(bot.latency * 1000)
    color = COLORS['success'] if latency < 200 else COLORS['support']
    embed = discord.Embed(title='Pong', description=f'**{latency}ms**', color=color)
    await ctx.reply(embed=embed)

# ==================== CLEAN HELP COMMAND ====================

class HelpView(View):
    def __init__(self, pages, author_id):
        super().__init__(timeout=180)
        self.pages = pages
        self.current_page = 0
        self.author_id = author_id
        self.update_buttons()

    def update_buttons(self):
        self.children[0].disabled = self.current_page == 0
        self.children[1].disabled = self.current_page == len(self.pages) - 1

    @discord.ui.button(label='Previous', style=discord.ButtonStyle.secondary, custom_id='help_prev')
    async def previous_button(self, interaction, button):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message('❌ This is not your help menu', ephemeral=True)
        self.current_page -= 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.pages[self.current_page], view=self)

    @discord.ui.button(label='Next', style=discord.ButtonStyle.secondary, custom_id='help_next')
    async def next_button(self, interaction, button):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message('❌ This is not your help menu', ephemeral=True)
        self.current_page += 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.pages[self.current_page], view=self)

@bot.command(name='help')
async def help_cmd(ctx):
    """Show ALL commands to everyone"""
    pages = []
    
    # PAGE 1 - Tickets (Everyone)
    e1 = discord.Embed(title='🎫 ticket commands', color=0x5865F2)
    e1.add_field(name='manage', value='`$close` `$claim` `$unclaim`', inline=False)
    e1.add_field(name='users', value='`$add @user` `$remove @user`', inline=False)
    e1.add_field(name='edit', value='`$rename <name>` `$transfer @user`', inline=False)
    e1.add_field(name='trade', value='`$proof` - post completion proof', inline=False)
    e1.set_footer(text='page 1/10')
    pages.append(e1)
    
    # PAGE 2 - Utility (Everyone)
    e2 = discord.Embed(title='🛠️ utility commands', color=0x5865F2)
    e2.add_field(name='💬 social', value='`$afk <reason>` `$afkoff`', inline=False)
    e2.add_field(name='📊 info', value='`$userinfo` / `$ui`\n`$serverinfo` / `$si`\n`$roleinfo @role` / `$ri`\n`$membercount` / `$mc`\n`$botinfo` / `$bi`\n`$ping`', inline=False)
    e2.add_field(name='🖼️ media', value='`$avatar [@user]` / `$av`\n`$banner [@user]` / `$bn`', inline=False)
    e2.add_field(name='👀 snipe', value='`$snipe` / `$sn` - deleted\n`$editsnipe` / `$es` - edited', inline=False)
    e2.set_footer(text='page 2/10')
    pages.append(e2)
    
    # PAGE 3 - Admin Setup
    e3 = discord.Embed(title='⚙️ setup commands', description='**requires:** administrator', color=0x5865F2)
    e3.add_field(name='🎫 tickets', value='`$setup` - create panel\n`$setcategory #cat`\n`$setlogs #channel`\n`$config` - view settings', inline=False)
    e3.add_field(name='🚔 jail', value='`$jail @user <reason>`\n`$unjail @user`\n`$jailed` - list jailed', inline=False)
    e3.add_field(name='🚫 blacklist', value='`$blacklist @user <reason>`\n`$unblacklist @user`\n`$blacklists` - view list', inline=False)
    e3.add_field(name='🧹 cleanup', value='`$clear` - delete bot msgs\n`$purge <amount> [@user]`', inline=False)
    e3.set_footer(text='page 3/10')
    pages.append(e3)
    
    # PAGE 4 - Moderation
    e4 = discord.Embed(title='🛡️ moderation commands', description='**requires:** mod role', color=0x5865F2)
    e4.add_field(name='🔇 mute', value='`$mute` / `$m @user <time> <reason>`\ntime: `10s` `5m` `1h` `1d`\n`$unmute` / `$um @user`', inline=False)
    e4.add_field(name='⚠️ warn', value='`$warn` / `$w @user <reason>`\n`$warnings` / `$ws @user`\n`$clearwarnings` / `$cw @user`', inline=False)
    e4.add_field(name='✏️ nickname', value='`$nick` / `$n @user <name>`', inline=False)
    e4.set_footer(text='page 4/10')
    pages.append(e4)
    
    # PAGE 5 - Admin Actions
    e5 = discord.Embed(title='🔨 admin commands', description='**requires:** admin role', color=0x5865F2)
    e5.add_field(name='🚪 ban & kick', value='`$ban` / `$b @user <reason>`\n`$unban` / `$ub <ID>`\n`$hackban` / `$hb <ID> <reason>`\n`$unhackban` / `$uhb <ID>`\n`$kick` / `$k @user <reason>`', inline=False)
    e5.add_field(name='🎭 role', value='`$role` / `$r @user <rolename>`', inline=False)
    e5.add_field(name='🔒 channel', value='`$slowmode` / `$sm <seconds>`\n`$lock` / `$lk` - lock channel\n`$unlock` / `$ulk`\n`$hide` / `$hd` - hide channel\n`$unhide` / `$uhd`', inline=False)
    e5.set_footer(text='page 5/10')
    pages.append(e5)
    
    # PAGE 6 - Permission Setup
    e6 = discord.Embed(title='👑 permission setup', description='**requires:** administrator', color=0x5865F2)
    e6.add_field(name='admin role', value='`$adminperms set @role` / `$ap set`\n`$adminperms show` / `$ap show`\n\ngives access to ban, kick, role, slowmode, lock, hide, purge', inline=False)
    e6.add_field(name='mod role', value='`$modperms set @role` / `$mp set`\n`$modperms show` / `$mp show`\n\ngives access to mute, warn, nick', inline=False)
    e6.set_footer(text='page 6/10')
    pages.append(e6)
    
    # PAGE 7 - Anti-Systems
    e7 = discord.Embed(title='🛡️ protection systems', description='**requires:** owner', color=0x5865F2)
    e7.add_field(name='anti-nuke', value='`$anti-nuke enable/disable/status`\n`$anti-nuke whitelist @role`\n`$anti-nuke unwhitelist @role`\n\nprotects: mass bans, kicks, deletes, bots, webhooks', inline=False)
    e7.add_field(name='anti-raid', value='`$anti-raid enable/disable/status`\n`$anti-raid action <kick/ban>`\n`$anti-raid accountage <days>`\n`$anti-raid avatar <on/off>`\n\nprotects: join spam, new accounts, no avatars', inline=False)
    e7.set_footer(text='page 7/10')
    pages.append(e7)
    
    # PAGE 8 - More Anti-Systems
    e8 = discord.Embed(title='🛡️ more protection', description='**requires:** owner', color=0x5865F2)
    e8.add_field(name='anti-spam', value='`$anti-spam enable/disable/status`\n`$anti-spam whitelist @role`\n\ndetects: 7+ msgs in 10s, escalating timeouts', inline=False)
    e8.add_field(name='anti-link', value='`$anti-link enable/disable/status`\n`$anti-link role @role` - allow links\n`$anti-link unrole @role`\n\nblocks all links unless whitelisted', inline=False)
    e8.add_field(name='anti-react', value='`$anti-react enable/disable/status`\n`$anti-react action <warn/timeout/kick>`\n\ndetects: 10+ reacts in 10s across 5+ msgs', inline=False)
    e8.set_footer(text='page 8/10')
    pages.append(e8)
    
    # PAGE 9 - AutoMod & Giveaways
    e9 = discord.Embed(title='🤖 automod & giveaways', color=0x5865F2)
    e9.add_field(name='🤖 automod', value='**requires:** owner\n`$automod enable/disable/status`\n`$automod add <word>` - add bad word\n`$automod remove <word>`\n`$automod list` - view filter\n\nfilters bad words, staff bypass', inline=False)
    e9.add_field(name='🎉 giveaways', value='**requires:** manage server\n`$giveaway start <time> <winners> <prize>`\n`$g start 1h 3 nitro` - example\n`$giveaway end <msg_id>`\n`$giveaway reroll <msg_id>`\n`$giveaway list`', inline=False)
    e9.set_footer(text='page 9/10')
    pages.append(e9)
    
    # PAGE 10 - Owner Advanced
    e10 = discord.Embed(title='⚡ shortcuts & advanced', description='**requires:** owner for advanced', color=0x5865F2)
    e10.add_field(name='🔐 advanced', value='`$lockdown` - lock all channels\n`$unlockdown`\n`$channelperm #ch @target enable/disable <perm>`\n`$channelpermall @target enable/disable <perm>`', inline=False)
    e10.add_field(name='⚡ shortcuts', value='`$b` ban | `$k` kick | `$m` mute | `$w` warn\n`$av` avatar | `$ui` userinfo | `$si` serverinfo\n`$sn` snipe | `$es` editsnipe\n`$ap` adminperms | `$mp` modperms\n\nand many more...', inline=False)
    e10.set_footer(text='page 10/10')
    pages.append(e10)
    
    view = HelpView(pages, ctx.author.id)
    await ctx.reply(embed=pages[0], view=view)
    await ctx.reply(embed=pages[0], view=view)

# ==================== EVENTS ====================

@bot.event
async def on_ready():
    global bot_start_time
    bot_start_time = datetime.now(timezone.utc)
    logger.info(f'Logged in as {bot.user}')
    try:
        await db.connect()
        logger.info('Database connected')
    except Exception as e:
        logger.error(f'Database error: {e}')
        return
    bot.add_view(TicketPanelView())
    bot.add_view(TicketControlView())
    logger.info('Bot ready!')

@bot.event
async def on_message_delete(message):
    if message.author.bot: return
    if message.content:
        snipe_data[message.channel.id] = {
            'content': message.content[:1000],
            'author': str(message.author),
            'avatar': message.author.display_avatar.url
        }

@bot.event
async def on_message_edit(before, after):
    if before.author.bot: return
    if before.content != after.content:
        edit_snipe_data[before.channel.id] = {
            'before': before.content[:500],
            'after': after.content[:500],
            'author': str(before.author),
            'avatar': before.author.display_avatar.url
        }

@bot.event
async def on_member_update(before, after):
    if not anti_nuke.enabled.get(after.guild.id): return
    if len(after.roles) > len(before.roles):
        new_role = set(after.roles) - set(before.roles)
        for role in new_role:
            if role.managed:
                async for entry in after.guild.audit_logs(limit=5, action=discord.AuditLogAction.bot_add):
                    if entry.target and entry.target.bot:
                        bot_member = entry.target
                        inviter = entry.user
                        if inviter.id == OWNER_ID: return
                        if not anti_nuke.can_add_bot(after.guild.id, inviter.id):
                            try:
                                await bot_member.kick(reason='Unauthorized bot add (Anti-Nuke)')
                                await inviter.ban(reason='Added bot without permission (Anti-Nuke)')
                            except: pass
                        break

@bot.event
async def on_integration_create(integration):
    if not anti_nuke.enabled.get(integration.guild.id): return
    try:
        async for entry in integration.guild.audit_logs(limit=1, action=discord.AuditLogAction.integration_create):
            creator = entry.user
            if creator.id == OWNER_ID: return
            if not anti_nuke.can_add_integration(integration.guild.id, creator.id):
                try:
                    await integration.delete()
                    await creator.ban(reason='Unauthorized integration (Anti-Nuke)')
                except Exception as e:
                    logger.error(f'Integration error: {e}')
            break
    except Exception as e:
        logger.error(f'Integration create event error: {e}')

@bot.event
async def on_guild_channel_delete(channel):
    async for entry in channel.guild.audit_logs(limit=1, action=discord.AuditLogAction.channel_delete):
        deleter = entry.user
        if deleter.id == OWNER_ID: return
        anti_nuke.add_channel_delete(channel.guild.id, deleter.id)
        if anti_nuke.is_nuke(channel.guild.id, deleter.id):
            try: await deleter.ban(reason='Mass channel deletion (Anti-Nuke)')
            except: pass
        break

@bot.event
async def on_message(message):
    if message.author.bot: return

    # AutoMod bad word filter
    if message.guild:
        has_badword, word = automod.check_message(message)
        if has_badword:
            try:
                await message.delete()
                await message.channel.send(f'{message.author.mention} watch your language', delete_after=5)
                logger.info(f'AUTOMOD: deleted message - bad word')
                try:
                    await send_log(message.guild, '🚨 AUTOMOD', f'deleted message from {message.author.mention}',
                                 COLORS['error'], {'Channel': message.channel.mention})
                except: pass
            except: pass
            return
    
    if not message.guild: return

    # AFK check
    if message.author.id in afk_users:
        del afk_users[message.author.id]
        try:
            msg = await message.channel.send(embed=discord.Embed(description=f'Welcome back {message.author.mention}, your AFK has been removed', color=COLORS['success']))
            await asyncio.sleep(5)
            await msg.delete()
        except: pass

    # AFK ping check
    if message.mentions:
        for mentioned in message.mentions:
            if mentioned.id in afk_users:
                afk_info = afk_users[mentioned.id]
                try:
                    await message.channel.send(embed=discord.Embed(description=f'{mentioned.mention} is AFK: {afk_info["reason"]}', color=COLORS['support']))
                except: pass

    # Ticket filter
    if message.channel.name.startswith('ticket-'):
        try:
            async with db.pool.acquire() as conn:
                ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', message.channel.id)
                if ticket:
                    # Owner can always talk
                    if message.author.id == OWNER_ID:
                        return
                    # If ticket is NOT claimed, allow ticket creator to talk
                    if not ticket['claimed_by']:
                        if message.author.id != ticket['user_id']:
                            # Check if they have staff role or tier role
                            has_permission = False
                            staff_role = message.guild.get_role(HARDCODED_ROLES['staff'])
                            if staff_role and staff_role in message.author.roles:
                                has_permission = True
                            # Check tier roles for middleman tickets
                            if ticket.get('tier') and ticket['tier'] in ['lowtier', 'midtier', 'hightier']:
                                tier_role = message.guild.get_role(HARDCODED_ROLES.get(ticket['tier']))
                                if tier_role and tier_role in message.author.roles:
                                    has_permission = True
                            if not has_permission:
                                try:
                                    await message.delete()
                                    await message.channel.send(f'{message.author.mention} You cannot send messages in this ticket.', delete_after=3)
                                except: pass
                                return
                    else:
                        # Ticket IS claimed - ticket creator, claimer, and added users can talk
                        allowed_users = [ticket['user_id'], ticket['claimed_by']]
                        # Check if user has channel permissions (was added via $add command)
                        overwrites = message.channel.overwrites_for(message.author)
                        if overwrites.send_messages == True:
                            # They were explicitly added
                            return
                        if message.author.id not in allowed_users:
                            try:
                                await message.delete()
                                await message.channel.send(f'{message.author.mention} Only the ticket creator, claimer, and added users can talk in claimed tickets.', delete_after=3)
                            except: pass
                            return
        except Exception as e:
            logger.error(f'Ticket filter error: {e}')

    # Anti-spam
    if not anti_spam.is_whitelisted(message.author):
        anti_spam.add_message(message.guild.id, message.author.id)
        if anti_spam.is_spam(message.guild.id, message.author.id):
            try:
                await message.delete()
                await message.channel.send(f'{message.author.mention} Stop spamming!', delete_after=3)
            except: pass
            return

    # Anti-link
    if anti_link.enabled.get(message.guild.id):
        if anti_link.is_link(message.content):
            if not anti_link.is_url_whitelisted(message.guild.id, message.content):
                if not anti_link.is_user_whitelisted(message.guild.id, message.author):
                    try:
                        await message.delete()
                        await message.channel.send(f'{message.author.mention} Links are not allowed!', delete_after=3)
                    except: pass
                    return

    
    # Anti-spam detection
    if anti_spam.enabled.get(message.guild.id) and not anti_spam.is_whitelisted(message.author):
        anti_spam.add_message(message.guild.id, message.author.id)
        
        if anti_spam.is_spam(message.guild.id, message.author.id):
            try:
                await message.delete()
                timeout_dur = anti_spam.get_timeout_duration(message.guild.id, message.author.id)
                await message.author.timeout(timeout_dur, reason='anti-spam')
                mins = int(timeout_dur.total_seconds() / 60)
                await message.channel.send(f'{message.author.mention} timed out for {mins} min for spamming', delete_after=5)
                logger.info(f'ANTI-SPAM: timed out {message.author}')
                try:
                    await send_log(message.guild, '🚨 ANTI-SPAM', f'timed out {message.author.mention}', COLORS['error'],
                                 {'Messages': f"{len(anti_spam.messages[(message.guild.id, message.author.id)])}", 'Duration': f'{mins} min'})
                except: pass
            except Exception as e:
                logger.error(f'anti-spam: {e}')
            return
        
        is_spam_msg, reasons = anti_spam.check_message(message)
        if is_spam_msg:
            try:
                await message.delete()
                await message.channel.send(f'{message.author.mention} message deleted: {", ".join(reasons)}', delete_after=3)
            except: pass
            return
    
    # Anti-link detection
    if anti_link.enabled.get(message.guild.id):
        if anti_link.is_link(message.content):
            if not anti_link.is_url_whitelisted(message.guild.id, message.content):
                if not anti_link.is_whitelisted(message.author):
                    try:
                        await message.delete()
                        await message.channel.send(f'{message.author.mention} no links allowed', delete_after=3)
                    except: pass
                    return
    
    await bot.process_commands(message)

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.reply('❌ You don\'t have permission')
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.reply(f'❌ Missing: `{error.param.name}`')
    elif isinstance(error, commands.CheckFailure):
        await ctx.reply('❌ You don\'t have the required role')
    elif isinstance(error, commands.MemberNotFound):
        await ctx.reply('❌ Member not found')
    elif isinstance(error, commands.RoleNotFound):
        await ctx.reply('❌ Role not found')
    else:
        logger.error(f'Unhandled error: {error}')

# ==================== MAIN ====================

async def main():
    await start_web_server()
    token = os.getenv('DISCORD_TOKEN')
    if not token:
        logger.error('DISCORD_TOKEN not found')
        return
    try:
        await bot.start(token)
    except KeyboardInterrupt:
        logger.info('Shutting down')
    finally:
        await db.close()
        await bot.close()

if __name__ == '__main__':
    asyncio.run(main())
# COMPLETE ANTI-SYSTEM EVENTS AND COMMANDS
# Add this to the end of the bot file

# ==================== ANTI-NUKE EVENTS ====================

@bot.event
async def on_guild_channel_delete(channel):
    if not anti_nuke.enabled.get(channel.guild.id): return
    try:
        async for entry in channel.guild.audit_logs(limit=1, action=discord.AuditLogAction.channel_delete):
            if not anti_nuke.is_recent(entry.created_at): return
            if not anti_nuke.is_after_restart(entry.created_at): return
            if entry.target.id != channel.id: continue
            
            deleter = entry.user
            if deleter.id == OWNER_ID: return
            if deleter.id == bot.user.id: return
            
            deleter_member = channel.guild.get_member(deleter.id)
            if anti_nuke.is_whitelisted(deleter_member): return
            
            anti_nuke.add_action(channel.guild.id, deleter.id, 'channel_deletes')
            
            if anti_nuke.check_nuke(channel.guild.id, deleter.id):
                try:
                    if deleter_member:
                        for role in deleter_member.roles:
                            if role.id != channel.guild.id:
                                try: await deleter_member.remove_roles(role)
                                except: pass
                        await deleter_member.ban(reason='anti-nuke: mass channel deletion')
                    logger.info(f'ANTI-NUKE: banned {deleter}')
                    try:
                        await send_log(channel.guild, '🚨 ANTI-NUKE', f'banned {deleter.mention} for mass channel deletion', 
                                     COLORS['error'], {'User': str(deleter), 'Action': 'Mass Channel Deletion'})
                    except: pass
                except Exception as e:
                    logger.error(f'anti-nuke: {e}')
            break
    except Exception as e:
        logger.error(f'channel delete event: {e}')

@bot.event
async def on_member_ban(guild, user):
    if not anti_nuke.enabled.get(guild.id): return
    try:
        async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.ban):
            if not anti_nuke.is_recent(entry.created_at): return
            if not anti_nuke.is_after_restart(entry.created_at): return
            if entry.target.id != user.id: continue
            
            banner = entry.user
            if banner.id == OWNER_ID: return
            if banner.id == bot.user.id: return
            
            banner_member = guild.get_member(banner.id)
            if anti_nuke.is_whitelisted(banner_member): return
            
            anti_nuke.add_action(guild.id, banner.id, 'bans')
            
            if anti_nuke.check_nuke(guild.id, banner.id):
                try:
                    if banner_member:
                        for role in banner_member.roles:
                            if role.id != guild.id:
                                try: await banner_member.remove_roles(role)
                                except: pass
                        await banner_member.ban(reason='anti-nuke: mass banning')
                    logger.info(f'ANTI-NUKE: banned {banner}')
                    try:
                        await send_log(guild, '🚨 ANTI-NUKE', f'banned {banner.mention} for mass banning users',
                                     COLORS['error'], {'User': str(banner), 'Action': 'Mass Banning'})
                    except: pass
                except Exception as e:
                    logger.error(f'anti-nuke: {e}')
            break
    except Exception as e:
        logger.error(f'ban event: {e}')

@bot.event
async def on_member_remove(member):
    if not anti_nuke.enabled.get(member.guild.id): return
    try:
        await asyncio.sleep(0.5)
        async for entry in member.guild.audit_logs(limit=1, action=discord.AuditLogAction.kick):
            if not anti_nuke.is_recent(entry.created_at): return
            if not anti_nuke.is_after_restart(entry.created_at): return
            if entry.target.id != member.id: continue
            
            kicker = entry.user
            if kicker.id == OWNER_ID: return
            if kicker.id == bot.user.id: return
            
            kicker_member = member.guild.get_member(kicker.id)
            if anti_nuke.is_whitelisted(kicker_member): return
            
            anti_nuke.add_action(member.guild.id, kicker.id, 'kicks')
            
            if anti_nuke.check_nuke(member.guild.id, kicker.id):
                try:
                    if kicker_member:
                        for role in kicker_member.roles:
                            if role.id != member.guild.id:
                                try: await kicker_member.remove_roles(role)
                                except: pass
                        await kicker_member.ban(reason='anti-nuke: mass kicking')
                    logger.info(f'ANTI-NUKE: banned {kicker}')
                    try:
                        await send_log(member.guild, '🚨 ANTI-NUKE', f'banned {kicker.mention} for mass kicking users',
                                     COLORS['error'], {'User': str(kicker), 'Action': 'Mass Kicking'})
                    except: pass
                except Exception as e:
                    logger.error(f'anti-nuke: {e}')
            break
    except Exception as e:
        logger.error(f'kick event: {e}')

@bot.event
async def on_guild_role_delete(role):
    if not anti_nuke.enabled.get(role.guild.id): return
    try:
        async for entry in role.guild.audit_logs(limit=1, action=discord.AuditLogAction.role_delete):
            if not anti_nuke.is_recent(entry.created_at): return
            if not anti_nuke.is_after_restart(entry.created_at): return
            if entry.target.id != role.id: continue
            
            deleter = entry.user
            if deleter.id == OWNER_ID: return
            if deleter.id == bot.user.id: return
            
            deleter_member = role.guild.get_member(deleter.id)
            if anti_nuke.is_whitelisted(deleter_member): return
            
            anti_nuke.add_action(role.guild.id, deleter.id, 'role_deletes')
            
            if anti_nuke.check_nuke(role.guild.id, deleter.id):
                try:
                    if deleter_member:
                        for r in deleter_member.roles:
                            if r.id != role.guild.id:
                                try: await deleter_member.remove_roles(r)
                                except: pass
                        await deleter_member.ban(reason='anti-nuke: mass role deletion')
                    logger.info(f'ANTI-NUKE: banned {deleter}')
                    try:
                        await send_log(role.guild, '🚨 ANTI-NUKE', f'banned {deleter.mention} for mass role deletion',
                                     COLORS['error'], {'User': str(deleter), 'Action': 'Mass Role Deletion'})
                    except: pass
                except Exception as e:
                    logger.error(f'anti-nuke: {e}')
            break
    except Exception as e:
        logger.error(f'role delete event: {e}')

@bot.event
async def on_guild_channel_create(channel):
    if not anti_nuke.enabled.get(channel.guild.id): return
    try:
        async for entry in channel.guild.audit_logs(limit=1, action=discord.AuditLogAction.channel_create):
            if not anti_nuke.is_recent(entry.created_at): return
            if not anti_nuke.is_after_restart(entry.created_at): return
            if entry.target.id != channel.id: continue
            
            creator = entry.user
            if creator.id == OWNER_ID: return
            if creator.id == bot.user.id: return
            
            creator_member = channel.guild.get_member(creator.id)
            if anti_nuke.is_whitelisted(creator_member): return
            
            anti_nuke.add_action(channel.guild.id, creator.id, 'channel_creates')
            
            if anti_nuke.check_nuke(channel.guild.id, creator.id):
                try:
                    if creator_member:
                        for role in creator_member.roles:
                            if role.id != channel.guild.id:
                                try: await creator_member.remove_roles(role)
                                except: pass
                        await creator_member.ban(reason='anti-nuke: channel spam')
                    logger.info(f'ANTI-NUKE: banned {creator}')
                    try:
                        await send_log(channel.guild, '🚨 ANTI-NUKE', f'banned {creator.mention} for channel spam',
                                     COLORS['error'], {'User': str(creator), 'Action': 'Mass Channel Creation'})
                    except: pass
                except Exception as e:
                    logger.error(f'anti-nuke: {e}')
            break
    except Exception as e:
        logger.error(f'channel create event: {e}')

@bot.event
async def on_member_update(before, after):
    if not anti_nuke.enabled.get(after.guild.id): return
    if len(after.roles) > len(before.roles):
        new_role = set(after.roles) - set(before.roles)
        for role in new_role:
            if role.managed:
                try:
                    async for entry in after.guild.audit_logs(limit=5, action=discord.AuditLogAction.bot_add):
                        if not anti_nuke.is_recent(entry.created_at): continue
                        if not anti_nuke.is_after_restart(entry.created_at): continue
                        if entry.target.id != after.id: continue
                        
                        inviter = entry.user
                        if inviter.id == OWNER_ID: return
                        
                        inviter_member = after.guild.get_member(inviter.id)
                        if anti_nuke.is_whitelisted(inviter_member): return
                        
                        try:
                            await after.kick(reason='anti-nuke: unauthorized bot')
                            if inviter_member:
                                await inviter_member.ban(reason='anti-nuke: added unauthorized bot')
                            logger.info(f'ANTI-NUKE: kicked bot {after} and banned {inviter}')
                            try:
                                await send_log(after.guild, '🚨 ANTI-NUKE', f'kicked bot {after.mention} and banned {inviter.mention}',
                                             COLORS['error'], {'Bot': str(after), 'Inviter': str(inviter)})
                            except: pass
                        except Exception as e:
                            logger.error(f'anti-nuke bot: {e}')
                        break
                except Exception as e:
                    logger.error(f'bot add event: {e}')

@bot.event
async def on_webhooks_update(channel):
    if not anti_nuke.enabled.get(channel.guild.id): return
    try:
        async for entry in channel.guild.audit_logs(limit=1, action=discord.AuditLogAction.webhook_create):
            if not anti_nuke.is_recent(entry.created_at): return
            if not anti_nuke.is_after_restart(entry.created_at): return
            
            creator = entry.user
            if creator.id == OWNER_ID: return
            
            creator_member = channel.guild.get_member(creator.id)
            if anti_nuke.is_whitelisted(creator_member): return
            
            try:
                webhooks = await channel.webhooks()
                for webhook in webhooks:
                    try:
                        await webhook.delete(reason='anti-nuke')
                    except: pass
                
                if creator_member:
                    await creator_member.ban(reason='anti-nuke: webhook')
                logger.info(f'ANTI-NUKE: deleted webhook and banned {creator}')
                try:
                    await send_log(channel.guild, '🚨 ANTI-NUKE', f'deleted webhook and banned {creator.mention}',
                                 COLORS['error'], {'User': str(creator), 'Channel': channel.mention})
                except: pass
            except Exception as e:
                logger.error(f'anti-nuke webhook: {e}')
            break
    except Exception as e:
        logger.error(f'webhooks event: {e}')

# ==================== ANTI-RAID EVENTS ====================

@bot.event
async def on_member_join(member):
    if not anti_raid.enabled.get(member.guild.id): return
    
    try:
        anti_raid.add_join(member.guild.id, member.id)
        
        if anti_raid.is_raid(member.guild.id):
            try:
                await member.kick(reason='anti-raid: join spam')
                logger.info(f'ANTI-RAID: kicked {member} during raid')
                try:
                    await send_log(member.guild, '🚨 ANTI-RAID: JOIN SPAM', f'kicked {member.mention}',
                                 COLORS['error'], {'Total Joins': f"{len(anti_raid.join_tracking[member.guild.id])}"})
                except: pass
            except Exception as e:
                logger.error(f'anti-raid kick: {e}')
            return
        
        is_sus, reasons = anti_raid.check_member(member)
        if is_sus and not anti_raid.is_whitelisted(member):
            settings = anti_raid.settings[member.guild.id]
            action = settings['action']
            
            try:
                if action == 'kick':
                    await member.kick(reason=f"anti-raid: {', '.join(reasons)}")
                elif action == 'ban':
                    await member.ban(reason=f"anti-raid: {', '.join(reasons)}")
                
                logger.info(f'ANTI-RAID: {action} {member}')
                try:
                    await send_log(member.guild, f'🚨 ANTI-RAID', f'{action} {member.mention}',
                                 COLORS['error'], {'Reasons': ', '.join(reasons)})
                except: pass
            except Exception as e:
                logger.error(f'anti-raid: {e}')
    
    except Exception as e:
        logger.error(f'member join event: {e}')

# ==================== ANTI-REACT EVENT ====================

@bot.event
async def on_raw_reaction_add(payload):
    if not payload.guild_id: return
    guild = bot.get_guild(payload.guild_id)
    if not guild or not anti_react.enabled.get(guild.id): return
    
    member = guild.get_member(payload.user_id)
    if not member or member.bot or anti_react.is_whitelisted(member): return
    
    anti_react.add_reaction(guild.id, member.id, payload.message_id)
    
    if anti_react.is_react_spam(guild.id, member.id):
        settings = anti_react.settings[guild.id]
        action = settings['action']
        message_ids = anti_react.get_recent_messages(guild.id, member.id)
        
        try:
            channel = guild.get_channel(payload.channel_id)
            if channel:
                for msg_id in message_ids:
                    try:
                        message = await channel.fetch_message(msg_id)
                        await message.clear_reactions()
                    except: pass
            
            if action == 'warn':
                try:
                    async with db.pool.acquire() as conn:
                        await conn.execute('INSERT INTO warnings (guild_id, user_id, reason, warned_by) VALUES ($1, $2, $3, $4)',
                                         guild.id, member.id, 'reaction spam', bot.user.id)
                    await channel.send(f'{member.mention} warned for reaction spam', delete_after=5)
                except: pass
            elif action == 'timeout':
                await member.timeout(timedelta(minutes=5), reason='reaction spam')
                await channel.send(f'{member.mention} timed out for 5 min', delete_after=5)
            elif action == 'kick':
                await member.kick(reason='reaction spam')
            
            logger.info(f'ANTI-REACT: {action} {member}')
            try:
                await send_log(guild, '🚨 ANTI-REACT', f'{action} {member.mention}', COLORS['error'],
                             {'Reactions': f"{len(anti_react.reactions[(guild.id, member.id)])}", 'Messages': f"{len(message_ids)}"})
            except: pass
        except Exception as e:
            logger.error(f'anti-react: {e}')

# ==================== ANTI-NUKE COMMANDS ====================

@bot.group(name='anti-nuke', invoke_without_command=True)
@is_owner()
async def anti_nuke_group(ctx):
    embed = discord.Embed(title='🛡️ anti-nuke', color=COLORS['support'])
    embed.add_field(name='control', value='`$anti-nuke enable/disable/status`')
    embed.add_field(name='whitelist', value='`$anti-nuke whitelist/unwhitelist @role`')
    await ctx.reply(embed=embed)

@anti_nuke_group.command(name='enable')
@is_owner()
async def anti_nuke_enable(ctx):
    if anti_nuke.enabled.get(ctx.guild.id):
        return await ctx.reply('❌ already enabled')
    anti_nuke.enabled[ctx.guild.id] = True
    embed = discord.Embed(title='✅ anti-nuke enabled', description='protects against mass bans/kicks/deletes, unauthorized bots/webhooks', color=COLORS['success'])
    await ctx.reply(embed=embed)
    try:
        await send_log(ctx.guild, '🛡️ anti-nuke enabled', f'by {ctx.author.mention}', COLORS['success'])
    except: pass

@anti_nuke_group.command(name='disable')
@is_owner()
async def anti_nuke_disable(ctx):
    if not anti_nuke.enabled.get(ctx.guild.id):
        return await ctx.reply('❌ already disabled')
    anti_nuke.enabled[ctx.guild.id] = False
    await ctx.reply(embed=discord.Embed(title='✅ anti-nuke disabled', color=COLORS['support']))
    try:
        await send_log(ctx.guild, '🛡️ anti-nuke disabled', f'by {ctx.author.mention}', COLORS['error'])
    except: pass

@anti_nuke_group.command(name='whitelist')
@is_owner()
async def anti_nuke_whitelist(ctx, role: discord.Role = None):
    if not role: return await ctx.reply('❌ specify role')
    if role.id in anti_nuke.whitelisted_roles.get(ctx.guild.id, []):
        return await ctx.reply('❌ already whitelisted')
    anti_nuke.whitelisted_roles[ctx.guild.id].append(role.id)
    await ctx.reply(embed=discord.Embed(title='✅ whitelisted', description=f'{role.mention} bypasses anti-nuke', color=COLORS['success']))

@anti_nuke_group.command(name='unwhitelist')
@is_owner()
async def anti_nuke_unwhitelist(ctx, role: discord.Role = None):
    if not role: return await ctx.reply('❌ specify role')
    if role.id not in anti_nuke.whitelisted_roles.get(ctx.guild.id, []):
        return await ctx.reply('❌ not whitelisted')
    anti_nuke.whitelisted_roles[ctx.guild.id].remove(role.id)
    await ctx.reply(embed=discord.Embed(title='✅ removed', color=COLORS['success']))

@anti_nuke_group.command(name='status')
@is_owner()
async def anti_nuke_status(ctx):
    enabled = anti_nuke.enabled.get(ctx.guild.id, False)
    wl_roles = anti_nuke.whitelisted_roles.get(ctx.guild.id, [])
    embed = discord.Embed(title='🛡️ anti-nuke status', color=COLORS['success'] if enabled else COLORS['error'])
    embed.add_field(name='status', value='✅ enabled' if enabled else '❌ disabled')
    embed.add_field(name='whitelisted', value=str(len(wl_roles)))
    if wl_roles:
        roles = []
        for rid in wl_roles[:5]:
            r = ctx.guild.get_role(rid)
            if r: roles.append(r.mention)
        if roles:
            embed.add_field(name='roles', value='\n'.join(roles), inline=False)
    await ctx.reply(embed=embed)

# ==================== ANTI-RAID COMMANDS ====================

@bot.group(name='anti-raid', invoke_without_command=True)
@is_owner()
async def anti_raid_group(ctx):
    embed = discord.Embed(title='🛡️ anti-raid', color=COLORS['support'])
    embed.add_field(name='control', value='`$anti-raid enable/disable/status`')
    embed.add_field(name='settings', value='`$anti-raid action/accountage/avatar`')
    await ctx.reply(embed=embed)

@anti_raid_group.command(name='enable')
@is_owner()
async def anti_raid_enable(ctx):
    if anti_raid.enabled.get(ctx.guild.id):
        return await ctx.reply('❌ already enabled')
    anti_raid.enabled[ctx.guild.id] = True
    await ctx.reply(embed=discord.Embed(title='✅ anti-raid enabled', description='detects join spam, new accounts, no avatars', color=COLORS['success']))

@anti_raid_group.command(name='disable')
@is_owner()
async def anti_raid_disable(ctx):
    if not anti_raid.enabled.get(ctx.guild.id):
        return await ctx.reply('❌ already disabled')
    anti_raid.enabled[ctx.guild.id] = False
    await ctx.reply(embed=discord.Embed(title='✅ anti-raid disabled', color=COLORS['support']))

@anti_raid_group.command(name='action')
@is_owner()
async def anti_raid_action(ctx, action: str = None):
    if not action or action not in ['kick', 'ban']:
        return await ctx.reply('❌ usage: `$anti-raid action <kick/ban>`')
    anti_raid.settings[ctx.guild.id]['action'] = action
    await ctx.reply(embed=discord.Embed(title='✅ updated', description=f'action: {action}', color=COLORS['success']))

@anti_raid_group.command(name='accountage')
@is_owner()
async def anti_raid_accountage(ctx, days: int = None):
    if not days or days < 0 or days > 365:
        return await ctx.reply('❌ usage: `$anti-raid accountage <1-365>`')
    anti_raid.settings[ctx.guild.id]['min_account_age'] = days
    await ctx.reply(embed=discord.Embed(title='✅ updated', description=f'min age: {days} days', color=COLORS['success']))

@anti_raid_group.command(name='avatar')
@is_owner()
async def anti_raid_avatar(ctx, toggle: str = None):
    if not toggle or toggle not in ['on', 'off']:
        return await ctx.reply('❌ usage: `$anti-raid avatar <on/off>`')
    anti_raid.settings[ctx.guild.id]['require_avatar'] = (toggle == 'on')
    await ctx.reply(embed=discord.Embed(title='✅ updated', description=f'avatar: {toggle}', color=COLORS['success']))

@anti_raid_group.command(name='status')
@is_owner()
async def anti_raid_status(ctx):
    enabled = anti_raid.enabled.get(ctx.guild.id, False)
    settings = anti_raid.settings[ctx.guild.id]
    embed = discord.Embed(title='🛡️ anti-raid status', color=COLORS['success'] if enabled else COLORS['error'])
    embed.add_field(name='status', value='✅ enabled' if enabled else '❌ disabled')
    embed.add_field(name='action', value=settings['action'])
    embed.add_field(name='settings', value=f"age: {settings['min_account_age']}d\navatar: {'yes' if settings['require_avatar'] else 'no'}", inline=False)
    await ctx.reply(embed=embed)

# ==================== ANTI-SPAM COMMANDS ====================

@bot.group(name='anti-spam', invoke_without_command=True)
@is_owner()
async def anti_spam_group(ctx):
    embed = discord.Embed(title='🛡️ anti-spam', color=COLORS['support'])
    embed.add_field(name='control', value='`$anti-spam enable/disable/status`')
    embed.add_field(name='whitelist', value='`$anti-spam whitelist/unwhitelist @role`')
    await ctx.reply(embed=embed)

@anti_spam_group.command(name='enable')
@is_owner()
async def anti_spam_enable(ctx):
    if anti_spam.enabled.get(ctx.guild.id):
        return await ctx.reply('❌ already enabled')
    anti_spam.enabled[ctx.guild.id] = True
    await ctx.reply(embed=discord.Embed(title='✅ anti-spam enabled', description='detects 7+ msgs in 10s, escalating timeouts', color=COLORS['success']))

@anti_spam_group.command(name='disable')
@is_owner()
async def anti_spam_disable(ctx):
    if not anti_spam.enabled.get(ctx.guild.id):
        return await ctx.reply('❌ already disabled')
    anti_spam.enabled[ctx.guild.id] = False
    await ctx.reply(embed=discord.Embed(title='✅ anti-spam disabled', color=COLORS['support']))

@anti_spam_group.command(name='whitelist')
@is_owner()
async def anti_spam_whitelist(ctx, role: discord.Role = None):
    if not role: return await ctx.reply('❌ specify role')
    if role.id in anti_spam.whitelisted_roles.get(ctx.guild.id, []):
        return await ctx.reply('❌ already whitelisted')
    anti_spam.whitelisted_roles[ctx.guild.id].append(role.id)
    await ctx.reply(embed=discord.Embed(title='✅ whitelisted', color=COLORS['success']))

@anti_spam_group.command(name='unwhitelist')
@is_owner()
async def anti_spam_unwhitelist(ctx, role: discord.Role = None):
    if not role: return await ctx.reply('❌ specify role')
    if role.id not in anti_spam.whitelisted_roles.get(ctx.guild.id, []):
        return await ctx.reply('❌ not whitelisted')
    anti_spam.whitelisted_roles[ctx.guild.id].remove(role.id)
    await ctx.reply(embed=discord.Embed(title='✅ removed', color=COLORS['success']))

@anti_spam_group.command(name='status')
@is_owner()
async def anti_spam_status(ctx):
    enabled = anti_spam.enabled.get(ctx.guild.id, False)
    embed = discord.Embed(title='🛡️ anti-spam status', color=COLORS['success'] if enabled else COLORS['error'])
    embed.add_field(name='status', value='✅ enabled' if enabled else '❌ disabled')
    await ctx.reply(embed=embed)

# ==================== ANTI-LINK COMMANDS ====================

@bot.group(name='anti-link', invoke_without_command=True)
@is_owner()
async def anti_link_group(ctx):
    embed = discord.Embed(title='🔗 anti-link', color=COLORS['support'])
    embed.add_field(name='control', value='`$anti-link enable/disable/status`')
    embed.add_field(name='whitelist', value='`$anti-link role/unrole @role`')
    await ctx.reply(embed=embed)

@anti_link_group.command(name='enable')
@is_owner()
async def anti_link_enable(ctx):
    if anti_link.enabled.get(ctx.guild.id):
        return await ctx.reply('❌ already enabled')
    anti_link.enabled[ctx.guild.id] = True
    await ctx.reply(embed=discord.Embed(title='✅ anti-link enabled', description='blocks all links unless whitelisted', color=COLORS['success']))

@anti_link_group.command(name='disable')
@is_owner()
async def anti_link_disable(ctx):
    if not anti_link.enabled.get(ctx.guild.id):
        return await ctx.reply('❌ already disabled')
    anti_link.enabled[ctx.guild.id] = False
    await ctx.reply(embed=discord.Embed(title='✅ anti-link disabled', color=COLORS['support']))

@anti_link_group.command(name='role')
@is_owner()
async def anti_link_role(ctx, role: discord.Role = None):
    if not role: return await ctx.reply('❌ specify role')
    if role.id in anti_link.whitelisted_roles.get(ctx.guild.id, []):
        return await ctx.reply('❌ already whitelisted')
    anti_link.whitelisted_roles[ctx.guild.id].append(role.id)
    await ctx.reply(embed=discord.Embed(title='✅ whitelisted', description=f'{role.mention} can post links', color=COLORS['success']))

@anti_link_group.command(name='unrole')
@is_owner()
async def anti_link_unrole(ctx, role: discord.Role = None):
    if not role: return await ctx.reply('❌ specify role')
    if role.id not in anti_link.whitelisted_roles.get(ctx.guild.id, []):
        return await ctx.reply('❌ not whitelisted')
    anti_link.whitelisted_roles[ctx.guild.id].remove(role.id)
    await ctx.reply(embed=discord.Embed(title='✅ removed', color=COLORS['success']))

@anti_link_group.command(name='status')
@is_owner()
async def anti_link_status(ctx):
    enabled = anti_link.enabled.get(ctx.guild.id, False)
    wl_roles = anti_link.whitelisted_roles.get(ctx.guild.id, [])
    embed = discord.Embed(title='🔗 anti-link status', color=COLORS['success'] if enabled else COLORS['error'])
    embed.add_field(name='status', value='✅ enabled' if enabled else '❌ disabled')
    embed.add_field(name='whitelisted', value=str(len(wl_roles)))
    await ctx.reply(embed=embed)

# ==================== ANTI-REACT COMMANDS ====================

@bot.group(name='anti-react', invoke_without_command=True)
@is_owner()
async def anti_react_group(ctx):
    embed = discord.Embed(title='🛡️ anti-react', color=COLORS['support'])
    embed.add_field(name='control', value='`$anti-react enable/disable/status`')
    embed.add_field(name='settings', value='`$anti-react action <warn/timeout/kick>`')
    await ctx.reply(embed=embed)

@anti_react_group.command(name='enable')
@is_owner()
async def anti_react_enable(ctx):
    if anti_react.enabled.get(ctx.guild.id):
        return await ctx.reply('❌ already enabled')
    anti_react.enabled[ctx.guild.id] = True
    await ctx.reply(embed=discord.Embed(title='✅ anti-react enabled', description='detects 10+ reactions in 10s across 5+ messages', color=COLORS['success']))

@anti_react_group.command(name='disable')
@is_owner()
async def anti_react_disable(ctx):
    if not anti_react.enabled.get(ctx.guild.id):
        return await ctx.reply('❌ already disabled')
    anti_react.enabled[ctx.guild.id] = False
    await ctx.reply(embed=discord.Embed(title='✅ anti-react disabled', color=COLORS['support']))

@anti_react_group.command(name='action')
@is_owner()
async def anti_react_action(ctx, action: str = None):
    if not action or action not in ['warn', 'timeout', 'kick']:
        return await ctx.reply('❌ usage: `$anti-react action <warn/timeout/kick>`')
    anti_react.settings[ctx.guild.id]['action'] = action
    await ctx.reply(embed=discord.Embed(title='✅ updated', description=f'action: {action}', color=COLORS['success']))

@anti_react_group.command(name='status')
@is_owner()
async def anti_react_status(ctx):
    enabled = anti_react.enabled.get(ctx.guild.id, False)
    settings = anti_react.settings[ctx.guild.id]
    embed = discord.Embed(title='🛡️ anti-react status', color=COLORS['success'] if enabled else COLORS['error'])
    embed.add_field(name='status', value='✅ enabled' if enabled else '❌ disabled')
    embed.add_field(name='action', value=settings['action'])
    await ctx.reply(embed=embed)

    # Auto-remove AFK when user sends message
    if message.author.id in afk_users:
        original = afk_users[message.author.id].get('original_nick')
        del afk_users[message.author.id]
        try:
            if message.author.display_name.startswith('[AFK]'):
                await message.author.edit(nick=original if original else None, reason='AFK auto-removed')
            await message.channel.send(f'Welcome back {message.author.mention}!', delete_after=3)
        except: pass
    

# AUTOMOD COMMANDS
@bot.group(name='automod', invoke_without_command=True)
@is_owner()
async def automod_group(ctx):
    embed = discord.Embed(title='🤖 automod', color=COLORS['support'])
    embed.add_field(name='control', value='`$automod enable/disable/status`')
    embed.add_field(name='words', value='`$automod add/remove/list <word>`')
    await ctx.reply(embed=embed)

@automod_group.command(name='enable')
@is_owner()
async def automod_enable(ctx):
    if automod.enabled.get(ctx.guild.id):
        return await ctx.reply('❌ already enabled')
    automod.enabled[ctx.guild.id] = True
    await ctx.reply(embed=discord.Embed(title='✅ automod enabled', description='filters bad words (staff bypass)', color=COLORS['success']))

@automod_group.command(name='disable')
@is_owner()
async def automod_disable(ctx):
    if not automod.enabled.get(ctx.guild.id):
        return await ctx.reply('❌ already disabled')
    automod.enabled[ctx.guild.id] = False
    await ctx.reply(embed=discord.Embed(title='✅ automod disabled', color=COLORS['support']))

@automod_group.command(name='add')
@is_owner()
async def automod_add(ctx, *, word: str = None):
    if not word:
        return await ctx.reply('❌ specify word')
    if automod.add_word(ctx.guild.id, word):
        await ctx.reply(embed=discord.Embed(title='✅ added', description=f'added ||{word}|| to filter', color=COLORS['success']))
    else:
        await ctx.reply('❌ already in filter')

@automod_group.command(name='remove')
@is_owner()
async def automod_remove(ctx, *, word: str = None):
    if not word:
        return await ctx.reply('❌ specify word')
    if automod.remove_word(ctx.guild.id, word):
        await ctx.reply(embed=discord.Embed(title='✅ removed', color=COLORS['success']))
    else:
        await ctx.reply('❌ not in filter')

@automod_group.command(name='list')
@is_owner()
async def automod_list(ctx):
    words = automod.bad_words.get(ctx.guild.id, [])
    if not words:
        return await ctx.reply('no words in filter')
    
    spoilered = [f'||{w}||' for w in words[:20]]
    embed = discord.Embed(title='🤖 automod filter', description='\n'.join(spoilered), color=COLORS['support'])
    if len(words) > 20:
        embed.set_footer(text=f'{len(words)-20} more words...')
    await ctx.reply(embed=embed)

@automod_group.command(name='status')
@is_owner()
async def automod_status(ctx):
    enabled = automod.enabled.get(ctx.guild.id, False)
    words = len(automod.bad_words.get(ctx.guild.id, []))
    embed = discord.Embed(title='🤖 automod status', color=COLORS['success'] if enabled else COLORS['error'])
    embed.add_field(name='status', value='✅ enabled' if enabled else '❌ disabled')
    embed.add_field(name='words', value=str(words))
    embed.add_field(name='staff bypass', value='yes', inline=False)
    await ctx.reply(embed=embed)

# ERROR HANDLERS - Show permission denied messages

@bot.event
async def on_command_error(ctx, error):
    """Handle command errors with friendly messages"""
    
    # Ignore command not found
    if isinstance(error, commands.CommandNotFound):
        return
    
    # Missing permissions (Discord permissions)
    if isinstance(error, commands.MissingPermissions):
        perms = ', '.join(error.missing_permissions).replace('_', ' ')
        return await ctx.reply(f'❌ you need **{perms}** permission to use this')
    
    # Bot missing permissions
    if isinstance(error, commands.BotMissingPermissions):
        perms = ', '.join(error.missing_permissions).replace('_', ' ')
        return await ctx.reply(f'❌ i need **{perms}** permission to do that')
    
    # Check failure (our custom decorators: @is_owner, @is_admin, @is_mod)
    if isinstance(error, commands.CheckFailure):
        # Try to determine which check failed
        if 'is_owner' in str(error) or ctx.command.name in ['anti-nuke', 'anti-raid', 'anti-spam', 'anti-link', 'anti-react', 'automod', 'lockdown', 'unlockdown', 'channelperm', 'channelpermall']:
            return await ctx.reply('❌ owner only command')
        elif 'is_admin' in str(error) or ctx.command.name in ['ban', 'unban', 'hackban', 'unhackban', 'kick', 'role', 'slowmode', 'lock', 'unlock', 'hide', 'unhide', 'purge', 'adminperms']:
            return await ctx.reply('❌ admin role required\n\nset admin role: `$adminperms set @role`')
        elif 'is_mod' in str(error) or ctx.command.name in ['mute', 'unmute', 'warn', 'warnings', 'clearwarnings', 'nick']:
            return await ctx.reply('❌ mod role required\n\nset mod role: `$modperms set @role`')
        else:
            return await ctx.reply('❌ you dont have permission to use this')
    
    # Missing required argument
    if isinstance(error, commands.MissingRequiredArgument):
        return await ctx.reply(f'❌ missing argument: **{error.param.name}**\n\ncheck `$help` for usage')
    
    # Bad argument (wrong type)
    if isinstance(error, commands.BadArgument):
        return await ctx.reply(f'❌ invalid argument\n\ncheck `$help` for usage')
    
    # User not found
    if isinstance(error, commands.UserNotFound):
        return await ctx.reply(f'❌ user not found')
    
    # Member not found
    if isinstance(error, commands.MemberNotFound):
        return await ctx.reply(f'❌ member not found')
    
    # Channel not found
    if isinstance(error, commands.ChannelNotFound):
        return await ctx.reply(f'❌ channel not found')
    
    # Role not found
    if isinstance(error, commands.RoleNotFound):
        return await ctx.reply(f'❌ role not found')
    
    # On cooldown
    if isinstance(error, commands.CommandOnCooldown):
        return await ctx.reply(f'❌ slow down bro, try again in {error.retry_after:.1f}s')
    
    # Other errors - log them
    logger.error(f'Command error in {ctx.command}: {error}')
    # Don't send error message for unknown errors

@bot.group(name='giveaway', aliases=['g'], invoke_without_command=True)
async def giveaway_group(ctx):
    embed = discord.Embed(title='🎉 giveaway system', color=COLORS['support'])
    embed.add_field(name='create', value='`$giveaway start <time> <winners> <prize>`\nExample: `$g start 1h 3 Nitro`')
    embed.add_field(name='manage', value='`$giveaway end <message_id>`\n`$giveaway reroll <message_id>`')
    await ctx.reply(embed=embed)

@giveaway_group.command(name='start', aliases=['create'])
@commands.has_permissions(manage_guild=True)
async def giveaway_start(ctx, duration: str, winners: int, *, prize: str):
    """Start a giveaway
    Example: $giveaway start 1h 3 Discord Nitro"""
    
    if winners < 1 or winners > 20:
        return await ctx.reply('❌ winners must be 1-20')
    
    try:
        # Parse duration
        dur = parse_duration(duration)
        if not dur:
            return await ctx.reply('❌ invalid duration\n\nExamples: 1h, 30m, 1d, 2h30m')
        
        if dur.total_seconds() < 60:
            return await ctx.reply('❌ giveaway must be at least 1 minute')
        
        if dur.total_seconds() > 604800:  # 7 days
            return await ctx.reply('❌ giveaway cant be longer than 7 days')
        
        await ctx.message.delete()
        giveaway_id = await giveaway.create_giveaway(ctx, dur, winners, prize)
        
        logger.info(f'GIVEAWAY: {ctx.author} started giveaway for {prize}')
        
        # Schedule auto-end
        async def auto_end():
            await asyncio.sleep(dur.total_seconds())
            await giveaway.end_giveaway(bot, giveaway_id)
        
        bot.loop.create_task(auto_end())
        
    except Exception as e:
        await ctx.reply(f'❌ error: {e}')

@giveaway_group.command(name='end')
@commands.has_permissions(manage_guild=True)
async def giveaway_end(ctx, message_id: int):
    """End a giveaway early"""
    winners, error = await giveaway.end_giveaway(bot, message_id)
    
    if error:
        return await ctx.reply(f'❌ {error}')
    
    await ctx.reply(f'✅ giveaway ended - {len(winners)} winner(s) picked')

@giveaway_group.command(name='reroll')
@commands.has_permissions(manage_guild=True)
async def giveaway_reroll(ctx, message_id: int):
    """Reroll giveaway winners"""
    winners, error = await giveaway.end_giveaway(bot, message_id, reroll=True)
    
    if error:
        return await ctx.reply(f'❌ {error}')
    
    winner_mentions = ' '.join([w.mention for w in winners])
    await ctx.send(f'🎉 **REROLL** - New winner(s): {winner_mentions}!')

@giveaway_group.command(name='list')
async def giveaway_list(ctx):
    """List active giveaways"""
    active = [g for g in giveaway.active_giveaways.values() if not g['ended'] and g['guild_id'] == ctx.guild.id]
    
    if not active:
        return await ctx.reply('no active giveaways')
    
    embed = discord.Embed(title='🎉 active giveaways', color=COLORS['support'])
    for g in active[:10]:
        channel = ctx.guild.get_channel(g['channel_id'])
        embed.add_field(
            name=g['prize'],
            value=f"Channel: {channel.mention}\nEnds: <t:{int(g['end_time'].timestamp())}:R>\nWinners: {g['winners']}",
            inline=False
        )
    
    await ctx.reply(embed=embed)

class Lockdown:
    def __init__(self):
        self.locked_channels = defaultdict(list)
    def is_locked(self, guild_id):
        return len(self.locked_channels.get(guild_id, [])) > 0

lockdown = Lockdown()

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
            await conn.execute('CREATE TABLE IF NOT EXISTS config (guild_id BIGINT PRIMARY KEY, ticket_category_id BIGINT, log_channel_id BIGINT)')
            await conn.execute('CREATE TABLE IF NOT EXISTS tickets (ticket_id TEXT PRIMARY KEY, guild_id BIGINT, channel_id BIGINT, user_id BIGINT, ticket_type TEXT, tier TEXT, claimed_by BIGINT, status TEXT DEFAULT \'open\', trade_details JSONB, created_at TIMESTAMP DEFAULT NOW())')
            await conn.execute('CREATE TABLE IF NOT EXISTS blacklist (user_id BIGINT PRIMARY KEY, guild_id BIGINT, reason TEXT, blacklisted_by BIGINT)')
            await conn.execute('CREATE TABLE IF NOT EXISTS ps_links (user_id BIGINT, game_key TEXT, game_name TEXT, link TEXT, PRIMARY KEY (user_id, game_key))')
            await conn.execute('CREATE TABLE IF NOT EXISTS jailed_users (user_id BIGINT PRIMARY KEY, guild_id BIGINT, saved_roles JSONB, reason TEXT, jailed_by BIGINT, jailed_at TIMESTAMP DEFAULT NOW())')
            await conn.execute('CREATE TABLE IF NOT EXISTS warnings (id SERIAL PRIMARY KEY, guild_id BIGINT, user_id BIGINT, reason TEXT, warned_by BIGINT, warned_at TIMESTAMP DEFAULT NOW())')
    async def close(self):
        if self.pool:
            await self.pool.close()

db = Database()

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.guilds = True
intents.bans = True
intents.integrations = True
