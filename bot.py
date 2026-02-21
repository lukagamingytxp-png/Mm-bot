import discord
from discord.ext import commands
from discord.ui import Button, View, Select, Modal, TextInput
import os
import asyncpg
from datetime import datetime, timedelta
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
bot_start_time = datetime.utcnow()

class RateLimiter:
    def __init__(self):
        self.cooldowns = defaultdict(float)
    def check_cooldown(self, user_id: int, command: str, cooldown: int = 3) -> bool:
        key = f"{user_id}:{command}"
        now = datetime.utcnow().timestamp()
        if key in self.cooldowns and now - self.cooldowns[key] < cooldown:
            return False
        self.cooldowns[key] = now
        return True

rate_limiter = RateLimiter()

class AntiSpam:
    def __init__(self):
        self.messages = defaultdict(list)
        self.enabled = {}
        self.whitelisted_users = defaultdict(list)
        self.whitelisted_roles = defaultdict(list)
    
    def add_message(self, guild_id, user_id):
        now = datetime.utcnow()
        self.messages[(guild_id, user_id)].append(now)
        self.messages[(guild_id, user_id)] = [m for m in self.messages[(guild_id, user_id)] if now - m < timedelta(seconds=10)]
    
    def get_spam_level(self, guild_id, user_id):
        if not self.enabled.get(guild_id): return 0
        count = len(self.messages.get((guild_id, user_id), []))
        return count
    
    def is_spam(self, guild_id, user_id):
        return self.get_spam_level(guild_id, user_id) >= 7
    
    def get_timeout_duration(self, guild_id, user_id):
        count = self.get_spam_level(guild_id, user_id)
        if count >= 15: return timedelta(minutes=30)
        elif count >= 12: return timedelta(minutes=15)
        elif count >= 10: return timedelta(minutes=10)
        elif count >= 7: return timedelta(minutes=5)
        return timedelta(minutes=1)
    
    def is_whitelisted(self, guild_id, member):
        if member.id in self.whitelisted_users.get(guild_id, []):
            return True
        for role in member.roles:
            if role.id in self.whitelisted_roles.get(guild_id, []):
                return True
        return False

anti_spam = AntiSpam()

class AntiPing:
    def __init__(self):
        self.pings = defaultdict(list)
        self.enabled = {}
        self.whitelisted_users = defaultdict(list)
        self.whitelisted_roles = defaultdict(list)
    
    def add_ping(self, guild_id, user_id, mention_count):
        now = datetime.utcnow()
        self.pings[(guild_id, user_id)].append((now, mention_count))
        self.pings[(guild_id, user_id)] = [(t, c) for t, c in self.pings[(guild_id, user_id)] if now - t < timedelta(seconds=10)]
    
    def is_ping_spam(self, guild_id, user_id):
        if not self.enabled.get(guild_id): return False
        total_pings = sum(count for _, count in self.pings.get((guild_id, user_id), []))
        return total_pings >= 3
    
    def is_whitelisted(self, guild_id, member):
        if member.id in self.whitelisted_users.get(guild_id, []):
            return True
        for role in member.roles:
            if role.id in self.whitelisted_roles.get(guild_id, []):
                return True
        return False

anti_ping = AntiPing()

class AntiLink:
    def __init__(self):
        self.enabled = {}
        self.whitelist = defaultdict(list)
        self.whitelisted_users = defaultdict(list)
        self.whitelisted_roles = defaultdict(list)
    def is_link(self, content):
        url_pattern = re.compile(r'https?://|discord\.gg/|\.com|\.net|\.org')
        return url_pattern.search(content) is not None
    def is_url_whitelisted(self, guild_id, content):
        for wl in self.whitelist.get(guild_id, []):
            if wl.lower() in content.lower():
                return True
        return False
    def is_user_whitelisted(self, guild_id, member):
        if member.id in self.whitelisted_users.get(guild_id, []):
            return True
        for role in member.roles:
            if role.id in self.whitelisted_roles.get(guild_id, []):
                return True
        return False

anti_link = AntiLink()

class AntiNuke:
    def __init__(self):
        self.enabled = {}
        self.channel_deletes = defaultdict(list)
        self.bot_adds = defaultdict(list)
        self.integration_adds = defaultdict(list)
        self.whitelisted_users = defaultdict(list)
        self.whitelisted_roles = defaultdict(list)
    def add_channel_delete(self, guild_id, user_id):
        now = datetime.utcnow()
        self.channel_deletes[(guild_id, user_id)].append(now)
        self.channel_deletes[(guild_id, user_id)] = [d for d in self.channel_deletes[(guild_id, user_id)] if now - d < timedelta(seconds=5)]
    def is_nuke(self, guild_id, user_id):
        if not self.enabled.get(guild_id): return False
        if user_id in self.whitelisted_users.get(guild_id, []): return False
        return len(self.channel_deletes.get((guild_id, user_id), [])) >= 3
    def add_bot_add(self, guild_id, user_id):
        self.bot_adds[(guild_id, user_id)] = datetime.utcnow()
    def can_add_bot(self, guild_id, user_id, user_roles=None):
        if not self.enabled.get(guild_id): return True
        if user_id in self.whitelisted_users.get(guild_id, []): return True
        if user_roles:
            for role in user_roles:
                if role.id in self.whitelisted_roles.get(guild_id, []):
                    return True
        return False
    def add_integration(self, guild_id, user_id):
        now = datetime.utcnow()
        self.integration_adds[(guild_id, user_id)].append(now)
        self.integration_adds[(guild_id, user_id)] = [i for i in self.integration_adds[(guild_id, user_id)] if now - i < timedelta(seconds=10)]
    def can_add_integration(self, guild_id, user_id, user_roles=None):
        if not self.enabled.get(guild_id): return True
        if user_id in self.whitelisted_users.get(guild_id, []): return True
        if user_roles:
            for role in user_roles:
                if role.id in self.whitelisted_roles.get(guild_id, []):
                    return True
        return False
    def is_whitelisted(self, guild_id, member):
        if member.id in self.whitelisted_users.get(guild_id, []):
            return True
        for role in member.roles:
            if role.id in self.whitelisted_roles.get(guild_id, []):
                return True
        return False

anti_nuke = AntiNuke()

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
        return await ctx.reply('❌ Missing arguments\n\n**Usage:**\n`$whitelist anti-link @Role/@Member`\n`$whitelist anti-spam @Role/@Member`\n`$whitelist anti-nuke @Role/@Member`\n`$whitelist anti-ping @Role/@Member`')
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
    elif protection == 'anti-ping':
        lst = anti_ping.whitelisted_roles if role else anti_ping.whitelisted_users
        if target_id in lst[ctx.guild.id]: return await ctx.reply(f'❌ Already whitelisted for anti-ping')
        lst[ctx.guild.id].append(target_id)
        embed = discord.Embed(title='✅ Anti-Ping Whitelist Added', description=f'{target_obj.mention} ({target_type}) is exempt from mass ping detection', color=COLORS['success'])
    else:
        return await ctx.reply('❌ Invalid protection\n\nOptions: `anti-link` `anti-spam` `anti-nuke` `anti-ping`')
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

@bot.group(name='anti-link', invoke_without_command=True)
@is_owner()
async def anti_link_group(ctx):
    await ctx.reply('Usage: `$anti-link enable/disable/whitelist/status`')

@anti_link_group.command(name='enable')
@is_owner()
async def anti_link_enable(ctx):
    anti_link.enabled[ctx.guild.id] = True
    await ctx.reply(embed=discord.Embed(title='🛡️ Anti-Link Enabled', description='All links will be deleted', color=COLORS['success']))

@anti_link_group.command(name='disable')
@is_owner()
async def anti_link_disable(ctx):
    anti_link.enabled[ctx.guild.id] = False
    await ctx.reply(embed=discord.Embed(title='🛡️ Anti-Link Disabled', description='Links are now allowed', color=COLORS['support']))

@anti_link_group.command(name='whitelist')
@is_owner()
async def anti_link_whitelist(ctx, action: str = None, *, url: str = None):
    if action == 'add' and url:
        anti_link.whitelist[ctx.guild.id].append(url)
        await ctx.reply(embed=discord.Embed(title='✅ URL Whitelisted', description=f'`{url}` is now allowed', color=COLORS['success']))
    elif action == 'remove' and url:
        if url in anti_link.whitelist[ctx.guild.id]:
            anti_link.whitelist[ctx.guild.id].remove(url)
            await ctx.reply(embed=discord.Embed(title='✅ URL Removed', description=f'`{url}` removed', color=COLORS['success']))
        else:
            await ctx.reply('❌ URL not in whitelist')
    elif action == 'list':
        wl = anti_link.whitelist.get(ctx.guild.id, [])
        if not wl: return await ctx.reply('No whitelisted URLs')
        embed = discord.Embed(title='✅ Whitelisted URLs', description='\n'.join([f'• `{u}`' for u in wl]), color=COLORS['support'])
        await ctx.reply(embed=embed)
    else:
        await ctx.reply('Usage: `$anti-link whitelist add/remove/list <url>`')

@anti_link_group.command(name='status')
@is_owner()
async def anti_link_status(ctx):
    enabled = anti_link.enabled.get(ctx.guild.id, False)
    wl_count = len(anti_link.whitelist.get(ctx.guild.id, []))
    embed = discord.Embed(title='🛡️ Anti-Link Status', color=COLORS['support'])
    embed.add_field(name='Status', value='✅ Enabled' if enabled else '❌ Disabled', inline=True)
    embed.add_field(name='URL Whitelist', value=f'{wl_count} URLs', inline=True)
    await ctx.reply(embed=embed)

@bot.group(name='anti-spam', invoke_without_command=True)
@is_owner()
async def anti_spam_group(ctx):
    await ctx.reply('Usage: `$anti-spam enable/disable/status`')

@anti_spam_group.command(name='enable')
@is_owner()
async def anti_spam_enable(ctx):
    anti_spam.enabled[ctx.guild.id] = True
    await ctx.reply(embed=discord.Embed(title='🛡️ Anti-Spam Enabled', description='3 messages in 2 seconds = spam deleted', color=COLORS['success']))

@anti_spam_group.command(name='disable')
@is_owner()
async def anti_spam_disable(ctx):
    anti_spam.enabled[ctx.guild.id] = False
    await ctx.reply(embed=discord.Embed(title='🛡️ Anti-Spam Disabled', color=COLORS['support']))

@anti_spam_group.command(name='status')
@is_owner()
async def anti_spam_status(ctx):
    enabled = anti_spam.enabled.get(ctx.guild.id, False)
    embed = discord.Embed(title='🛡️ Anti-Spam Status', color=COLORS['support'])
    embed.add_field(name='Status', value='✅ Enabled' if enabled else '❌ Disabled', inline=True)
    embed.add_field(name='Trigger', value='3 msgs / 2 sec', inline=True)
    await ctx.reply(embed=embed)

@bot.group(name='anti-nuke', invoke_without_command=True)
@is_owner()
async def anti_nuke_group(ctx):
    await ctx.reply('Usage: `$anti-nuke enable/disable/status`')

@anti_nuke_group.command(name='enable')
@is_owner()
async def anti_nuke_enable(ctx):
    anti_nuke.enabled[ctx.guild.id] = True
    embed = discord.Embed(title='🛡️ Anti-Nuke Enabled', color=COLORS['success'])
    embed.description = '**Protected against:**\n• Mass channel deletes\n• Unauthorized bot adds\n• Unauthorized integrations\n\nUse `$whitelist anti-nuke @user` to allow trusted users'
    await ctx.reply(embed=embed)

@anti_nuke_group.command(name='disable')
@is_owner()
async def anti_nuke_disable(ctx):
    anti_nuke.enabled[ctx.guild.id] = False
    await ctx.reply(embed=discord.Embed(title='🛡️ Anti-Nuke Disabled', color=COLORS['support']))

@anti_nuke_group.command(name='status')
@is_owner()
async def anti_nuke_status(ctx):
    enabled = anti_nuke.enabled.get(ctx.guild.id, False)
    wl_users = len(anti_nuke.whitelisted_users.get(ctx.guild.id, []))
    wl_roles = len(anti_nuke.whitelisted_roles.get(ctx.guild.id, []))
    embed = discord.Embed(title='🛡️ Anti-Nuke Status', color=COLORS['support'])
    embed.add_field(name='Status', value='✅ Enabled' if enabled else '❌ Disabled', inline=True)
    embed.add_field(name='Whitelisted Users', value=str(wl_users), inline=True)
    embed.add_field(name='Whitelisted Roles', value=str(wl_roles), inline=True)
    await ctx.reply(embed=embed)

# ==================== ANTI-PING COMMANDS ====================

@bot.group(name='anti-ping', invoke_without_command=True)
@is_owner()
async def anti_ping_group(ctx):
    await ctx.reply('usage: `$anti-ping enable/disable/status`')

@anti_ping_group.command(name='enable')
@is_owner()
async def anti_ping_enable(ctx):
    anti_ping.enabled[ctx.guild.id] = True
    embed = discord.Embed(title='🛡️ anti-ping enabled', description='3+ mentions in 10 sec = ban\nwebhooks/bots get deleted\nmembers get roles stripped then banned', color=COLORS['success'])
    await ctx.reply(embed=embed)

@anti_ping_group.command(name='disable')
@is_owner()
async def anti_ping_disable(ctx):
    anti_ping.enabled[ctx.guild.id] = False
    await ctx.reply(embed=discord.Embed(title='🛡️ anti-ping disabled', color=COLORS['support']))

@anti_ping_group.command(name='status')
@is_owner()
async def anti_ping_status(ctx):
    enabled = anti_ping.enabled.get(ctx.guild.id, False)
    wl_users = len(anti_ping.whitelisted_users.get(ctx.guild.id, []))
    wl_roles = len(anti_ping.whitelisted_roles.get(ctx.guild.id, []))
    embed = discord.Embed(title='🛡️ anti-ping status', color=COLORS['support'])
    embed.add_field(name='status', value='✅ enabled' if enabled else '❌ disabled', inline=True)
    embed.add_field(name='trigger', value='3+ pings / 10s', inline=True)
    embed.add_field(name='whitelisted', value=f'{wl_users} users, {wl_roles} roles', inline=True)
    await ctx.reply(embed=embed)

# ==================== LOCKDOWN COMMANDS ====================

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
async def afk_cmd(ctx, *, reason: str = "AFK"):
    afk_users[ctx.author.id] = {'reason': reason, 'guild': ctx.guild.id}
    embed = discord.Embed(title='💤 AFK Set', description=f'You are now AFK\n**Reason:** {reason}', color=COLORS['support'])
    await ctx.reply(embed=embed)

@bot.command(name='afkoff')
async def afkoff_cmd(ctx):
    if ctx.author.id in afk_users:
        del afk_users[ctx.author.id]
        await ctx.reply(embed=discord.Embed(title='✅ AFK Removed', description='You are no longer AFK', color=COLORS['success']))
    else:
        await ctx.reply('❌ You are not AFK')

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
    now = datetime.utcnow()
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
    now = datetime.utcnow()
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
    pages = []
    is_admin = ctx.author.guild_permissions.administrator or ctx.author.id == OWNER_ID
    has_admin = is_admin
    if not has_admin:
        admin_role_id = admin_perms.get(ctx.guild.id)
        if admin_role_id:
            admin_role = ctx.guild.get_role(admin_role_id)
            if admin_role:
                for role in ctx.author.roles:
                    if role >= admin_role:
                        has_admin = True
    has_mod = has_admin
    if not has_mod:
        mod_role_id = mod_perms.get(ctx.guild.id)
        if mod_role_id:
            mod_role = ctx.guild.get_role(mod_role_id)
            if mod_role:
                for role in ctx.author.roles:
                    if role >= mod_role:
                        has_mod = True

    total_pages = 3
    if has_mod: total_pages += 2
    if has_admin: total_pages += 1
    if ctx.author.id == OWNER_ID: total_pages += 3

    # PAGE 1 - Tickets
    e1 = discord.Embed(title='🎫 Ticket Commands', color=0x5865F2)
    e1.add_field(name='Manage', value='`$close` `$claim` `$unclaim`', inline=False)
    e1.add_field(name='Users', value='`$add @user` `$remove @user`', inline=False)
    e1.add_field(name='Edit', value='`$rename <name>` `$transfer @user`', inline=False)
    e1.add_field(name='Trade', value='`$proof` - Post completion proof', inline=False)
    e1.set_footer(text=f'Page 1/{total_pages}')
    pages.append(e1)

    # PAGE 2 - Utility
    e2 = discord.Embed(title='🛠️ Utility Commands', color=0x5865F2)
    e2.add_field(name='💬 Social', value='`$afk <reason>` `$afkoff`', inline=False)
    e2.add_field(name='📊 Info', value='`$userinfo` / `$ui` - User info\n`$serverinfo` / `$si` - Server info\n`$roleinfo` / `$ri` - Role info\n`$membercount` / `$mc` - Member count\n`$botinfo` / `$bi` - Bot stats\n`$ping` - Latency', inline=False)
    e2.add_field(name='🖼️ Media', value='`$avatar` / `$av` - User avatar\n`$banner` / `$bn` - User banner', inline=False)
    e2.add_field(name='👀 Snipe', value='`$snipe` / `$sn` - Deleted messages\n`$editsnipe` / `$es` - Edited messages', inline=False)
    e2.set_footer(text=f'Page 2/{total_pages}')
    pages.append(e2)

    # PAGE 3 - Admin Setup
    e3 = discord.Embed(title='⚙️ Setup Commands', description='Administrator only', color=0x5865F2)
    e3.add_field(name='🎫 Ticket Setup', value='`$setup` - Create panel\n`$setcategory #cat` - Set category\n`$setlogs #channel` - Set logs\n`$config` - View config', inline=False)
    e3.add_field(name='🚔 Jail', value='`$jail @user <reason>`\n`$unjail @user`\n`$jailed` - List jailed', inline=False)
    e3.add_field(name='🚫 Blacklist', value='`$blacklist @user <reason>`\n`$unblacklist @user`\n`$blacklists` - View list', inline=False)
    e3.add_field(name='🧹 Cleanup', value='`$clear` - Delete bot messages\n`$purge <amount> [@user]` - Bulk delete', inline=False)
    e3.set_footer(text=f'Page 3/{total_pages}')
    pages.append(e3)

    if has_mod:
        # PAGE 4 - Moderation
        e4 = discord.Embed(title='🛡️ Moderation Commands', description='Mod role required', color=0x5865F2)
        e4.add_field(name='🔇 Mute', value='`$mute` / `$m @user <time> <reason>`\nTime: `10s` `5m` `1h` `1d`\n`$unmute` / `$um @user`', inline=False)
        e4.add_field(name='⚠️ Warn', value='`$warn` / `$w @user <reason>`\n`$warnings` / `$ws @user`\n`$clearwarnings` / `$cw @user`', inline=False)
        e4.add_field(name='✏️ Other', value='`$nick` / `$n @user <name>` - Change nickname', inline=False)
        e4.set_footer(text=f'Page 4/{total_pages}')
        pages.append(e4)

        # PAGE 5 - Admin Actions
        e5 = discord.Embed(title='🔨 Admin Commands', description='Admin role required', color=0x5865F2)
        e5.add_field(name='🚪 Ban & Kick', value='`$ban` / `$b @user <reason>`\n`$unban` / `$ub <ID>`\n`$hackban` / `$hb <ID> <reason>`\n`$unhackban` / `$uhb <ID>`\n`$kick` / `$k @user <reason>`', inline=False)
        e5.add_field(name='🎭 Role', value='`$role` / `$r @user <rolename>`\nAdd/remove role by name', inline=False)
        e5.add_field(name='🔒 Channel', value='`$slowmode` / `$sm <seconds>`\n`$lock` / `$lk` - Lock channel\n`$unlock` / `$ulk` - Unlock channel\n`$hide` / `$hd` - Hide channel\n`$unhide` / `$uhd` - Unhide channel', inline=False)
        e5.set_footer(text=f'Page 5/{total_pages}')
        pages.append(e5)

    if is_admin:
        # PAGE 6 - Perm Setup
        e6 = discord.Embed(title='⚙️ Permission Setup', description='Administrator only', color=0x5865F2)
        e6.add_field(name='👑 Admin Role', value='`$adminperms set @role` / `$ap set`\n`$adminperms show` / `$ap show`\nGives access to: ban kick hackban role slowmode lock hide warn mute purge', inline=False)
        e6.add_field(name='🛡️ Mod Role', value='`$modperms set @role` / `$mp set`\n`$modperms show` / `$mp show`\nGives access to: mute warn nick', inline=False)
        e6.set_footer(text=f'Page 6/{total_pages}')
        pages.append(e6)

    if ctx.author.id == OWNER_ID:
        # PAGE 7 - Owner Whitelist
        e7 = discord.Embed(title='🛡️ Whitelist System', description='Owner only', color=0x5865F2)
        e7.add_field(name='Add', value='`$whitelist anti-link @Role/@Member`\n`$whitelist anti-spam @Role/@Member`\n`$whitelist anti-nuke @Role/@Member`', inline=False)
        e7.add_field(name='Remove', value='`$unwhitelist anti-link @Role/@Member`\n`$unwhitelist anti-spam @Role/@Member`\n`$unwhitelist anti-nuke @Role/@Member`', inline=False)
        e7.add_field(name='View', value='`$whitelisted anti-link`\n`$whitelisted anti-spam`\n`$whitelisted anti-nuke`', inline=False)
        e7.set_footer(text=f'Page 7/{total_pages}')
        pages.append(e7)

        # PAGE 8 - Owner Protection
        e8 = discord.Embed(title='🛡️ Protection Systems', description='Owner only', color=0x5865F2)
        e8.add_field(name='🔗 Anti-Link', value='`$anti-link enable/disable/status`\n`$anti-link whitelist add/remove/list <url>`', inline=False)
        e8.add_field(name='💬 Anti-Spam', value='`$anti-spam enable/disable/status`', inline=False)
        e8.add_field(name='💣 Anti-Nuke', value='`$anti-nuke enable/disable/status`', inline=False)
        e8.add_field(name='🔐 Server Lock', value='`$lockdown` - Lock all channels\n`$unlockdown` - Unlock all', inline=False)
        e8.set_footer(text=f'Page 8/{total_pages}')
        pages.append(e8)

        # PAGE 9 - Owner Advanced
        e9 = discord.Embed(title='⚙️ Advanced Settings', description='Owner only', color=0x5865F2)
        e9.add_field(name='Channel Permissions', value='`$channelperm #ch @target enable/disable <perm>`\n`$channelpermall @target enable/disable <perm>`', inline=False)
        e9.add_field(name='Common Shortcuts', value='`$b` ban | `$ub` unban | `$hb` hackban | `$uhb` unhackban\n`$k` kick | `$m` mute | `$um` unmute\n`$w` warn | `$ws` warnings | `$cw` clearwarnings\n`$r` role | `$n` nick | `$sm` slowmode\n`$lk` lock | `$ulk` unlock | `$hd` hide | `$uhd` unhide\n`$av` avatar | `$bn` banner | `$ui` userinfo | `$si` serverinfo\n`$ri` roleinfo | `$mc` membercount | `$bi` botinfo\n`$sn` snipe | `$es` editsnipe\n`$ap` adminperms | `$mp` modperms', inline=False)
        e9.set_footer(text=f'Page 9/{total_pages}')
        pages.append(e9)

    view = HelpView(pages, ctx.author.id)
    await ctx.reply(embed=pages[0], view=view)

# ==================== EVENTS ====================

@bot.event
async def on_ready():
    global bot_start_time
    bot_start_time = datetime.utcnow()
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
                # INSTANT detection when bot role is added
                async for entry in after.guild.audit_logs(limit=5, action=discord.AuditLogAction.bot_add):
                    if entry.target and entry.target.bot:
                        bot_member = entry.target
                        inviter = entry.user
                        if inviter.id == OWNER_ID: return
                        inviter_member = after.guild.get_member(inviter.id)
                        inviter_roles = inviter_member.roles if inviter_member else []
                        if not anti_nuke.can_add_bot(after.guild.id, inviter.id, inviter_roles):
                            try:
                                await bot_member.kick(reason='anti-nuke: unauthorized bot')
                                await inviter.ban(reason='anti-nuke: added bot without permission')
                                logger.info(f'anti-nuke: INSTANT kicked bot {bot_member} and banned {inviter}')
                            except Exception as e:
                                logger.error(f'anti-nuke bot kick failed: {e}')
                        break

@bot.event
async def on_integration_create(integration):
    if not anti_nuke.enabled.get(integration.guild.id): return
    try:
        # Check immediately - no delay
        async for entry in integration.guild.audit_logs(limit=1, action=discord.AuditLogAction.webhook_create):
            creator = entry.user
            if creator.id == OWNER_ID: return
            creator_member = integration.guild.get_member(creator.id)
            creator_roles = creator_member.roles if creator_member else []
            if not anti_nuke.can_add_integration(integration.guild.id, creator.id, creator_roles):
                try:
                    # Delete the webhook
                    webhooks = await integration.guild.webhooks()
                    for webhook in webhooks:
                        if webhook.user and webhook.user.id == bot.user.id:
                            await webhook.delete(reason='anti-nuke: unauthorized')
                    # Ban creator
                    await creator.ban(reason='anti-nuke: unauthorized webhook/integration')
                    logger.info(f'anti-nuke: INSTANT deleted integration and banned {creator}')
                except Exception as e:
                    logger.error(f'anti-nuke integration delete failed: {e}')
            break
    except Exception as e:
        logger.error(f'integration create event error: {e}')

@bot.event
async def on_webhooks_update(channel):
    if not anti_nuke.enabled.get(channel.guild.id): return
    try:
        # Check immediately - no delay
        async for entry in channel.guild.audit_logs(limit=1, action=discord.AuditLogAction.webhook_create):
            creator = entry.user
            if creator.id == OWNER_ID: return
            creator_member = channel.guild.get_member(creator.id)
            creator_roles = creator_member.roles if creator_member else []
            if not anti_nuke.can_add_integration(channel.guild.id, creator.id, creator_roles):
                try:
                    webhooks = await channel.webhooks()
                    for webhook in webhooks:
                        try:
                            await webhook.delete(reason='anti-nuke: unauthorized webhook')
                            logger.info(f'anti-nuke: INSTANT deleted webhook {webhook.name} in {channel.name}')
                        except: pass
                    await creator.ban(reason='anti-nuke: unauthorized webhook')
                    logger.info(f'anti-nuke: INSTANT banned {creator} for webhook')
                except Exception as e:
                    logger.error(f'anti-nuke webhook ban failed: {e}')
            break
    except Exception as e:
        logger.error(f'webhooks update event error: {e}')

@bot.event
async def on_guild_channel_delete(channel):
    # INSTANT detection when channel is deleted
    async for entry in channel.guild.audit_logs(limit=1, action=discord.AuditLogAction.channel_delete):
        deleter = entry.user
        if deleter.id == OWNER_ID: return
        anti_nuke.add_channel_delete(channel.guild.id, deleter.id)
        if anti_nuke.is_nuke(channel.guild.id, deleter.id):
            try:
                await deleter.ban(reason='anti-nuke: mass channel deletion')
                logger.info(f'anti-nuke: INSTANT banned {deleter} for mass channel deletion')
            except Exception as e:
                logger.error(f'anti-nuke channel delete ban failed: {e}')
        break

@bot.event
async def on_message(message):
    if message.author.bot: return
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
                    # If ticket is NOT claimed
                    if not ticket['claimed_by']:
                        # Owner can talk in unclaimed tickets
                        if message.author.id == OWNER_ID:
                            pass  # Continue to anti checks below
                        elif message.author.id != ticket['user_id']:
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
                                    await message.channel.send(f'{message.author.mention} you can\'t talk here', delete_after=3)
                                except: pass
                                return
                    else:
                        # Ticket IS claimed - only ticket creator, claimer, and NEWLY added users can talk
                        # Owner CANNOT talk after claimed
                        allowed_users = [ticket['user_id'], ticket['claimed_by']]
                        
                        # Check if user was EXPLICITLY added via $add command (has channel override)
                        overwrites = message.channel.overwrites_for(message.author)
                        if overwrites.send_messages == True:
                            # They were added after claim, let them talk
                            pass  # Continue to anti checks below
                        elif message.author.id not in allowed_users:
                            try:
                                await message.delete()
                                await message.channel.send(f'{message.author.mention} only ticket creator, claimer, and added users can talk here', delete_after=3)
                            except: pass
                            return
        except Exception as e:
            logger.error(f'ticket filter error: {e}')

    # Anti-spam (runs in tickets too)
    if not anti_spam.is_whitelisted(message.guild.id, message.author):
        anti_spam.add_message(message.guild.id, message.author.id)
        if anti_spam.is_spam(message.guild.id, message.author.id):
            try:
                await message.delete()
                timeout_duration = anti_spam.get_timeout_duration(message.guild.id, message.author.id)
                spam_count = anti_spam.get_spam_level(message.guild.id, message.author.id)
                await message.author.timeout(timeout_duration, reason=f'spamming ({spam_count} msgs in 10s)')
                minutes = int(timeout_duration.total_seconds() / 60)
                await message.channel.send(f'{message.author.mention} timed out for {minutes} min for spamming', delete_after=5)
                logger.info(f'anti-spam: timed out {message.author} for {minutes} min ({spam_count} messages)')
            except Exception as e:
                logger.error(f'anti-spam timeout failed: {e}')
            return

    # Anti-link (runs in tickets too)
    if anti_link.enabled.get(message.guild.id):
        if anti_link.is_link(message.content):
            if not anti_link.is_url_whitelisted(message.guild.id, message.content):
                if not anti_link.is_user_whitelisted(message.guild.id, message.author):
                    try:
                        await message.delete()
                        await message.channel.send(f'{message.author.mention} no links allowed', delete_after=3)
                    except: pass
                    return
    
    # Anti-ping
    if anti_ping.enabled.get(message.guild.id):
        mention_count = len(message.mentions)
        if mention_count > 0:
            if not anti_ping.is_whitelisted(message.guild.id, message.author):
                anti_ping.add_ping(message.guild.id, message.author.id, mention_count)
                if anti_ping.is_ping_spam(message.guild.id, message.author.id):
                    try:
                        await message.delete()
                        # Check if webhook
                        if message.webhook_id:
                            webhooks = await message.channel.webhooks()
                            for webhook in webhooks:
                                if webhook.id == message.webhook_id:
                                    await webhook.delete(reason='mass ping spam')
                                    break
                            await message.channel.send('deleted webhook for mass pinging', delete_after=5)
                            return
                        # Check if bot
                        if message.author.bot:
                            try:
                                await message.author.ban(reason='bot mass ping spam')
                                await message.channel.send(f'banned bot {message.author} for mass pinging', delete_after=5)
                            except: pass
                            return
                        # Regular member - strip roles and ban
                        try:
                            saved_roles = [r.id for r in message.author.roles if r.id != message.guild.id]
                            for role in message.author.roles:
                                if role.id != message.guild.id:
                                    try:
                                        await message.author.remove_roles(role)
                                    except: pass
                            await message.author.ban(reason='mass ping spam (3+ pings in 10s)')
                            await message.channel.send(f'banned {message.author} for mass pinging', delete_after=5)
                            logger.info(f'anti-ping: banned {message.author} for mass pinging')
                        except Exception as e:
                            logger.error(f'anti-ping ban failed: {e}')
                    except Exception as e:
                        logger.error(f'anti-ping error: {e}')
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
