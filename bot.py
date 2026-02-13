# ========================================
# PART 1/10 - IMPORTS AND UTILITIES
# ========================================

import discord
from discord.ext import commands
from discord.ui import Button, View, Select, Modal, TextInput
import os
import asyncpg
from datetime import datetime
from aiohttp import web
import logging
import random
import string
from typing import Optional, Dict, List
import asyncio
from collections import defaultdict
import json
import io
import aiohttp

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('TicketBot')

class RateLimiter:
    def __init__(self):
        self.cooldowns = defaultdict(float)
    def check_cooldown(self, user_id: int, command: str, cooldown: int = 3) -> bool:
        key = f"{user_id}:{command}"
        now = datetime.utcnow().timestamp()
        if key in self.cooldowns:
            if now - self.cooldowns[key] < cooldown:
                return False
        self.cooldowns[key] = now
        return True
    async def cleanup_old_entries(self):
        while True:
            await asyncio.sleep(300)
            now = datetime.utcnow().timestamp()
            self.cooldowns = {k: v for k, v in self.cooldowns.items() if now - v < 3600}

rate_limiter = RateLimiter()

class RobloxAPI:
    @staticmethod
    async def get_user_id(username: str) -> Optional[int]:
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post('https://users.roblox.com/v1/usernames/users', json={'usernames': [username], 'excludeBannedUsers': True}) as resp:
                    data = await resp.json()
                    if data.get('data'): return data['data'][0]['id']
            except: pass
        return None
    
    @staticmethod
    async def get_private_servers(user_id: int) -> Dict[str, str]:
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(f'https://games.roblox.com/v1/users/{user_id}/games') as resp:
                    data = await resp.json()
                    ps_links = {}
                    if data.get('data'):
                        for game in data['data']:
                            game_id, game_name = game.get('id'), game.get('name')
                            try:
                                async with session.get(f'https://games.roblox.com/v1/games/{game_id}/private-servers') as ps_resp:
                                    ps_data = await ps_resp.json()
                                    if ps_data.get('data'):
                                        for server in ps_data['data']:
                                            if server.get('vipServerId'):
                                                link = f"https://www.roblox.com/games/{game['rootPlaceId']}?privateServerLinkCode={server['vipServerId']}"
                                                ps_links[game_name.lower().replace(' ', '')] = {'name': game_name, 'link': link}
                                                break
                            except: continue
                    return ps_links
            except: pass
        return {}


# ========================================
# PART 2/10 - DATABASE CLASS
# ========================================

class Database:
    def __init__(self):
        self.pool = None
    async def connect(self):
        database_url = os.getenv('DATABASE_URL')
        if not database_url: raise Exception("DATABASE_URL not set")
        if database_url.startswith('postgres://'): database_url = database_url.replace('postgres://', 'postgresql://', 1)
        self.pool = await asyncpg.create_pool(database_url, min_size=1, max_size=10)
        await self.create_tables()
    async def create_tables(self):
        async with self.pool.acquire() as conn:
            await conn.execute('CREATE TABLE IF NOT EXISTS config (guild_id BIGINT PRIMARY KEY, ticket_category_id BIGINT, log_channel_id BIGINT, proof_channel_id BIGINT, panel_message_id BIGINT, updated_at TIMESTAMP DEFAULT NOW())')
            await conn.execute('CREATE TABLE IF NOT EXISTS ticket_roles (guild_id BIGINT, tier TEXT, role_id BIGINT, PRIMARY KEY (guild_id, tier))')
            await conn.execute('CREATE TABLE IF NOT EXISTS tier_colors (guild_id BIGINT, tier TEXT, color INT, PRIMARY KEY (guild_id, tier))')
            await conn.execute('CREATE TABLE IF NOT EXISTS tickets (ticket_id TEXT PRIMARY KEY, guild_id BIGINT, channel_id BIGINT, user_id BIGINT, ticket_type TEXT, tier TEXT, claimed_by BIGINT, status TEXT DEFAULT \'open\', trade_details JSONB, created_at TIMESTAMP DEFAULT NOW(), closed_at TIMESTAMP)')
            await conn.execute('CREATE TABLE IF NOT EXISTS stats (user_id BIGINT PRIMARY KEY, tickets_claimed INT DEFAULT 0, tickets_closed INT DEFAULT 0)')
            await conn.execute('CREATE TABLE IF NOT EXISTS ratings (id SERIAL PRIMARY KEY, rated_user_id BIGINT, rater_user_id BIGINT, stars INT, created_at TIMESTAMP DEFAULT NOW(), UNIQUE(rated_user_id, rater_user_id))')
            await conn.execute('CREATE TABLE IF NOT EXISTS blacklist (user_id BIGINT PRIMARY KEY, guild_id BIGINT, reason TEXT, blacklisted_by BIGINT, blacklisted_at TIMESTAMP DEFAULT NOW())')
            await conn.execute('CREATE TABLE IF NOT EXISTS ps_links (user_id BIGINT, game_key TEXT, game_name TEXT, link TEXT, roblox_username TEXT, last_updated TIMESTAMP DEFAULT NOW(), PRIMARY KEY (user_id, game_key))')
    async def get_config(self, guild_id: int) -> Optional[Dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM config WHERE guild_id = $1', guild_id)
            return dict(row) if row else None
    async def set_config(self, guild_id: int, **kwargs):
        async with self.pool.acquire() as conn:
            await conn.execute('INSERT INTO config (guild_id, ticket_category_id, log_channel_id, proof_channel_id, panel_message_id, updated_at) VALUES ($1, $2, $3, $4, $5, NOW()) ON CONFLICT (guild_id) DO UPDATE SET ticket_category_id = COALESCE($2, config.ticket_category_id), log_channel_id = COALESCE($3, config.log_channel_id), proof_channel_id = COALESCE($4, config.proof_channel_id), panel_message_id = COALESCE($5, config.panel_message_id), updated_at = NOW()', guild_id, kwargs.get('ticket_category_id'), kwargs.get('log_channel_id'), kwargs.get('proof_channel_id'), kwargs.get('panel_message_id'))
    async def set_ticket_role(self, guild_id: int, tier: str, role_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('INSERT INTO ticket_roles (guild_id, tier, role_id) VALUES ($1, $2, $3) ON CONFLICT (guild_id, tier) DO UPDATE SET role_id = $3', guild_id, tier, role_id)
    async def get_ticket_role(self, guild_id: int, tier: str) -> Optional[int]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT role_id FROM ticket_roles WHERE guild_id = $1 AND tier = $2', guild_id, tier)
            return row['role_id'] if row else None
    async def get_all_ticket_roles(self, guild_id: int) -> List[Dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('SELECT tier, role_id FROM ticket_roles WHERE guild_id = $1', guild_id)
            return [dict(row) for row in rows]
    async def set_tier_color(self, guild_id: int, tier: str, color: int):
        async with self.pool.acquire() as conn:
            await conn.execute('INSERT INTO tier_colors (guild_id, tier, color) VALUES ($1, $2, $3) ON CONFLICT (guild_id, tier) DO UPDATE SET color = $3', guild_id, tier, color)
    async def get_tier_color(self, guild_id: int, tier: str) -> int:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT color FROM tier_colors WHERE guild_id = $1 AND tier = $2', guild_id, tier)
            if row: return row['color']
            defaults = {'lowtier': 0x57F287, 'midtier': 0xFEE75C, 'hightier': 0xED4245, 'support': 0x5865F2}
            return defaults.get(tier, 0x5865F2)
    async def create_ticket(self, ticket_id: str, guild_id: int, channel_id: int, user_id: int, ticket_type: str, tier: str = None, trade_details: Dict = None):
        async with self.pool.acquire() as conn:
            trade_details_json = json.dumps(trade_details) if trade_details else None
            await conn.execute('INSERT INTO tickets (ticket_id, guild_id, channel_id, user_id, ticket_type, tier, trade_details) VALUES ($1, $2, $3, $4, $5, $6, $7)', ticket_id, guild_id, channel_id, user_id, ticket_type, tier, trade_details_json)
    async def claim_ticket(self, ticket_id: str, user_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('UPDATE tickets SET claimed_by = $2, status = \'claimed\' WHERE ticket_id = $1', ticket_id, user_id)
            await conn.execute('INSERT INTO stats (user_id, tickets_claimed) VALUES ($1, 1) ON CONFLICT (user_id) DO UPDATE SET tickets_claimed = stats.tickets_claimed + 1', user_id)
    async def unclaim_ticket(self, ticket_id: str):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT claimed_by FROM tickets WHERE ticket_id = $1', ticket_id)
            if row and row['claimed_by']:
                await conn.execute('UPDATE stats SET tickets_claimed = tickets_claimed - 1 WHERE user_id = $1 AND tickets_claimed > 0', row['claimed_by'])
            await conn.execute('UPDATE tickets SET claimed_by = NULL, status = \'open\' WHERE ticket_id = $1', ticket_id)
    async def close_ticket(self, ticket_id: str):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT claimed_by FROM tickets WHERE ticket_id = $1', ticket_id)
            await conn.execute('UPDATE tickets SET status = \'closed\', closed_at = NOW() WHERE ticket_id = $1', ticket_id)
            if row and row['claimed_by']:
                await conn.execute('UPDATE stats SET tickets_closed = tickets_closed + 1 WHERE user_id = $1', row['claimed_by'])
    async def get_user_stats(self, user_id: int) -> Dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM stats WHERE user_id = $1', user_id)
            if row: return dict(row)
            return {'tickets_claimed': 0, 'tickets_closed': 0}
    async def add_rating(self, rated_user_id: int, rater_user_id: int, stars: int):
        async with self.pool.acquire() as conn:
            await conn.execute('INSERT INTO ratings (rated_user_id, rater_user_id, stars) VALUES ($1, $2, $3) ON CONFLICT (rated_user_id, rater_user_id) DO UPDATE SET stars = $3, created_at = NOW()', rated_user_id, rater_user_id, stars)
    async def get_user_ratings(self, user_id: int) -> List[Dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('SELECT rater_user_id, stars, created_at FROM ratings WHERE rated_user_id = $1 ORDER BY created_at DESC', user_id)
            return [dict(row) for row in rows]
    async def blacklist_user(self, user_id: int, guild_id: int, reason: str, blacklisted_by: int):
        async with self.pool.acquire() as conn:
            await conn.execute('INSERT INTO blacklist (user_id, guild_id, reason, blacklisted_by) VALUES ($1, $2, $3, $4) ON CONFLICT (user_id) DO UPDATE SET reason = $3, blacklisted_by = $4, blacklisted_at = NOW()', user_id, guild_id, reason, blacklisted_by)
    async def unblacklist_user(self, user_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('DELETE FROM blacklist WHERE user_id = $1', user_id)
    async def is_blacklisted(self, user_id: int, guild_id: int) -> Optional[Dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM blacklist WHERE user_id = $1 AND guild_id = $2', user_id, guild_id)
            return dict(row) if row else None
    async def save_ps_links(self, user_id: int, roblox_username: str, ps_links: Dict[str, Dict]):
        async with self.pool.acquire() as conn:
            await conn.execute('DELETE FROM ps_links WHERE user_id = $1', user_id)
            for game_key, data in ps_links.items():
                await conn.execute('INSERT INTO ps_links (user_id, game_key, game_name, link, roblox_username, last_updated) VALUES ($1, $2, $3, $4, $5, NOW())', user_id, game_key, data['name'], data['link'], roblox_username)
    async def get_ps_links(self, user_id: int) -> List[Dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('SELECT * FROM ps_links WHERE user_id = $1', user_id)
            return [dict(row) for row in rows]
    async def get_ps_link(self, user_id: int, game_key: str) -> Optional[str]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT link FROM ps_links WHERE user_id = $1 AND game_key = $2', user_id, game_key)
            return row['link'] if row else None
    async def remove_ps_link(self, user_id: int, game_key: str):
        async with self.pool.acquire() as conn:
            await conn.execute('DELETE FROM ps_links WHERE user_id = $1 AND game_key = $2', user_id, game_key)
    async def close(self):
        if self.pool: await self.pool.close()

db = Database()

# ========================================
# PART 3/10 - TICKET UTILITY FUNCTIONS
# ========================================

async def generate_ticket_id():
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))

async def create_ticket(guild, user, ticket_type, tier=None, trade_details=None):
    blacklist_data = await db.is_blacklisted(user.id, guild.id)
    if blacklist_data: raise Exception(f"Blacklisted: {blacklist_data['reason']}")
    config = await db.get_config(guild.id)
    if not config or not config.get('ticket_category_id'): raise Exception('Category not configured')
    category = guild.get_channel(config['ticket_category_id'])
    if not category: raise Exception('Category not found')
    ticket_id = await generate_ticket_id()
    channel_name = f'ticket-mm-{user.name}-{ticket_id}' if ticket_type == 'middleman' else f'ticket-{ticket_type}-{user.name}-{ticket_id}'
    overwrites = {guild.default_role: discord.PermissionOverwrite(read_messages=False), user: discord.PermissionOverwrite(read_messages=True, send_messages=True), guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True, manage_channels=True)}
    if tier:
        role_id = await db.get_ticket_role(guild.id, tier)
        if role_id:
            role = guild.get_role(role_id)
            if role: overwrites[role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
    channel = await category.create_text_channel(name=channel_name, overwrites=overwrites)
    await db.create_ticket(ticket_id, guild.id, channel.id, user.id, ticket_type, tier, trade_details)
    color = await db.get_tier_color(guild.id, tier if tier else ticket_type)
    embed = discord.Embed(title=f'{"Middleman" if ticket_type == "middleman" else "Support"} Ticket', description=f'{user.mention}\n\n{"A middleman will assist you shortly" if ticket_type == "middleman" else "Our team will help you shortly"}', color=color)
    if ticket_type == 'middleman' and trade_details:
        embed.add_field(name='Trading With', value=trade_details.get('trader', 'N/A'), inline=False)
        embed.add_field(name='You Give', value=trade_details.get('giving', 'N/A'), inline=True)
        embed.add_field(name='You Receive', value=trade_details.get('receiving', 'N/A'), inline=True)
        if trade_details.get('tip') and trade_details['tip'].lower() != 'none':
            embed.add_field(name='Tip', value=trade_details['tip'], inline=False)
    embed.set_footer(text=f'Ticket ID: {ticket_id}')
    view = TicketControlView()
    ping_msg = user.mention
    if tier:
        role_id = await db.get_ticket_role(guild.id, tier)
        if role_id:
            tier_role = guild.get_role(role_id)
            if tier_role: ping_msg += f" {tier_role.mention}"
    await channel.send(content=ping_msg, embed=embed, view=view)
    return channel

async def close_ticket(channel, closed_by):
    if not channel.name.startswith('ticket-'): raise Exception('Not a ticket channel')
    tickets = await db.pool.fetch('SELECT * FROM tickets WHERE channel_id = $1 AND status != $2', channel.id, 'closed')
    if not tickets: raise Exception('Ticket not found')
    ticket = dict(tickets[0])
    await db.close_ticket(ticket['ticket_id'])
    embed = discord.Embed(title='Closing Ticket', description=f'Closed by {closed_by.mention}\n\nDeleting in 5 seconds', color=0xED4245)
    await channel.send(embed=embed)
    config = await db.get_config(channel.guild.id)
    if config and config.get('log_channel_id'):
        log_channel = channel.guild.get_channel(config['log_channel_id'])
        if log_channel:
            opener = channel.guild.get_member(ticket['user_id'])
            claimer = channel.guild.get_member(ticket['claimed_by']) if ticket.get('claimed_by') else None
            transcript = f"TICKET TRANSCRIPT\n{'='*60}\nTicket ID: {ticket['ticket_id']}\nType: {ticket['ticket_type'].title()}\nOpened by: {opener.name if opener else 'Unknown'}\nClaimed by: {claimer.name if claimer else 'Unclaimed'}\nClosed by: {closed_by.name}\nCreated: {ticket['created_at'].strftime('%Y-%m-%d %H:%M:%S')}\n{'='*60}\n\n"
            messages = []
            async for msg in channel.history(limit=100, oldest_first=True):
                timestamp = msg.created_at.strftime('%H:%M:%S')
                content = msg.content if msg.content else '[No content]'
                messages.append(f"[{timestamp}] {msg.author.name}: {content}")
            transcript += '\n'.join(messages)
            transcript_file = discord.File(fp=io.BytesIO(transcript.encode('utf-8')), filename=f"transcript-{ticket['ticket_id']}.txt")
            log_embed = discord.Embed(title='Ticket Closed', color=0xED4245)
            log_embed.add_field(name='ID', value=ticket['ticket_id'], inline=True)
            log_embed.add_field(name='Type', value=ticket['ticket_type'].title(), inline=True)
            log_embed.add_field(name='Opened', value=opener.mention if opener else 'Unknown', inline=True)
            log_embed.add_field(name='Claimed', value=claimer.mention if claimer else 'Unclaimed', inline=True)
            log_embed.add_field(name='Closed', value=closed_by.mention, inline=True)
            await log_channel.send(embed=log_embed, file=transcript_file)
    await asyncio.sleep(5)
    await channel.delete()

async def claim_ticket(channel, claimer):
    tickets = await db.pool.fetch('SELECT * FROM tickets WHERE channel_id = $1', channel.id)
    if not tickets: raise Exception('Ticket not found')
    ticket = dict(tickets[0])
    if ticket.get('claimed_by'): raise Exception('Already claimed')
    await db.claim_ticket(ticket['ticket_id'], claimer.id)
    embed = discord.Embed(title='Claimed', description=f'By {claimer.mention}', color=0x57F287)
    await channel.send(embed=embed)

async def unclaim_ticket(channel):
    tickets = await db.pool.fetch('SELECT * FROM tickets WHERE channel_id = $1', channel.id)
    if not tickets: raise Exception('Ticket not found')
    ticket = dict(tickets[0])
    if not ticket.get('claimed_by'): raise Exception('Not claimed')
    await db.unclaim_ticket(ticket['ticket_id'])
    embed = discord.Embed(title='Unclaimed', description='Ticket available', color=0x5865F2)
    await channel.send(embed=embed)

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
    logger.info(f'Server started on {port}')



# ========================================
# PART 4/10 - BOT SETUP AND MODALS
# ========================================

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.guilds = True
bot = commands.Bot(command_prefix='$', intents=intents, help_command=None)

class RatingModal(Modal, title='Rate User'):
    def __init__(self, rated_user):
        super().__init__()
        self.rated_user = rated_user
        self.stars = TextInput(label='Rating (1-5 stars)', placeholder='1, 2, 3, 4, or 5', required=True, max_length=1)
        self.add_item(self.stars)
    async def on_submit(self, interaction):
        try:
            rating = int(self.stars.value)
            if rating < 1 or rating > 5: return await interaction.response.send_message('Must be 1-5', ephemeral=True)
            if interaction.user.id == self.rated_user.id: return await interaction.response.send_message('Cannot rate yourself', ephemeral=True)
            await db.add_rating(self.rated_user.id, interaction.user.id, rating)
            stars = '⭐' * rating
            embed = discord.Embed(title='Rating Submitted', description=f'Rated {self.rated_user.mention}\n\n{stars}', color=0x57F287)
            await interaction.response.send_message(embed=embed, ephemeral=True)
        except: await interaction.response.send_message('Invalid number', ephemeral=True)

class MiddlemanModal(Modal, title='Middleman Request'):
    def __init__(self, tier, interaction_msg):
        super().__init__()
        self.tier, self.interaction_msg = tier, interaction_msg
        self.trader = TextInput(label='Trading with', placeholder='@username or ID', required=True, max_length=100)
        self.giving = TextInput(label='You give', placeholder='e.g., 1 garam', style=discord.TextStyle.paragraph, required=True, max_length=500)
        self.receiving = TextInput(label='You receive', placeholder='e.g., 296 Robux', style=discord.TextStyle.paragraph, required=True, max_length=500)
        self.tip = TextInput(label='Tip (optional)', placeholder='Optional', required=False, max_length=200)
        self.add_item(self.trader)
        self.add_item(self.giving)
        self.add_item(self.receiving)
        self.add_item(self.tip)
    async def on_submit(self, interaction):
        if not rate_limiter.check_cooldown(interaction.user.id, 'ticket_create', 10):
            return await interaction.response.send_message('Wait 10 seconds', ephemeral=True)
        await interaction.response.defer(ephemeral=True)
        try:
            channel = await create_ticket(interaction.guild, interaction.user, 'middleman', self.tier, {'trader': self.trader.value, 'giving': self.giving.value, 'receiving': self.receiving.value, 'tip': self.tip.value or 'None'})
            embed = discord.Embed(title='Ticket Created', description=f'{channel.mention}', color=0x57F287)
            await self.interaction_msg.edit(embed=embed, view=None)
        except Exception as e:
            await interaction.followup.send(f'{str(e)}', ephemeral=True)



# ========================================
# PART 5/10 - VIEWS AND DROPDOWNS
# ========================================

class MiddlemanTierSelect(Select):
    def __init__(self, interaction_msg):
        self.interaction_msg = interaction_msg
        super().__init__(placeholder='Select trade value', custom_id='mm_tier_select', options=[
            discord.SelectOption(label='Low Value', value='lowtier', emoji='🟢', description='- Only for low valued stuff'),
            discord.SelectOption(label='Mid Value', value='midtier', emoji='🟡', description='- Only for mid valued stuff'),
            discord.SelectOption(label='High Value', value='hightier', emoji='🔴', description='- Only for high valued stuff')
        ])
    async def callback(self, interaction):
        modal = MiddlemanModal(self.values[0], self.interaction_msg)
        await interaction.response.send_modal(modal)

class TradeConfirmView(View):
    def __init__(self):
        super().__init__(timeout=None)
        self.confirmed = set()
    @discord.ui.button(label='Confirm Trade', style=discord.ButtonStyle.success, emoji='✅', custom_id='confirm_trade')
    async def confirm_button(self, interaction, button):
        self.confirmed.add(interaction.user.id)
        if len(self.confirmed) >= 2:
            embed = discord.Embed(title='Trade Confirmed', description='Both parties confirmed\n\nReady to close', color=0x57F287)
            await interaction.response.edit_message(embed=embed, view=None)
        else:
            await interaction.response.send_message(f'✅ Confirmed ({len(self.confirmed)}/2)', ephemeral=True)

class TicketPanelView(View):
    def __init__(self):
        super().__init__(timeout=None)
    @discord.ui.button(label='Support', style=discord.ButtonStyle.primary, emoji='🎫', custom_id='support_btn')
    async def support_button(self, interaction, button):
        if not rate_limiter.check_cooldown(interaction.user.id, 'ticket_create', 10):
            return await interaction.response.send_message('Wait 10 seconds', ephemeral=True)
        await interaction.response.defer(ephemeral=True)
        try:
            channel = await create_ticket(interaction.guild, interaction.user, 'support')
            embed = discord.Embed(title='Ticket Created', description=f'{channel.mention}', color=0x57F287)
            await interaction.followup.send(embed=embed, ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f'{str(e)}', ephemeral=True)
    @discord.ui.button(label='Middleman', style=discord.ButtonStyle.success, emoji='⚖️', custom_id='middleman_btn')
    async def middleman_button(self, interaction, button):
        embed = discord.Embed(title='Select Trade Value', description='Choose tier', color=0xFEE75C)
        view = View(timeout=300)
        select = MiddlemanTierSelect(None)
        view.add_item(select)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        msg = await interaction.original_response()
        select.interaction_msg = msg

class TicketControlView(View):
    def __init__(self):
        super().__init__(timeout=None)
    @discord.ui.button(label='Claim', style=discord.ButtonStyle.green, custom_id='claim_ticket', emoji='✋')
    async def claim_button(self, interaction, button):
        if not rate_limiter.check_cooldown(interaction.user.id, 'claim', 2): return await interaction.response.send_message('Wait', ephemeral=True)
        try:
            await claim_ticket(interaction.channel, interaction.user)
            await interaction.response.send_message('Claimed', ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f'{str(e)}', ephemeral=True)
    @discord.ui.button(label='Unclaim', style=discord.ButtonStyle.gray, custom_id='unclaim_ticket', emoji='↩️')
    async def unclaim_button(self, interaction, button):
        if not rate_limiter.check_cooldown(interaction.user.id, 'unclaim', 2): return await interaction.response.send_message('Wait', ephemeral=True)
        tickets = await db.pool.fetch('SELECT * FROM tickets WHERE channel_id = $1', interaction.channel.id)
        if not tickets: return await interaction.response.send_message('Not a ticket', ephemeral=True)
        ticket = dict(tickets[0])
        if not ticket.get('claimed_by'): return await interaction.response.send_message('Not claimed', ephemeral=True)
        if ticket['claimed_by'] != interaction.user.id: return await interaction.response.send_message('Only claimer can unclaim', ephemeral=True)
        try:
            await unclaim_ticket(interaction.channel)
            await interaction.response.send_message('Unclaimed', ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f'{str(e)}', ephemeral=True)
    @discord.ui.button(label='Close', style=discord.ButtonStyle.red, custom_id='close_ticket', emoji='🔒')
    async def close_button(self, interaction, button):
        if not rate_limiter.check_cooldown(interaction.user.id, 'close', 3): return await interaction.response.send_message('Wait', ephemeral=True)
        tickets = await db.pool.fetch('SELECT * FROM tickets WHERE channel_id = $1', interaction.channel.id)
        if not tickets: return await interaction.response.send_message('Not a ticket', ephemeral=True)
        ticket = dict(tickets[0])
        if ticket.get('claimed_by') and ticket['claimed_by'] != interaction.user.id: return await interaction.response.send_message('Only claimer can close', ephemeral=True)
        await interaction.response.send_message('Closing...', ephemeral=True)
        try:
            await close_ticket(interaction.channel, interaction.user)
        except Exception as e:
            await interaction.followup.send(f'{str(e)}', ephemeral=True)



# ========================================
# PART 6/10 - TICKET COMMANDS
# ========================================

@bot.command(name='close')
async def close_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'): return await ctx.reply('Not a ticket channel')
    if not rate_limiter.check_cooldown(ctx.author.id, 'close_cmd', 3): return await ctx.reply('Wait 3 seconds')
    tickets = await db.pool.fetch('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
    if not tickets: return await ctx.reply('Ticket not found')
    ticket = dict(tickets[0])
    if ticket.get('claimed_by') and ticket['claimed_by'] != ctx.author.id: return await ctx.reply('Only claimer can close')
    try:
        await close_ticket(ctx.channel, ctx.author)
    except Exception as e:
        await ctx.reply(f'{str(e)}')

@bot.command(name='claim')
async def claim_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'): return await ctx.reply('Not a ticket channel')
    if not rate_limiter.check_cooldown(ctx.author.id, 'claim_cmd', 2): return await ctx.reply('Wait')
    try:
        await claim_ticket(ctx.channel, ctx.author)
    except Exception as e:
        await ctx.reply(f'{str(e)}')

@bot.command(name='unclaim')
async def unclaim_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'): return await ctx.reply('Not a ticket channel')
    if not rate_limiter.check_cooldown(ctx.author.id, 'unclaim_cmd', 2): return await ctx.reply('Wait')
    tickets = await db.pool.fetch('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
    if not tickets: return await ctx.reply('Not found')
    ticket = dict(tickets[0])
    if ticket['claimed_by'] != ctx.author.id: return await ctx.reply('Only claimer can unclaim')
    try:
        await unclaim_ticket(ctx.channel)
    except Exception as e:
        await ctx.reply(f'{str(e)}')

@bot.command(name='add')
async def add_user(ctx, member: discord.Member):
    if not ctx.channel.name.startswith('ticket-'): return await ctx.reply('Not a ticket channel')
    tickets = await db.pool.fetch('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
    if not tickets: return await ctx.reply('Ticket not found')
    ticket = dict(tickets[0])
    if ticket.get('claimed_by') and ticket['claimed_by'] != ctx.author.id and not ctx.author.guild_permissions.administrator:
        return await ctx.reply('Only claimer can add users')
    await ctx.channel.set_permissions(member, read_messages=True, send_messages=True)
    embed = discord.Embed(title='User Added', description=f'{member.mention} added', color=0x57F287)
    await ctx.reply(embed=embed)

@bot.command(name='remove')
async def remove_user(ctx, member: discord.Member):
    if not ctx.channel.name.startswith('ticket-'): return await ctx.reply('Not a ticket channel')
    tickets = await db.pool.fetch('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
    if not tickets: return await ctx.reply('Ticket not found')
    ticket = dict(tickets[0])
    if ticket.get('claimed_by') and ticket['claimed_by'] != ctx.author.id and not ctx.author.guild_permissions.administrator:
        return await ctx.reply('Only claimer can remove users')
    await ctx.channel.set_permissions(member, overwrite=None)
    embed = discord.Embed(title='User Removed', description=f'{member.mention} removed', color=0xED4245)
    await ctx.reply(embed=embed)

@bot.command(name='rename')
async def rename_ticket(ctx, *, new_name: str):
    if not ctx.channel.name.startswith('ticket-'): return await ctx.reply('Not a ticket channel')
    tickets = await db.pool.fetch('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
    if not tickets: return await ctx.reply('Ticket not found')
    ticket = dict(tickets[0])
    if ticket.get('claimed_by') and ticket['claimed_by'] != ctx.author.id and not ctx.author.guild_permissions.administrator:
        return await ctx.reply('Only claimer can rename')
    await ctx.channel.edit(name=f"ticket-{new_name}")
    embed = discord.Embed(title='Renamed', description=f'Now: `ticket-{new_name}`', color=0x5865F2)
    await ctx.reply(embed=embed)

@bot.command(name='confirm')
async def confirm_trade(ctx):
    if not ctx.channel.name.startswith('ticket-'): return await ctx.reply('Not a ticket channel')
    embed = discord.Embed(title='Confirm Trade', description='Both parties click below', color=0xFEE75C)
    view = TradeConfirmView()
    await ctx.send(embed=embed, view=view)

@bot.command(name='proof')
async def proof_command(ctx):
    if not ctx.channel.name.startswith('ticket-'): return await ctx.reply('Only in tickets')
    tickets = await db.pool.fetch('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
    if not tickets: return await ctx.reply('Ticket not found')
    ticket = dict(tickets[0])
    config = await db.get_config(ctx.guild.id)
    if not config or not config.get('proof_channel_id'): return await ctx.reply('Proof channel not configured')
    proof_channel = ctx.guild.get_channel(config['proof_channel_id'])
    if not proof_channel: return await ctx.reply('Proof channel not found')
    opener = ctx.guild.get_member(ticket['user_id'])
    embed = discord.Embed(title='Trade Completed', color=0x57F287)
    embed.add_field(name='Middleman', value=ctx.author.mention, inline=False)
    embed.add_field(name='Type', value='MM', inline=False)
    if ticket.get('tier'):
        tier_names = {'lowtier': 'Low Value', 'midtier': 'Mid Value', 'hightier': 'High Value'}
        embed.add_field(name='Tier', value=tier_names.get(ticket['tier'], ticket['tier']), inline=False)
    embed.add_field(name='Requester', value=opener.mention if opener else 'Unknown', inline=False)
    if ticket.get('trade_details'):
        try:
            details = json.loads(ticket['trade_details']) if isinstance(ticket['trade_details'], str) else ticket['trade_details']
            embed.add_field(name='Trader', value=details.get('trader', 'Unknown'), inline=False)
            embed.add_field(name='Giving', value=details.get('giving', 'N/A'), inline=True)
            embed.add_field(name='Receiving', value=details.get('receiving', 'N/A'), inline=True)
            if details.get('tip') and details['tip'].lower() != 'none':
                embed.add_field(name='Tip', value=details['tip'], inline=False)
        except: pass
    embed.set_footer(text=f'ID: {ticket["ticket_id"]}')
    await proof_channel.send(embed=embed)
    await ctx.reply('Proof sent')




# ========================================
# PART 7/10 - RATING AND PS COMMANDS
# ========================================

@bot.command(name='rateme')
async def rate_user(ctx, member: discord.Member):
    modal = RatingModal(member)
    view = View(timeout=60)
    button = Button(label=f'Rate {member.display_name}', style=discord.ButtonStyle.primary, emoji='⭐')
    async def button_callback(interaction):
        if interaction.user.id != ctx.author.id: return await interaction.response.send_message('Not your button', ephemeral=True)
        await interaction.response.send_modal(modal)
    button.callback = button_callback
    view.add_item(button)
    embed = discord.Embed(title='Rate User', description=f'Click to rate {member.mention}', color=0xFEE75C)
    await ctx.reply(embed=embed, view=view)

@bot.command(name='stats')
async def stats(ctx, member: discord.Member = None):
    target = member or ctx.author
    user_stats = await db.get_user_stats(target.id)
    ratings = await db.get_user_ratings(target.id)
    embed = discord.Embed(title=f'{target.display_name}\'s Stats', color=0x5865F2)
    embed.add_field(name='Tickets', value=f'Claimed: {user_stats["tickets_claimed"]}\nClosed: {user_stats["tickets_closed"]}', inline=False)
    if ratings:
        rating_text = ""
        for rating in ratings:
            rater = ctx.guild.get_member(rating['rater_user_id'])
            rater_name = rater.display_name if rater else 'Unknown'
            stars = '⭐' * rating['stars']
            rating_text += f"{rater_name} rated {stars}\n"
        embed.add_field(name=f'Ratings ({len(ratings)})', value=rating_text, inline=False)
    else:
        embed.add_field(name='Ratings', value='No ratings yet', inline=False)
    await ctx.reply(embed=embed)

@bot.command(name='setps')
async def set_ps(ctx, roblox_username: str):
    msg = await ctx.reply('Scanning Roblox account...')
    user_id = await RobloxAPI.get_user_id(roblox_username)
    if not user_id: return await msg.edit(content='Roblox user not found')
    ps_links = await RobloxAPI.get_private_servers(user_id)
    if not ps_links: return await msg.edit(content='No private servers found')
    await db.save_ps_links(ctx.author.id, roblox_username, ps_links)
    games_list = '\n'.join([f'• {data["name"]}' for data in ps_links.values()])
    embed = discord.Embed(title='Private Servers Saved', description=f'Found {len(ps_links)} server(s)\n\n{games_list}', color=0x57F287)
    await msg.edit(content=None, embed=embed)

@bot.command(name='pslist')
async def ps_list(ctx):
    ps_links = await db.get_ps_links(ctx.author.id)
    if not ps_links: return await ctx.reply('No private servers saved. Use `$setps <username>`')
    games_list = '\n'.join([f'• {ps["game_name"]} (`{ps["game_key"]}`)' for ps in ps_links])
    embed = discord.Embed(title='Your Private Servers', description=games_list, color=0x5865F2)
    await ctx.reply(embed=embed)

@bot.command(name='ps')
async def send_ps(ctx, game_key: str = None):
    if not ctx.channel.name.startswith('ticket-'): return await ctx.reply('Only in ticket channels')
    ps_links = await db.get_ps_links(ctx.author.id)
    if not ps_links: return await ctx.reply('No servers saved', ephemeral=True)
    if not game_key:
        games = '\n'.join([f'• `{ps["game_key"]}` - {ps["game_name"]}' for ps in ps_links])
        return await ctx.reply(f'Specify game:\n{games}', ephemeral=True)
    link = await db.get_ps_link(ctx.author.id, game_key.lower())
    if not link: return await ctx.reply(f'No PS link for `{game_key}`', ephemeral=True)
    await ctx.send(f'🔗 Private Server:\n{link}')

@bot.command(name='removeps')
async def remove_ps(ctx, game_key: str):
    await db.remove_ps_link(ctx.author.id, game_key.lower())
    embed = discord.Embed(title='Removed', description=f'Removed `{game_key}`', color=0x57F287)
    await ctx.reply(embed=embed)

@bot.command(name='updateps')
async def update_ps(ctx):
    ps_links = await db.get_ps_links(ctx.author.id)
    if not ps_links: return await ctx.reply('No PS links. Use `$setps <username>` first')
    roblox_username = ps_links[0]['roblox_username']
    msg = await ctx.reply('Updating...')
    user_id = await RobloxAPI.get_user_id(roblox_username)
    if not user_id: return await msg.edit(content='Failed to fetch user')
    new_ps_links = await RobloxAPI.get_private_servers(user_id)
    if not new_ps_links: return await msg.edit(content='No servers found')
    await db.save_ps_links(ctx.author.id, roblox_username, new_ps_links)
    embed = discord.Embed(title='Updated', description=f'Updated {len(new_ps_links)} server(s)', color=0x57F287)
    await msg.edit(content=None, embed=embed)



# ========================================
# PART 8/10 - ADMIN SETUP COMMANDS
# ========================================

@bot.command(name='setup')
@commands.has_permissions(administrator=True)
async def setup_panel(ctx):
    embed = discord.Embed(title='Support Tickets', description='Select ticket type below', color=0x5865F2)
    embed.add_field(name='Options', value='🎫 Support\n⚖️ Middleman', inline=False)
    view = TicketPanelView()
    await ctx.send(embed=embed, view=view)
    await ctx.message.delete()

@bot.command(name='setcategory')
@commands.has_permissions(administrator=True)
async def set_category(ctx, category: discord.CategoryChannel):
    await db.set_config(ctx.guild.id, ticket_category_id=category.id)
    embed = discord.Embed(title='Category Set', description=f'Set to {category.mention}', color=0x57F287)
    await ctx.reply(embed=embed)

@bot.command(name='setlogs')
@commands.has_permissions(administrator=True)
async def set_logs(ctx, channel: discord.TextChannel):
    await db.set_config(ctx.guild.id, log_channel_id=channel.id)
    embed = discord.Embed(title='Log Channel Set', description=f'Set to {channel.mention}', color=0x57F287)
    await ctx.reply(embed=embed)

@bot.command(name='setproof')
@commands.has_permissions(administrator=True)
async def set_proof(ctx, channel: discord.TextChannel):
    await db.set_config(ctx.guild.id, proof_channel_id=channel.id)
    embed = discord.Embed(title='Proof Channel Set', description=f'Set to {channel.mention}', color=0x57F287)
    await ctx.reply(embed=embed)

# REPLACE THE TICKETROLE COMMAND IN YOUR bot.py WITH THIS:

@bot.command(name='ticketrole')
@commands.has_permissions(administrator=True)
async def ticket_role(ctx, tier: str, *, role_input: str):
    valid = ['lowtier', 'midtier', 'hightier', 'support']
    if tier.lower() not in valid: 
        return await ctx.reply(f'Invalid tier. Use: {", ".join(valid)}')
    
    # Try to convert role_input to a role
    role = None
    
    # Check if it's a role ID (just numbers)
    if role_input.isdigit():
        role = ctx.guild.get_role(int(role_input))
    else:
        # Try to find role by mention or name
        try:
            role = await commands.RoleConverter().convert(ctx, role_input)
        except:
            pass
    
    if not role:
        return await ctx.reply('Role not found')
    
    await db.set_ticket_role(ctx.guild.id, tier.lower(), role.id)
    tier_names = {'lowtier': 'Low Value', 'midtier': 'Mid Value', 'hightier': 'High Value', 'support': 'Support'}
    embed = discord.Embed(title='Role Set', description=f'{tier_names[tier.lower()]} → {role.mention}', color=0x57F287)
    await ctx.reply(embed=embed)
    

@bot.command(name='setcolor')
@commands.has_permissions(administrator=True)
async def set_color(ctx, tier: str, color_hex: str):
    valid = ['lowtier', 'midtier', 'hightier', 'support']
    if tier.lower() not in valid: return await ctx.reply(f'Invalid. Use: {", ".join(valid)}')
    try:
        color_hex = color_hex.replace('#', '')
        color_int = int(color_hex, 16)
    except:
        return await ctx.reply('Invalid hex. Use: #RRGGBB')
    await db.set_tier_color(ctx.guild.id, tier.lower(), color_int)
    tier_names = {'lowtier': 'Low Value', 'midtier': 'Mid Value', 'hightier': 'High Value', 'support': 'Support'}
    embed = discord.Embed(title='Color Set', description=f'{tier_names[tier.lower()]} color updated', color=color_int)
    await ctx.reply(embed=embed)

@bot.command(name='config')
@commands.has_permissions(administrator=True)
async def view_config(ctx):
    config = await db.get_config(ctx.guild.id)
    roles = await db.get_all_ticket_roles(ctx.guild.id)
    embed = discord.Embed(title='Server Config', color=0x5865F2)
    if config:
        category = ctx.guild.get_channel(config.get('ticket_category_id')) if config.get('ticket_category_id') else None
        log_channel = ctx.guild.get_channel(config.get('log_channel_id')) if config.get('log_channel_id') else None
        proof_channel = ctx.guild.get_channel(config.get('proof_channel_id')) if config.get('proof_channel_id') else None
        embed.add_field(name='Channels', value=f'Category: {category.mention if category else "Not set"}\nLogs: {log_channel.mention if log_channel else "Not set"}\nProof: {proof_channel.mention if proof_channel else "Not set"}', inline=False)
    if roles:
        role_text = ""
        tier_names = {'lowtier': 'Low Value', 'midtier': 'Mid Value', 'hightier': 'High Value', 'support': 'Support'}
        for role_data in roles:
            role = ctx.guild.get_role(role_data['role_id'])
            tier_name = tier_names.get(role_data['tier'], role_data['tier'])
            role_text += f"{tier_name}: {role.mention if role else 'Not found'}\n"
        embed.add_field(name='Roles', value=role_text, inline=False)
    await ctx.reply(embed=embed)





# ========================================
# PART 9/10 - MODERATION & UTILITY
# ========================================

@bot.command(name='blacklist')
@commands.has_permissions(administrator=True)
async def blacklist_user(ctx, member: discord.Member, *, reason: str = "No reason"):
    await db.blacklist_user(member.id, ctx.guild.id, reason, ctx.author.id)
    embed = discord.Embed(title='User Blacklisted', description=f'{member.mention} blocked', color=0xED4245)
    embed.add_field(name='Reason', value=reason, inline=False)
    await ctx.reply(embed=embed)

@bot.command(name='unblacklist')
@commands.has_permissions(administrator=True)
async def unblacklist_user(ctx, member: discord.Member):
    await db.unblacklist_user(member.id)
    embed = discord.Embed(title='User Unblacklisted', description=f'{member.mention} unblocked', color=0x57F287)
    await ctx.reply(embed=embed)

@bot.command(name='blacklists')
@commands.has_permissions(administrator=True)
async def view_blacklist(ctx):
    async with db.pool.acquire() as conn:
        rows = await conn.fetch('SELECT * FROM blacklist WHERE guild_id = $1', ctx.guild.id)
    if not rows: return await ctx.reply('No blacklisted users')
    embed = discord.Embed(title='Blacklisted Users', color=0xED4245)
    for row in rows:
        user = ctx.guild.get_member(row['user_id'])
        username = user.mention if user else f"ID: {row['user_id']}"
        embed.add_field(name=username, value=f"Reason: {row['reason']}", inline=False)
    await ctx.reply(embed=embed)

@bot.command(name='clear')
@commands.has_permissions(manage_messages=True)
async def clear_bot_messages(ctx):
    deleted = 0
    async for message in ctx.channel.history(limit=100):
        if message.author == bot.user:
            try:
                await message.delete()
                deleted += 1
                await asyncio.sleep(0.5)
            except: pass
    msg = await ctx.send(f'Deleted {deleted} messages')
    await asyncio.sleep(3)
    await msg.delete()
    try:
        await ctx.message.delete()
    except: pass

@bot.command(name='ping')
async def ping(ctx):
    latency = round(bot.latency * 1000)
    embed = discord.Embed(title='Pong', description=f'Latency: {latency}ms', color=0x57F287 if latency < 200 else 0xFEE75C if latency < 500 else 0xED4245)
    await ctx.reply(embed=embed)

@bot.command(name='help')
async def help_command(ctx):
    pages = []
    page1 = discord.Embed(title='Help - Tickets', description='Ticket commands', color=0x5865F2)
    page1.add_field(name='Commands', value='`$close` `$claim` `$unclaim`\n`$add @user` `$remove @user`\n`$rename <name>` `$confirm`\n`$proof` `$rateme @user`\n`$stats [@user]`', inline=False)
    page1.set_footer(text='Page 1/4')
    pages.append(page1)
    page2 = discord.Embed(title='Help - PS Links', description='Private server commands', color=0x5865F2)
    page2.add_field(name='Commands', value='`$setps <username>` - Scan account\n`$pslist` - View links\n`$ps <game>` - Send link\n`$updateps` - Update links\n`$removeps <game>` - Remove', inline=False)
    page2.set_footer(text='Page 2/4')
    pages.append(page2)
    if ctx.author.guild_permissions.administrator:
        page3 = discord.Embed(title='Help - Admin', description='Setup commands', color=0x5865F2)
        page3.add_field(name='Commands', value='`$setup` `$setcategory #cat`\n`$setlogs #ch` `$setproof #ch`\n`$ticketrole <tier> @role`\n`$setcolor <tier> <hex>`\n`$config`', inline=False)
        page3.set_footer(text='Page 3/4')
        pages.append(page3)
        page4 = discord.Embed(title='Help - Moderation', description='Mod commands', color=0x5865F2)
        page4.add_field(name='Commands', value='`$blacklist @user <reason>`\n`$unblacklist @user`\n`$blacklists` `$clear`', inline=False)
        page4.set_footer(text='Page 4/4')
        pages.append(page4)
    else:
        page1.set_footer(text='Page 1/2')
        page2.set_footer(text='Page 2/2')
    class HelpView(View):
        def __init__(self, pages):
            super().__init__(timeout=60)
            self.pages, self.current_page = pages, 0
        @discord.ui.button(emoji='◀️', style=discord.ButtonStyle.gray)
        async def prev_button(self, interaction, button):
            if interaction.user.id != ctx.author.id: return await interaction.response.send_message('Not your menu', ephemeral=True)
            self.current_page = (self.current_page - 1) % len(self.pages)
            await interaction.response.edit_message(embed=self.pages[self.current_page])
        @discord.ui.button(emoji='▶️', style=discord.ButtonStyle.gray)
        async def next_button(self, interaction, button):
            if interaction.user.id != ctx.author.id: return await interaction.response.send_message('Not your menu', ephemeral=True)
            self.current_page = (self.current_page + 1) % len(self.pages)
            await interaction.response.edit_message(embed=self.pages[self.current_page])
    view = HelpView(pages) if len(pages) > 1 else None
    await ctx.reply(embed=pages[0], view=view)


# ========================================
# PART 10/10 - EVENTS AND MAIN
# ========================================

@bot.event
async def on_ready():
    logger.info(f'Logged in as {bot.user}')
    try:
        await db.connect()
        logger.info('Database connected')
    except Exception as e:
        logger.error(f'Database error: {e}')
        return
    bot.add_view(TicketPanelView())
    bot.add_view(TicketControlView())
    bot.loop.create_task(rate_limiter.cleanup_old_entries())
    await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name='tickets'))
    logger.info('Bot ready')

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.reply('No permission')
    elif isinstance(error, commands.MemberNotFound):
        await ctx.reply('Member not found')
    elif isinstance(error, commands.ChannelNotFound):
        await ctx.reply('Channel not found')
    elif isinstance(error, commands.RoleNotFound):
        await ctx.reply('Role not found')
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.reply(f'Missing: {error.param.name}')
    else:
        logger.error(f'Error: {error}')

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
