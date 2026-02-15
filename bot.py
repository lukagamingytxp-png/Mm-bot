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
from typing import Optional, Dict
import asyncio
from collections import defaultdict
import json
import io

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('TicketBot')

HARDCODED_ROLES = {
    'lowtier': 1453757017218093239,
    'midtier': 1434610759140118640,
    'hightier': 1453757157144137911,
    'staff': 1432081794647199895
}

COLORS = {
    'lowtier': 0x57F287,
    'midtier': 0xFEE75C,
    'hightier': 0xED4245,
    'support': 0x5865F2,
    'success': 0x57F287,
    'error': 0xED4245
}

STATUSES = ["tickets 🎫", "for scammers 👀", "middleman requests ⚖️", "over trades 🔒", "the server 🛡️"]

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
            await conn.execute('CREATE TABLE IF NOT EXISTS config (guild_id BIGINT PRIMARY KEY, ticket_category_id BIGINT, log_channel_id BIGINT, proof_channel_id BIGINT)')
            await conn.execute('CREATE TABLE IF NOT EXISTS tickets (ticket_id TEXT PRIMARY KEY, guild_id BIGINT, channel_id BIGINT, user_id BIGINT, ticket_type TEXT, tier TEXT, claimed_by BIGINT, status TEXT DEFAULT \'open\', trade_details JSONB, created_at TIMESTAMP DEFAULT NOW())')
            await conn.execute('CREATE TABLE IF NOT EXISTS blacklist (user_id BIGINT PRIMARY KEY, guild_id BIGINT, reason TEXT, blacklisted_by BIGINT)')
            await conn.execute('CREATE TABLE IF NOT EXISTS ps_links (user_id BIGINT, game_key TEXT, game_name TEXT, link TEXT, PRIMARY KEY (user_id, game_key))')
    async def close(self):
        if self.pool:
            await self.pool.close()

db = Database()

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.guilds = True
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

async def rotate_status():
    await bot.wait_until_ready()
    while not bot.is_closed():
        status = random.choice(STATUSES)
        await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name=status))
        await asyncio.sleep(300)

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
            channel_name = f'ticket-{user.name}-{ticket_id}'
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
            tier_names = {'lowtier': 'Low Value', 'midtier': 'Mid Value', 'hightier': 'High Value'}
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
            discord.SelectOption(label='Low Value', value='lowtier', emoji='🟢'),
            discord.SelectOption(label='Mid Value', value='midtier', emoji='🟡'),
            discord.SelectOption(label='High Value', value='hightier', emoji='🔴')
        ]
        super().__init__(placeholder='Select trade value', options=options)
    async def callback(self, interaction):
        modal = MiddlemanModal(self.values[0])
        await interaction.response.send_modal(modal)

class TradeConfirmView(View):
    def __init__(self):
        super().__init__(timeout=None)
        self.confirmed = set()
    @discord.ui.button(label='Confirm', style=discord.ButtonStyle.success, emoji='✅', custom_id='confirm_trade')
    async def confirm_button(self, interaction, button):
        self.confirmed.add(interaction.user.id)
        if len(self.confirmed) >= 2:
            embed = discord.Embed(title='✅ Trade Confirmed', description='Both parties confirmed', color=COLORS['success'])
            await interaction.response.edit_message(embed=embed, view=None)
        else:
            await interaction.response.send_message(f'✅ Confirmed ({len(self.confirmed)}/2)', ephemeral=True)

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
            channel_name = f'ticket-{user.name}-{ticket_id}'
            overwrites = {
                guild.default_role: discord.PermissionOverwrite(read_messages=False),
                user: discord.PermissionOverwrite(read_messages=True, send_messages=True),
                guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True, manage_channels=True)
            }
            staff_role_id = HARDCODED_ROLES.get('staff')
            if staff_role_id:
                staff_role = guild.get_role(staff_role_id)
                if staff_role:
                    overwrites[staff_role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
            channel = await category.create_text_channel(name=channel_name, overwrites=overwrites)
            async with db.pool.acquire() as conn:
                await conn.execute('INSERT INTO tickets (ticket_id, guild_id, channel_id, user_id, ticket_type) VALUES ($1, $2, $3, $4, $5)', ticket_id, guild.id, channel.id, user.id, 'support')
            embed = discord.Embed(color=COLORS['support'])
            embed.set_author(name=user.display_name, icon_url=user.display_avatar.url)
            embed.title = '🎫 Support Ticket'
            embed.description = 'Our team will help you shortly.\n\n📋 **Guidelines:**\n• Be patient and respectful\n• Provide all info\n• Don\'t spam\n• Wait for staff'
            embed.set_footer(text=f'ID: {ticket_id}')
            view = TicketControlView()
            ping_msg = user.mention
            if staff_role_id:
                staff_role = guild.get_role(staff_role_id)
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
        embed = discord.Embed(title='⚖️ Select Trade Value', description='Choose tier', color=COLORS['support'])
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
            embed = discord.Embed(title='↩️ Unclaimed', description='Ticket available', color=COLORS['support'])
            await interaction.response.send_message(embed=embed)
        except Exception as e:
            logger.error(f'Unclaim error: {e}')
            await interaction.response.send_message(f'❌ Error: {str(e)}', ephemeral=True)

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
            if ticket['claimed_by'] != ctx.author.id:
                return await ctx.reply('❌ Only claimer can close')
            await conn.execute('UPDATE tickets SET status = $1 WHERE ticket_id = $2', 'closed', ticket['ticket_id'])
        embed = discord.Embed(title='🔒 Closing', description=f'Closed by {ctx.author.mention}', color=COLORS['error'])
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
            if not ticket:
                return await ctx.reply('❌ Not found')
            if ticket['claimed_by']:
                return await ctx.reply('❌ Already claimed')
            await conn.execute('UPDATE tickets SET claimed_by = $1, status = $2 WHERE ticket_id = $3', ctx.author.id, 'claimed', ticket['ticket_id'])
        embed = discord.Embed(title='✋ Claimed', description=f'By {ctx.author.mention}', color=COLORS['success'])
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
            if not ticket:
                return await ctx.reply('❌ Not found')
            if not ticket['claimed_by']:
                return await ctx.reply('❌ Not claimed')
            if ticket['claimed_by'] != ctx.author.id:
                return await ctx.reply('❌ Only claimer can unclaim')
            await conn.execute('UPDATE tickets SET claimed_by = NULL, status = $1 WHERE ticket_id = $2', 'open', ticket['ticket_id'])
        embed = discord.Embed(title='↩️ Unclaimed', description='Ticket available', color=COLORS['support'])
        await ctx.send(embed=embed)
    except Exception as e:
        await ctx.reply(f'❌ Error: {str(e)}')

@bot.command(name='add')
async def add_cmd(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply('❌ Missing user\n\nExample: `$add @John`')
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply('❌ Not a ticket')
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
            if not ticket:
                return await ctx.reply('❌ Not found')
            if not ticket['claimed_by']:
                return await ctx.reply('❌ Must be claimed first')
            if ticket['claimed_by'] != ctx.author.id and not ctx.author.guild_permissions.administrator:
                return await ctx.reply('❌ Only claimer can add')
        await ctx.channel.set_permissions(member, read_messages=True, send_messages=True)
        embed = discord.Embed(title='✅ Added', description=f'{member.mention} added', color=COLORS['success'])
        await ctx.reply(embed=embed)
    except Exception as e:
        await ctx.reply(f'❌ Error: {str(e)}')

@bot.command(name='remove')
async def remove_cmd(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply('❌ Missing user\n\nExample: `$remove @John`')
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply('❌ Not a ticket')
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
            if not ticket:
                return await ctx.reply('❌ Not found')
            if not ticket['claimed_by']:
                return await ctx.reply('❌ Must be claimed first')
            if ticket['claimed_by'] != ctx.author.id and not ctx.author.guild_permissions.administrator:
                return await ctx.reply('❌ Only claimer can remove')
        await ctx.channel.set_permissions(member, overwrite=None)
        embed = discord.Embed(title='❌ Removed', description=f'{member.mention} removed', color=COLORS['error'])
        await ctx.reply(embed=embed)
    except Exception as e:
        await ctx.reply(f'❌ Error: {str(e)}')

@bot.command(name='rename')
async def rename_cmd(ctx, *, new_name: str = None):
    if not new_name:
        return await ctx.reply('❌ Missing name\n\nExample: `$rename urgent`')
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply('❌ Not a ticket')
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
            if not ticket:
                return await ctx.reply('❌ Not found')
            if ticket.get('claimed_by') and ticket['claimed_by'] != ctx.author.id and not ctx.author.guild_permissions.administrator:
                return await ctx.reply('❌ Only claimer can rename')
        await ctx.channel.edit(name=f"ticket-{new_name}")
        embed = discord.Embed(title='✏️ Renamed', description=f'Now: `ticket-{new_name}`', color=COLORS['support'])
        await ctx.reply(embed=embed)
    except Exception as e:
        await ctx.reply(f'❌ Error: {str(e)}')

@bot.command(name='confirm')
async def confirm_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply('❌ Not a ticket')
    embed = discord.Embed(title='✅ Confirm Trade', description='Both parties click below', color=COLORS['support'])
    view = TradeConfirmView()
    await ctx.send(embed=embed, view=view)

@bot.command(name='proof')
async def proof_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply('❌ Only in tickets')
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
            if not ticket:
                return await ctx.reply('❌ Not found')
            config = await conn.fetchrow('SELECT * FROM config WHERE guild_id = $1', ctx.guild.id)
            if not config or not config.get('proof_channel_id'):
                return await ctx.reply('❌ Proof channel not set\n\nUse `$setproof #channel`')
        proof_channel = ctx.guild.get_channel(config['proof_channel_id'])
        if not proof_channel:
            return await ctx.reply('❌ Proof channel not found')
        opener = ctx.guild.get_member(ticket['user_id'])
        embed = discord.Embed(title='✅ Trade Completed', color=COLORS['success'])
        embed.add_field(name='Middleman', value=ctx.author.mention, inline=True)
        if ticket.get('tier'):
            tier_names = {'lowtier': 'Low Value', 'midtier': 'Mid Value', 'hightier': 'High Value'}
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
            except:
                pass
        embed.set_footer(text=f'ID: {ticket["ticket_id"]}')
        await proof_channel.send(embed=embed)
        success = discord.Embed(title='✅ Proof Sent', description=f'Posted to {proof_channel.mention}', color=COLORS['success'])
        await ctx.reply(embed=success)
    except Exception as e:
        logger.error(f'Proof error: {e}')
        await ctx.reply(f'❌ Error: {str(e)}')

@bot.command(name='setps')
async def setps_cmd(ctx, game: str = None, link: str = None, *, name: str = None):
    if not any(role.id in HARDCODED_ROLES.values() for role in ctx.author.roles):
        return await ctx.reply('❌ Need a middleman role')
    if not game or not link or not name:
        return await ctx.reply('❌ Missing arguments\n\nExample: `$setps bloxfruits https://link Bobs Mansion`')
    async with db.pool.acquire() as conn:
        await conn.execute('INSERT INTO ps_links (user_id, game_key, game_name, link) VALUES ($1, $2, $3, $4) ON CONFLICT (user_id, game_key) DO UPDATE SET game_name = $3, link = $4', ctx.author.id, game.lower(), name, link)
    embed = discord.Embed(title='✅ PS Saved', color=COLORS['success'])
    embed.add_field(name='Game', value=f"`{game}`", inline=True)
    embed.add_field(name='Name', value=name, inline=True)
    await ctx.reply(embed=embed)

@bot.command(name='psupdate')
async def psupdate_cmd(ctx, game: str = None, *, new_link: str = None):
    if not any(role.id in HARDCODED_ROLES.values() for role in ctx.author.roles):
        return await ctx.reply('❌ Need a middleman role')
    if not game or not new_link:
        return await ctx.reply('❌ Missing arguments\n\nExample: `$psupdate bloxfruits https://new-link`')
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow('SELECT game_name FROM ps_links WHERE user_id = $1 AND game_key = $2', ctx.author.id, game.lower())
        if not row:
            return await ctx.reply(f'❌ No PS for `{game}`\n\nUse `$setps` first')
        await conn.execute('UPDATE ps_links SET link = $1 WHERE user_id = $2 AND game_key = $3', new_link, ctx.author.id, game.lower())
    embed = discord.Embed(title='✅ PS Updated', color=COLORS['success'])
    embed.add_field(name='Game', value=f"`{game}`", inline=True)
    await ctx.reply(embed=embed)

@bot.command(name='pslist')
async def pslist_cmd(ctx):
    if not any(role.id in HARDCODED_ROLES.values() for role in ctx.author.roles):
        return await ctx.reply('❌ Need a middleman role')
    async with db.pool.acquire() as conn:
        rows = await conn.fetch('SELECT game_key, game_name FROM ps_links WHERE user_id = $1', ctx.author.id)
    if not rows:
        return await ctx.reply('❌ No PS links\n\nExample: `$setps bloxfruits https://link Bobs Mansion`')
    embed = discord.Embed(title='🔗 Your PS Links', color=COLORS['support'])
    links = '\n'.join([f'**{row["game_key"]}** — {row["game_name"]}' for row in rows])
    embed.description = links
    await ctx.reply(embed=embed)

@bot.command(name='ps')
async def ps_cmd(ctx, *, identifier: str = None):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply('❌ Only in tickets')
    if not any(role.id in HARDCODED_ROLES.values() for role in ctx.author.roles):
        return await ctx.reply('❌ Need a middleman role')
    if not identifier:
        async with db.pool.acquire() as conn:
            rows = await conn.fetch('SELECT game_key, game_name FROM ps_links WHERE user_id = $1', ctx.author.id)
        if not rows:
            return await ctx.reply('❌ No PS links\n\nUse `$setps`')
        links = '\n'.join([f'**{row["game_key"]}** — {row["game_name"]}' for row in rows])
        embed = discord.Embed(title='🔗 Your PS Links', description=links, color=COLORS['support'])
        embed.set_footer(text='Use: $ps <game> or $ps <n>')
        return await ctx.reply(embed=embed)
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow('SELECT link, game_name FROM ps_links WHERE user_id = $1 AND (game_key = $2 OR game_name ILIKE $3)', ctx.author.id, identifier.lower(), f'%{identifier}%')
    if not row:
        return await ctx.reply(f'❌ No PS for `{identifier}`\n\nUse `$pslist`')
    embed = discord.Embed(title='🔗 Private Server', color=COLORS['success'])
    embed.add_field(name='Link', value=f'[Click to Join]({row["link"]})', inline=False)
    embed.set_footer(text=f'{row["game_name"]} • By {ctx.author.display_name}')
    await ctx.send(embed=embed)

@bot.command(name='removeps')
async def removeps_cmd(ctx, game: str = None):
    if not any(role.id in HARDCODED_ROLES.values() for role in ctx.author.roles):
        return await ctx.reply('❌ Need a middleman role')
    if not game:
        return await ctx.reply('❌ Missing game\n\nExample: `$removeps bloxfruits`')
    async with db.pool.acquire() as conn:
        await conn.execute('DELETE FROM ps_links WHERE user_id = $1 AND game_key = $2', ctx.author.id, game.lower())
    embed = discord.Embed(title='✅ Removed', description=f'Deleted `{game}`', color=COLORS['success'])
    await ctx.reply(embed=embed)

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

    view = TicketPanelView()
    await ctx.send(embed=embed, view=view)

    try:
        await ctx.message.delete()
    except:
        pass

@bot.command(name='setcategory')
@commands.has_permissions(administrator=True)
async def setcategory_cmd(ctx, category: discord.CategoryChannel = None):
    if not category:
        return await ctx.reply('❌ Missing category\n\nExample: `$setcategory #tickets`')
    async with db.pool.acquire() as conn:
        await conn.execute('INSERT INTO config (guild_id, ticket_category_id) VALUES ($1, $2) ON CONFLICT (guild_id) DO UPDATE SET ticket_category_id = $2', ctx.guild.id, category.id)
    embed = discord.Embed(title='✅ Category Set', description=f'{category.mention}', color=COLORS['success'])
    await ctx.reply(embed=embed)

@bot.command(name='setlogs')
@commands.has_permissions(administrator=True)
async def setlogs_cmd(ctx, channel: discord.TextChannel = None):
    if not channel:
        return await ctx.reply('❌ Missing channel\n\nExample: `$setlogs #logs`')
    async with db.pool.acquire() as conn:
        await conn.execute('INSERT INTO config (guild_id, log_channel_id) VALUES ($1, $2) ON CONFLICT (guild_id) DO UPDATE SET log_channel_id = $2', ctx.guild.id, channel.id)
    embed = discord.Embed(title='✅ Logs Set', description=f'{channel.mention}', color=COLORS['success'])
    await ctx.reply(embed=embed)

@bot.command(name='setproof')
@commands.has_permissions(administrator=True)
async def setproof_cmd(ctx, channel: discord.TextChannel = None):
    if not channel:
        return await ctx.reply('❌ Missing channel\n\nExample: `$setproof #proofs`')
    try:
        async with db.pool.acquire() as conn:
            await conn.execute('INSERT INTO config (guild_id, proof_channel_id) VALUES ($1, $2) ON CONFLICT (guild_id) DO UPDATE SET proof_channel_id = $2', ctx.guild.id, channel.id)
        embed = discord.Embed(title='✅ Proof Set', description=f'{channel.mention}', color=COLORS['success'])
        await ctx.reply(embed=embed)
    except Exception as e:
        logger.error(f'Setproof error: {e}')
        await ctx.reply(f'❌ Error: {str(e)}')

@bot.command(name='config')
@commands.has_permissions(administrator=True)
async def config_cmd(ctx):
    async with db.pool.acquire() as conn:
        config = await conn.fetchrow('SELECT * FROM config WHERE guild_id = $1', ctx.guild.id)
    embed = discord.Embed(title='⚙️ Config', color=COLORS['support'])
    if config:
        category = ctx.guild.get_channel(config.get('ticket_category_id')) if config.get('ticket_category_id') else None
        log_channel = ctx.guild.get_channel(config.get('log_channel_id')) if config.get('log_channel_id') else None
        proof_channel = ctx.guild.get_channel(config.get('proof_channel_id')) if config.get('proof_channel_id') else None
        embed.add_field(name='Category', value=category.mention if category else '❌ Not set', inline=True)
        embed.add_field(name='Logs', value=log_channel.mention if log_channel else '❌ Not set', inline=True)
        embed.add_field(name='Proof', value=proof_channel.mention if proof_channel else '❌ Not set', inline=True)
    role_text = ""
    for tier, role_id in HARDCODED_ROLES.items():
        role = ctx.guild.get_role(role_id)
        tier_names = {'lowtier': 'Low Value', 'midtier': 'Mid Value', 'hightier': 'High Value', 'staff': 'Staff'}
        tier_name = tier_names.get(tier, tier)
        role_text += f"**{tier_name}:** {role.mention if role else '❌ Not found'}\n"
    embed.add_field(name='Hardcoded Roles', value=role_text, inline=False)
    await ctx.reply(embed=embed)

@bot.command(name='blacklist')
@commands.has_permissions(administrator=True)
async def blacklist_cmd(ctx, member: discord.Member = None, *, reason: str = "No reason"):
    if not member:
        return await ctx.reply('❌ Missing user\n\nExample: `$blacklist @Scammer Fraud`')
    async with db.pool.acquire() as conn:
        await conn.execute('INSERT INTO blacklist (user_id, guild_id, reason, blacklisted_by) VALUES ($1, $2, $3, $4) ON CONFLICT (user_id) DO UPDATE SET reason = $3, blacklisted_by = $4', member.id, ctx.guild.id, reason, ctx.author.id)
    embed = discord.Embed(title='🚫 Blacklisted', color=COLORS['error'])
    embed.add_field(name='User', value=member.mention, inline=True)
    embed.add_field(name='Reason', value=reason, inline=True)
    await ctx.reply(embed=embed)

@bot.command(name='unblacklist')
@commands.has_permissions(administrator=True)
async def unblacklist_cmd(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply('❌ Missing user\n\nExample: `$unblacklist @John`')
    async with db.pool.acquire() as conn:
        await conn.execute('DELETE FROM blacklist WHERE user_id = $1', member.id)
    embed = discord.Embed(title='✅ Unblacklisted', description=f'{member.mention} can create tickets', color=COLORS['success'])
    await ctx.reply(embed=embed)

@bot.command(name='blacklists')
@commands.has_permissions(administrator=True)
async def blacklists_cmd(ctx):
    async with db.pool.acquire() as conn:
        rows = await conn.fetch('SELECT * FROM blacklist WHERE guild_id = $1', ctx.guild.id)
    if not rows:
        return await ctx.reply('✅ No blacklisted users')
    embed = discord.Embed(title='🚫 Blacklisted Users', color=COLORS['error'])
    for row in rows:
        user = ctx.guild.get_member(row['user_id'])
        username = user.mention if user else f"ID: {row['user_id']}"
        embed.add_field(name=username, value=f"Reason: {row['reason']}", inline=False)
    await ctx.reply(embed=embed)

@bot.command(name='clear')
@commands.has_permissions(manage_messages=True)
async def clear_cmd(ctx):
    deleted = 0
    async for message in ctx.channel.history(limit=100):
        if message.author == bot.user:
            try:
                await message.delete()
                deleted += 1
                await asyncio.sleep(0.5)
            except:
                pass
    embed = discord.Embed(title='🧹 Cleared', description=f'Deleted {deleted} messages', color=COLORS['success'])
    msg = await ctx.send(embed=embed)
    await asyncio.sleep(3)
    await msg.delete()
    try:
        await ctx.message.delete()
    except:
        pass

@bot.command(name='ping')
async def ping_cmd(ctx):
    latency = round(bot.latency * 1000)
    color = COLORS['success'] if latency < 200 else COLORS['support']
    embed = discord.Embed(title='🏓 Pong', description=f'**{latency}ms**', color=color)
    await ctx.reply(embed=embed)

@bot.command(name='help')
async def help_cmd(ctx):
    embed = discord.Embed(title='📚 Commands', color=COLORS['support'])
    embed.add_field(name='🎫 Tickets', value='`$close` `$claim` `$unclaim` `$add` `$remove` `$rename`', inline=False)
    embed.add_field(name='⚖️ Trade', value='`$confirm` `$proof`', inline=False)
    embed.add_field(name='🔗 PS (MM only)', value='`$setps` `$psupdate` `$ps` `$pslist` `$removeps`', inline=False)
    if ctx.author.guild_permissions.administrator:
        embed.add_field(name='⚙️ Admin', value='`$setup` `$setcategory` `$setlogs` `$setproof` `$config`', inline=False)
        embed.add_field(name='🛡️ Mod', value='`$blacklist` `$unblacklist` `$blacklists` `$clear`', inline=False)
    embed.add_field(name='🔧 Utility', value='`$ping` `$help`', inline=False)
    embed.set_footer(text='Beautiful Ticket Bot')
    await ctx.reply(embed=embed)

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
    bot.loop.create_task(rotate_status())
    logger.info('Bot ready!')

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.reply('❌ No permission')
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.reply(f'❌ Missing: {error.param.name}')
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
