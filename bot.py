# ============================================================
# TICKET BOT
# ============================================================

import os
import re
import io
import json
import random
import string
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional
from collections import defaultdict

import discord
from discord import TextInput, Modal, ButtonStyle
from discord.ext import commands
from discord.ui import View, Select
import asyncpg
import aiohttp

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ==================== CONSTANTS ====================

OWNER_ID = 1029438856069656576

HARDCODED_ROLES = {
    'lowtier':  1453757017218093239,
    'midtier':  1434610759140118640,
    'hightier': 1453757157144137911,
    'staff':    1432081794647199895,
}

HARDCODED_CHANNELS = {
    'proof': 1472695529883435091,
}

COLORS = {
    'lowtier':  0x57F287,
    'midtier':  0xFEE75C,
    'hightier': 0xED4245,
    'support':  0x5865F2,
    'success':  0x57F287,
    'error':    0xED4245,
}

TIER_NAMES = {
    'lowtier':  '💎 Low Tier  •  100–900 RBX',
    'midtier':  '💰 Mid Tier  •  1K–3K RBX',
    'hightier': '💸 High Tier  •  3.1K–5K+ RBX',
    'support':  '🎫 Support',
}

TIER_SHORT = {
    'lowtier':  '100-900rbx',
    'midtier':  '1k-3krbx',
    'hightier': '3k-5krbx',
    'support':  'support',
}

MAX_OPEN_TICKETS = 1   # max tickets a user can have open at once
tickets_locked   = {}  # guild_id -> bool

# ==================== RATE LIMITER ====================

class RateLimiter:
    def __init__(self):
        self._store: dict = defaultdict(dict)

    def check(self, user_id: int, action: str, seconds: int) -> bool:
        now  = datetime.now(timezone.utc).timestamp()
        last = self._store[action].get(user_id, 0)
        if now - last < seconds:
            return False
        self._store[action][user_id] = now
        return True

rate_limiter = RateLimiter()

# ==================== DATABASE ====================

class Database:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        url = os.getenv('DATABASE_URL', '')
        if not url:
            raise RuntimeError('DATABASE_URL not set')
        if url.startswith('postgres://'):
            url = url.replace('postgres://', 'postgresql://', 1)
        self.pool = await asyncpg.create_pool(url, min_size=1, max_size=5, command_timeout=30)
        await self._create_tables()
        logger.info('Database connected')

    async def _create_tables(self):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS config (
                    guild_id           BIGINT PRIMARY KEY,
                    ticket_category_id BIGINT,
                    log_channel_id     BIGINT,
                    ticket_counter     INT DEFAULT 0
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS tickets (
                    ticket_id     TEXT PRIMARY KEY,
                    guild_id      BIGINT,
                    channel_id    BIGINT,
                    user_id       BIGINT,
                    ticket_type   TEXT,
                    tier          TEXT,
                    claimed_by    BIGINT,
                    status        TEXT DEFAULT 'open',
                    trade_details JSONB,
                    created_at    TIMESTAMP DEFAULT NOW()
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS blacklist (
                    user_id        BIGINT,
                    guild_id       BIGINT,
                    reason         TEXT,
                    blacklisted_by BIGINT,
                    blacklisted_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (user_id, guild_id)
                )
            ''')
            # Add ticket_counter column if it doesn't exist yet (migration)
            try:
                await conn.execute('ALTER TABLE config ADD COLUMN IF NOT EXISTS ticket_counter INT DEFAULT 0')
            except Exception:
                pass
            try:
                await conn.execute('ALTER TABLE blacklist ADD COLUMN IF NOT EXISTS blacklisted_at TIMESTAMP DEFAULT NOW()')
            except Exception:
                pass

    async def next_ticket_number(self, guild_id: int) -> int:
        async with self.pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO config (guild_id, ticket_counter) VALUES ($1, 1) ON CONFLICT (guild_id) DO UPDATE SET ticket_counter = config.ticket_counter + 1',
                guild_id
            )
            row = await conn.fetchrow('SELECT ticket_counter FROM config WHERE guild_id=$1', guild_id)
            return row['ticket_counter']

    async def close(self):
        if self.pool:
            await self.pool.close()

db = Database()

# ==================== BOT ====================

intents = discord.Intents.default()
intents.message_content = True
intents.members         = True
intents.guilds          = True

bot = commands.Bot(command_prefix='$', intents=intents, help_command=None)

# ==================== HELPERS ====================

def is_owner():
    async def predicate(ctx):
        return ctx.author.id == OWNER_ID
    return commands.check(predicate)

def has_ticket_staff_perms():
    async def predicate(ctx):
        staff = ctx.guild.get_role(HARDCODED_ROLES['staff'])
        if staff and staff in ctx.author.roles:
            return True
        for key in ['lowtier', 'midtier', 'hightier']:
            r = ctx.guild.get_role(HARDCODED_ROLES[key])
            if r and r in ctx.author.roles:
                return True
        return False
    return commands.check(predicate)

def member_is_staff(member: discord.Member) -> bool:
    staff = member.guild.get_role(HARDCODED_ROLES['staff'])
    if staff and staff in member.roles:
        return True
    for key in ['lowtier', 'midtier', 'hightier']:
        r = member.guild.get_role(HARDCODED_ROLES[key])
        if r and r in member.roles:
            return True
    return False

async def send_log(guild, title, description=None, color=0x5865F2, fields=None):
    try:
        async with db.pool.acquire() as conn:
            cfg = await conn.fetchrow('SELECT log_channel_id FROM config WHERE guild_id=$1', guild.id)
        if not cfg or not cfg['log_channel_id']:
            return
        ch = guild.get_channel(cfg['log_channel_id'])
        if not ch:
            return
        e = discord.Embed(title=title, color=color, timestamp=datetime.now(timezone.utc))
        if description:
            e.description = description
        for k, v in (fields or {}).items():
            e.add_field(name=k, value=str(v), inline=True)
        await ch.send(embed=e)
    except Exception as ex:
        logger.error(f'send_log: {ex}')

async def ticket_has_permission(ctx, ticket) -> bool:
    if ticket['ticket_type'] == 'support':
        r = ctx.guild.get_role(HARDCODED_ROLES['staff'])
        if r and r in ctx.author.roles:
            return True
    elif ticket['ticket_type'] == 'middleman':
        tier = ticket.get('tier')
        if tier in HARDCODED_ROLES:
            r = ctx.guild.get_role(HARDCODED_ROLES[tier])
            if r and r in ctx.author.roles:
                return True
    if ticket.get('claimed_by') and ctx.author.id == ticket['claimed_by']:
        return True
    return False

async def lock_ticket_channel(channel, claimer: discord.Member, creator: discord.Member = None):
    """On claim: strip staff/tier roles. Only claimer + creator + $add'd users can talk."""
    for key in ['staff', 'lowtier', 'midtier', 'hightier']:
        r = channel.guild.get_role(HARDCODED_ROLES[key])
        if r:
            ow = channel.overwrites_for(r)
            ow.send_messages = False
            await channel.set_permissions(r, overwrite=ow)
    # Give claimer talk perms
    await channel.set_permissions(claimer, read_messages=True, send_messages=True)
    # Keep creator talk perms
    if creator and creator.id != claimer.id:
        await channel.set_permissions(creator, read_messages=True, send_messages=True)

async def unlock_ticket_channel(channel, old_claimer: discord.Member = None):
    """On unclaim: restore staff/tier roles, remove claimer's explicit overwrite."""
    for key in ['staff', 'lowtier', 'midtier', 'hightier']:
        r = channel.guild.get_role(HARDCODED_ROLES[key])
        if r:
            ow = channel.overwrites_for(r)
            ow.send_messages = True
            await channel.set_permissions(r, overwrite=ow)
    if old_claimer:
        # Keep read but remove explicit send — falls back to role perms
        await channel.set_permissions(old_claimer, read_messages=True, send_messages=None)

# ==================== TICKET UI ====================

class MiddlemanModal(Modal, title='Middleman Request'):
    def __init__(self, tier: str):
        super().__init__()
        self.tier      = tier
        self.trader    = TextInput(label='Who are you trading with?', placeholder='Username or ID', required=True)
        self.giving    = TextInput(label='What are you giving?', placeholder='e.g. 1 garam', style=discord.TextStyle.paragraph, required=True)
        self.receiving = TextInput(label='What are you receiving?', placeholder='e.g. 500 Robux', style=discord.TextStyle.paragraph, required=True)
        self.tip       = TextInput(label='Leaving a tip?', placeholder='Optional — leave blank to skip', required=False)
        self.add_item(self.trader)
        self.add_item(self.giving)
        self.add_item(self.receiving)
        self.add_item(self.tip)

    async def on_submit(self, interaction: discord.Interaction):
        if not rate_limiter.check(interaction.user.id, 'ticket_open', 10):
            return await interaction.response.send_message('slow down, wait a few seconds before opening another ticket', ephemeral=True)
        await interaction.response.defer(ephemeral=True)
        guild, user = interaction.guild, interaction.user

        # Locked?
        if tickets_locked.get(guild.id):
            return await interaction.followup.send('tickets are currently closed, check back later', ephemeral=True)

        async with db.pool.acquire() as conn:
            bl  = await conn.fetchrow('SELECT * FROM blacklist WHERE user_id=$1 AND guild_id=$2', user.id, guild.id)
            cfg = await conn.fetchrow('SELECT * FROM config WHERE guild_id=$1', guild.id)
            open_count = await conn.fetchval(
                "SELECT COUNT(*) FROM tickets WHERE user_id=$1 AND guild_id=$2 AND status NOT IN ('closed')",
                user.id, guild.id
            )

        if bl:
            by = guild.get_member(bl['blacklisted_by'])
            when = bl['blacklisted_at'].strftime('%b %d, %Y') if bl.get('blacklisted_at') else 'unknown date'
            reason = bl['reason'] or 'no reason given'
            return await interaction.followup.send(
                f"you're blacklisted from opening tickets\n**reason:** {reason}\n**by:** {by.mention if by else 'staff'} on {when}\n\nif you think this is a mistake, dm a staff member",
                ephemeral=True
            )

        if open_count >= MAX_OPEN_TICKETS:
            return await interaction.followup.send(
                f"you already have {open_count} open ticket{'s' if open_count > 1 else ''}. close it before opening a new one",
                ephemeral=True
            )

        if not cfg or not cfg['ticket_category_id']:
            return await interaction.followup.send("tickets aren't set up yet, ping a staff member", ephemeral=True)

        category = guild.get_channel(cfg['ticket_category_id'])
        if not category:
            return await interaction.followup.send("couldn't find the ticket category, ping a staff member", ephemeral=True)

        try:
            num = await db.next_ticket_number(guild.id)
            short = TIER_SHORT.get(self.tier, self.tier)
            ticket_id = f'{num:04d}'
            ch_name = f'ticket-{short}-{num:04d}-{user.name}'

            overwrites = {
                guild.default_role: discord.PermissionOverwrite(read_messages=False),
                guild.me:           discord.PermissionOverwrite(read_messages=True, send_messages=True, manage_channels=True),
                user:               discord.PermissionOverwrite(read_messages=True, send_messages=True),
            }
            role_id = HARDCODED_ROLES.get(self.tier)
            if role_id:
                tr = guild.get_role(role_id)
                if tr:
                    overwrites[tr] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
            staff_role = guild.get_role(HARDCODED_ROLES['staff'])
            if staff_role:
                overwrites[staff_role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)

            channel = await category.create_text_channel(name=ch_name, overwrites=overwrites)

            trade = {
                'trader':    self.trader.value,
                'giving':    self.giving.value,
                'receiving': self.receiving.value,
                'tip':       self.tip.value or 'None',
            }

            async with db.pool.acquire() as conn:
                await conn.execute(
                    'INSERT INTO tickets (ticket_id,guild_id,channel_id,user_id,ticket_type,tier,trade_details) VALUES ($1,$2,$3,$4,$5,$6,$7)',
                    ticket_id, guild.id, channel.id, user.id, 'middleman', self.tier, json.dumps(trade)
                )

            color = COLORS.get(self.tier, COLORS['support'])
            embed = discord.Embed(color=color)
            embed.set_author(name=f'{user.display_name}  •  {TIER_NAMES.get(self.tier)}', icon_url=user.display_avatar.url)
            embed.description = (
                '**sit tight** — a middleman will be with you shortly\n\n'
                '> you cannot send messages until someone claims this ticket\n'
                '> be patient, do not ping staff repeatedly\n'
                '> have your trade details ready'
            )
            embed.add_field(name='Trading with', value=trade['trader'],    inline=False)
            embed.add_field(name='Giving',       value=trade['giving'],    inline=True)
            embed.add_field(name='Receiving',    value=trade['receiving'], inline=True)
            if trade['tip'] != 'None':
                embed.add_field(name='Tip', value=trade['tip'], inline=True)
            embed.set_footer(text=f'ticket #{ticket_id}  •  {datetime.now(timezone.utc).strftime("%b %d %Y")}')

            ping = user.mention
            if role_id and (tr := guild.get_role(role_id)):
                ping += f' {tr.mention}'
            await channel.send(content=ping, embed=embed, view=TicketControlView())

            await send_log(guild, 'ticket opened',
                f'{user.mention} opened a {TIER_NAMES.get(self.tier)} ticket',
                color, {'ticket': f'#{ticket_id}', 'channel': channel.mention})

            await interaction.followup.send(
                f'ticket created → {channel.mention}',
                ephemeral=True
            )
        except Exception as e:
            logger.error(f'middleman ticket error: {e}')
            await interaction.followup.send(f'something went wrong: {e}', ephemeral=True)


class MiddlemanTierSelect(Select):
    def __init__(self):
        super().__init__(
            placeholder='pick your trade value range',
            options=[
                discord.SelectOption(label='Low Tier  |  100–900 RBX',   value='lowtier',  emoji='💎', description='100 to 900 robux — quick trades'),
                discord.SelectOption(label='Mid Tier  |  1K–3K RBX',     value='midtier',  emoji='💰', description='1,000 to 3,000 robux'),
                discord.SelectOption(label='High Tier  |  3.1K–5K+ RBX', value='hightier', emoji='💸', description='3,100 robux and above — trusted staff only'),
            ]
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(MiddlemanModal(self.values[0]))


class TicketPanelView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Support', style=ButtonStyle.primary, emoji='🎫', custom_id='support_btn')
    async def support_button(self, interaction: discord.Interaction, _):
        if not rate_limiter.check(interaction.user.id, 'ticket_open', 10):
            return await interaction.response.send_message('slow down, wait a few seconds', ephemeral=True)
        await interaction.response.defer(ephemeral=True)
        guild, user = interaction.guild, interaction.user

        if tickets_locked.get(guild.id):
            return await interaction.followup.send('tickets are currently closed', ephemeral=True)

        async with db.pool.acquire() as conn:
            bl  = await conn.fetchrow('SELECT * FROM blacklist WHERE user_id=$1 AND guild_id=$2', user.id, guild.id)
            cfg = await conn.fetchrow('SELECT * FROM config WHERE guild_id=$1', guild.id)
            open_count = await conn.fetchval(
                "SELECT COUNT(*) FROM tickets WHERE user_id=$1 AND guild_id=$2 AND status NOT IN ('closed')",
                user.id, guild.id
            )

        if bl:
            by = guild.get_member(bl['blacklisted_by'])
            when = bl['blacklisted_at'].strftime('%b %d, %Y') if bl.get('blacklisted_at') else 'unknown date'
            reason = bl['reason'] or 'no reason given'
            return await interaction.followup.send(
                f"you're blacklisted from opening tickets\n**reason:** {reason}\n**by:** {by.mention if by else 'staff'} on {when}\n\nif you think this is a mistake, dm a staff member",
                ephemeral=True
            )

        if open_count >= MAX_OPEN_TICKETS:
            return await interaction.followup.send(
                f"you already have {open_count} open ticket{'s' if open_count > 1 else ''}. close it first",
                ephemeral=True
            )

        if not cfg or not cfg['ticket_category_id']:
            return await interaction.followup.send("tickets aren't set up, ping a staff member", ephemeral=True)

        try:
            num = await db.next_ticket_number(guild.id)
            ticket_id = f'{num:04d}'
            ch_name = f'ticket-support-{num:04d}-{user.name}'

            overwrites = {
                guild.default_role: discord.PermissionOverwrite(read_messages=False),
                guild.me:           discord.PermissionOverwrite(read_messages=True, send_messages=True, manage_channels=True),
                user:               discord.PermissionOverwrite(read_messages=True, send_messages=True),
            }
            staff_role = guild.get_role(HARDCODED_ROLES['staff'])
            if staff_role:
                overwrites[staff_role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)

            category = guild.get_channel(cfg['ticket_category_id'])
            channel  = await category.create_text_channel(name=ch_name, overwrites=overwrites)

            async with db.pool.acquire() as conn:
                await conn.execute(
                    'INSERT INTO tickets (ticket_id,guild_id,channel_id,user_id,ticket_type,tier) VALUES ($1,$2,$3,$4,$5,$6)',
                    ticket_id, guild.id, channel.id, user.id, 'support', 'support'
                )

            embed = discord.Embed(color=COLORS['support'])
            embed.set_author(name=f'{user.display_name}  •  Support Ticket', icon_url=user.display_avatar.url)
            embed.description = (
                '**hang tight** — staff will be with you shortly\n\n'
                '> you cannot send messages until someone claims this ticket\n'
                '> explain your issue clearly when staff arrives\n'
                '> do not ping staff repeatedly'
            )
            embed.set_footer(text=f'ticket #{ticket_id}  •  {datetime.now(timezone.utc).strftime("%b %d %Y")}')

            ping = user.mention
            if staff_role:
                ping += f' {staff_role.mention}'
            await channel.send(content=ping, embed=embed, view=TicketControlView())
            await interaction.followup.send(f'ticket created → {channel.mention}', ephemeral=True)
        except Exception as e:
            logger.error(f'support ticket error: {e}')
            await interaction.followup.send(f'something went wrong: {e}', ephemeral=True)

    @discord.ui.button(label='Middleman', style=ButtonStyle.success, emoji='⚖️', custom_id='middleman_btn')
    async def middleman_button(self, interaction: discord.Interaction, _):
        if tickets_locked.get(interaction.guild.id):
            return await interaction.response.send_message('tickets are currently closed', ephemeral=True)
        view = View(timeout=300)
        view.add_item(MiddlemanTierSelect())
        await interaction.response.send_message(
            embed=discord.Embed(description='pick the range that matches your trade value', color=COLORS['support']),
            view=view, ephemeral=True
        )


class TicketControlView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Claim', style=ButtonStyle.green, custom_id='claim_ticket', emoji='✋')
    async def claim_button(self, interaction: discord.Interaction, _):
        if not rate_limiter.check(interaction.user.id, 'claim', 2):
            return await interaction.response.send_message('slow down', ephemeral=True)
        if not member_is_staff(interaction.user):
            return await interaction.response.send_message("you don't have a staff role", ephemeral=True)
        try:
            async with db.pool.acquire() as conn:
                ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', interaction.channel.id)
                if not ticket:
                    return await interaction.response.send_message('no ticket found for this channel', ephemeral=True)
                if ticket['claimed_by']:
                    claimer = interaction.guild.get_member(ticket['claimed_by'])
                    return await interaction.response.send_message(
                        f"already claimed by {claimer.mention if claimer else 'someone'}", ephemeral=True
                    )
                await conn.execute(
                    "UPDATE tickets SET claimed_by=$1, status='claimed' WHERE ticket_id=$2",
                    interaction.user.id, ticket['ticket_id']
                )
            await lock_ticket_channel(interaction.channel, interaction.user, interaction.guild.get_member(ticket['user_id']))
            embed = discord.Embed(color=COLORS['success'])
            embed.description = f'claimed by {interaction.user.mention}\n\nuse `$add @user` to let someone else talk in here'
            await interaction.response.send_message(embed=embed)
            await send_log(interaction.guild, 'ticket claimed',
                f'{interaction.user.mention} claimed ticket #{ticket["ticket_id"]}',
                COLORS['success'])
        except Exception as e:
            await interaction.response.send_message(f'something went wrong: {e}', ephemeral=True)

    @discord.ui.button(label='Unclaim', style=ButtonStyle.gray, custom_id='unclaim_ticket', emoji='↩️')
    async def unclaim_button(self, interaction: discord.Interaction, _):
        if not rate_limiter.check(interaction.user.id, 'unclaim', 2):
            return await interaction.response.send_message('slow down', ephemeral=True)
        try:
            async with db.pool.acquire() as conn:
                ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', interaction.channel.id)
                if not ticket:
                    return await interaction.response.send_message('no ticket found', ephemeral=True)
                if not ticket['claimed_by']:
                    return await interaction.response.send_message("this ticket isn't claimed", ephemeral=True)
                if ticket['claimed_by'] != interaction.user.id:
                    return await interaction.response.send_message("you didn't claim this", ephemeral=True)
                await conn.execute(
                    "UPDATE tickets SET claimed_by=NULL, status='open' WHERE ticket_id=$1",
                    ticket['ticket_id']
                )
            old = interaction.guild.get_member(ticket['claimed_by'])
            await unlock_ticket_channel(interaction.channel, old)
            await interaction.response.send_message(
                embed=discord.Embed(description='ticket is open again — any staff can claim it', color=COLORS['support'])
            )
        except Exception as e:
            await interaction.response.send_message(f'something went wrong: {e}', ephemeral=True)


# ==================== TICKET COMMANDS ====================

class CloseConfirmView(View):
    def __init__(self, ctx, ticket):
        super().__init__(timeout=30)
        self.ctx    = ctx
        self.ticket = ticket

    async def do_close(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            async with db.pool.acquire() as conn:
                await conn.execute("UPDATE tickets SET status='closed' WHERE ticket_id=$1", self.ticket['ticket_id'])
            await interaction.message.edit(
                embed=discord.Embed(description=f'closing — transcript saved', color=COLORS['error']),
                view=None
            )
            async with db.pool.acquire() as conn:
                cfg = await conn.fetchrow('SELECT log_channel_id FROM config WHERE guild_id=$1', interaction.guild.id)
            if cfg and cfg.get('log_channel_id'):
                lc = interaction.guild.get_channel(cfg['log_channel_id'])
                if lc:
                    opener  = interaction.guild.get_member(self.ticket['user_id'])
                    claimer = interaction.guild.get_member(self.ticket['claimed_by']) if self.ticket['claimed_by'] else None
                    header = (
                        f"TRANSCRIPT\n{'='*50}\n"
                        f"ticket: #{self.ticket['ticket_id']}\n"
                        f"opened by: {opener.name if opener else self.ticket['user_id']}\n"
                        f"claimed by: {claimer.name if claimer else 'nobody'}\n"
                        f"closed by: {interaction.user.name}\n"
                        f"{'='*50}\n\n"
                    )
                    msgs = []
                    async for m in interaction.channel.history(limit=500, oldest_first=True):
                        content = m.content or '[embed/attachment]'
                        msgs.append(f'[{m.created_at.strftime("%H:%M:%S")}] {m.author.name}: {content}')
                    transcript = header + '\n'.join(msgs)
                    file = discord.File(
                        fp=io.BytesIO(transcript.encode('utf-8')),
                        filename=f"transcript-{self.ticket['ticket_id']}.txt"
                    )
                    e = discord.Embed(title='ticket closed', color=COLORS['error'])
                    e.add_field(name='#',         value=self.ticket['ticket_id'],                  inline=True)
                    e.add_field(name='opened by', value=opener.mention if opener else 'unknown',   inline=True)
                    e.add_field(name='closed by', value=interaction.user.mention,                  inline=True)
                    await lc.send(embed=e, file=file)
            await asyncio.sleep(0.5)
            await interaction.channel.delete()
        except Exception as e:
            logger.error(f'close confirm error: {e}')

    @discord.ui.button(label='yes, close it', style=ButtonStyle.red, emoji='🔒')
    async def confirm(self, interaction: discord.Interaction, _):
        if interaction.user.id != self.ctx.author.id:
            return await interaction.response.send_message("you didn't run this command", ephemeral=True)
        await self.do_close(interaction)

    @discord.ui.button(label='cancel', style=ButtonStyle.gray)
    async def cancel(self, interaction: discord.Interaction, _):
        if interaction.user.id != self.ctx.author.id:
            return await interaction.response.send_message("you didn't run this command", ephemeral=True)
        await interaction.message.edit(embed=discord.Embed(description='close cancelled', color=COLORS['support']), view=None)
        await interaction.response.defer()

    async def on_timeout(self):
        try:
            await self.message.edit(embed=discord.Embed(description='close cancelled — timed out', color=COLORS['support']), view=None)
        except Exception:
            pass


@bot.command(name='close')
@has_ticket_staff_perms()
async def close_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply("this isn't a ticket channel")
    if not rate_limiter.check(ctx.author.id, 'close', 3):
        return await ctx.reply('wait a few seconds')
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
        if not ticket:
            return await ctx.reply("can't find this ticket in the database")
        # Only the claimer can close
        if not ticket.get('claimed_by') or ticket['claimed_by'] != ctx.author.id:
            return await ctx.reply("only the claimer can close this ticket")
        view = CloseConfirmView(ctx, ticket)
        msg = await ctx.send(
            embed=discord.Embed(description='close this ticket? this will save a transcript and delete the channel', color=COLORS['warning']),
            view=view
        )
        view.message = msg
    except Exception as e:
        logger.error(f'close_cmd: {e}')
        await ctx.reply(f'something went wrong: {e}')


@bot.command(name='claim')
@has_ticket_staff_perms()
async def claim_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply("this isn't a ticket channel")
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
            if not ticket:           return await ctx.reply("ticket not found")
            if ticket['claimed_by']:
                claimer = ctx.guild.get_member(ticket['claimed_by'])
                return await ctx.reply(f"already claimed by {claimer.mention if claimer else 'someone'}")
            if not await ticket_has_permission(ctx, ticket):
                return await ctx.reply("you don't have the right role for this tier")
            await conn.execute(
                "UPDATE tickets SET claimed_by=$1, status='claimed' WHERE ticket_id=$2",
                ctx.author.id, ticket['ticket_id']
            )
        await lock_ticket_channel(ctx.channel, ctx.author, ctx.guild.get_member(ticket['user_id']))
        await ctx.send(embed=discord.Embed(
            description=f'claimed by {ctx.author.mention}\n\nuse `$add @user` to let someone else talk',
            color=COLORS['success']
        ))
    except Exception as e:
        await ctx.reply(f'something went wrong: {e}')


@bot.command(name='unclaim')
@has_ticket_staff_perms()
async def unclaim_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply("this isn't a ticket channel")
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
            if not ticket:               return await ctx.reply("ticket not found")
            if not ticket['claimed_by']: return await ctx.reply("this ticket isn't claimed")
            if ticket['claimed_by'] != ctx.author.id:
                return await ctx.reply("you didn't claim this ticket")
            await conn.execute(
                "UPDATE tickets SET claimed_by=NULL, status='open' WHERE ticket_id=$1",
                ticket['ticket_id']
            )
        old = ctx.guild.get_member(ticket['claimed_by'])
        await unlock_ticket_channel(ctx.channel, old)
        await ctx.send(embed=discord.Embed(
            description='ticket is open again — any staff can claim it now',
            color=COLORS['support']
        ))
    except Exception as e:
        await ctx.reply(f'something went wrong: {e}')


@bot.command(name='add')
@has_ticket_staff_perms()
async def add_cmd(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply('usage: `$add @user`')
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply("this isn't a ticket channel")
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
            if not ticket:
                return await ctx.reply("ticket not found")
            if not ticket['claimed_by']:
                return await ctx.reply("claim the ticket first before using `$add`")
            if not await ticket_has_permission(ctx, ticket):
                return await ctx.reply("only the claimer can add users")
        await ctx.channel.set_permissions(member, read_messages=True, send_messages=True)
        await ctx.reply(f'{member.mention} can now talk in this ticket')
    except Exception as e:
        await ctx.reply(f'something went wrong: {e}')


@bot.command(name='remove')
@has_ticket_staff_perms()
async def remove_cmd(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply('usage: `$remove @user`')
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply("this isn't a ticket channel")
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
            if not ticket: return await ctx.reply("ticket not found")
            if not await ticket_has_permission(ctx, ticket): return await ctx.reply("no permission")
        await ctx.channel.set_permissions(member, overwrite=None)
        await ctx.reply(f'{member.mention} removed from this ticket')
    except Exception as e:
        await ctx.reply(f'something went wrong: {e}')


@bot.command(name='rename')
@has_ticket_staff_perms()
async def rename_cmd(ctx, *, new_name: str = None):
    if not new_name: return await ctx.reply('usage: `$rename <name>`')
    if not ctx.channel.name.startswith('ticket-'): return await ctx.reply("this isn't a ticket channel")
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
            if not ticket: return await ctx.reply("ticket not found")
            if not await ticket_has_permission(ctx, ticket): return await ctx.reply("no permission")
        safe = re.sub(r'[^a-z0-9\-]', '-', new_name.lower())
        await ctx.channel.edit(name=f'ticket-{safe}')
        await ctx.reply(f'renamed to `ticket-{safe}`')
    except Exception as e:
        await ctx.reply(f'something went wrong: {e}')


@bot.command(name='transfer')
@has_ticket_staff_perms()
async def transfer_cmd(ctx, member: discord.Member = None):
    if not member: return await ctx.reply('usage: `$transfer @user`')
    if not ctx.channel.name.startswith('ticket-'): return await ctx.reply("this isn't a ticket channel")
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
            if not ticket:               return await ctx.reply("ticket not found")
            if not ticket['claimed_by']: return await ctx.reply("claim it first before transferring")
            if not await ticket_has_permission(ctx, ticket): return await ctx.reply("no permission")
            old = ctx.guild.get_member(ticket['claimed_by'])
            if old:
                await ctx.channel.set_permissions(old, read_messages=True, send_messages=False)
            await ctx.channel.set_permissions(member, read_messages=True, send_messages=True)
            await conn.execute('UPDATE tickets SET claimed_by=$1 WHERE ticket_id=$2', member.id, ticket['ticket_id'])
        await ctx.send(f'ticket transferred from {ctx.author.mention} to {member.mention}')
    except Exception as e:
        await ctx.reply(f'something went wrong: {e}')


@bot.command(name='proof')
@has_ticket_staff_perms()
async def proof_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply("this isn't a ticket channel")
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
            if not ticket: return await ctx.reply("ticket not found")
        proof_ch = ctx.guild.get_channel(HARDCODED_CHANNELS['proof'])
        if not proof_ch: return await ctx.reply("proof channel not found")
        opener = ctx.guild.get_member(ticket['user_id'])
        embed = discord.Embed(title='trade completed', color=COLORS['success'])
        embed.add_field(name='middleman', value=ctx.author.mention, inline=True)
        embed.add_field(name='tier',      value=TIER_NAMES.get(ticket.get('tier'), 'support'), inline=True)
        embed.add_field(name='client',    value=opener.mention if opener else 'unknown', inline=True)
        if ticket.get('trade_details'):
            try:
                d = ticket['trade_details'] if isinstance(ticket['trade_details'], dict) else json.loads(ticket['trade_details'])
                embed.add_field(name='trading with', value=d.get('trader', '?'), inline=False)
                embed.add_field(name='gave',         value=d.get('giving', '?'), inline=True)
                embed.add_field(name='received',     value=d.get('receiving', '?'), inline=True)
                if d.get('tip') and d['tip'] != 'None':
                    embed.add_field(name='tip', value=d['tip'], inline=True)
            except Exception:
                pass
        embed.set_footer(text=f'#{ticket["ticket_id"]}')
        await proof_ch.send(embed=embed)
        await ctx.reply(f'proof posted to {proof_ch.mention}')
    except Exception as e:
        await ctx.reply(f'something went wrong: {e}')


# ==================== SETUP & OWNER COMMANDS ====================

@bot.command(name='setup')
@is_owner()
async def setup_cmd(ctx):
    embed = discord.Embed(color=COLORS['support'])
    embed.set_author(name='Ticket Center')
    embed.description = (
        '**🎫 Support**\n'
        '╰ general questions, issues, giveaway prizes\n\n'
        '**⚖️ Middleman**\n'
        '╰ secure trade assistance\n'
        '╰ 3 tiers based on trade value\n'
        '╰ handled by verified staff only'
    )
    embed.set_footer(text='pick a category below to open a ticket')
    await ctx.send(embed=embed, view=TicketPanelView())
    try: await ctx.message.delete()
    except: pass


class RewardTypeSelect(Select):
    def __init__(self):
        super().__init__(
            placeholder='what are you claiming?',
            custom_id='reward_type_select',
            options=[
                discord.SelectOption(label='Giveaway Prize',      value='giveaway',  emoji='🎉', description='won a giveaway and need your prize'),
                discord.SelectOption(label='Invite Rewards',      value='invite',    emoji='📨', description='claiming your invite milestone reward'),
                discord.SelectOption(label='Event Reward',        value='event',     emoji='🏆', description='reward from a server event'),
                discord.SelectOption(label='Other Reward',        value='other',     emoji='🎁', description='something else not listed above'),
            ]
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(RewardClaimModal(self.values[0]))


class RewardClaimModal(Modal, title='Claim Your Reward'):
    def __init__(self, reward_type: str):
        super().__init__()
        self.reward_type = reward_type
        self.proof  = TextInput(label='Proof', placeholder='screenshot link, giveaway message link, etc.', style=discord.TextStyle.paragraph, required=True)
        self.detail = TextInput(label='What are you claiming?', placeholder='describe the reward you won', required=True)
        self.add_item(self.proof)
        self.add_item(self.detail)

    async def on_submit(self, interaction: discord.Interaction):
        if not rate_limiter.check(interaction.user.id, 'ticket_open', 10):
            return await interaction.response.send_message('slow down, wait a few seconds', ephemeral=True)
        await interaction.response.defer(ephemeral=True)
        guild, user = interaction.guild, interaction.user

        if tickets_locked.get(guild.id):
            return await interaction.followup.send('tickets are currently closed', ephemeral=True)

        async with db.pool.acquire() as conn:
            bl         = await conn.fetchrow('SELECT * FROM blacklist WHERE user_id=$1 AND guild_id=$2', user.id, guild.id)
            cfg        = await conn.fetchrow('SELECT * FROM config WHERE guild_id=$1', guild.id)
            open_count = await conn.fetchval(
                "SELECT COUNT(*) FROM tickets WHERE user_id=$1 AND guild_id=$2 AND status NOT IN ('closed')",
                user.id, guild.id
            )

        if bl:
            by     = guild.get_member(bl['blacklisted_by'])
            when   = bl['blacklisted_at'].strftime('%b %d, %Y') if bl.get('blacklisted_at') else 'unknown date'
            reason = bl['reason'] or 'no reason given'
            return await interaction.followup.send(
                f"you're blacklisted\n**reason:** {reason}\n**by:** {by.mention if by else 'staff'} on {when}",
                ephemeral=True
            )

        if open_count >= MAX_OPEN_TICKETS:
            return await interaction.followup.send(
                f"you already have {open_count} open ticket{'s' if open_count > 1 else ''}, close it first",
                ephemeral=True
            )

        if not cfg or not cfg['ticket_category_id']:
            return await interaction.followup.send("tickets aren't set up, ping staff", ephemeral=True)

        try:
            num       = await db.next_ticket_number(guild.id)
            ticket_id = f'{num:04d}'
            ch_name   = f'ticket-reward-{num:04d}-{user.name}'

            overwrites = {
                guild.default_role: discord.PermissionOverwrite(read_messages=False),
                guild.me:           discord.PermissionOverwrite(read_messages=True, send_messages=True, manage_channels=True),
                user:               discord.PermissionOverwrite(read_messages=True, send_messages=True),
            }
            staff_role = guild.get_role(HARDCODED_ROLES['staff'])
            if staff_role:
                overwrites[staff_role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)

            category = guild.get_channel(cfg['ticket_category_id'])
            channel  = await category.create_text_channel(name=ch_name, overwrites=overwrites)

            async with db.pool.acquire() as conn:
                await conn.execute(
                    'INSERT INTO tickets (ticket_id,guild_id,channel_id,user_id,ticket_type,tier,trade_details) VALUES ($1,$2,$3,$4,$5,$6,$7)',
                    ticket_id, guild.id, channel.id, user.id, 'support', 'reward',
                    json.dumps({'type': self.reward_type, 'detail': self.detail.value, 'proof': self.proof.value})
                )

            reward_labels = {
                'giveaway': '🎉 Giveaway Prize',
                'invite':   '📨 Invite Reward',
                'event':    '🏆 Event Reward',
                'other':    '🎁 Other Reward',
            }

            embed = discord.Embed(color=0xF1C40F)
            embed.set_author(name=f'{user.display_name}  •  {reward_labels.get(self.reward_type)}', icon_url=user.display_avatar.url)
            embed.description = (
                '**staff will sort this out shortly**\n\n'
                '> do not ping staff repeatedly\n'
                '> make sure your proof is valid\n'
                '> be patient'
            )
            embed.add_field(name='claiming',  value=self.detail.value, inline=False)
            embed.add_field(name='proof',     value=self.proof.value,  inline=False)
            embed.set_footer(text=f'ticket #{ticket_id}  •  {datetime.now(timezone.utc).strftime("%b %d %Y")}')

            ping = user.mention
            if staff_role:
                ping += f' {staff_role.mention}'
            await channel.send(content=ping, embed=embed, view=TicketControlView())
            await interaction.followup.send(f'ticket created → {channel.mention}', ephemeral=True)

        except Exception as e:
            logger.error(f'reward claim error: {e}')
            await interaction.followup.send(f'something went wrong: {e}', ephemeral=True)


class RewardPanelView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Claim Reward', style=ButtonStyle.success, emoji='🎁', custom_id='claim_reward_btn')
    async def claim_reward_button(self, interaction: discord.Interaction, _):
        if tickets_locked.get(interaction.guild.id):
            return await interaction.response.send_message('claims are currently closed', ephemeral=True)
        view = View(timeout=300)
        view.add_item(RewardTypeSelect())
        await interaction.response.send_message(
            embed=discord.Embed(description='what are you claiming?', color=0xF1C40F),
            view=view, ephemeral=True
        )


@bot.command(name='setuprewards')
@is_owner()
async def setuprewards_cmd(ctx):
    embed = discord.Embed(color=0xF1C40F)
    embed.set_author(name='Claim Center')
    embed.description = (
        '**🎉 Giveaway Prizes**\n'
        '╰ won a giveaway? claim your prize here\n\n'
        '**📨 Invite Rewards**\n'
        '╰ hit an invite milestone? grab your reward\n\n'
        '**🏆 Event Rewards**\n'
        '╰ participated in an event and won?\n\n'
        '**🎁 Other**\n'
        '╰ anything else reward related'
    )
    embed.set_footer(text='click below to open a claim ticket')
    await ctx.send(embed=embed, view=RewardPanelView())
    try: await ctx.message.delete()
    except: pass


@bot.command(name='setcategory')
@is_owner()
async def setcategory_cmd(ctx, category: discord.CategoryChannel = None):
    if not category: return await ctx.reply('usage: `$setcategory #Tickets`')
    async with db.pool.acquire() as conn:
        await conn.execute(
            'INSERT INTO config (guild_id, ticket_category_id) VALUES ($1,$2) ON CONFLICT (guild_id) DO UPDATE SET ticket_category_id=$2',
            ctx.guild.id, category.id
        )
    await ctx.reply(f'ticket category set to {category.mention}')


@bot.command(name='setlogs')
@is_owner()
async def setlogs_cmd(ctx, channel: discord.TextChannel = None):
    if not channel: return await ctx.reply('usage: `$setlogs #logs`')
    async with db.pool.acquire() as conn:
        await conn.execute(
            'INSERT INTO config (guild_id, log_channel_id) VALUES ($1,$2) ON CONFLICT (guild_id) DO UPDATE SET log_channel_id=$2',
            ctx.guild.id, channel.id
        )
    await ctx.reply(f'log channel set to {channel.mention}')


@bot.command(name='config')
@is_owner()
async def config_cmd(ctx):
    async with db.pool.acquire() as conn:
        cfg = await conn.fetchrow('SELECT * FROM config WHERE guild_id=$1', ctx.guild.id)
    embed = discord.Embed(title='bot config', color=COLORS['support'])
    if cfg:
        cat = ctx.guild.get_channel(cfg['ticket_category_id']) if cfg.get('ticket_category_id') else None
        log = ctx.guild.get_channel(cfg['log_channel_id'])     if cfg.get('log_channel_id')     else None
        embed.add_field(name='category',  value=cat.mention if cat else 'not set', inline=True)
        embed.add_field(name='logs',      value=log.mention if log else 'not set', inline=True)
        embed.add_field(name='tickets',   value=str(cfg.get('ticket_counter', 0)),  inline=True)
    else:
        embed.description = 'not configured yet — use `$setcategory` and `$setlogs`'
    proof_ch = ctx.guild.get_channel(HARDCODED_CHANNELS['proof'])
    embed.add_field(name='proof channel', value=proof_ch.mention if proof_ch else 'not found', inline=True)
    embed.add_field(name='lock status',   value='🔒 locked' if tickets_locked.get(ctx.guild.id) else '🔓 open', inline=True)
    await ctx.reply(embed=embed)


@bot.command(name='lock')
@is_owner()
async def lock_cmd(ctx):
    tickets_locked[ctx.guild.id] = True
    await ctx.reply('tickets are now **locked** — nobody can open new ones until you run `$unlock`')


@bot.command(name='unlock')
@is_owner()
async def unlock_cmd(ctx):
    tickets_locked[ctx.guild.id] = False
    await ctx.reply('tickets are **open** again')


@bot.command(name='blacklist')
@is_owner()
async def blacklist_cmd(ctx, member: discord.Member = None, *, reason: str = 'no reason given'):
    if not member: return await ctx.reply('usage: `$blacklist @user <reason>`')
    async with db.pool.acquire() as conn:
        await conn.execute(
            'INSERT INTO blacklist (user_id, guild_id, reason, blacklisted_by) VALUES ($1,$2,$3,$4) ON CONFLICT (user_id, guild_id) DO UPDATE SET reason=$3, blacklisted_by=$4, blacklisted_at=NOW()',
            member.id, ctx.guild.id, reason, ctx.author.id
        )
    await ctx.reply(f'{member.mention} blacklisted — **reason:** {reason}')


@bot.command(name='unblacklist')
@is_owner()
async def unblacklist_cmd(ctx, member: discord.Member = None):
    if not member: return await ctx.reply('usage: `$unblacklist @user`')
    async with db.pool.acquire() as conn:
        await conn.execute('DELETE FROM blacklist WHERE user_id=$1 AND guild_id=$2', member.id, ctx.guild.id)
    await ctx.reply(f'{member.mention} removed from the blacklist')


@bot.command(name='blacklists')
@is_owner()
async def blacklists_cmd(ctx):
    async with db.pool.acquire() as conn:
        rows = await conn.fetch('SELECT * FROM blacklist WHERE guild_id=$1 ORDER BY blacklisted_at DESC', ctx.guild.id)
    if not rows:
        return await ctx.reply('nobody is blacklisted')
    lines = []
    for r in rows:
        m    = ctx.guild.get_member(r['user_id'])
        by   = ctx.guild.get_member(r['blacklisted_by'])
        when = r['blacklisted_at'].strftime('%b %d') if r.get('blacklisted_at') else '?'
        lines.append(f'**{m.display_name if m else r["user_id"]}** — {r["reason"]} *(by {by.display_name if by else "?"} on {when})*')
    embed = discord.Embed(title=f'blacklist  •  {len(rows)} user{"s" if len(rows) > 1 else ""}', description='\n'.join(lines), color=COLORS['error'])
    await ctx.reply(embed=embed)


# ==================== HELP ====================

@bot.command(name='help')
async def help_cmd(ctx):
    embed = discord.Embed(title='commands', color=COLORS['support'])
    embed.add_field(
        name='🎫 ticket  •  staff & middleman only',
        value=(
            '`$claim` — claim a ticket\n'
            '`$unclaim` — drop your claim\n'
            '`$add @user` — let someone talk\n'
            '`$remove @user` — remove someone\n'
            '`$close` — close & save transcript\n'
            '`$rename <n>` — rename the channel\n'
            '`$transfer @user` — hand off your claim\n'
            '`$proof` — post trade proof'
        ),
        inline=False
    )
    embed.add_field(
        name='⚙️ setup  •  owner only',
        value=(
            '`$setup` — post ticket panel\n'
            '`$setcategory` — set ticket category\n'
            '`$setlogs` — set log channel\n'
            '`$config` — view current config\n'
            '`$lock` / `$unlock` — open or close tickets\n'
            '`$blacklist @user <reason>`\n'
            '`$unblacklist @user`\n'
            '`$blacklists`'
        ),
        inline=False
    )
    await ctx.reply(embed=embed)


# ==================== EVENTS ====================

@bot.event
async def on_ready():
    logger.info(f'online as {bot.user}')
    try:
        await db.connect()
    except Exception as e:
        logger.error(f'db failed: {e}')
        return
    bot.add_view(TicketPanelView())
    bot.add_view(TicketControlView())
    bot.add_view(RewardPanelView())
    await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name='tickets'))
    logger.info(f'serving {len(bot.guilds)} guild(s)')


@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return
    if isinstance(error, commands.CheckFailure):
        await ctx.reply("you can't use that")
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.reply(f'missing: `{error.param.name}`')
    else:
        logger.error(f'{ctx.command}: {error}')


# ==================== WEB SERVER ====================

async def web_server():
    async def handle(request):
        return aiohttp.web.Response(text='OK')
    app = aiohttp.web.Application()
    app.router.add_get('/', handle)
    app.router.add_get('/health', handle)
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv('PORT', 8080))
    site = aiohttp.web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f'web server on :{port}')


async def main():
    async with bot:
        await asyncio.gather(
            web_server(),
            bot.start(os.getenv('BOT_TOKEN', ''))
        )

if __name__ == '__main__':
    asyncio.run(main())
