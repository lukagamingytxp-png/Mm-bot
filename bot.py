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
import aiohttp
import aiohttp.web
import asyncpg
from discord import ButtonStyle
from discord.ui import View, Select, Modal, TextInput
from discord.ext import commands
from discord import app_commands

logging.basicConfig(level=logging.INFO, format='%(asctime)s  %(levelname)s  %(message)s')
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ constants

OWNER_ID = 1029438856069656576

ROLES = {
    'staff':    1432081794647199895,
    'lowtier':  1453757017218093239,
    'midtier':  1434610759140118640,
    'hightier': 1453757157144137911,
}

PROOF_CHANNEL   = 1472695529883435091
WELCOME_CHANNEL = 1472691302402359460
INVITE_CHANNEL  = 1478500573304193134
VERIFIED_ROLE   = 1447695298272166090
UNVERIFIED_ROLE = 1455440583316475989
MEMBER_ROLE     = 1438945860410151076
VERIFY_CHANNEL  = 1447694834742595745

TIER_COLOR = {
    'lowtier':  0x57F287,
    'midtier':  0xFEE75C,
    'hightier': 0xED4245,
    'support':  0x5865F2,
    'reward':   0xF1C40F,
}

TIER_LABEL = {
    'lowtier':  'Low Tier  •  100–900 RBX',
    'midtier':  'Mid Tier  •  1K–3K RBX',
    'hightier': 'High Tier  •  3.1K–5K+ RBX',
    'support':  'Support',
    'reward':   'Reward Claim',
}

TIER_SLUG = {
    'lowtier':  '100-900rbx',
    'midtier':  '1k-3krbx',
    'hightier': '3k-5krbx',
    'support':  'support',
    'reward':   'reward',
}

MAX_OPEN        = 1
tickets_locked  = {}
verify_config   = {}
welcome_config  = {}
captchas        = {}
invite_cache    = {}  # guild_id -> {code: uses}


# ------------------------------------------------------------------ rate limiter

class RateLimiter:
    def __init__(self):
        self._b = defaultdict(dict)

    def check(self, uid: int, action: str, cd: int) -> bool:
        now  = datetime.now(timezone.utc).timestamp()
        last = self._b[action].get(uid, 0)
        if now - last < cd:
            return False
        self._b[action][uid] = now
        return True

limiter = RateLimiter()


# ------------------------------------------------------------------ database

class Database:
    pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        url = os.getenv('DATABASE_URL', '')
        if not url:
            raise RuntimeError('DATABASE_URL not set')
        if url.startswith('postgres://'):
            url = 'postgresql://' + url[len('postgres://'):]
        self.pool = await asyncpg.create_pool(
            url, min_size=2, max_size=10,
            command_timeout=15,
            max_inactive_connection_lifetime=300
        )
        await self._setup()
        logger.info('database ready')

    async def _setup(self):
        async with self.pool.acquire() as c:
            await c.execute('''
                CREATE TABLE IF NOT EXISTS config (
                    guild_id           BIGINT PRIMARY KEY,
                    ticket_category_id BIGINT,
                    log_channel_id     BIGINT,
                    ticket_counter     INT DEFAULT 0
                )
            ''')
            await c.execute('''
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
            await c.execute('''
                CREATE TABLE IF NOT EXISTS blacklist (
                    user_id        BIGINT,
                    guild_id       BIGINT,
                    reason         TEXT,
                    blacklisted_by BIGINT,
                    created_at     TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (user_id, guild_id)
                )
            ''')
            await c.execute('''
                CREATE TABLE IF NOT EXISTS invite_stats (
                    guild_id   BIGINT,
                    inviter_id BIGINT,
                    joins      INT DEFAULT 0,
                    leaves     INT DEFAULT 0,
                    fake       INT DEFAULT 0,
                    rejoins    INT DEFAULT 0,
                    verified   INT DEFAULT 0,
                    PRIMARY KEY (guild_id, inviter_id)
                )
            ''')
            await c.execute('''
                CREATE TABLE IF NOT EXISTS member_invites (
                    guild_id   BIGINT,
                    user_id    BIGINT,
                    inviter_id BIGINT,
                    joined_at  TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (guild_id, user_id)
                )
            ''')
            await c.execute('''
                CREATE TABLE IF NOT EXISTS member_left (
                    guild_id BIGINT,
                    user_id  BIGINT,
                    left_at  TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (guild_id, user_id)
                )
            ''')
            for sql in [
                'ALTER TABLE config ADD COLUMN IF NOT EXISTS ticket_counter INT DEFAULT 0',
                'ALTER TABLE blacklist ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()',
                'ALTER TABLE config ADD COLUMN IF NOT EXISTS verify_unverified_id BIGINT',
                'ALTER TABLE config ADD COLUMN IF NOT EXISTS verify_verified_id BIGINT',
                'ALTER TABLE config ADD COLUMN IF NOT EXISTS verify_member_id BIGINT',
                'ALTER TABLE config ADD COLUMN IF NOT EXISTS verify_channel_id BIGINT',
                'ALTER TABLE config ADD COLUMN IF NOT EXISTS welcome_channel_id BIGINT',
                'ALTER TABLE invite_stats ADD COLUMN IF NOT EXISTS rejoins INT DEFAULT 0',
                'ALTER TABLE member_invites ADD COLUMN IF NOT EXISTS is_rejoin BOOLEAN DEFAULT FALSE',
                """CREATE TABLE IF NOT EXISTS giveaways (
                    id               SERIAL PRIMARY KEY,
                    guild_id         BIGINT,
                    channel_id       BIGINT,
                    message_id       BIGINT,
                    prize            TEXT,
                    winners_count    INT DEFAULT 1,
                    host_id          BIGINT,
                    required_role_id BIGINT,
                    image_url        TEXT,
                    ends_at          TIMESTAMPTZ,
                    ended            BOOLEAN DEFAULT FALSE
                )""",
                """CREATE TABLE IF NOT EXISTS giveaway_entries (
                    id           SERIAL PRIMARY KEY,
                    giveaway_id  INT REFERENCES giveaways(id) ON DELETE CASCADE,
                    user_id      BIGINT
                )""",
                """CREATE TABLE IF NOT EXISTS giveaway_last_winners (
                    guild_id   BIGINT,
                    prize      TEXT,
                    winner_ids BIGINT[],
                    PRIMARY KEY (guild_id, prize)
                )""",
                """CREATE TABLE IF NOT EXISTS giveaway_bonus_roles (
                    guild_id  BIGINT,
                    role_id   BIGINT,
                    entries   INT DEFAULT 1,
                    PRIMARY KEY (guild_id, role_id)
                )""",
                'ALTER TABLE invite_stats ADD COLUMN IF NOT EXISTS verified INT DEFAULT 0',
            ]:
                try:
                    await c.execute(sql)
                except Exception:
                    pass

    async def next_num(self, guild_id: int) -> int:
        async with self.pool.acquire() as c:
            await c.execute(
                '''INSERT INTO config (guild_id, ticket_counter) VALUES ($1, 1)
                   ON CONFLICT (guild_id) DO UPDATE SET ticket_counter = config.ticket_counter + 1''',
                guild_id
            )
            row = await c.fetchrow('SELECT ticket_counter FROM config WHERE guild_id = $1', guild_id)
            return row['ticket_counter']

db = Database()


# ------------------------------------------------------------------ bot setup

intents = discord.Intents.default()
intents.message_content = True
intents.members         = True
intents.guilds          = True

bot = commands.Bot(command_prefix='$', intents=intents, help_command=None)


# ------------------------------------------------------------------ permission checks

def owner_only():
    async def pred(ctx):
        return ctx.author.id == OWNER_ID
    return commands.check(pred)

def staff_only():
    async def pred(ctx):
        return _is_staff(ctx.author)
    return commands.check(pred)

def _is_staff(member: discord.Member) -> bool:
    if member.guild_permissions.administrator:
        return True
    for key in ('staff', 'lowtier', 'midtier', 'hightier'):
        r = member.guild.get_role(ROLES[key])
        if r and r in member.roles:
            return True
    return False

async def _can_manage(ctx, ticket) -> bool:
    ttype = ticket['ticket_type']
    tier  = ticket.get('tier')
    # middleman claimed tickets: ONLY the claimer can manage, not even admins
    if ttype == 'middleman' and ticket.get('claimed_by'):
        return ticket['claimed_by'] == ctx.author.id
    # support/reward: admins can always manage
    if ctx.author.guild_permissions.administrator:
        return True
    if ttype == 'support':
        r = ctx.guild.get_role(ROLES['staff'])
        if r and r in ctx.author.roles:
            return True
    elif ttype == 'middleman' and tier in ROLES:
        # unclaimed mm ticket — any tier/staff can claim
        r = ctx.guild.get_role(ROLES[tier])
        if r and r in ctx.author.roles:
            return True
    if ticket.get('claimed_by') == ctx.author.id:
        return True
    return False


# ------------------------------------------------------------------ helpers

async def send_log(guild, title, desc=None, color=0x5865F2, fields=None):
    try:
        async with db.pool.acquire() as c:
            cfg = await c.fetchrow('SELECT log_channel_id FROM config WHERE guild_id = $1', guild.id)
        if not cfg or not cfg['log_channel_id']:
            return
        ch = guild.get_channel(cfg['log_channel_id'])
        if not ch:
            return
        e = discord.Embed(title=title, color=color)
        if desc:
            e.description = desc
        for k, v in (fields or {}).items():
            e.add_field(name=k, value=str(v), inline=True)
        await ch.send(embed=e)
    except Exception as ex:
        logger.error(f'log error: {ex}')

async def claim_lock(channel, claimer, creator=None, ticket_type='middleman'):
    if ticket_type != 'middleman':
        return
    for key in ('staff', 'lowtier', 'midtier', 'hightier'):
        r = channel.guild.get_role(ROLES[key])
        if r:
            ow = channel.overwrites_for(r)
            ow.send_messages = False
            await channel.set_permissions(r, overwrite=ow)
    await channel.set_permissions(claimer, read_messages=True, send_messages=True)
    if creator and creator.id != claimer.id:
        await channel.set_permissions(creator, read_messages=True, send_messages=True)

async def claim_unlock(channel, old_claimer=None, ticket_type='middleman'):
    if ticket_type != 'middleman':
        return
    for key in ('staff', 'lowtier', 'midtier', 'hightier'):
        r = channel.guild.get_role(ROLES[key])
        if r:
            ow = channel.overwrites_for(r)
            ow.send_messages = True
            await channel.set_permissions(r, overwrite=ow)
    if old_claimer:
        await channel.set_permissions(old_claimer, read_messages=True, send_messages=None)

def make_ticket_embed(user, tier, ticket_id, extra_fields=None, desc=None):
    color = TIER_COLOR.get(tier, 0x5865F2)
    e = discord.Embed(color=color)
    e.set_author(name=f'{user.display_name}  •  {TIER_LABEL.get(tier, tier)}', icon_url=user.display_avatar.url)
    e.description = desc or '👋 hey! a staff member will be with you shortly\n\nplease be patient and avoid pinging anyone'
    if extra_fields:
        for name, value, inline in extra_fields:
            e.add_field(name=name, value=value, inline=inline)
    e.set_footer(text=f'ticket #{ticket_id}')
    return e

async def pre_open_checks(interaction, guild, user):
    if tickets_locked.get(guild.id):
        e = discord.Embed(description='🔒 tickets are currently closed, check back later', color=0xED4245)
        await interaction.followup.send(embed=e, ephemeral=True)
        return False
    async with db.pool.acquire() as c:
        bl      = await c.fetchrow('SELECT * FROM blacklist WHERE user_id = $1 AND guild_id = $2', user.id, guild.id)
        cfg     = await c.fetchrow('SELECT * FROM config WHERE guild_id = $1', guild.id)
        tickets = await c.fetch(
            "SELECT ticket_id, channel_id FROM tickets WHERE user_id = $1 AND guild_id = $2 AND status != 'closed'",
            user.id, guild.id
        )
        ghost_ids = [t['ticket_id'] for t in tickets if guild.get_channel(t['channel_id']) is None]
        real_open = len(tickets) - len(ghost_ids)
        if ghost_ids:
            await c.execute(
                "UPDATE tickets SET status = 'closed' WHERE ticket_id = ANY($1::text[])",
                ghost_ids
            )
    if bl:
        by     = guild.get_member(bl['blacklisted_by'])
        date   = bl['created_at'].strftime('%b %d, %Y') if bl.get('created_at') else 'unknown'
        reason = bl['reason'] or 'no reason given'
        e = discord.Embed(title='🚫 you\'re blacklisted', color=0xED4245)
        e.add_field(name='Reason', value=reason, inline=False)
        e.add_field(name='By', value=by.mention if by else 'staff', inline=True)
        e.add_field(name='Date', value=date, inline=True)
        e.set_footer(text='if you think this is wrong, dm a staff member')
        await interaction.followup.send(embed=e, ephemeral=True)
        return False
    if real_open >= MAX_OPEN:
        e = discord.Embed(description='⚠️ you already have an open ticket — close it before making a new one', color=0xFEE75C)
        await interaction.followup.send(embed=e, ephemeral=True)
        return False
    if not cfg or not cfg['ticket_category_id']:
        e = discord.Embed(description='⚠️ tickets aren\'t configured yet, ping a staff member', color=0xFEE75C)
        await interaction.followup.send(embed=e, ephemeral=True)
        return False
    return cfg

async def save_transcript(channel, ticket, closer):
    async with db.pool.acquire() as c:
        cfg = await c.fetchrow('SELECT log_channel_id FROM config WHERE guild_id = $1', channel.guild.id)
    if not cfg or not cfg['log_channel_id']:
        return
    lc = channel.guild.get_channel(cfg['log_channel_id'])
    if not lc:
        return
    opener  = channel.guild.get_member(ticket['user_id'])
    claimer = channel.guild.get_member(ticket['claimed_by']) if ticket['claimed_by'] else None
    header  = '\n'.join([
        'TRANSCRIPT',
        '=' * 48,
        f"ticket:     {ticket['ticket_id']}",
        f"opened by:  {opener.name if opener else ticket['user_id']}",
        f"claimed by: {claimer.name if claimer else 'nobody'}",
        f"closed by:  {closer.name}",
        '=' * 48,
        '',
    ])
    msgs = []
    async for m in channel.history(limit=500, oldest_first=True):
        msgs.append(f"[{m.created_at.strftime('%H:%M:%S')}] {m.author.name}: {m.content or '[embed/file]'}")
    file = discord.File(
        fp=io.BytesIO((header + '\n'.join(msgs)).encode()),
        filename=f"transcript-{ticket['ticket_id']}.txt"
    )
    e = discord.Embed(title='📋 ticket closed', color=0xED4245)
    e.add_field(name='Ticket', value=f"#{ticket['ticket_id']}", inline=True)
    e.add_field(name='Opened by', value=opener.mention if opener else '?', inline=True)
    e.add_field(name='Closed by', value=closer.mention, inline=True)
    await lc.send(embed=e, file=file)


# ================================================================== UI


class MiddlemanModal(Modal, title='Middleman Request'):
    def __init__(self, tier):
        super().__init__()
        self.tier      = tier
        self.trader    = TextInput(label='Who are you trading with?', placeholder='their username or ID', required=True)
        self.giving    = TextInput(label='What are you giving?', placeholder='e.g. 1 garam', style=discord.TextStyle.paragraph, required=True)
        self.receiving = TextInput(label='What are you receiving?', placeholder='e.g. 500 Robux', style=discord.TextStyle.paragraph, required=True)
        self.tip       = TextInput(label='Leaving a tip? (optional)', placeholder='leave blank to skip', required=False)
        self.add_item(self.trader)
        self.add_item(self.giving)
        self.add_item(self.receiving)
        self.add_item(self.tip)

    async def on_submit(self, interaction: discord.Interaction):
        if not limiter.check(interaction.user.id, 'open', 10):
            return await interaction.response.send_message(
                embed=discord.Embed(description='⏳ slow down a bit, wait a few seconds', color=0xFEE75C),
                ephemeral=True
            )
        try:
            await interaction.response.defer(ephemeral=True)
        except Exception:
            return
        guild, user = interaction.guild, interaction.user
        try:
            cfg = await pre_open_checks(interaction, guild, user)
        except Exception as ex:
            logger.error(f'pre_open_checks: {ex}')
            try:
                await interaction.followup.send(
                    embed=discord.Embed(description='❌ something went wrong, try again', color=0xED4245),
                    ephemeral=True
                )
            except Exception:
                pass
            return
        if not cfg:
            return
        try:
            num      = await db.next_num(guild.id)
            tid      = f'{num:04d}'
            slug     = TIER_SLUG.get(self.tier, self.tier)
            ch_name  = f'ticket-{slug}-{tid}-{user.name}'
            category = guild.get_channel(cfg['ticket_category_id'])
            role_id  = ROLES.get(self.tier)
            staff_r  = guild.get_role(ROLES['staff'])
            tier_r   = guild.get_role(role_id) if role_id else None

            overwrites = {
                guild.default_role: discord.PermissionOverwrite(read_messages=False),
                guild.me:           discord.PermissionOverwrite(read_messages=True, send_messages=True, manage_channels=True),
                user:               discord.PermissionOverwrite(read_messages=True, send_messages=True),
            }
            if staff_r:
                overwrites[staff_r] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
            if tier_r:
                overwrites[tier_r] = discord.PermissionOverwrite(read_messages=True, send_messages=True)

            channel = await category.create_text_channel(name=ch_name, overwrites=overwrites)
            trade   = {
                'trader':    self.trader.value,
                'giving':    self.giving.value,
                'receiving': self.receiving.value,
                'tip':       self.tip.value or None,
            }
            async with db.pool.acquire() as c:
                await c.execute(
                    'INSERT INTO tickets (ticket_id, guild_id, channel_id, user_id, ticket_type, tier, trade_details) VALUES ($1,$2,$3,$4,$5,$6,$7)',
                    tid, guild.id, channel.id, user.id, 'middleman', self.tier, json.dumps(trade)
                )
            fields = [
                ('Trading with', trade['trader'],    False),
                ('Giving',       trade['giving'],    True),
                ('Receiving',    trade['receiving'], True),
            ]
            if trade['tip']:
                fields.append(('Tip', trade['tip'], True))
            e = make_ticket_embed(user, self.tier, tid, fields)
            ping = user.mention
            if tier_r:
                ping += f' {tier_r.mention}'
            await channel.send(content=ping, embed=e, view=ControlView())
            await interaction.followup.send(
                embed=discord.Embed(description=f'✅ ticket created — {channel.mention}', color=0x57F287),
                ephemeral=True
            )
            await send_log(guild, '🎫 ticket opened', f'{user.mention} opened a {TIER_LABEL[self.tier]} ticket', TIER_COLOR[self.tier])
        except Exception as ex:
            logger.error(f'middleman open: {ex}')
            try:
                await interaction.followup.send(
                    embed=discord.Embed(description=f'❌ something went wrong — {ex}', color=0xED4245),
                    ephemeral=True
                )
            except Exception:
                pass


class TierSelect(Select):
    def __init__(self):
        super().__init__(
            placeholder='pick your trade value range',
            options=[
                discord.SelectOption(label='Low Tier  •  100–900 RBX',   value='lowtier',  emoji='💎', description='fast trades under 900 robux'),
                discord.SelectOption(label='Mid Tier  •  1K–3K RBX',     value='midtier',  emoji='💰', description='trades between 1,000 and 3,000 robux'),
                discord.SelectOption(label='High Tier  •  3.1K–5K+ RBX', value='hightier', emoji='💸', description='3,100 robux and above, senior staff only'),
            ]
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(MiddlemanModal(self.values[0]))


class RewardModal(Modal, title='Claim a Reward'):
    def __init__(self, reward_type):
        super().__init__()
        self.rtype = reward_type
        self.what  = TextInput(label='What are you claiming?', placeholder='describe what you won', required=True)
        self.proof = TextInput(label='Proof', placeholder='message link, screenshot link, etc.', style=discord.TextStyle.paragraph, required=True)
        self.add_item(self.what)
        self.add_item(self.proof)

    async def on_submit(self, interaction: discord.Interaction):
        if not limiter.check(interaction.user.id, 'open', 10):
            return await interaction.response.send_message(
                embed=discord.Embed(description='⏳ slow down a bit, wait a few seconds', color=0xFEE75C),
                ephemeral=True
            )
        try:
            await interaction.response.defer(ephemeral=True)
        except Exception:
            return
        guild, user = interaction.guild, interaction.user
        try:
            cfg = await pre_open_checks(interaction, guild, user)
        except Exception as ex:
            logger.error(f'pre_open_checks: {ex}')
            try:
                await interaction.followup.send(
                    embed=discord.Embed(description='❌ something went wrong, try again', color=0xED4245),
                    ephemeral=True
                )
            except Exception:
                pass
            return
        if not cfg:
            return
        try:
            num      = await db.next_num(guild.id)
            tid      = f'{num:04d}'
            ch_name  = f'ticket-reward-{tid}-{user.name}'
            category = guild.get_channel(cfg['ticket_category_id'])
            staff_r  = guild.get_role(ROLES['staff'])

            overwrites = {
                guild.default_role: discord.PermissionOverwrite(read_messages=False),
                guild.me:           discord.PermissionOverwrite(read_messages=True, send_messages=True, manage_channels=True),
                user:               discord.PermissionOverwrite(read_messages=True, send_messages=True),
            }
            if staff_r:
                overwrites[staff_r] = discord.PermissionOverwrite(read_messages=True, send_messages=True)

            channel = await category.create_text_channel(name=ch_name, overwrites=overwrites)
            data    = {'type': self.rtype, 'what': self.what.value, 'proof': self.proof.value}

            async with db.pool.acquire() as c:
                await c.execute(
                    'INSERT INTO tickets (ticket_id, guild_id, channel_id, user_id, ticket_type, tier, trade_details) VALUES ($1,$2,$3,$4,$5,$6,$7)',
                    tid, guild.id, channel.id, user.id, 'support', 'reward', json.dumps(data)
                )

            fields = [
                ('Claiming', self.what.value,  False),
                ('Proof',    self.proof.value, False),
            ]
            e = make_ticket_embed(user, 'reward', tid, fields)
            ping = user.mention
            if staff_r:
                ping += f' {staff_r.mention}'
            await channel.send(content=ping, embed=e, view=ControlView())
            await interaction.followup.send(
                embed=discord.Embed(description=f'✅ ticket created — {channel.mention}', color=0x57F287),
                ephemeral=True
            )
        except Exception as ex:
            logger.error(f'reward open: {ex}')
            try:
                await interaction.followup.send(
                    embed=discord.Embed(description=f'❌ something went wrong — {ex}', color=0xED4245),
                    ephemeral=True
                )
            except Exception:
                pass


class RewardSelect(Select):
    def __init__(self):
        super().__init__(
            placeholder='what kind of reward?',
            custom_id='reward_type_select',
            options=[
                discord.SelectOption(label='Giveaway Prize',  value='giveaway', emoji='🎉', description='you won a giveaway'),
                discord.SelectOption(label='Invite Reward',   value='invite',   emoji='📨', description='invite milestone reward'),
                discord.SelectOption(label='Event Reward',    value='event',    emoji='🏆', description='reward from a server event'),
                discord.SelectOption(label='Bonus Reward',    value='bonus',    emoji='💰', description='activity bonus or special reward'),
                discord.SelectOption(label='Other',           value='other',    emoji='🎁', description='anything else reward related'),
            ]
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(RewardModal(self.values[0]))


class TicketPanel(View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Support', style=ButtonStyle.primary, emoji='🎫', custom_id='btn_support')
    async def support(self, interaction: discord.Interaction, _):
        if not limiter.check(interaction.user.id, 'open', 10):
            return await interaction.response.send_message(
                embed=discord.Embed(description='⏳ slow down a bit, wait a few seconds', color=0xFEE75C),
                ephemeral=True
            )
        try:
            await interaction.response.defer(ephemeral=True)
        except Exception:
            return
        guild, user = interaction.guild, interaction.user
        try:
            cfg = await pre_open_checks(interaction, guild, user)
        except Exception as ex:
            logger.error(f'pre_open_checks: {ex}')
            try:
                await interaction.followup.send(
                    embed=discord.Embed(description='❌ something went wrong, try again', color=0xED4245),
                    ephemeral=True
                )
            except Exception:
                pass
            return
        if not cfg:
            return
        try:
            num     = await db.next_num(guild.id)
            tid     = f'{num:04d}'
            ch_name = f'ticket-support-{tid}-{user.name}'
            cat     = guild.get_channel(cfg['ticket_category_id'])
            staff_r = guild.get_role(ROLES['staff'])

            overwrites = {
                guild.default_role: discord.PermissionOverwrite(read_messages=False),
                guild.me:           discord.PermissionOverwrite(read_messages=True, send_messages=True, manage_channels=True),
                user:               discord.PermissionOverwrite(read_messages=True, send_messages=True),
            }
            if staff_r:
                overwrites[staff_r] = discord.PermissionOverwrite(read_messages=True, send_messages=True)

            channel = await cat.create_text_channel(name=ch_name, overwrites=overwrites)
            async with db.pool.acquire() as c:
                await c.execute(
                    'INSERT INTO tickets (ticket_id, guild_id, channel_id, user_id, ticket_type, tier) VALUES ($1,$2,$3,$4,$5,$6)',
                    tid, guild.id, channel.id, user.id, 'support', 'support'
                )
            e = make_ticket_embed(user, 'support', tid)
            ping = user.mention
            if staff_r:
                ping += f' {staff_r.mention}'
            await channel.send(content=ping, embed=e, view=ControlView())
            await interaction.followup.send(
                embed=discord.Embed(description=f'✅ ticket created — {channel.mention}', color=0x57F287),
                ephemeral=True
            )
        except Exception as ex:
            logger.error(f'support open: {ex}')
            try:
                await interaction.followup.send(
                    embed=discord.Embed(description=f'❌ something went wrong — {ex}', color=0xED4245),
                    ephemeral=True
                )
            except Exception:
                pass

    @discord.ui.button(label='Middleman', style=ButtonStyle.success, emoji='⚖️', custom_id='btn_middleman')
    async def middleman(self, interaction: discord.Interaction, _):
        if tickets_locked.get(interaction.guild.id):
            return await interaction.response.send_message(
                embed=discord.Embed(description='🔒 tickets are currently closed', color=0xED4245),
                ephemeral=True
            )
        v = View(timeout=300)
        v.add_item(TierSelect())
        await interaction.response.send_message(
            embed=discord.Embed(description='💰 pick the tier that matches your trade value', color=TIER_COLOR['support']),
            view=v, ephemeral=True
        )


class RewardPanel(View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Claim Reward', style=ButtonStyle.success, emoji='🎁', custom_id='btn_reward')
    async def claim(self, interaction: discord.Interaction, _):
        if tickets_locked.get(interaction.guild.id):
            return await interaction.response.send_message(
                embed=discord.Embed(description='🔒 claims are currently closed', color=0xED4245),
                ephemeral=True
            )
        v = View(timeout=300)
        v.add_item(RewardSelect())
        await interaction.response.send_message(
            embed=discord.Embed(description='🎁 what are you here to claim?', color=TIER_COLOR['reward']),
            view=v, ephemeral=True
        )


class ControlView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Claim', style=ButtonStyle.green, emoji='✋', custom_id='btn_claim')
    async def claim(self, interaction: discord.Interaction, _):
        if not limiter.check(interaction.user.id, 'claim', 2):
            return await interaction.response.send_message(
                embed=discord.Embed(description='⏳ slow down', color=0xFEE75C), ephemeral=True
            )
        if not _is_staff(interaction.user):
            return await interaction.response.send_message(
                embed=discord.Embed(description='❌ you need a staff role to claim tickets', color=0xED4245), ephemeral=True
            )
        async with db.pool.acquire() as c:
            ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', interaction.channel.id)
            if not ticket:
                return await interaction.response.send_message(
                    embed=discord.Embed(description='❌ no ticket found here', color=0xED4245), ephemeral=True
                )
            if ticket['claimed_by']:
                who = interaction.guild.get_member(ticket['claimed_by'])
                return await interaction.response.send_message(
                    embed=discord.Embed(description=f'❌ already claimed by {who.mention if who else "someone"}', color=0xED4245),
                    ephemeral=True
                )
            await c.execute(
                "UPDATE tickets SET claimed_by = $1, status = 'claimed' WHERE ticket_id = $2",
                interaction.user.id, ticket['ticket_id']
            )
        creator = interaction.guild.get_member(ticket['user_id'])
        await claim_lock(interaction.channel, interaction.user, creator, ticket['ticket_type'])
        e = discord.Embed(color=0x57F287)
        e.description = f'✅ claimed by {interaction.user.mention}\n\nuse `$add @user` to let someone else talk in here'
        await interaction.response.send_message(embed=e)
        await send_log(interaction.guild, '✋ ticket claimed',
            f'{interaction.user.mention} claimed ticket #{ticket["ticket_id"]}', 0x57F287)

    @discord.ui.button(label='Unclaim', style=ButtonStyle.gray, emoji='↩️', custom_id='btn_unclaim')
    async def unclaim(self, interaction: discord.Interaction, _):
        if not limiter.check(interaction.user.id, 'unclaim', 2):
            return await interaction.response.send_message(
                embed=discord.Embed(description='⏳ slow down', color=0xFEE75C), ephemeral=True
            )
        async with db.pool.acquire() as c:
            ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', interaction.channel.id)
            if not ticket:
                return await interaction.response.send_message(
                    embed=discord.Embed(description='❌ no ticket found here', color=0xED4245), ephemeral=True
                )
            if not ticket['claimed_by']:
                return await interaction.response.send_message(
                    embed=discord.Embed(description="❌ this ticket hasn't been claimed", color=0xED4245), ephemeral=True
                )
            is_mm      = ticket['ticket_type'] == 'middleman'
            is_claimer = ticket['claimed_by'] == interaction.user.id
            is_admin   = interaction.user.guild_permissions.administrator
            if is_mm and not is_claimer:
                return await interaction.response.send_message(
                    embed=discord.Embed(description="❌ only the middleman who claimed this can unclaim it", color=0xED4245), ephemeral=True
                )
            if not is_mm and not is_claimer and not is_admin:
                return await interaction.response.send_message(
                    embed=discord.Embed(description="❌ you didn't claim this", color=0xED4245), ephemeral=True
                )
            await c.execute(
                "UPDATE tickets SET claimed_by = NULL, status = 'open' WHERE ticket_id = $1",
                ticket['ticket_id']
            )
        old = interaction.guild.get_member(ticket['claimed_by'])
        await claim_unlock(interaction.channel, old, ticket['ticket_type'])
        e = discord.Embed(color=0x5865F2)
        e.description = '↩️ unclaimed — any staff member can pick this up now'
        await interaction.response.send_message(embed=e)


class CloseConfirm(View):
    def __init__(self, ctx, ticket):
        super().__init__(timeout=30)
        self.ctx    = ctx
        self.ticket = ticket
        self.msg    = None

    @discord.ui.button(label='Close', style=ButtonStyle.red, emoji='🔒')
    async def confirm(self, interaction: discord.Interaction, _):
        if interaction.user.id != self.ctx.author.id:
            return await interaction.response.send_message(
                embed=discord.Embed(description="❌ that's not yours to click", color=0xED4245), ephemeral=True
            )
        await interaction.response.defer()
        async with db.pool.acquire() as c:
            await c.execute("UPDATE tickets SET status = 'closed' WHERE ticket_id = $1", self.ticket['ticket_id'])
        e = discord.Embed(color=0xED4245)
        e.description = f'🔒 closing ticket\ntranscript saved — closed by {interaction.user.mention}'
        await interaction.message.edit(embed=e, view=None)
        await save_transcript(interaction.channel, self.ticket, interaction.user)
        await asyncio.sleep(0.5)
        await interaction.channel.delete()

    @discord.ui.button(label='Cancel', style=ButtonStyle.gray, emoji='✖️')
    async def cancel(self, interaction: discord.Interaction, _):
        if interaction.user.id != self.ctx.author.id:
            return await interaction.response.send_message(
                embed=discord.Embed(description="❌ that's not yours to click", color=0xED4245), ephemeral=True
            )
        await interaction.response.defer()
        e = discord.Embed(color=0x5865F2)
        e.description = '✖️ close cancelled'
        await interaction.message.edit(embed=e, view=None)

    async def on_timeout(self):
        try:
            e = discord.Embed(color=0x5865F2)
            e.description = '⏰ close timed out'
            await self.msg.edit(embed=e, view=None)
        except Exception:
            pass


# ================================================================== ticket commands

@bot.command(name='close')
@staff_only()
async def close_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description="❌ this isn't a ticket channel", color=0xED4245))
    if not limiter.check(ctx.author.id, 'close', 3):
        return await ctx.reply(embed=discord.Embed(description='⏳ wait a moment', color=0xFEE75C))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
    if not ticket:
        return await ctx.reply(embed=discord.Embed(description="❌ can't find this ticket", color=0xED4245))
    is_mm         = ticket['ticket_type'] == 'middleman'
    is_mm_claimed = is_mm and ticket.get('claimed_by')
    is_claimer    = ticket.get('claimed_by') == ctx.author.id
    is_admin      = ctx.author.guild_permissions.administrator
    is_staff      = _is_staff(ctx.author)
    # mm claimed: ONLY claimer can close, no exceptions
    if is_mm_claimed and not is_claimer:
        return await ctx.reply(embed=discord.Embed(description='❌ only the middleman who claimed this can close it', color=0xED4245))
    # support/reward claimed: claimer or admin
    if not is_mm and ticket.get('claimed_by') and not is_claimer and not is_admin:
        return await ctx.reply(embed=discord.Embed(description='❌ only the claimer or an admin can close this', color=0xED4245))
    # support/reward unclaimed: any staff can close
    if not is_mm and not ticket.get('claimed_by') and not is_staff:
        return await ctx.reply(embed=discord.Embed(description='❌ you need a staff role to close tickets', color=0xED4245))
    # mm unclaimed: any staff with right tier can close
    if is_mm and not ticket.get('claimed_by') and not is_staff:
        return await ctx.reply(embed=discord.Embed(description='❌ you need a staff role to close tickets', color=0xED4245))
    e = discord.Embed(color=0xFEE75C)
    e.description = '⚠️ are you sure you want to close this ticket?\nthe channel will be deleted and a transcript will be saved'
    view = CloseConfirm(ctx, ticket)
    view.msg = await ctx.send(embed=e, view=view)


@bot.command(name='claim')
@staff_only()
async def claim_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description="❌ this isn't a ticket channel", color=0xED4245))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
        if not ticket:
            return await ctx.reply(embed=discord.Embed(description='❌ no ticket found here', color=0xED4245))
        if ticket['claimed_by']:
            who = ctx.guild.get_member(ticket['claimed_by'])
            return await ctx.reply(embed=discord.Embed(description=f'❌ already claimed by {who.mention if who else "someone"}', color=0xED4245))
        if not await _can_manage(ctx, ticket):
            return await ctx.reply(embed=discord.Embed(description="❌ you don't have the right role for this ticket type", color=0xED4245))
        await c.execute(
            "UPDATE tickets SET claimed_by = $1, status = 'claimed' WHERE ticket_id = $2",
            ctx.author.id, ticket['ticket_id']
        )
    creator = ctx.guild.get_member(ticket['user_id'])
    await claim_lock(ctx.channel, ctx.author, creator, ticket['ticket_type'])
    e = discord.Embed(color=0x57F287)
    e.description = (
        f'✅ **claimed by {ctx.author.mention}**\n\n'
        f'`$unclaim` — drop the claim\n'
        f'`$add @user` — let someone else talk\n'
        f'`$transfer @user` — hand off to another staff\n'
        f'`$close` — close & save transcript'
    )
    e.set_footer(text=f'Claimed by {ctx.author.display_name}')
    await ctx.send(embed=e)


@bot.command(name='unclaim')
@staff_only()
async def unclaim_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description="❌ this isn't a ticket channel", color=0xED4245))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
        if not ticket:
            return await ctx.reply(embed=discord.Embed(description='❌ no ticket found here', color=0xED4245))
        if not ticket['claimed_by']:
            return await ctx.reply(embed=discord.Embed(description="❌ this ticket isn't claimed", color=0xED4245))
        is_mm = ticket['ticket_type'] == 'middleman'
        is_claimer = ticket['claimed_by'] == ctx.author.id
        is_admin   = ctx.author.guild_permissions.administrator
        if is_mm and not is_claimer:
            return await ctx.reply(embed=discord.Embed(description="❌ only the middleman who claimed this can unclaim it", color=0xED4245))
        if not is_mm and not is_claimer and not is_admin:
            return await ctx.reply(embed=discord.Embed(description="❌ you didn't claim this", color=0xED4245))
        await c.execute(
            "UPDATE tickets SET claimed_by = NULL, status = 'open' WHERE ticket_id = $1",
            ticket['ticket_id']
        )
    old = ctx.guild.get_member(ticket['claimed_by'])
    await claim_unlock(ctx.channel, old, ticket['ticket_type'])
    e = discord.Embed(color=0x5865F2)
    e.description = f'↩️ **unclaimed** by {ctx.author.mention} — any eligible staff can now claim this'
    await ctx.send(embed=e)


@bot.command(name='add')
@staff_only()
async def add_cmd(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply(embed=discord.Embed(description='usage: `$add @user`', color=0x5865F2))
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description="❌ this isn't a ticket channel", color=0xED4245))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
    if not ticket:
        return await ctx.reply(embed=discord.Embed(description='❌ no ticket found here', color=0xED4245))
    if not ticket.get('claimed_by'):
        return await ctx.reply(embed=discord.Embed(description='❌ claim the ticket first before adding someone', color=0xED4245))
    if ticket['claimed_by'] != ctx.author.id and not ctx.author.guild_permissions.administrator:
        return await ctx.reply(embed=discord.Embed(description='❌ only the claimer can add people', color=0xED4245))
    await ctx.channel.set_permissions(member, read_messages=True, send_messages=True)
    e = discord.Embed(color=0x57F287)
    e.description = f'✅ {member.mention} has been added to this ticket by {ctx.author.mention}'
    await ctx.reply(embed=e)


@bot.command(name='remove')
@staff_only()
async def remove_cmd(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply(embed=discord.Embed(description='usage: `$remove @user`', color=0x5865F2))
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description="❌ this isn't a ticket channel", color=0xED4245))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
    if not ticket or not await _can_manage(ctx, ticket):
        return await ctx.reply(embed=discord.Embed(description="❌ you don't have permission to do that", color=0xED4245))
    await ctx.channel.set_permissions(member, overwrite=None)
    e = discord.Embed(color=0x57F287)
    e.description = f'✅ {member.mention} has been removed from this ticket by {ctx.author.mention}'
    await ctx.reply(embed=e)


@bot.command(name='rename')
@staff_only()
async def rename_cmd(ctx, *, new_name: str = None):
    if not new_name:
        return await ctx.reply(embed=discord.Embed(description='usage: `$rename new name`', color=0x5865F2))
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description="❌ this isn't a ticket channel", color=0xED4245))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
    if not ticket or not await _can_manage(ctx, ticket):
        return await ctx.reply(embed=discord.Embed(description="❌ you can't rename this", color=0xED4245))
    safe    = re.sub(r'[^a-z0-9\-]', '-', new_name.lower())
    old_name = ctx.channel.name
    await ctx.channel.edit(name=f'ticket-{safe}')
    e = discord.Embed(color=0x57F287)
    e.description = f'✅ renamed by {ctx.author.mention}\n`{old_name}` → `ticket-{safe}`'
    e.set_footer(text=f'Renamed by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='transfer')
@staff_only()
async def transfer_cmd(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply(embed=discord.Embed(description='usage: `$transfer @user`', color=0x5865F2))
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description="❌ this isn't a ticket channel", color=0xED4245))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
        if not ticket:
            return await ctx.reply(embed=discord.Embed(description='❌ no ticket found here', color=0xED4245))
        if not ticket['claimed_by']:
            return await ctx.reply(embed=discord.Embed(description='❌ nobody has claimed this yet', color=0xED4245))
        is_mm = ticket['ticket_type'] == 'middleman'
        is_claimer = ticket['claimed_by'] == ctx.author.id
        is_admin   = ctx.author.guild_permissions.administrator
        if is_mm and not is_claimer:
            return await ctx.reply(embed=discord.Embed(description="❌ only the middleman who claimed this can transfer it", color=0xED4245))
        if not is_mm and not is_claimer and not is_admin:
            return await ctx.reply(embed=discord.Embed(description="❌ you didn't claim this", color=0xED4245))
        old = ctx.guild.get_member(ticket['claimed_by'])
        if old:
            await ctx.channel.set_permissions(old, read_messages=True, send_messages=False)
        await ctx.channel.set_permissions(member, read_messages=True, send_messages=True)
        await c.execute('UPDATE tickets SET claimed_by = $1 WHERE ticket_id = $2', member.id, ticket['ticket_id'])
    e = discord.Embed(color=0x57F287)
    e.description = (
        f'🔄 **ticket transferred**\n\n'
        f'**From:** {ctx.author.mention}\n'
        f'**To:** {member.mention}'
    )
    e.set_footer(text=f'Transferred by {ctx.author.display_name}')
    await ctx.send(embed=e)


@bot.command(name='proof')
@staff_only()
async def proof_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description="❌ this isn't a ticket channel", color=0xED4245))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
    if not ticket:
        return await ctx.reply(embed=discord.Embed(description='❌ no ticket found here', color=0xED4245))
    proof_ch = ctx.guild.get_channel(PROOF_CHANNEL)
    if not proof_ch:
        return await ctx.reply(embed=discord.Embed(description='❌ proof channel not found', color=0xED4245))
    opener = ctx.guild.get_member(ticket['user_id'])
    e = discord.Embed(title='✅ Trade Completed', color=0x57F287)
    e.add_field(name='Middleman', value=ctx.author.mention,                                inline=True)
    e.add_field(name='Tier',      value=TIER_LABEL.get(ticket.get('tier'), 'unknown'),     inline=True)
    e.add_field(name='Client',    value=opener.mention if opener else 'unknown',           inline=True)
    if ticket.get('trade_details'):
        try:
            d = ticket['trade_details'] if isinstance(ticket['trade_details'], dict) else json.loads(ticket['trade_details'])
            e.add_field(name='Trading with', value=d.get('trader', '?'),    inline=False)
            e.add_field(name='Gave',         value=d.get('giving', '?'),    inline=True)
            e.add_field(name='Received',     value=d.get('receiving', '?'), inline=True)
            if d.get('tip'):
                e.add_field(name='Tip', value=d['tip'], inline=True)
        except Exception:
            pass
    e.set_footer(text=f'ticket #{ticket["ticket_id"]}')
    await proof_ch.send(embed=e)
    e = discord.Embed(color=0x57F287)
    e.description = f'✅ trade proof posted to {proof_ch.mention}'
    e.set_footer(text=f'Posted by {ctx.author.display_name}')
    await ctx.reply(embed=e)


# ================================================================== channel perm commands

PERM_ALIASES = {
    'send':     'send_messages',
    'read':     'read_messages',
    'view':     'read_messages',
    'react':    'add_reactions',
    'attach':   'attach_files',
    'files':    'attach_files',
    'embed':    'embed_links',
    'embeds':   'embed_links',
    'history':  'read_message_history',
    'mentions': 'mention_everyone',
    'pin':      'manage_messages',
    'manage':   'manage_messages',
    'threads':  'create_public_threads',
    'voice':    'connect',
    'speak':    'speak',
    'stream':   'stream',
    'slash':    'use_application_commands',
    'commands': 'use_application_commands',
    'external': 'use_external_emojis',
    'emojis':   'use_external_emojis',
}

def resolve_perm(perm: str) -> str:
    p = perm.lower().replace('-', '_').replace(' ', '_')
    return PERM_ALIASES.get(p, p)

def resolve_toggle(toggle: str):
    if toggle.lower() in ('enable', 'on', 'true', 'allow', '1'):
        return True
    if toggle.lower() in ('disable', 'off', 'false', 'deny', '0'):
        return False
    return None

async def resolve_target(ctx, raw: str):
    try:
        return await commands.MemberConverter().convert(ctx, raw)
    except Exception:
        pass
    try:
        return await commands.RoleConverter().convert(ctx, raw)
    except Exception:
        pass
    if raw.lower() in ('everyone', '@everyone'):
        return ctx.guild.default_role
    return None


@bot.command(name='channelperm')
@staff_only()
async def channelperm_cmd(ctx, channel: discord.TextChannel = None, target: str = None, perm: str = None, toggle: str = None):
    if not channel or not target or not perm or not toggle:
        e = discord.Embed(title='📋 channelperm usage', color=0x5865F2)
        e.description = (
            '`$channelperm #channel @target permission enable/disable`\n\n'
            '**example:** `$channelperm #general @members send disable`'
        )
        return await ctx.reply(embed=e)
    resolved_target = await resolve_target(ctx, target)
    if not resolved_target:
        return await ctx.reply(embed=discord.Embed(description=f"❌ couldn't find `{target}`", color=0xED4245))
    resolved_perm   = resolve_perm(perm)
    resolved_toggle = resolve_toggle(toggle)
    if resolved_toggle is None:
        return await ctx.reply(embed=discord.Embed(description='❌ toggle must be `enable` or `disable`', color=0xED4245))
    ow = channel.overwrites_for(resolved_target)
    try:
        setattr(ow, resolved_perm, resolved_toggle)
    except AttributeError:
        return await ctx.reply(embed=discord.Embed(description=f"❌ `{perm}` isn't a valid permission", color=0xED4245))
    await channel.set_permissions(resolved_target, overwrite=ow)
    action = '✅ enabled' if resolved_toggle else '🚫 disabled'
    name   = resolved_target.name if hasattr(resolved_target, 'name') else str(resolved_target)
    e = discord.Embed(
        description=f'{action} `{resolved_perm}` for **{name}** in {channel.mention}',
        color=0x57F287 if resolved_toggle else 0xED4245
    )
    e.set_footer(text=f'Set by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='channelpermall')
@staff_only()
async def channelpermall_cmd(ctx, target: str = None, perm: str = None, toggle: str = None):
    if not target or not perm or not toggle:
        e = discord.Embed(title='📋 channelpermall usage', color=0x5865F2)
        e.description = (
            '`$channelpermall @target permission enable/disable`\n\n'
            '**example:** `$channelpermall @members send disable`'
        )
        return await ctx.reply(embed=e)
    resolved_target = await resolve_target(ctx, target)
    if not resolved_target:
        return await ctx.reply(embed=discord.Embed(description=f"❌ couldn't find `{target}`", color=0xED4245))
    resolved_perm   = resolve_perm(perm)
    resolved_toggle = resolve_toggle(toggle)
    if resolved_toggle is None:
        return await ctx.reply(embed=discord.Embed(description='❌ toggle must be `enable` or `disable`', color=0xED4245))

    channels = [c for c in ctx.guild.channels if isinstance(c, (discord.TextChannel, discord.VoiceChannel))]
    action   = 'enabled' if resolved_toggle else 'disabled'
    name     = resolved_target.name if hasattr(resolved_target, 'name') else str(resolved_target)

    msg = await ctx.reply(embed=discord.Embed(
        description=f'⏳ updating `{resolved_perm}` for **{name}** across {len(channels)} channels...',
        color=0xFEE75C
    ))
    failed = 0
    for ch in channels:
        try:
            ow = ch.overwrites_for(resolved_target)
            setattr(ow, resolved_perm, resolved_toggle)
            await ch.set_permissions(resolved_target, overwrite=ow)
        except Exception:
            failed += 1

    result = f'{"✅" if resolved_toggle else "🚫"} `{resolved_perm}` {action} for **{name}** in all channels'
    if failed:
        result += f'\n⚠️ {failed} channel(s) failed — check bot permissions'
    await msg.edit(embed=discord.Embed(
        description=result,
        color=0x57F287 if resolved_toggle else 0xED4245
    ))


# ================================================================== owner / setup commands

@bot.command(name='setup')
@owner_only()
async def setup_cmd(ctx):
    e = discord.Embed(color=TIER_COLOR['support'])
    e.set_author(name="Trial's Cross Trade  •  Middleman Service")
    e.description = (
        '🛠️ **Support**\n'
        '• General help & questions\n'
        '• Report a scammer or issue\n'
        '• Partnership requests\n'
        '• Anything else\n\n'
        '⚖️ **Middleman**\n'
        '• Secure & verified trading\n'
        '• 3 tiers based on trade value\n'
        '• Every trade handled by trusted staff\n'
        '• Full protection from start to finish'
    )
    e.set_footer(text="Trial's Cross Trade  •  pick a category below")
    await ctx.send(embed=e, view=TicketPanel())
    try:
        await ctx.message.delete()
    except Exception:
        pass


@bot.command(name='setuprewards')
@owner_only()
async def setuprewards_cmd(ctx):
    e = discord.Embed(color=TIER_COLOR['reward'])
    e.set_author(name="Trial's Cross Trade  •  Reward Claims")
    e.description = (
        '🎉 **Giveaway Prizes**\n'
        '• Won a giveaway? claim your prize here\n\n'
        '📨 **Invite Rewards**\n'
        '• Hit an invite milestone? grab your reward\n\n'
        '🏆 **Event Rewards**\n'
        '• Placed or won in a server event\n\n'
        '💰 **Bonus Rewards**\n'
        '• Activity bonuses & special rewards\n\n'
        '🎁 **Other**\n'
        '• Anything else reward related\n\n'
        '*make sure you have proof ready before opening — it speeds things up*'
    )
    e.set_footer(text="Trial's Cross Trade  •  click below to claim")
    await ctx.send(embed=e, view=RewardPanel())
    try:
        await ctx.message.delete()
    except Exception:
        pass


@bot.command(name='setcategory')
@owner_only()
async def setcategory_cmd(ctx, category: discord.CategoryChannel = None):
    if not category:
        return await ctx.reply(embed=discord.Embed(description='usage: `$setcategory #category`', color=0x5865F2))
    async with db.pool.acquire() as c:
        await c.execute(
            'INSERT INTO config (guild_id, ticket_category_id) VALUES ($1,$2) ON CONFLICT (guild_id) DO UPDATE SET ticket_category_id = $2',
            ctx.guild.id, category.id
        )
    e = discord.Embed(color=0x57F287)
    e.description = f'✅ ticket category set to **{category.name}**\nnew tickets will be created here'
    e.set_footer(text=f'Set by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='setlogs')
@owner_only()
async def setlogs_cmd(ctx, channel: discord.TextChannel = None):
    if not channel:
        return await ctx.reply(embed=discord.Embed(description='usage: `$setlogs #channel`', color=0x5865F2))
    async with db.pool.acquire() as c:
        await c.execute(
            'INSERT INTO config (guild_id, log_channel_id) VALUES ($1,$2) ON CONFLICT (guild_id) DO UPDATE SET log_channel_id = $2',
            ctx.guild.id, channel.id
        )
    e = discord.Embed(color=0x57F287)
    e.description = f'✅ log channel set to {channel.mention}\nticket transcripts and events will be logged here'
    e.set_footer(text=f'Set by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='config')
@owner_only()
async def config_cmd(ctx):
    async with db.pool.acquire() as c:
        cfg = await c.fetchrow('SELECT * FROM config WHERE guild_id = $1', ctx.guild.id)
    e = discord.Embed(title='⚙️ Bot Config', color=TIER_COLOR['support'])

    # configured via commands
    cat   = ctx.guild.get_channel(cfg['ticket_category_id']) if cfg and cfg.get('ticket_category_id') else None
    logs  = ctx.guild.get_channel(cfg['log_channel_id'])     if cfg and cfg.get('log_channel_id')     else None
    e.add_field(name='📁 Ticket Category', value=cat.mention  if cat  else 'not set', inline=True)
    e.add_field(name='📋 Log Channel',     value=logs.mention if logs else 'not set', inline=True)
    e.add_field(name='🎫 Tickets Made',    value=str(cfg['ticket_counter'] if cfg else 0), inline=True)
    e.add_field(name='🔒 Ticket Status',   value='🔴 locked' if tickets_locked.get(ctx.guild.id) else '🟢 open', inline=True)

    # hardcoded channels
    welcome_ch = ctx.guild.get_channel(WELCOME_CHANNEL)
    invite_ch  = ctx.guild.get_channel(INVITE_CHANNEL)
    verify_ch  = ctx.guild.get_channel(VERIFY_CHANNEL)
    proof_ch   = ctx.guild.get_channel(PROOF_CHANNEL)
    e.add_field(name='👋 Welcome Channel', value=welcome_ch.mention if welcome_ch else 'not found', inline=True)
    e.add_field(name='📨 Invite Log',      value=invite_ch.mention  if invite_ch  else 'not found', inline=True)
    e.add_field(name='🔐 Verify Channel',  value=verify_ch.mention  if verify_ch  else 'not found', inline=True)
    e.add_field(name='📸 Proof Channel',   value=proof_ch.mention   if proof_ch   else 'not found', inline=True)

    # hardcoded roles
    unverified_r = ctx.guild.get_role(UNVERIFIED_ROLE)
    verified_r   = ctx.guild.get_role(VERIFIED_ROLE)
    member_r     = ctx.guild.get_role(MEMBER_ROLE)
    e.add_field(name='🔴 Unverified Role', value=unverified_r.mention if unverified_r else 'not found', inline=True)
    e.add_field(name='🟢 Verified Role',   value=verified_r.mention   if verified_r   else 'not found', inline=True)
    e.add_field(name='👥 Member Role',     value=member_r.mention     if member_r     else 'not found', inline=True)

    footer = 'run $setcategory and $setlogs to finish setup' if not cfg else f'Requested by {ctx.author.display_name}'
    e.set_footer(text=footer)
    await ctx.reply(embed=e)


@bot.command(name='lock')
@owner_only()
async def lock_cmd(ctx):
    tickets_locked[ctx.guild.id] = True
    e = discord.Embed(color=0xED4245)
    e.description = f'🔒 **tickets locked** by {ctx.author.mention}\nnobody can open new tickets until `$unlock` is run'
    e.set_footer(text=f'Locked by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='unlock')
@owner_only()
async def unlock_cmd(ctx):
    tickets_locked[ctx.guild.id] = False
    e = discord.Embed(color=0x57F287)
    e.description = f'🔓 **tickets unlocked** by {ctx.author.mention} — members can open tickets again'
    e.set_footer(text=f'Unlocked by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='blacklist')
@owner_only()
async def blacklist_cmd(ctx, member: discord.Member = None, *, reason: str = 'no reason given'):
    if not member:
        return await ctx.reply(embed=discord.Embed(description='usage: `$blacklist @user reason`', color=0x5865F2))
    async with db.pool.acquire() as c:
        await c.execute(
            'INSERT INTO blacklist (user_id, guild_id, reason, blacklisted_by) VALUES ($1,$2,$3,$4) ON CONFLICT (user_id, guild_id) DO UPDATE SET reason=$3, blacklisted_by=$4, created_at=NOW()',
            member.id, ctx.guild.id, reason, ctx.author.id
        )
    from datetime import datetime as dt
    e = discord.Embed(title='🚫 User Blacklisted', color=0xED4245)
    e.add_field(name='User',   value=member.mention,               inline=True)
    e.add_field(name='By',     value=ctx.author.mention,           inline=True)
    e.add_field(name='Reason', value=reason,                       inline=False)
    e.set_thumbnail(url=member.display_avatar.url)
    e.set_footer(text=f'Blacklisted by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='unblacklist')
@owner_only()
async def unblacklist_cmd(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply(embed=discord.Embed(description='usage: `$unblacklist @user`', color=0x5865F2))
    async with db.pool.acquire() as c:
        result = await c.execute('DELETE FROM blacklist WHERE user_id = $1 AND guild_id = $2', member.id, ctx.guild.id)
    if result == 'DELETE 0':
        return await ctx.reply(embed=discord.Embed(description=f"⚠️ {member.mention} isn't blacklisted", color=0xFEE75C))
    e = discord.Embed(color=0x57F287)
    e.description = f'✅ {member.mention} removed from the blacklist'
    e.set_footer(text=f'Removed by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='blacklists')
@owner_only()
async def blacklists_cmd(ctx):
    async with db.pool.acquire() as c:
        rows = await c.fetch('SELECT * FROM blacklist WHERE guild_id = $1 ORDER BY created_at DESC', ctx.guild.id)
    if not rows:
        return await ctx.reply(embed=discord.Embed(description='✅ nobody is blacklisted', color=0x57F287))
    lines = []
    for r in rows:
        m    = ctx.guild.get_member(r['user_id'])
        by   = ctx.guild.get_member(r['blacklisted_by'])
        date = r['created_at'].strftime('%b %d') if r.get('created_at') else '?'
        name = m.display_name if m else str(r['user_id'])
        lines.append(f"**{name}** — {r['reason']}  *(by {by.display_name if by else '?'} on {date})*")
    e = discord.Embed(title=f'🚫 Blacklist  •  {len(rows)} user{"s" if len(rows) > 1 else ""}', color=0xED4245)
    e.description = '\n'.join(lines)
    e.set_footer(text=f'Requested by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='invites')
async def invites_cmd(ctx, member: discord.Member = None):
    member = member or ctx.author
    async with db.pool.acquire() as c:
        row = await c.fetchrow(
            'SELECT * FROM invite_stats WHERE guild_id=$1 AND inviter_id=$2',
            ctx.guild.id, member.id
        )
        # count how many people they invited who are currently verified
        verified_count = row['verified'] if row else 0
    joins   = row['joins']   if row else 0
    leaves  = row['leaves']  if row else 0
    fake    = row['fake']    if row else 0
    rejoins = row['rejoins'] if row else 0
    real    = joins - leaves - fake

    word = 'invite' if real == 1 else 'invites'

    e = discord.Embed(color=0x5865F2)
    e.set_author(name='Invite Log', icon_url=member.display_avatar.url)
    e.description = (
        f'## » {member.display_name} has {real} {word}\n\n'
        f'**Joins**    :  {joins}\n'
        f'**Left**     :  {leaves}\n'
        f'**Fake**     :  {fake}\n'
        f'**Rejoins**  :  {rejoins} *(7d)*\n'
        f'**Verified** :  {verified_count}'
    )
    e.set_thumbnail(url=member.display_avatar.url)
    e.set_footer(text=f'Requested by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='lb', aliases=['leaderboardinvites', 'lbi', 'invitelb'])
async def lb_cmd(ctx):
    async with db.pool.acquire() as c:
        rows = await c.fetch(
            '''SELECT inviter_id, joins, leaves, fake, rejoins, verified
               FROM invite_stats
               WHERE guild_id=$1
               ORDER BY (joins - leaves - fake) DESC
               LIMIT 10''',
            ctx.guild.id
        )
    if not rows:
        return await ctx.reply(embed=discord.Embed(
            description='📊 no invite data yet',
            color=0x5865F2
        ))

    lines = []
    medals = {1: '🥇', 2: '🥈', 3: '🥉'}
    for i, row in enumerate(rows, 1):
        member = ctx.guild.get_member(row['inviter_id'])
        name   = member.mention if member else f'`{row["inviter_id"]}`'
        real   = row['joins'] - row['leaves'] - row['fake']
        word   = 'invite' if real == 1 else 'invites'
        medal  = medals.get(i, f'`{i}.`')
        lines.append(
            f'{medal} {name} — **{real}** {word}\n'
            f'**Left:** {row["leaves"]}  **Fake:** {row["fake"]}  '
            f'**Rejoins:** {row["rejoins"]}  **Verified:** {row["verified"]}'
        )

    e = discord.Embed(title=f'📊 Invite Leaderboard  •  {ctx.guild.name}', color=0x5865F2)
    e.description = '\n'.join(lines)
    e.set_footer(text=f'Requested by {ctx.author.display_name}')
    await ctx.reply(embed=e)



@bot.command(name='clearinvites')
@owner_only()
async def clearinvites_cmd(ctx, target: str = None):
    if not target:
        e = discord.Embed(title='📋 clearinvites usage', color=0x5865F2)
        e.description = (
            '`$clearinvites all`      reset everyone\'s invites\n'
            '`$clearinvites @user`    reset one person\'s invites'
        )
        return await ctx.reply(embed=e)

    if target.lower() == 'all':
        async with db.pool.acquire() as c:
            await c.execute(
                'DELETE FROM invite_stats WHERE guild_id=$1', ctx.guild.id
            )
            await c.execute(
                'DELETE FROM member_invites WHERE guild_id=$1', ctx.guild.id
            )
            await c.execute(
                'DELETE FROM member_left WHERE guild_id=$1', ctx.guild.id
            )
        e = discord.Embed(color=0x57F287)
        e.description = f'✅ all invite stats reset for **{ctx.guild.name}**\neveryone starts from 0'
        e.set_footer(text=f'Cleared by {ctx.author.display_name}')
        await ctx.reply(embed=e)
        return

    # try to resolve as a member
    try:
        member = await commands.MemberConverter().convert(ctx, target)
    except Exception:
        return await ctx.reply(embed=discord.Embed(
            description=f"❌ couldn't find `{target}` — use `$clearinvites all` or mention a user",
            color=0xED4245
        ))

    async with db.pool.acquire() as c:
        await c.execute(
            'DELETE FROM invite_stats WHERE guild_id=$1 AND inviter_id=$2',
            ctx.guild.id, member.id
        )
    e = discord.Embed(color=0x57F287)
    e.description = f'✅ invite stats cleared for {member.mention}\ntheir joins, leaves, fakes and rejoins are all reset to 0'
    await ctx.reply(embed=e)


# ================================================================== utility commands

# snipe cache: channel_id -> {content, author, avatar}
snipe_cache = {}


@bot.event
async def on_message_delete(message: discord.Message):
    if message.author.bot:
        return
    snipe_cache[message.channel.id] = {
        'content': message.content or '*[embed or attachment]*',
        'author':  message.author,
        'avatar':  message.author.display_avatar.url,
    }


@bot.command(name='snipe', aliases=['sn'])
async def snipe_cmd(ctx):
    data = snipe_cache.get(ctx.channel.id)
    if not data:
        return await ctx.reply(embed=discord.Embed(
            description='🔍 nothing to snipe in this channel',
            color=0x5865F2
        ))
    e = discord.Embed(title='🔍 Deleted Message', color=0x5865F2)
    e.set_author(name=data['author'].display_name, icon_url=data['avatar'])
    e.description = data['content']
    e.set_footer(text=f'Sniped in #{ctx.channel.name} by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='membercount', aliases=['mc'])
async def membercount_cmd(ctx):
    guild   = ctx.guild
    total   = guild.member_count
    bots    = sum(1 for m in guild.members if m.bot)
    humans  = total - bots
    online  = sum(1 for m in guild.members if m.status != discord.Status.offline and not m.bot)
    e = discord.Embed(title=f'👥 {guild.name}  •  Member Count', color=0x5865F2)
    e.set_thumbnail(url=guild.icon.url if guild.icon else None)
    e.add_field(name='👥 Total',   value=str(total),  inline=True)
    e.add_field(name='🧑 Humans',  value=str(humans), inline=True)
    e.add_field(name='🤖 Bots',    value=str(bots),   inline=True)
    e.add_field(name='🟢 Online',  value=str(online), inline=True)
    e.set_footer(text=f'Requested by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='newest', aliases=['nw'])
async def newest_cmd(ctx):
    members = sorted(
        [m for m in ctx.guild.members if not m.bot],
        key=lambda m: m.joined_at or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True
    )[:5]
    e = discord.Embed(title='🆕 Newest Members', color=0x57F287)
    lines = []
    for i, m in enumerate(members, 1):
        joined  = m.joined_at.strftime('%b %d, %Y') if m.joined_at else '?'
        created = m.created_at.strftime('%b %d, %Y') if m.created_at else '?'
        lines.append(f'**{i}.** {m.mention}  •  joined {joined}  •  acc created {created}')
    e.description = '\n'.join(lines)
    e.set_footer(text=f'Requested by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='oldest', aliases=['ol'])
async def oldest_cmd(ctx):
    members = sorted(
        [m for m in ctx.guild.members if not m.bot],
        key=lambda m: m.joined_at or datetime.now(timezone.utc)
    )[:5]
    e = discord.Embed(title='🏅 Longest Standing Members', color=0xF1C40F)
    lines = []
    for i, m in enumerate(members, 1):
        joined  = m.joined_at.strftime('%b %d, %Y') if m.joined_at else '?'
        created = m.created_at.strftime('%b %d, %Y') if m.created_at else '?'
        lines.append(f'**{i}.** {m.mention}  •  joined {joined}  •  acc created {created}')
    e.description = '\n'.join(lines)
    e.set_footer(text=f'Requested by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='botlist', aliases=['bl'])
async def botlist_cmd(ctx):
    bots = sorted([m for m in ctx.guild.members if m.bot], key=lambda m: m.name.lower())
    if not bots:
        return await ctx.reply(embed=discord.Embed(
            description='🤖 no bots found',
            color=0x5865F2
        ))
    lines = [f'**{i}.** {b.mention}  •  `{b.name}`' for i, b in enumerate(bots, 1)]
    e = discord.Embed(title=f'🤖 Bots in {ctx.guild.name}  •  {len(bots)} total', color=0x5865F2)
    e.description = '\n'.join(lines)
    e.set_footer(text=f'Requested by {ctx.author.display_name}')
    await ctx.reply(embed=e)



HELP_PAGES = [
    {
        'title': '📋 Commands  •  Page 1 / 6',
        'name':  '🎫 Tickets  •  Staff & Middleman',
        'value': (
            '`$claim`                  claim a ticket\n'
            '`$unclaim`                drop your claim\n'
            '`$close`                  close & save transcript\n'
            '`$add @user`              let someone talk in ticket\n'
            '`$remove @user`           remove someone from ticket\n'
            '`$rename name`            rename the ticket channel\n'
            '`$transfer @user`         hand off your claim\n'
            '`$proof`                  post completed trade proof'
        ),
    },
    {
        'title': '📋 Commands  •  Page 2 / 6',
        'name':  '🔧 Staff Tools',
        'value': (
            '`$channelperm #ch @target perm on/off`\n'
            '→ set a permission in one channel\n\n'
            '`$channelpermall @target perm on/off`\n'
            '→ set a permission across all channels\n\n'
            '**Perm aliases:** `send` `read` `react` `attach` `embed`\n'
            '`history` `voice` `speak` `commands` `external`'
        ),
    },
    {
        'title': '📋 Commands  •  Page 3 / 6',
        'name':  '🔍 Utility  •  Everyone',
        'value': (
            '`$snipe` / `$sn`             last deleted message in channel\n'
            '`$membercount` / `$mc`       total, human, bot & online count\n'
            '`$newest` / `$nw`            5 most recently joined members\n'
            '`$oldest` / `$ol`            5 longest standing members\n'
            '`$botlist` / `$bl`           all bots in the server'
        ),
    },
    {
        'title': '📋 Commands  •  Page 4 / 6',
        'name':  '📨 Invites  •  Everyone',
        'value': (
            '`$invites [@user]`              view invite stats\n'
            '`$lb` / `$lbi` / `$invitelb`   invite leaderboard top 10\n'
            '`$clearinvites all`             reset all server invites\n'
            '`$clearinvites @user`           reset one user\'s invites'
        ),
    },
    {
        'title': '📋 Commands  •  Page 5 / 6',
        'name':  '⚙️ Setup  •  Owner Only',
        'value': (
            '`$setup`               post ticket panel\n'
            '`$setuprewards`        post reward claim panel\n'
            '`$setupverify`         post verification panel\n'
            '`$setcategory`         set ticket category\n'
            '`$setlogs`             set log channel\n'
            '`$config`              view full bot config\n'
            '`$lock` / `$unlock`    open or close tickets\n'
            '`$blacklist @user`     blacklist from tickets\n'
            '`$unblacklist @user`   remove from blacklist\n'
            '`$blacklists`          view all blacklisted users'
        ),
    },
    {
        'title': '📋 Commands  •  Page 6 / 6',
        'name':  '🎉 Giveaways  •  Slash Commands',
        'value': (
            '`/giveaway create`         create a giveaway\n'
            '→ `duration` `winners` `prize` `channel` `host` `image` `required_role`\n\n'
            '`/giveaway end`            end a giveaway early\n'
            '`/giveaway reroll`         reroll a new winner\n'
            '`/giveaway addentries`     add a bonus entry role\n'
            '`/giveaway removeentries`  remove a bonus entry role\n\n'
            '**Entries:** everyone gets 1 base entry\n'
            'Bonus roles stack to highest tier only\n'
            'Required role = no entry without it'
        ),
    },
]


def make_help_embed(page: int) -> discord.Embed:
    p = HELP_PAGES[page]
    e = discord.Embed(title=p['title'], color=TIER_COLOR['support'])
    e.add_field(name=p['name'], value=p['value'], inline=False)
    e.set_footer(text="Trial's Cross Trade  •  use the buttons to navigate")
    return e


class HelpView(View):
    def __init__(self, author_id: int, page: int = 0):
        super().__init__(timeout=60)
        self.author_id = author_id
        self.page      = page
        self._update_buttons()

    def _update_buttons(self):
        self.prev_btn.disabled = self.page == 0
        self.next_btn.disabled = self.page == len(HELP_PAGES) - 1

    @discord.ui.button(label='◀ Prev', style=ButtonStyle.gray, custom_id='help_prev')
    async def prev_btn(self, interaction: discord.Interaction, _):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message(
                embed=discord.Embed(description='❌ this isn\'t your help menu', color=0xED4245),
                ephemeral=True
            )
        self.page -= 1
        self._update_buttons()
        await interaction.response.edit_message(embed=make_help_embed(self.page), view=self)

    @discord.ui.button(label='Next ▶', style=ButtonStyle.gray, custom_id='help_next')
    async def next_btn(self, interaction: discord.Interaction, _):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message(
                embed=discord.Embed(description='❌ this isn\'t your help menu', color=0xED4245),
                ephemeral=True
            )
        self.page += 1
        self._update_buttons()
        await interaction.response.edit_message(embed=make_help_embed(self.page), view=self)

    async def on_timeout(self):
        try:
            for item in self.children:
                item.disabled = True
            e = make_help_embed(self.page)
            e.set_footer(text="This help menu has timed out — run $help again")
            await self.message.edit(embed=e, view=self)
        except Exception:
            pass


@bot.command(name='help')
async def help_cmd(ctx):
    view         = HelpView(author_id=ctx.author.id)
    view.message = await ctx.reply(embed=make_help_embed(0), view=view)


# ================================================================== verification

def gen_captcha(length=6) -> str:
    chars = string.ascii_uppercase + string.digits
    for c in 'O0I1':
        chars = chars.replace(c, '')
    return ''.join(random.choices(chars, k=length))


class VerifyView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Verify', style=ButtonStyle.green, emoji='✅', custom_id='btn_verify')
    async def verify(self, interaction: discord.Interaction, _):
        user = interaction.user
        verified_role = interaction.guild.get_role(VERIFIED_ROLE)
        if verified_role and verified_role in user.roles:
            return await interaction.response.send_message(
                embed=discord.Embed(description="✅ you're already verified!", color=0x57F287),
                ephemeral=True
            )
        code = gen_captcha()
        captchas[user.id] = code
        e = discord.Embed(title='🔐 Captcha Verification', color=0x5865F2)
        e.description = f'your code is:\n\n# `{code}`\n\ntype it in this channel to verify'
        e.set_footer(text='code is case insensitive')
        await interaction.response.send_message(embed=e, ephemeral=True)


@bot.event
async def on_member_join(member: discord.Member):
    guild = member.guild

    # --- verification: assign unverified role
    unverified_role = guild.get_role(UNVERIFIED_ROLE)
    if unverified_role:
        try:
            await member.add_roles(unverified_role, reason='joined — awaiting verification')
        except Exception as ex:
            logger.error(f'on_member_join verify: {ex}')

    # --- welcome message (hardcoded channel)
    welcome_ch = guild.get_channel(WELCOME_CHANNEL)
    if welcome_ch:
        try:
            await welcome_ch.send(
                f'{member.mention} Welcome to **{guild.name}** Hope you enjoy ur stay 👋'
            )
        except Exception as ex:
            logger.error(f'welcome send: {ex}')

    # --- DM welcome
    try:
        e = discord.Embed(color=0x57F287)
        e.set_author(name=f'Welcome to {guild.name}!', icon_url=guild.icon.url if guild.icon else None)
        e.description = (
            f'Hey {member.mention}! 👋\n\n'
            f'Welcome to **{guild.name}** — hope you enjoy your stay!\n\n'
            f'Head over to the server to get started.'
        )
        e.set_thumbnail(url=member.display_avatar.url)
        await member.send(embed=e)
    except Exception:
        pass

    # --- invite tracking
    invite_ch = guild.get_channel(INVITE_CHANNEL)
    if not invite_ch:
        return

    # bots
    if member.bot:
        try:
            await invite_ch.send(
                f'{member.mention} has joined **{guild.name}**, i dont know how they joined.'
            )
        except Exception as ex:
            logger.error(f'invite log bot: {ex}')
        return

    # figure out which invite was used by comparing cache
    inviter = None
    used_code = None
    try:
        live_invites = await guild.invites()
        new_invites  = {inv.code: inv.uses for inv in live_invites}
        old_invites  = invite_cache.get(guild.id, {})
        for inv in live_invites:
            if inv.uses > old_invites.get(inv.code, 0):
                used_code = inv.code
                inviter   = inv.inviter
                break
        invite_cache[guild.id] = new_invites
    except Exception as ex:
        logger.error(f'invite fetch: {ex}')

    # check if vanity
    vanity_used = False
    if not inviter and used_code is None:
        try:
            vanity = await guild.vanity_invite()
            if vanity:
                old_vanity = invite_cache.get(f'{guild.id}_vanity', 0)
                if vanity.uses and vanity.uses > old_vanity:
                    vanity_used = True
                    invite_cache[f'{guild.id}_vanity'] = vanity.uses
        except Exception:
            pass

    if vanity_used:
        try:
            await invite_ch.send(
                f'{member.mention} has joined **{guild.name}**, they have joined thru vanity link.'
            )
        except Exception as ex:
            logger.error(f'invite log vanity: {ex}')
        return

    if inviter:
        try:
            # ── determine join type ──────────────────────────────────
            now_utc = datetime.now(timezone.utc)

            # 1. account age < 5 days = fake
            acc_age = now_utc - member.created_at.replace(tzinfo=timezone.utc) if member.created_at.tzinfo is None else now_utc - member.created_at
            is_new_account = acc_age.days < 5

            # 2. rejoin = left within 7 days
            # 3. fake (left) = left more than 7 days ago
            async with db.pool.acquire() as c:
                left_row = await c.fetchrow(
                    'SELECT left_at FROM member_left WHERE guild_id=$1 AND user_id=$2',
                    guild.id, member.id
                )

            is_rejoin = False
            is_fake   = False
            if is_new_account:
                is_fake = True
            elif left_row:
                left_at = left_row['left_at'].replace(tzinfo=timezone.utc) if left_row['left_at'].tzinfo is None else left_row['left_at']
                delta   = now_utc - left_at
                if delta.days <= 7:
                    is_rejoin = True
                # left > 7 days ago = just a normal join, no penalty



            # ── update DB ────────────────────────────────────────────
            async with db.pool.acquire() as c:
                if is_rejoin:
                    # rejoins are purely informational — don't touch joins or leaves
                    await c.execute(
                        '''INSERT INTO invite_stats (guild_id, inviter_id, rejoins) VALUES ($1,$2,1)
                           ON CONFLICT (guild_id, inviter_id) DO UPDATE
                           SET rejoins = invite_stats.rejoins + 1''',
                        guild.id, inviter.id
                    )
                elif is_fake:
                    await c.execute(
                        '''INSERT INTO invite_stats (guild_id, inviter_id, joins, fake) VALUES ($1,$2,1,1)
                           ON CONFLICT (guild_id, inviter_id) DO UPDATE
                           SET joins = invite_stats.joins + 1, fake = invite_stats.fake + 1''',
                        guild.id, inviter.id
                    )
                else:
                    await c.execute(
                        '''INSERT INTO invite_stats (guild_id, inviter_id, joins) VALUES ($1,$2,1)
                           ON CONFLICT (guild_id, inviter_id) DO UPDATE SET joins = invite_stats.joins + 1''',
                        guild.id, inviter.id
                    )

                await c.execute(
                    '''INSERT INTO member_invites (guild_id, user_id, inviter_id, is_rejoin)
                       VALUES ($1,$2,$3,$4)
                       ON CONFLICT (guild_id, user_id) DO UPDATE SET inviter_id=$3, joined_at=NOW(), is_rejoin=$4''',
                    guild.id, member.id, inviter.id, is_rejoin
                )
                await c.execute(
                    'DELETE FROM member_left WHERE guild_id=$1 AND user_id=$2',
                    guild.id, member.id
                )
                row = await c.fetchrow(
                    'SELECT joins, leaves, fake, rejoins FROM invite_stats WHERE guild_id=$1 AND inviter_id=$2',
                    guild.id, inviter.id
                )

            joins   = row['joins']   if row else 1
            leaves  = row['leaves']  if row else 0
            fake    = row['fake']    if row else 0
            rejoins = row['rejoins'] if row else 0
            real    = joins - leaves - fake
            word    = 'invite' if real == 1 else 'invites'

            if is_rejoin:
                note = ' *(rejoin)*'
            elif is_fake and is_new_account:
                note = ' *(fake — account too new)*'
            elif is_fake:
                note = ' *(fake)*'

            else:
                note = ''

            await invite_ch.send(
                f'{member.mention} has joined **{guild.name}**, invited by {inviter.mention}, '
                f'who now has **{real}** {word}.{note}'
            )
        except Exception as ex:
            logger.error(f'invite log inviter: {ex}')
    else:
        try:
            await invite_ch.send(
                f'{member.mention} has joined **{guild.name}**, i dont know how they joined.'
            )
        except Exception as ex:
            logger.error(f'invite log unknown: {ex}')




@bot.event
async def on_member_remove(member: discord.Member):
    try:
        async with db.pool.acquire() as c:
            # record when they left for rejoin detection
            await c.execute(
                '''INSERT INTO member_left (guild_id, user_id, left_at)
                   VALUES ($1,$2,NOW())
                   ON CONFLICT (guild_id, user_id) DO UPDATE SET left_at=NOW()''',
                member.guild.id, member.id
            )
            # find who invited them and increment their leave count
            inv_row = await c.fetchrow(
                'SELECT inviter_id, is_rejoin FROM member_invites WHERE guild_id=$1 AND user_id=$2',
                member.guild.id, member.id
            )
            if inv_row and not inv_row.get('is_rejoin'):
                # only count leave if they joined as a real invite (not rejoin)
                await c.execute(
                    '''INSERT INTO invite_stats (guild_id, inviter_id, leaves) VALUES ($1,$2,1)
                       ON CONFLICT (guild_id, inviter_id) DO UPDATE SET leaves = invite_stats.leaves + 1''',
                    member.guild.id, inv_row['inviter_id']
                )
    except Exception as ex:
        logger.error(f'on_member_remove: {ex}')

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        await bot.process_commands(message)
        return

    user_id = message.author.id
    if user_id in captchas and message.guild:
        if message.channel.id == VERIFY_CHANNEL:
            code = captchas[user_id]
            if message.content.strip().upper() == code:
                try:
                    unverified = message.guild.get_role(UNVERIFIED_ROLE)
                    verified   = message.guild.get_role(VERIFIED_ROLE)
                    member_r   = message.guild.get_role(MEMBER_ROLE)
                    if unverified and unverified in message.author.roles:
                        await message.author.remove_roles(unverified, reason='verified')
                    if verified:
                        await message.author.add_roles(verified, reason='verified')
                    if member_r:
                        await message.author.add_roles(member_r, reason='verified')
                    captchas.pop(user_id, None)
                    # increment inviter's verified count
                    try:
                        async with db.pool.acquire() as c:
                            inv_row = await c.fetchrow(
                                'SELECT inviter_id, is_rejoin FROM member_invites WHERE guild_id=$1 AND user_id=$2',
                                message.guild.id, message.author.id
                            )
                            if inv_row:
                                await c.execute(
                                    '''INSERT INTO invite_stats (guild_id, inviter_id, verified) VALUES ($1,$2,1)
                                       ON CONFLICT (guild_id, inviter_id) DO UPDATE SET verified = invite_stats.verified + 1''',
                                    message.guild.id, inv_row['inviter_id']
                                )
                    except Exception as ex:
                        logger.error(f'verified count update: {ex}')
                    try:
                        await message.delete()
                    except Exception:
                        pass
                    confirm = await message.channel.send(
                        embed=discord.Embed(
                            description=f'✅ {message.author.mention} you\'re verified — welcome to the server!',
                            color=0x57F287
                        )
                    )
                    await asyncio.sleep(4)
                    try:
                        await confirm.delete()
                    except Exception:
                        pass
                except Exception as ex:
                    logger.error(f'verify assign: {ex}')
            else:
                new_code = gen_captcha()
                captchas[user_id] = new_code
                try:
                    await message.delete()
                except Exception:
                    pass
                wrong = await message.channel.send(
                    embed=discord.Embed(
                        description=f'❌ {message.author.mention} wrong code — your new code is `{new_code}`',
                        color=0xED4245
                    )
                )
                await asyncio.sleep(6)
                try:
                    await wrong.delete()
                except Exception:
                    pass
            return

    await bot.process_commands(message)


@bot.command(name='setupverify')
@owner_only()
async def setupverify_cmd(ctx):
    e = discord.Embed(color=0x57F287)
    e.set_author(name="Trial's Cross Trade  •  Verification")
    e.description = (
        "👋 Welcome to **Trial's Cross Trade!**\n\n"
        "Before you can access the server, you need to verify that you're human.\n\n"
        "**How it works:**\n"
        "• Hit the Verify button below\n"
        "• You'll receive a short code only you can see\n"
        "• Type it in this channel and you're in\n\n"
        "*takes less than 10 seconds — keeps us safe from bots & raiders*"
    )
    e.set_footer(text="Trial's Cross Trade  •  click below to get started")
    await ctx.send(embed=e, view=VerifyView())
    try:
        await ctx.message.delete()
    except Exception:
        pass


# ================================================================== giveaway system

def parse_duration(s: str) -> int:
    s = s.strip().lower()
    units = {'d': 86400, 'h': 3600, 'm': 60, 's': 1}
    for unit, mult in units.items():
        if s.endswith(unit):
            try:
                return int(s[:-1]) * mult
            except ValueError:
                return 0
    try:
        return int(s) * 60
    except ValueError:
        return 0


def format_end_date(dt: datetime) -> str:
    return dt.strftime('%m/%d/%Y')


def format_time_left(dt: datetime) -> str:
    now  = datetime.now(timezone.utc)
    diff = dt - now
    if diff.total_seconds() <= 0:
        return 'ended'
    d = diff.days
    h, rem = divmod(diff.seconds, 3600)
    m, _   = divmod(rem, 60)
    if d > 0:   return f'in {d}d {h}h'
    if h > 0:   return f'in {h}h {m}m'
    return f'in {m}m'


async def get_entry_count(member: discord.Member, bonus_roles: list) -> int:
    best = 0
    for role_id, entries in bonus_roles:
        if member.get_role(role_id):
            best = max(best, entries)
    return 1 + best


async def build_giveaway_embed(prize, winners, host, ends_at, required_role, image, bonus_roles, guild=None):
    e = discord.Embed(title=f'🎉 {prize}', color=0x2ECC71)
    e.description = (
        f'Click 🎉 to enter!\n'
        f'**Winners:** {winners}\n'
        f'**Hosted by:** {host.mention}\n'
        f'**Ends:** {format_time_left(ends_at)}'
    )
    if bonus_roles and guild:
        lines = []
        for role_id, entries in bonus_roles:
            r = guild.get_role(role_id)
            if r:
                lines.append(f'{r.mention}: **{entries}** entries')
        if lines:
            e.add_field(name='**Extra Entries:**', value='\n'.join(lines), inline=False)
    if required_role:
        e.add_field(name='\u200b', value=f'Must have the role: {required_role.mention}', inline=False)
    if image:
        e.set_image(url=image)
    e.set_footer(text=f'Ends at | {format_end_date(ends_at)}')
    return e


class GiveawayEntryView(View):
    def __init__(self, giveaway_id: int):
        super().__init__(timeout=None)
        self.giveaway_id = giveaway_id
        self.enter_btn.custom_id = f'gw_enter:{giveaway_id}'
        self.parts_btn.custom_id = f'gw_parts:{giveaway_id}'

    @discord.ui.button(label='0', emoji='🎉', style=ButtonStyle.primary, custom_id='gw_enter:0')
    async def enter_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        gw_id = int(button.custom_id.split(':')[1])
        async with db.pool.acquire() as c:
            gw = await c.fetchrow('SELECT * FROM giveaways WHERE id=$1', gw_id)
            if not gw or gw['ended']:
                return await interaction.response.send_message(
                    embed=discord.Embed(description='❌ this giveaway has ended', color=0xED4245),
                    ephemeral=True
                )
            if gw['required_role_id']:
                if not interaction.user.get_role(gw['required_role_id']):
                    role = interaction.guild.get_role(gw['required_role_id'])
                    return await interaction.response.send_message(
                        embed=discord.Embed(
                            description=f'❌ you need {role.mention if role else "a required role"} to enter',
                            color=0xED4245
                        ),
                        ephemeral=True
                    )
            existing = await c.fetchrow(
                'SELECT 1 FROM giveaway_entries WHERE giveaway_id=$1 AND user_id=$2',
                gw_id, interaction.user.id
            )
            if existing:
                await c.execute(
                    'DELETE FROM giveaway_entries WHERE giveaway_id=$1 AND user_id=$2',
                    gw_id, interaction.user.id
                )
                count = await c.fetchval(
                    'SELECT COUNT(DISTINCT user_id) FROM giveaway_entries WHERE giveaway_id=$1', gw_id
                )
                button.label = str(count)
                await interaction.response.edit_message(view=self)
                return await interaction.followup.send(
                    embed=discord.Embed(description='↩️ you left the giveaway', color=0xFEE75C),
                    ephemeral=True
                )
            bonus_rows  = await c.fetch(
                'SELECT role_id, entries FROM giveaway_bonus_roles WHERE guild_id=$1', interaction.guild_id
            )
            bonus_roles = [(r['role_id'], r['entries']) for r in bonus_rows]
            total       = await get_entry_count(interaction.user, bonus_roles)
            for _ in range(total):
                await c.execute(
                    'INSERT INTO giveaway_entries (giveaway_id, user_id) VALUES ($1,$2)',
                    gw_id, interaction.user.id
                )
            count = await c.fetchval(
                'SELECT COUNT(DISTINCT user_id) FROM giveaway_entries WHERE giveaway_id=$1', gw_id
            )
        button.label = str(count)
        await interaction.response.edit_message(view=self)
        msg = f'🎉 you entered with **{total}** entr{"y" if total == 1 else "ies"}!'
        await interaction.followup.send(
            embed=discord.Embed(description=msg, color=0x57F287), ephemeral=True
        )

    @discord.ui.button(label='Participants', emoji='👥', style=ButtonStyle.secondary, custom_id='gw_parts:0')
    async def parts_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        gw_id = int(button.custom_id.split(':')[1])
        async with db.pool.acquire() as c:
            rows = await c.fetch(
                'SELECT DISTINCT user_id FROM giveaway_entries WHERE giveaway_id=$1', gw_id
            )
        if not rows:
            return await interaction.response.send_message(
                embed=discord.Embed(description='nobody has entered yet', color=0x5865F2), ephemeral=True
            )
        mentions = [f'<@{r["user_id"]}>' for r in rows]
        # split into pages of 20 per page
        per_page = 20
        pages    = [mentions[i:i+per_page] for i in range(0, len(mentions), per_page)]
        total    = len(mentions)

        def make_page(idx):
            e = discord.Embed(
                title=f'👥 Participants — {total}  •  Page {idx+1}/{len(pages)}',
                color=0x5865F2
            )
            e.description = '  '.join(pages[idx])
            return e

        class PartsView(View):
            def __init__(self):
                super().__init__(timeout=60)
                self.page = 0
                self._sync()

            def _sync(self):
                self.prev.disabled = self.page == 0
                self.nxt.disabled  = self.page == len(pages) - 1

            @discord.ui.button(label='◀', style=ButtonStyle.gray)
            async def prev(self, inter: discord.Interaction, _):
                if inter.user.id != interaction.user.id:
                    return await inter.response.send_message('❌ not your menu', ephemeral=True)
                self.page -= 1
                self._sync()
                await inter.response.edit_message(embed=make_page(self.page), view=self)

            @discord.ui.button(label='▶', style=ButtonStyle.gray)
            async def nxt(self, inter: discord.Interaction, _):
                if inter.user.id != interaction.user.id:
                    return await inter.response.send_message('❌ not your menu', ephemeral=True)
                self.page += 1
                self._sync()
                await inter.response.edit_message(embed=make_page(self.page), view=self)

            async def on_timeout(self):
                try:
                    for item in self.children:
                        item.disabled = True
                    await self.message.edit(view=self)
                except Exception:
                    pass

        if len(pages) == 1:
            # fits on one page — no buttons needed
            return await interaction.response.send_message(
                embed=make_page(0), ephemeral=True
            )
        view         = PartsView()
        view.message = await interaction.response.send_message(
            embed=make_page(0), view=view, ephemeral=True
        )


async def end_giveaway(giveaway_id: int, guild: discord.Guild):
    async with db.pool.acquire() as c:
        gw = await c.fetchrow('SELECT * FROM giveaways WHERE id=$1', giveaway_id)
        if not gw or gw['ended']:
            return
        await c.execute('UPDATE giveaways SET ended=TRUE WHERE id=$1', giveaway_id)
        entries = await c.fetch('SELECT user_id FROM giveaway_entries WHERE giveaway_id=$1', giveaway_id)

    channel = guild.get_channel(gw['channel_id'])
    if not channel:
        return
    try:
        msg = await channel.fetch_message(gw['message_id'])
    except Exception:
        return

    if not entries:
        e = discord.Embed(title=f'🎉 {gw["prize"]}', description='Giveaway ended — no entries!', color=0xED4245)
        await msg.edit(embed=e, view=None)
        await channel.send(embed=discord.Embed(description=f'🎉 **{gw["prize"]}** ended with no entries.', color=0xED4245))
        return

    pool         = [r['user_id'] for r in entries]  # weighted pool (dupes = more entries)
    unique_users = list(set(pool))
    num_win      = min(gw['winners_count'], len(unique_users))

    # ── NUCLEAR RANDOM WINNER SELECTION ────────────────────────────
    import secrets, hashlib, time, os, struct

    def nuke_seed(ref_list):
        parts = [
            os.urandom(64),
            str(time.time_ns()).encode(),
            str(time.perf_counter_ns()).encode(),
            secrets.token_bytes(64),
            struct.pack('>Q', id(ref_list)),
            struct.pack('>Q', id(time.time)),
            hashlib.sha256(''.join(str(u) for u in ref_list).encode()).digest(),
        ]
        h = b''.join(parts)
        for _ in range(3):
            h = hashlib.sha512(h + os.urandom(32)).digest()
        return int.from_bytes(h, 'big')

    # fetch last winners — no back-to-back
    async with db.pool.acquire() as c:
        last_row = await c.fetchrow(
            """SELECT winner_ids FROM giveaway_last_winners
               WHERE guild_id=$1 AND prize=$2""",
            gw['guild_id'], gw['prize']
        )
    last_winners = set(last_row['winner_ids']) if last_row else set()
    eligible     = [u for u in unique_users if u not in last_winners]
    if len(eligible) < num_win:
        eligible = unique_users

    weighted = [u for u in pool if u in set(eligible)]
    if not weighted:
        weighted = list(pool)

    # 7 Fisher-Yates passes, nuclear seed each pass
    for _ in range(7):
        rng = random.Random(nuke_seed(weighted))
        for i in range(len(weighted) - 1, 0, -1):
            j = rng.randint(0, i)
            weighted[i], weighted[j] = weighted[j], weighted[i]

    # rotate by random offset so position-0 bias is impossible
    pick_rng = random.Random(nuke_seed(weighted))
    offset   = pick_rng.randint(0, max(len(weighted) - 1, 0))
    rotated  = weighted[offset:] + weighted[:offset]

    winners = []
    used    = set()
    for u in rotated:
        if u not in used:
            winners.append(u)
            used.add(u)
        if len(winners) == num_win:
            break
    if len(winners) < num_win:
        for u in unique_users:
            if u not in used:
                winners.append(u)
                used.add(u)
            if len(winners) == num_win:
                break

    # store winners for back-to-back prevention
    async with db.pool.acquire() as c:
        await c.execute(
            """INSERT INTO giveaway_last_winners (guild_id, prize, winner_ids)
               VALUES ($1,$2,$3)
               ON CONFLICT (guild_id, prize) DO UPDATE SET winner_ids=$3""",
            gw['guild_id'], gw['prize'], winners
        )

    e = discord.Embed(title=f'🎉 {gw["prize"]}', color=0xF1C40F)
    e.description = f'**Winner{"s" if num_win > 1 else ""}:** {mentions}'
    e.set_footer(text='Giveaway ended')
    dead_view = GiveawayEntryView(giveaway_id)
    for item in dead_view.children:
        item.disabled = True
    await msg.edit(embed=e, view=dead_view)
    await channel.send(
        content=mentions,
        embed=discord.Embed(
            description=f'🎉 Congratulations {mentions}! You won **{gw["prize"]}**!',
            color=0xF1C40F
        )
    )


async def schedule_giveaway(giveaway_id: int, ends_at: datetime, guild: discord.Guild):
    delay = (ends_at - datetime.now(timezone.utc)).total_seconds()
    if delay > 0:
        await asyncio.sleep(delay)
    await end_giveaway(giveaway_id, guild)


# ── slash command group ───────────────────────────────────────────

giveaway_group = app_commands.Group(name='giveaway', description='Giveaway commands')


@giveaway_group.command(name='create', description='Create a giveaway')
@app_commands.describe(
    duration='How long the giveaway lasts (e.g. 10d, 2h, 30m)',
    winners='Number of winners',
    prize='What is being given away',
    channel='Channel to post in (default: current)',
    host='Who is hosting (default: you)',
    image='Image URL to attach',
    required_role='Role required to enter',
)
async def giveaway_create(
    interaction: discord.Interaction,
    duration: str,
    winners: int,
    prize: str,
    channel: discord.TextChannel = None,
    host: discord.Member = None,
    image: str = None,
    required_role: discord.Role = None,
):
    if not interaction.user.guild_permissions.manage_guild and not _is_staff(interaction.user):
        return await interaction.response.send_message(
            embed=discord.Embed(description='❌ you need staff or Manage Server to create giveaways', color=0xED4245),
            ephemeral=True
        )
    secs = parse_duration(duration)
    if not secs:
        return await interaction.response.send_message(
            embed=discord.Embed(description='❌ invalid duration — use formats like `10d`, `2h`, `30m`', color=0xED4245),
            ephemeral=True
        )
    if winners < 1:
        return await interaction.response.send_message(
            embed=discord.Embed(description='❌ winners must be at least 1', color=0xED4245),
            ephemeral=True
        )
    channel  = channel or interaction.channel
    host     = host or interaction.user
    ends_at  = datetime.now(timezone.utc).replace(microsecond=0)
    from datetime import timedelta
    ends_at  = ends_at + timedelta(seconds=secs)

    async with db.pool.acquire() as c:
        bonus_rows  = await c.fetch(
            'SELECT role_id, entries FROM giveaway_bonus_roles WHERE guild_id=$1', interaction.guild_id
        )
        bonus_roles = [(r['role_id'], r['entries']) for r in bonus_rows]

    embed = await build_giveaway_embed(
        prize, winners, host, ends_at, required_role, image, bonus_roles, guild=interaction.guild
    )
    await interaction.response.send_message(
        embed=discord.Embed(description='⏳ posting giveaway...', color=0x5865F2), ephemeral=True
    )
    temp_view = GiveawayEntryView(0)
    msg = await channel.send(embed=embed, view=temp_view)

    async with db.pool.acquire() as c:
        gw_id = await c.fetchval(
            """INSERT INTO giveaways
               (guild_id, channel_id, message_id, prize, winners_count, host_id,
                required_role_id, image_url, ends_at, ended)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,FALSE)
               RETURNING id""",
            interaction.guild_id, channel.id, msg.id, prize, winners, host.id,
            required_role.id if required_role else None, image, ends_at
        )

    real_view = GiveawayEntryView(gw_id)
    await msg.edit(view=real_view)
    await interaction.edit_original_response(
        embed=discord.Embed(description=f'✅ giveaway posted in {channel.mention}', color=0x57F287)
    )
    asyncio.create_task(schedule_giveaway(gw_id, ends_at, interaction.guild))


@giveaway_group.command(name='end', description='End a giveaway early')
@app_commands.describe(message_id='The message ID of the giveaway to end')
async def giveaway_end(interaction: discord.Interaction, message_id: str):
    if not interaction.user.guild_permissions.manage_guild and not _is_staff(interaction.user):
        return await interaction.response.send_message(
            embed=discord.Embed(description='❌ staff only', color=0xED4245), ephemeral=True
        )
    try:
        mid = int(message_id)
    except ValueError:
        return await interaction.response.send_message(
            embed=discord.Embed(description='❌ invalid message ID', color=0xED4245), ephemeral=True
        )
    async with db.pool.acquire() as c:
        gw = await c.fetchrow(
            'SELECT * FROM giveaways WHERE message_id=$1 AND guild_id=$2', mid, interaction.guild_id
        )
    if not gw:
        return await interaction.response.send_message(
            embed=discord.Embed(description='❌ giveaway not found', color=0xED4245), ephemeral=True
        )
    if gw['ended']:
        return await interaction.response.send_message(
            embed=discord.Embed(description='❌ already ended', color=0xED4245), ephemeral=True
        )
    await interaction.response.send_message(
        embed=discord.Embed(description='⏳ ending...', color=0xFEE75C), ephemeral=True
    )
    await end_giveaway(gw['id'], interaction.guild)
    await interaction.edit_original_response(
        embed=discord.Embed(description='✅ giveaway ended', color=0x57F287)
    )


@giveaway_group.command(name='reroll', description='Reroll a giveaway winner')
@app_commands.describe(message_id='The message ID of the giveaway to reroll')
async def giveaway_reroll(interaction: discord.Interaction, message_id: str):
    if not interaction.user.guild_permissions.manage_guild and not _is_staff(interaction.user):
        return await interaction.response.send_message(
            embed=discord.Embed(description='❌ staff only', color=0xED4245), ephemeral=True
        )
    try:
        mid = int(message_id)
    except ValueError:
        return await interaction.response.send_message(
            embed=discord.Embed(description='❌ invalid message ID', color=0xED4245), ephemeral=True
        )
    async with db.pool.acquire() as c:
        gw = await c.fetchrow(
            'SELECT * FROM giveaways WHERE message_id=$1 AND guild_id=$2', mid, interaction.guild_id
        )
        if not gw:
            return await interaction.response.send_message(
                embed=discord.Embed(description='❌ giveaway not found', color=0xED4245), ephemeral=True
            )
        entries = await c.fetch('SELECT user_id FROM giveaway_entries WHERE giveaway_id=$1', gw['id'])
    if not entries:
        return await interaction.response.send_message(
            embed=discord.Embed(description='❌ no entries to reroll from', color=0xED4245), ephemeral=True
        )
    pool         = [r['user_id'] for r in entries]  # weighted pool (dupes = more entries)
    unique_users = list(set(pool))
    num_win      = min(gw['winners_count'], len(unique_users))

    import secrets, hashlib, time, os, struct

    def nuke_seed_r(ref_list):
        parts = [
            os.urandom(64),
            str(time.time_ns()).encode(),
            str(time.perf_counter_ns()).encode(),
            secrets.token_bytes(64),
            struct.pack('>Q', id(ref_list)),
            hashlib.sha256(''.join(str(u) for u in ref_list).encode()).digest(),
        ]
        h = b''.join(parts)
        for _ in range(3):
            h = hashlib.sha512(h + os.urandom(32)).digest()
        return int.from_bytes(h, 'big')

    weighted = list(pool)
    for _ in range(7):
        rng = random.Random(nuke_seed_r(weighted))
        for i in range(len(weighted) - 1, 0, -1):
            j = rng.randint(0, i)
            weighted[i], weighted[j] = weighted[j], weighted[i]

    pick_rng = random.Random(nuke_seed_r(weighted))
    offset   = pick_rng.randint(0, max(len(weighted) - 1, 0))
    rotated  = weighted[offset:] + weighted[:offset]

    winners = []
    used    = set()
    for u in rotated:
        if u not in used:
            winners.append(u)
            used.add(u)
        if len(winners) == num_win:
            break
    if len(winners) < num_win:
        for u in unique_users:
            if u not in used:
                winners.append(u)
                used.add(u)
            if len(winners) == num_win:
                break
    mentions = ' '.join(f'<@{w}>' for w in winners)
    channel  = interaction.guild.get_channel(gw['channel_id'])
    if channel:
        await channel.send(
            content=mentions,
            embed=discord.Embed(
                description=f'🎲 Reroll! Congratulations {mentions}! You won **{gw["prize"]}**!',
                color=0xF1C40F
            )
        )
    await interaction.response.send_message(
        embed=discord.Embed(description=f'✅ rerolled — new winner: {mentions}', color=0x57F287),
        ephemeral=True
    )


@giveaway_group.command(name='addentries', description='Add a bonus entry role')
@app_commands.describe(role='The role to give bonus entries', entries='Number of bonus entries')
async def giveaway_addentries(interaction: discord.Interaction, role: discord.Role, entries: int):
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message(
            embed=discord.Embed(description='❌ Manage Server required', color=0xED4245), ephemeral=True
        )
    if entries < 1:
        return await interaction.response.send_message(
            embed=discord.Embed(description='❌ entries must be at least 1', color=0xED4245), ephemeral=True
        )
    async with db.pool.acquire() as c:
        await c.execute(
            """INSERT INTO giveaway_bonus_roles (guild_id, role_id, entries)
               VALUES ($1,$2,$3)
               ON CONFLICT (guild_id, role_id) DO UPDATE SET entries=$3""",
            interaction.guild_id, role.id, entries
        )
    await interaction.response.send_message(
        embed=discord.Embed(
            description=f'✅ {role.mention} now gives **{entries}** bonus entr{"y" if entries == 1 else "ies"}',
            color=0x57F287
        ),
        ephemeral=True
    )


@giveaway_group.command(name='removeentries', description='Remove a bonus entry role')
@app_commands.describe(role='The role to remove from bonus entries')
async def giveaway_removeentries(interaction: discord.Interaction, role: discord.Role):
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message(
            embed=discord.Embed(description='❌ Manage Server required', color=0xED4245), ephemeral=True
        )
    async with db.pool.acquire() as c:
        result = await c.execute(
            'DELETE FROM giveaway_bonus_roles WHERE guild_id=$1 AND role_id=$2',
            interaction.guild_id, role.id
        )
    if result == 'DELETE 0':
        return await interaction.response.send_message(
            embed=discord.Embed(description=f"⚠️ {role.mention} wasn't in the bonus list", color=0xFEE75C),
            ephemeral=True
        )
    await interaction.response.send_message(
        embed=discord.Embed(description=f'✅ {role.mention} removed from bonus entries', color=0x57F287),
        ephemeral=True
    )


# ================================================================== events

@bot.event
async def on_ready():
    logger.info(f'logged in as {bot.user}')
    bot.tree.add_command(giveaway_group)
    await bot.tree.sync()
    logger.info('slash commands synced')

    try:
        await db.connect()
    except Exception as ex:
        logger.error(f'db failed: {ex}')
        return
    bot.add_view(TicketPanel())
    bot.add_view(ControlView())
    bot.add_view(RewardPanel())
    bot.add_view(VerifyView())
    # re-register giveaway views for persistent buttons
    try:
        async with db.pool.acquire() as c:
            active_gws = await c.fetch('SELECT id FROM giveaways WHERE ended=FALSE')
        for gw in active_gws:
            bot.add_view(GiveawayEntryView(gw['id']))
    except Exception:
        pass

    try:
        async with db.pool.acquire() as c:
            rows = await c.fetch(
                'SELECT guild_id, welcome_channel_id FROM config WHERE welcome_channel_id IS NOT NULL'
            )
        for row in rows:
            welcome_config[row['guild_id']] = {'channel_id': row['welcome_channel_id']}
        logger.info(f'loaded welcome config for {len(rows)} guild(s)')
    except Exception as ex:
        logger.error(f'verify config load: {ex}')
    # cache invites for all guilds
    for guild in bot.guilds:
        try:
            invites = await guild.invites()
            invite_cache[guild.id] = {inv.code: inv.uses for inv in invites}
        except Exception:
            pass
    logger.info('invite cache loaded')
    await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name='tickets'))
    logger.info(f'ready  •  {len(bot.guilds)} server(s)')


@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return
    if isinstance(error, commands.CheckFailure):
        await ctx.reply(embed=discord.Embed(description="❌ you don't have permission to do that", color=0xED4245))
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.reply(embed=discord.Embed(description=f'⚠️ missing argument: `{error.param.name}`', color=0xFEE75C))
    else:
        logger.error(f'{ctx.command}: {error}')


# ------------------------------------------------------------------ web server

async def web_server():
    async def handle(r):
        return aiohttp.web.Response(text='ok')
    app = aiohttp.web.Application()
    app.router.add_get('/', handle)
    app.router.add_get('/health', handle)
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv('PORT', 8080))
    await aiohttp.web.TCPSite(runner, '0.0.0.0', port).start()
    logger.info(f'web server on :{port}')


async def main():
    async with bot:
        await asyncio.gather(
            web_server(),
            bot.start(os.getenv('BOT_TOKEN', ''))
        )

if __name__ == '__main__':
    asyncio.run(main())
