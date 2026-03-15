import os
import re
import io
import json
import random
import string
import asyncio
import logging
import secrets
import hashlib
import struct
import time as _time
from datetime import datetime, timezone, timedelta
from typing import Optional
from collections import defaultdict

import discord
import aiohttp
import aiohttp.web
import asyncpg
from PIL import Image, ImageDraw, ImageFont
from discord import ButtonStyle
from discord.ui import View, Select, Modal, TextInput
from discord.ext import commands
from discord.ext import tasks
from discord import app_commands

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)s  %(message)s'
)
logger   = logging.getLogger(__name__)
BOT_START = datetime.now(timezone.utc)


# ================================================================== constants

OWNER_ID = 1029438856069656576

ROLES = {
    'staff':    1432081794647199895,
    'lowtier':  1453757017218093239,
    'midtier':  1434610759140118640,
    'hightier': 1453757157144137911,
}

GW_HOST_ROLE    = 1434621552330014913
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
    'lowtier':  'Low Tier  •  Up to 100 RBX',
    'midtier':  'Mid Tier  •  100–500 RBX',
    'hightier': 'High Tier  •  500+ RBX',
    'support':  'Support',
    'reward':   'Reward Claim',
}

TIER_SLUG = {
    'lowtier':  'lowtier',
    'midtier':  'midtier',
    'hightier': 'hightier',
    'support':  'support',
    'reward':   'reward',
}

MAX_OPEN       = 1
tickets_locked = {}
captchas       = {}
invite_cache   = {}   # guild_id -> {code: uses}
snipe_cache    = {}   # channel_id -> {content, author, avatar, attachments}
esnipe_cache   = {}   # channel_id -> {before, after, author, avatar}

# ── daily stats (reset at midnight UTC) ─────────────────────────────
from collections import defaultdict
daily_stats    = defaultdict(lambda: {'commands': 0, 'tickets': 0, 'verifications': 0})
message_counts = defaultdict(lambda: defaultdict(int))  # guild_id -> user_id -> count
STATS_DATE     = datetime.now(timezone.utc).date()

STATUS_ROTATION = [
    ('watching',  'tickets'),
    ('watching',  'trades go down'),
    ("watching",  "Trial's Cross Trade Middleman Service"),
    ('playing',   'middleman service'),
    ('listening', 'your ticket'),
]

_bot_ready = False    # guard against duplicate on_ready side-effects


# ================================================================== rate limiter

class RateLimiter:
    """
    Per-action cooldowns + global per-user spam detection.
    Uses sliding-window counters to auto-suppress command spammers.
    """
    def __init__(self):
        self._buckets:   dict = defaultdict(dict)   # action -> {uid: last_ts}
        self._history:   dict = defaultdict(list)   # uid -> [timestamps]
        self._suppressed: dict = {}                  # uid -> suppress_until

    # ── per-action cooldown ────────────────────────────────────────
    def check(self, uid: int, action: str, cd: int) -> bool:
        """Returns True if the action is allowed, False if on cooldown."""
        now  = datetime.now(timezone.utc).timestamp()
        last = self._buckets[action].get(uid, 0)
        if now - last < cd:
            return False
        self._buckets[action][uid] = now
        return True

    def remaining(self, uid: int, action: str, cd: int) -> float:
        """Seconds left on a cooldown, 0 if not cooling down."""
        now  = datetime.now(timezone.utc).timestamp()
        last = self._buckets[action].get(uid, 0)
        return max(0.0, cd - (now - last))

    # ── global anti-spam gate ──────────────────────────────────────
    def global_check(self, uid: int,
                     window: int = 6,
                     max_cmds: int = 5,
                     lockout: int = 30) -> tuple[bool, float]:
        """
        Sliding-window global limiter.
        Returns (allowed, suppress_remaining).
        If a user fires more than max_cmds commands in `window` seconds
        they get locked out for `lockout` seconds.
        """
        now = datetime.now(timezone.utc).timestamp()

        # check if currently suppressed
        until = self._suppressed.get(uid, 0)
        if now < until:
            return False, until - now

        # prune history older than window
        self._history[uid] = [t for t in self._history[uid] if now - t < window]
        self._history[uid].append(now)

        if len(self._history[uid]) > max_cmds:
            self._suppressed[uid] = now + lockout
            self._history[uid].clear()
            return False, float(lockout)

        return True, 0.0

    # ── interaction anti-spam (buttons) ───────────────────────────
    def interaction_check(self, uid: int, window: int = 3, max_hits: int = 4) -> bool:
        """
        Lighter version for button interactions — just returns False if spamming.
        """
        now = datetime.now(timezone.utc).timestamp()
        key = f'__btn_{uid}'
        self._history[key] = [t for t in self._history.get(key, []) if now - t < window]
        self._history[key].append(now)
        return len(self._history[key]) <= max_hits

    # ── cleanup (call periodically) ────────────────────────────────
    def cleanup(self):
        now = datetime.now(timezone.utc).timestamp()
        cutoff = now - 300  # 5 minutes
        for action in list(self._buckets):
            self._buckets[action] = {
                uid: ts for uid, ts in self._buckets[action].items()
                if ts > cutoff
            }
        for uid in list(self._history):
            self._history[uid] = [t for t in self._history[uid] if now - t < 60]
            if not self._history[uid]:
                del self._history[uid]
        self._suppressed = {uid: ts for uid, ts in self._suppressed.items() if ts > now}


limiter = RateLimiter()


# ================================================================== database

class Database:
    pool: Optional[asyncpg.Pool] = None

    async def connect(self, retries: int = 5, delay: int = 4):
        url = os.getenv('DATABASE_URL', '')
        if not url:
            raise RuntimeError('DATABASE_URL is not set')
        if url.startswith('postgres://'):
            url = 'postgresql://' + url[len('postgres://'):]
        for attempt in range(1, retries + 1):
            try:
                self.pool = await asyncpg.create_pool(
                    url,
                    min_size=1,
                    max_size=5,
                    command_timeout=15,
                    max_inactive_connection_lifetime=300,
                )
                await self._setup()
                logger.info('database ready')
                return
            except Exception as ex:
                logger.warning(f'db connect attempt {attempt}/{retries} failed: {ex}')
                if attempt < retries:
                    await asyncio.sleep(delay)
        raise RuntimeError('database failed to connect after all retries')

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
                    is_rejoin  BOOLEAN DEFAULT FALSE,
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

            migrations = [
                'ALTER TABLE config ADD COLUMN IF NOT EXISTS ticket_counter INT DEFAULT 0',
                'ALTER TABLE config ADD COLUMN IF NOT EXISTS verify_unverified_id BIGINT',
                'ALTER TABLE config ADD COLUMN IF NOT EXISTS verify_verified_id BIGINT',
                'ALTER TABLE config ADD COLUMN IF NOT EXISTS verify_member_id BIGINT',
                'ALTER TABLE config ADD COLUMN IF NOT EXISTS verify_channel_id BIGINT',
                'ALTER TABLE config ADD COLUMN IF NOT EXISTS welcome_channel_id BIGINT',
                'ALTER TABLE blacklist ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()',
                'ALTER TABLE invite_stats ADD COLUMN IF NOT EXISTS rejoins INT DEFAULT 0',
                'ALTER TABLE invite_stats ADD COLUMN IF NOT EXISTS verified INT DEFAULT 0',
                'ALTER TABLE member_invites ADD COLUMN IF NOT EXISTS is_rejoin BOOLEAN DEFAULT FALSE',
                '''CREATE TABLE IF NOT EXISTS ticket_stats (
                    guild_id BIGINT,
                    user_id  BIGINT,
                    claimed  INT DEFAULT 0,
                    closed   INT DEFAULT 0,
                    PRIMARY KEY (guild_id, user_id)
                )''',
                '''CREATE TABLE IF NOT EXISTS ticket_ratings (
                    guild_id   BIGINT,
                    ticket_id  TEXT,
                    claimer_id BIGINT,
                    user_id    BIGINT,
                    rating     INT,
                    PRIMARY KEY (guild_id, ticket_id)
                )''',
                'ALTER TABLE ticket_stats ADD COLUMN IF NOT EXISTS total_rating BIGINT DEFAULT 0',
                'ALTER TABLE ticket_stats ADD COLUMN IF NOT EXISTS rating_count INT DEFAULT 0',
                '''CREATE TABLE IF NOT EXISTS custom_invites (
                    guild_id   BIGINT NOT NULL,
                    code       TEXT NOT NULL,
                    user_id    BIGINT NOT NULL,
                    created_by BIGINT NOT NULL,
                    PRIMARY KEY (guild_id, code)
                )''',
            ]
            for sql in migrations:
                try:
                    await c.execute(sql)
                except Exception:
                    pass

    async def next_num(self, guild_id: int) -> int:
        async with self.pool.acquire() as c:
            await c.execute(
                '''INSERT INTO config (guild_id, ticket_counter) VALUES ($1, 1)
                   ON CONFLICT (guild_id) DO UPDATE
                   SET ticket_counter = config.ticket_counter + 1''',
                guild_id
            )
            row = await c.fetchrow(
                'SELECT ticket_counter FROM config WHERE guild_id = $1', guild_id
            )
            return row['ticket_counter']

db = Database()


# ================================================================== bot setup

intents = discord.Intents.default()
intents.message_content = True
intents.members         = True
intents.presences       = True
intents.guilds          = True

bot = commands.Bot(command_prefix='$', intents=intents, help_command=None)

@bot.check
async def guild_only(ctx):
    if not ctx.guild:
        await ctx.reply(embed=discord.Embed(description='This command can only be used inside a server, not in DMs.', color=0xED4245))
        return False
    return True




# ================================================================== embed helpers

def ok(desc: str = None, title: str = None) -> discord.Embed:
    e = discord.Embed(color=0x57F287)
    if title: e.title = title
    if desc:  e.description = desc
    return e

def warn(desc: str = None, title: str = None) -> discord.Embed:
    e = discord.Embed(color=0xFEE75C)
    if title: e.title = title
    if desc:  e.description = desc
    return e

def err(desc: str = None, title: str = None) -> discord.Embed:
    e = discord.Embed(color=0xED4245)
    if title: e.title = title
    if desc:  e.description = desc
    return e

def info(desc: str = None, title: str = None, color: int = 0x5865F2) -> discord.Embed:
    e = discord.Embed(color=color)
    if title: e.title = title
    if desc:  e.description = desc
    return e


def fmt_uptime(delta) -> str:
    total = int(delta.total_seconds())
    d, rem = divmod(total, 86400)
    h, rem = divmod(rem, 3600)
    m, s   = divmod(rem, 60)
    parts  = []
    if d: parts.append(f'{d}d')
    if h: parts.append(f'{h}h')
    if m: parts.append(f'{m}m')
    if s or not parts: parts.append(f'{s}s')
    return ' '.join(parts)

# ================================================================== permission checks

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
    # middleman claimed: ONLY the claimer can manage — no admin override
    if ttype == 'middleman' and ticket.get('claimed_by'):
        return ticket['claimed_by'] == ctx.author.id
    # support / reward: admins can always manage
    if ctx.author.guild_permissions.administrator:
        return True
    if ttype == 'support':
        r = ctx.guild.get_role(ROLES['staff'])
        if r and r in ctx.author.roles:
            return True
    elif ttype == 'middleman' and ticket.get('tier') in ROLES:
        r = ctx.guild.get_role(ROLES[ticket['tier']])
        if r and r in ctx.author.roles:
            return True
    if ticket.get('claimed_by') == ctx.author.id:
        return True
    return False


# ================================================================== helpers

async def send_log(guild, title, desc=None, color=0x5865F2, fields=None):
    """Sends an embed to the log channel with automatic rate-limit retry."""
    try:
        async with db.pool.acquire() as c:
            cfg = await c.fetchrow(
                'SELECT log_channel_id FROM config WHERE guild_id = $1', guild.id
            )
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
        for attempt in range(3):
            try:
                await ch.send(embed=e)
                return
            except discord.HTTPException as http_ex:
                if http_ex.status == 429:
                    retry = getattr(http_ex, 'retry_after', 1.0)
                    logger.warning(f'send_log rate limited — retrying in {retry:.2f}s')
                    await asyncio.sleep(retry)
                else:
                    raise
    except Exception as ex:
        logger.error(f'send_log: {ex}')


async def claim_lock(channel, claimer, creator=None, ticket_type='middleman'):
    # Lock every staff/tier role out of sending, for all ticket types
    roles_to_lock = list(ROLES.keys())
    for key in roles_to_lock:
        r = channel.guild.get_role(ROLES[key])
        if r:
            ow = channel.overwrites_for(r)
            ow.send_messages = False
            await channel.set_permissions(r, overwrite=ow)
    # Explicitly allow claimer and creator
    await channel.set_permissions(claimer, read_messages=True, send_messages=True)
    if creator and creator.id != claimer.id:
        await channel.set_permissions(creator, read_messages=True, send_messages=True)


async def claim_unlock(channel, old_claimer=None, ticket_type='middleman'):
    # Restore send permissions for all staff/tier roles
    for key in ROLES.keys():
        r = channel.guild.get_role(ROLES[key])
        if r:
            ow = channel.overwrites_for(r)
            ow.send_messages = True
            await channel.set_permissions(r, overwrite=ow)
    # Remove individual overwrite from old claimer (back to role-based)
    if old_claimer:
        await channel.set_permissions(old_claimer, read_messages=True, send_messages=None)


def make_ticket_embed(user, tier, ticket_id, extra_fields=None, desc=None):
    color = TIER_COLOR.get(tier, 0x5865F2)
    e = discord.Embed(color=color)
    e.set_author(
        name=f'{user.display_name}  •  {TIER_LABEL.get(tier, tier)}',
        icon_url=user.display_avatar.url
    )
    e.description = desc or (
        '👋  **Your ticket has been opened.**\n'
        '> A staff member will be with you shortly.\n'
        '> Please do not ping anyone — someone will claim this ticket soon.'
    )
    if extra_fields:
        for name, value, inline in extra_fields:
            e.add_field(name=name, value=value, inline=inline)
    e.set_footer(text=f'ticket #{ticket_id}')
    return e


async def pre_open_checks(interaction, guild, user):
    if tickets_locked.get(guild.id):
        await interaction.followup.send(
            embed=discord.Embed(title='🔒  Tickets Are Closed', description='Ticket creation is currently disabled. Please check back later.', color=0xED4245),
            ephemeral=True
        )
        return False
    async with db.pool.acquire() as c:
        bl      = await c.fetchrow(
            'SELECT * FROM blacklist WHERE user_id = $1 AND guild_id = $2', user.id, guild.id
        )
        cfg     = await c.fetchrow(
            'SELECT * FROM config WHERE guild_id = $1', guild.id
        )
        tickets = await c.fetch(
            "SELECT ticket_id, channel_id FROM tickets WHERE user_id=$1 AND guild_id=$2 AND status!='closed'",
            user.id, guild.id
        )
        ghost_ids = [t['ticket_id'] for t in tickets if guild.get_channel(t['channel_id']) is None]
        real_open = len(tickets) - len(ghost_ids)
        if ghost_ids:
            await c.execute(
                "UPDATE tickets SET status='closed' WHERE ticket_id=ANY($1::text[])",
                ghost_ids
            )
    if bl:
        by     = guild.get_member(bl['blacklisted_by'])
        date   = bl['created_at'].strftime('%b %d, %Y') if bl.get('created_at') else 'unknown'
        reason = bl['reason'] or 'no reason given'
        e = discord.Embed(title='🚫  You Are Blacklisted', color=0xED4245)
        e.description = 'You are not permitted to open tickets in this server.'
        e.add_field(name='📋  Reason', value=reason,                       inline=False)
        e.add_field(name='👤  Blacklisted By', value=by.mention if by else 'Staff', inline=True)
        e.add_field(name='📅  Date',   value=date,                         inline=True)
        e.set_footer(text='If you believe this is a mistake, please DM a staff member.')
        await interaction.followup.send(embed=e, ephemeral=True)
        return False
    if real_open >= MAX_OPEN:
        await interaction.followup.send(
            embed=discord.Embed(title='⚠️  Ticket Limit Reached', description='You already have an open ticket. Please close it before opening a new one.', color=0xFEE75C),
            ephemeral=True
        )
        return False
    if not cfg or not cfg['ticket_category_id']:
        await interaction.followup.send(
            embed=discord.Embed(description='⚠️ The bot is not fully set up yet. Please contact a staff member.', color=0xFEE75C),
            ephemeral=True
        )
        return False
    return cfg


async def save_transcript(channel, ticket, closer):
    async with db.pool.acquire() as c:
        cfg = await c.fetchrow(
            'SELECT log_channel_id FROM config WHERE guild_id = $1', channel.guild.id
        )
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
    msg_count = len(msgs)
    e = discord.Embed(title='🔒  Ticket Closed', color=0xED4245)
    e.add_field(name='🎫  Ticket',    value=f"#{ticket['ticket_id']}",         inline=True)
    e.add_field(name='📋  Type',       value=f"{ticket['ticket_type'].title()} / {ticket.get('tier', '-').title()}", inline=True)
    e.add_field(name='💬  Messages',   value=str(msg_count),                    inline=True)
    e.add_field(name='👤  Opened By',  value=opener.mention if opener else 'Unknown', inline=True)
    e.add_field(name='🔒  Closed By',  value=closer.mention,                    inline=True)
    if claimer:
        e.add_field(name='✋  Claimed By', value=claimer.mention, inline=True)
    await lc.send(embed=e, file=file)



# ================================================================== UI components

class MiddlemanModal(Modal, title='Middleman Request'):
    def __init__(self, tier):
        super().__init__()
        self.tier      = tier
        self.trader    = TextInput(label='Who are you trading with?',       placeholder='their username or ID', required=True)
        self.giving    = TextInput(label='What are you giving?',            placeholder='e.g. 1 garam', style=discord.TextStyle.paragraph, required=True)
        self.receiving = TextInput(label='What are you receiving?',         placeholder='e.g. 500 Robux', style=discord.TextStyle.paragraph, required=True)
        self.tip       = TextInput(label='Leaving a tip? (optional)',       placeholder='leave blank to skip', required=False)
        self.add_item(self.trader)
        self.add_item(self.giving)
        self.add_item(self.receiving)
        self.add_item(self.tip)

    async def on_submit(self, interaction: discord.Interaction):
        if not limiter.check(interaction.user.id, 'open', 10):
            return await interaction.response.send_message(
                embed=discord.Embed(title='⏳  Slow Down', description='You are opening tickets too fast. Please wait before trying again.', color=0xFEE75C),
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
                    embed=discord.Embed(description='Something went wrong. Please try again or contact a staff member.', color=0xED4245),
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
                guild.me:           discord.PermissionOverwrite(read_messages=True, send_messages=True, manage_channels=True, manage_messages=True),
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
            daily_stats[guild.id]['tickets'] += 1
            fields = [
                ('**Trading with**', trade['trader'],    False),
                ('**Giving**',       trade['giving'],    True),
                ('**Receiving**',    trade['receiving'], True),
            ]
            if trade['tip']:
                fields.append(('**Tip**', trade['tip'], True))
            e    = make_ticket_embed(user, self.tier, tid, fields)
            ping = user.mention
            if tier_r:
                ping += f' {tier_r.mention}'
            await channel.send(content=ping, embed=e, view=ControlView())
            await interaction.followup.send(
                embed=discord.Embed(title='✅  Ticket Opened', description=f'Your ticket has been created — {channel.mention}', color=0x57F287),
                ephemeral=True
            )
            await send_log(guild, 'Ticket Opened',
                f'{user.mention} opened a {TIER_LABEL[self.tier]} ticket',
                TIER_COLOR[self.tier])
        except Exception as ex:
            logger.error(f'middleman open: {ex}')
            try:
                await interaction.followup.send(
                    embed=discord.Embed(description='Something went wrong. Please try again or contact a staff member.', color=0xED4245),
                    ephemeral=True
                )
            except Exception:
                pass


class TierSelect(Select):
    def __init__(self):
        super().__init__(
            placeholder='pick your trade value range',
            options=[
                discord.SelectOption(label='Low Tier  •  Up to 100 RBX', value='lowtier',  emoji='💎', description='Small trades up to 100 RBX  ·  Trial MM and above'),
                discord.SelectOption(label='Mid Tier  •  100–500 RBX',    value='midtier',  emoji='💰', description='Medium trades 100–500 RBX  ·  Middleman and above'),
                discord.SelectOption(label='High Tier  •  500+ RBX',      value='hightier', emoji='💸', description='Large trades 500+ RBX  ·  Pro Middleman and above'),
            ]
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(MiddlemanModal(self.values[0]))


class RewardModal(Modal, title='Claim a Reward'):
    def __init__(self, reward_type):
        super().__init__()
        self.rtype = reward_type
        self.what  = TextInput(label='What are you claiming?', placeholder='Describe what you won', required=True)
        self.add_item(self.what)

    async def on_submit(self, interaction: discord.Interaction):
        if not limiter.check(interaction.user.id, 'open', 10):
            return await interaction.response.send_message(
                embed=discord.Embed(title='⏳  Slow Down', description='You are opening tickets too fast. Please wait before trying again.', color=0xFEE75C),
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
                    embed=discord.Embed(description='Something went wrong. Please try again or contact a staff member.', color=0xED4245),
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
                guild.me:           discord.PermissionOverwrite(read_messages=True, send_messages=True, manage_channels=True, manage_messages=True),
                user:               discord.PermissionOverwrite(read_messages=True, send_messages=True),
            }
            if staff_r:
                overwrites[staff_r] = discord.PermissionOverwrite(read_messages=True, send_messages=True)

            channel = await category.create_text_channel(name=ch_name, overwrites=overwrites)
            data    = {'type': self.rtype, 'what': self.what.value}

            async with db.pool.acquire() as c:
                await c.execute(
                    'INSERT INTO tickets (ticket_id, guild_id, channel_id, user_id, ticket_type, tier, trade_details) VALUES ($1,$2,$3,$4,$5,$6,$7)',
                    tid, guild.id, channel.id, user.id, 'support', 'reward', json.dumps(data)
                )
            daily_stats[guild.id]['tickets'] += 1
            fields = [
                ('**Claiming**', self.what.value, False),
            ]
            e    = make_ticket_embed(user, 'reward', tid, fields)
            ping = user.mention
            if staff_r:
                ping += f' {staff_r.mention}'
            await channel.send(content=ping, embed=e, view=ControlView())
            await interaction.followup.send(
                embed=discord.Embed(title='✅  Ticket Opened', description=f'Your ticket has been created — {channel.mention}', color=0x57F287),
                ephemeral=True
            )
        except Exception as ex:
            logger.error(f'reward open: {ex}')
            try:
                await interaction.followup.send(
                    embed=discord.Embed(description='Something went wrong. Please try again or contact a staff member.', color=0xED4245),
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
                discord.SelectOption(label='Invite Reward',   value='invite',   emoji='📨', description='Claim an invite milestone reward'),
                discord.SelectOption(label='Event Reward',    value='event',    emoji='🏆', description='Claim an event prize or reward'),
                discord.SelectOption(label='Bonus Reward',    value='bonus',    emoji='💰', description='Claim a bonus or special reward'),
                discord.SelectOption(label='Other',           value='other',    emoji='🎁', description='Anything else reward related'),
            ]
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(RewardModal(self.values[0]))


class TicketPanel(View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Support', style=ButtonStyle.primary, emoji='🎫', custom_id='btn_support')
    async def support(self, interaction: discord.Interaction, _):
        if not limiter.interaction_check(interaction.user.id):
            return await interaction.response.send_message(
                embed=discord.Embed(title='⏳  Slow Down', description='You are clicking too fast. Please slow down.', color=0xFEE75C),
                ephemeral=True
            )
        if not limiter.check(interaction.user.id, 'open', 10):
            rem = limiter.remaining(interaction.user.id, 'open', 10)
            return await interaction.response.send_message(
                embed=discord.Embed(title='⏳  Slow Down', description=f'You opened a ticket recently. Please wait **{int(rem)}s** before opening another.', color=0xFEE75C),
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
                    embed=discord.Embed(description='Something went wrong. Please try again or contact a staff member.', color=0xED4245),
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
                guild.me:           discord.PermissionOverwrite(read_messages=True, send_messages=True, manage_channels=True, manage_messages=True),
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
            daily_stats[guild.id]['tickets'] += 1
            e    = make_ticket_embed(user, 'support', tid)
            ping = user.mention
            if staff_r:
                ping += f' {staff_r.mention}'
            await channel.send(content=ping, embed=e, view=ControlView())
            await interaction.followup.send(
                embed=discord.Embed(title='✅  Ticket Opened', description=f'Your ticket has been created — {channel.mention}', color=0x57F287),
                ephemeral=True
            )
        except Exception as ex:
            logger.error(f'support open: {ex}')
            try:
                await interaction.followup.send(
                    embed=discord.Embed(description='Something went wrong. Please try again or contact a staff member.', color=0xED4245),
                    ephemeral=True
                )
            except Exception:
                pass

    @discord.ui.button(label='Middleman', style=ButtonStyle.success, emoji='⚖️', custom_id='btn_middleman')
    async def middleman(self, interaction: discord.Interaction, _):
        if tickets_locked.get(interaction.guild.id):
            return await interaction.response.send_message(
                embed=discord.Embed(description='Tickets are currently closed. Please check back later.', color=0xED4245),
                ephemeral=True
            )
        v = View(timeout=300)
        v.add_item(TierSelect())
        await interaction.response.send_message(
            embed=discord.Embed(title='⚖️  Select Your Tier', description='Pick the tier that best matches the value of your trade.', color=TIER_COLOR['support']),
            view=v, ephemeral=True
        )

    @discord.ui.button(label='Claim Reward', style=ButtonStyle.secondary, emoji='🎁', custom_id='btn_reward')
    async def reward(self, interaction: discord.Interaction, _):
        if tickets_locked.get(interaction.guild.id):
            return await interaction.response.send_message(
                embed=discord.Embed(description='Tickets are currently closed. Please check back later.', color=0xED4245),
                ephemeral=True
            )
        v = View(timeout=300)
        v.add_item(RewardSelect())
        await interaction.response.send_message(
            embed=discord.Embed(title='🎁  Claim a Reward', description='Select the type of reward you are claiming.', color=TIER_COLOR['reward']),
            view=v, ephemeral=True
        )


class ControlView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Claim', style=ButtonStyle.green, emoji='✋', custom_id='btn_claim')
    async def claim(self, interaction: discord.Interaction, _):
        if not limiter.interaction_check(interaction.user.id):
            return await interaction.response.send_message(
                embed=discord.Embed(description='You are clicking too fast. Please slow down.', color=0xFEE75C), ephemeral=True
            )
        if not limiter.check(interaction.user.id, 'claim', 2):
            rem = limiter.remaining(interaction.user.id, 'claim', 2)
            return await interaction.response.send_message(
                embed=discord.Embed(title='⏳  Slow Down', description=f'Please wait **{rem:.1f}s** before trying again.', color=0xFEE75C), ephemeral=True
            )
        if not _is_staff(interaction.user):
            return await interaction.response.send_message(
                embed=discord.Embed(description='You need a staff role to claim tickets.', color=0xED4245),
                ephemeral=True
            )
        async with db.pool.acquire() as c:
            ticket = await c.fetchrow(
                'SELECT * FROM tickets WHERE channel_id = $1', interaction.channel.id
            )
            if not ticket:
                return await interaction.response.send_message(
                    embed=discord.Embed(description='No ticket found in this channel.', color=0xED4245), ephemeral=True
                )
            if ticket['claimed_by']:
                who = interaction.guild.get_member(ticket['claimed_by'])
                return await interaction.response.send_message(
                    embed=discord.Embed(
                        description=f'This ticket is already claimed by {who.mention if who else "someone"}.',
                        color=0xED4245
                    ),
                    ephemeral=True
                )
            await c.execute(
                "UPDATE tickets SET claimed_by=$1, status='claimed' WHERE ticket_id=$2",
                interaction.user.id, ticket['ticket_id']
            )
            await c.execute(
                '''INSERT INTO ticket_stats (guild_id, user_id, claimed) VALUES ($1,$2,1)
                   ON CONFLICT (guild_id, user_id) DO UPDATE SET claimed = ticket_stats.claimed + 1''',
                interaction.guild.id, interaction.user.id
            )
        creator = interaction.guild.get_member(ticket['user_id'])
        await claim_lock(interaction.channel, interaction.user, creator, ticket['ticket_type'])
        e = discord.Embed(color=0x57F287)
        e.title       = '✅  Ticket Claimed'
        e.description = (
            f'**{interaction.user.mention}** is now handling this ticket.\n\n'
            f'> `$add @user` — grant someone access\n'
            f'> `$remove @user` — revoke someone\'s access\n'
            f'> `$transfer @user` — reassign to another staff member\n'
            f'> `$locktic` — lock the ticket to claimer + creator only\n'
            f'> `$close` — close the ticket and save a transcript'
        )
        e.set_footer(text=f'Claimed by {interaction.user.display_name}')
        await interaction.response.send_message(embed=e)
        await send_log(
            interaction.guild, 'Ticket Claimed',
            f'{interaction.user.mention} claimed ticket #{ticket["ticket_id"]}',
            0x57F287
        )

    @discord.ui.button(label='Unclaim', style=ButtonStyle.gray, emoji='↩️', custom_id='btn_unclaim')
    async def unclaim(self, interaction: discord.Interaction, _):
        if not limiter.interaction_check(interaction.user.id):
            return await interaction.response.send_message(
                embed=discord.Embed(description='You are clicking too fast. Please slow down.', color=0xFEE75C), ephemeral=True
            )
        if not limiter.check(interaction.user.id, 'unclaim', 2):
            rem = limiter.remaining(interaction.user.id, 'unclaim', 2)
            return await interaction.response.send_message(
                embed=discord.Embed(title='⏳  Slow Down', description=f'Please wait **{rem:.1f}s** before trying again.', color=0xFEE75C), ephemeral=True
            )
        async with db.pool.acquire() as c:
            ticket = await c.fetchrow(
                'SELECT * FROM tickets WHERE channel_id = $1', interaction.channel.id
            )
            if not ticket:
                return await interaction.response.send_message(
                    embed=discord.Embed(description='No ticket found in this channel.', color=0xED4245), ephemeral=True
                )
            if not ticket['claimed_by']:
                return await interaction.response.send_message(
                    embed=discord.Embed(description="This ticket hasn't been claimed yet.", color=0xED4245), ephemeral=True
                )
            is_mm      = ticket['ticket_type'] == 'middleman'
            is_claimer = ticket['claimed_by'] == interaction.user.id
            is_admin   = interaction.user.guild_permissions.administrator
            if is_mm and not is_claimer:
                return await interaction.response.send_message(
                    embed=discord.Embed(description='Only the staff member who claimed this ticket can unclaim it.', color=0xED4245),
                    ephemeral=True
                )
            if not is_mm and not is_claimer and not is_admin:
                return await interaction.response.send_message(
                    embed=discord.Embed(description="You didn't claim this ticket.", color=0xED4245),
                    ephemeral=True
                )
            await c.execute(
                "UPDATE tickets SET claimed_by=NULL, status='open' WHERE ticket_id=$1",
                ticket['ticket_id']
            )
        old = interaction.guild.get_member(ticket['claimed_by'])
        await claim_unlock(interaction.channel, old, ticket['ticket_type'])
        e = discord.Embed(color=0x5865F2)
        e.title       = '↩️  Ticket Unclaimed'
        e.description = f'**{interaction.user.mention}** has unclaimed this ticket. It is now available for any eligible staff member to pick up.'
        await interaction.response.send_message(embed=e)

    @discord.ui.button(label='Close', style=ButtonStyle.red, emoji='🔒', custom_id='btn_close')
    async def close_btn(self, interaction: discord.Interaction, _):
        if not _is_staff(interaction.user) and not interaction.user.guild_permissions.administrator:
            return await interaction.response.send_message(
                embed=discord.Embed(description='You need a staff role to close tickets.', color=0xED4245),
                ephemeral=True
            )
        async with db.pool.acquire() as c:
            ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', interaction.channel.id)
        if not ticket:
            return await interaction.response.send_message(
                embed=discord.Embed(description='No ticket found in this channel.', color=0xED4245),
                ephemeral=True
            )
        if not ticket.get('claimed_by'):
            return await interaction.response.send_message(
                embed=discord.Embed(
                    title='🔒  Cannot Close Yet',
                    description='This ticket must be **claimed** before it can be closed.\n\nPress **Claim** first, then close.',
                    color=0xFEE75C
                ),
                ephemeral=True
            )
        is_claimer = ticket['claimed_by'] == interaction.user.id
        is_admin   = interaction.user.guild_permissions.administrator
        is_mm      = ticket['ticket_type'] == 'middleman'
        if is_mm and not is_claimer and not is_admin:
            return await interaction.response.send_message(
                embed=discord.Embed(description='Only the staff member who claimed this ticket can close it.', color=0xED4245),
                ephemeral=True
            )
        # Show confirm prompt
        e = discord.Embed(color=0xFEE75C)
        e.title = '🔒  Close Ticket'
        e.description = (
            f'Are you sure you want to close ticket **#{ticket["ticket_id"]}**?\n\n'
            f'The channel will be permanently deleted and a full transcript will be saved to the log channel.'
        )
        # Re-use CloseConfirm but we need a fake ctx-like object — use a simple inline approach
        class _FakeCtx:
            author = interaction.user
        view = CloseConfirm(_FakeCtx(), ticket)
        await interaction.response.send_message(embed=e, view=view, ephemeral=False)
        view.msg = await interaction.original_response()


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
                embed=discord.Embed(description='This is not your close request.', color=0xED4245),
                ephemeral=True
            )
        await interaction.response.defer()
        async with db.pool.acquire() as c:
            await c.execute(
                "UPDATE tickets SET status='closed' WHERE ticket_id=$1",
                self.ticket['ticket_id']
            )
            if self.ticket.get('claimed_by'):
                await c.execute(
                    '''INSERT INTO ticket_stats (guild_id, user_id, closed) VALUES ($1,$2,1)
                       ON CONFLICT (guild_id, user_id) DO UPDATE SET closed = ticket_stats.closed + 1''',
                    interaction.guild.id, self.ticket['claimed_by']
                )
        e = discord.Embed(color=0xED4245)
        e.title       = '🔒  Closing Ticket'
        e.description = f'Closing ticket **#{self.ticket["ticket_id"]}** — saving the transcript. This channel will be deleted shortly.'
        await interaction.message.edit(embed=e, view=None)
        await save_transcript(interaction.channel, self.ticket, interaction.user)
        await asyncio.sleep(0.5)
        await interaction.channel.delete()

    @discord.ui.button(label='Cancel', style=ButtonStyle.gray, emoji='✖️')
    async def cancel(self, interaction: discord.Interaction, _):
        if interaction.user.id != self.ctx.author.id:
            return await interaction.response.send_message(
                embed=discord.Embed(description='This is not your close request.', color=0xED4245),
                ephemeral=True
            )
        await interaction.response.defer()
        e = discord.Embed(color=0x5865F2)
        e.title       = '↩️  Cancelled'
        e.description = 'The close request was cancelled. This ticket remains open.'
        await interaction.message.edit(embed=e, view=None)

    async def on_timeout(self):
        try:
            e = discord.Embed(color=0x5865F2)
            e.title       = '⏰  Request Expired'
            e.description = 'The close request timed out. Run `$close` again if you still want to close this ticket.'
            await self.msg.edit(embed=e, view=None)
        except Exception:
            pass


# ================================================================== ticket commands

@bot.command(name='close')
@staff_only()
async def close_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description='This command can only be used inside a ticket channel.', color=0xED4245))
    if not limiter.check(ctx.author.id, 'close', 3):
        rem = limiter.remaining(ctx.author.id, 'close', 3)
        return await ctx.reply(embed=discord.Embed(description=f'Please wait **{rem:.1f}s** before trying to close again.', color=0xFEE75C))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
    if not ticket:
        return await ctx.reply(embed=discord.Embed(description='Could not find a ticket record for this channel.', color=0xED4245))
    is_mm         = ticket['ticket_type'] == 'middleman'
    is_mm_claimed = is_mm and ticket.get('claimed_by')
    is_claimer    = ticket.get('claimed_by') == ctx.author.id
    is_admin      = ctx.author.guild_permissions.administrator
    is_staff      = _is_staff(ctx.author)
    if is_mm_claimed and not is_claimer:
        return await ctx.reply(embed=discord.Embed(description='Only the staff member who claimed this ticket can close it.', color=0xED4245))
    if not is_mm and ticket.get('claimed_by') and not is_claimer and not is_admin:
        return await ctx.reply(embed=discord.Embed(description='Only the claimer or a server administrator can close this ticket.', color=0xED4245))
    if not is_mm and not ticket.get('claimed_by') and not is_staff:
        return await ctx.reply(embed=discord.Embed(description='You need a staff role to close this ticket.', color=0xED4245))
    if is_mm and not ticket.get('claimed_by') and not is_staff:
        return await ctx.reply(embed=discord.Embed(description='You need a staff role to close tickets.', color=0xED4245))
    e = discord.Embed(color=0xFEE75C)
    e.title = '🔒  Close Ticket'
    e.description = (
        f'Are you sure you want to close ticket **#{ticket["ticket_id"]}**?\n\n'
        f'The channel will be permanently deleted and a full transcript will be saved to the log channel.'
    )
    view = CloseConfirm(ctx, ticket)
    view.msg = await ctx.send(embed=e, view=view)


@bot.command(name='claim')
@staff_only()
async def claim_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description='This command can only be used inside a ticket channel.', color=0xED4245))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
        if not ticket:
            return await ctx.reply(embed=discord.Embed(description='No ticket found in this channel.', color=0xED4245))
        if ticket['claimed_by']:
            who = ctx.guild.get_member(ticket['claimed_by'])
            return await ctx.reply(embed=discord.Embed(
                description=f'This ticket is already claimed by {who.mention if who else "someone"}.',
                color=0xED4245
            ))
        if not await _can_manage(ctx, ticket):
            return await ctx.reply(embed=discord.Embed(description="You don't have the required role to claim this type of ticket.", color=0xED4245))
        await c.execute(
            "UPDATE tickets SET claimed_by=$1, status='claimed' WHERE ticket_id=$2",
            ctx.author.id, ticket['ticket_id']
        )
        await c.execute(
            '''INSERT INTO ticket_stats (guild_id, user_id, claimed) VALUES ($1,$2,1)
               ON CONFLICT (guild_id, user_id) DO UPDATE SET claimed = ticket_stats.claimed + 1''',
            ctx.guild.id, ctx.author.id
        )
    creator = ctx.guild.get_member(ticket['user_id'])
    await claim_lock(ctx.channel, ctx.author, creator, ticket['ticket_type'])
    e = discord.Embed(color=0x57F287)
    e.title       = '✅  Ticket Claimed'
    e.description = (
        f'**{ctx.author.mention}** is now handling this ticket.\n\n'
        f'> `$add @user` — grant someone access\n'
        f'> `$remove @user` — revoke someone\'s access\n'
        f'> `$transfer @user` — reassign to another staff member\n'
        f'> `$locktic` — lock the ticket to claimer + creator only\n'
        f'> `$close` — close the ticket and save a transcript'
    )
    e.set_footer(text=f'Claimed by {ctx.author.display_name}')
    await ctx.send(embed=e)


@bot.command(name='unclaim')
@staff_only()
async def unclaim_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description='This command can only be used inside a ticket channel.', color=0xED4245))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
        if not ticket:
            return await ctx.reply(embed=discord.Embed(description='No ticket found in this channel.', color=0xED4245))
        if not ticket['claimed_by']:
            return await ctx.reply(embed=discord.Embed(description="This ticket hasn't been claimed yet.", color=0xED4245))
        is_mm      = ticket['ticket_type'] == 'middleman'
        is_claimer = ticket['claimed_by'] == ctx.author.id
        is_admin   = ctx.author.guild_permissions.administrator
        if is_mm and not is_claimer:
            return await ctx.reply(embed=discord.Embed(description='Only the staff member who claimed this ticket can unclaim it.', color=0xED4245))
        if not is_mm and not is_claimer and not is_admin:
            return await ctx.reply(embed=discord.Embed(description="You didn't claim this ticket.", color=0xED4245))
        await c.execute(
            "UPDATE tickets SET claimed_by=NULL, status='open' WHERE ticket_id=$1",
            ticket['ticket_id']
        )
    old = ctx.guild.get_member(ticket['claimed_by'])
    await claim_unlock(ctx.channel, old, ticket['ticket_type'])
    e = discord.Embed(color=0x5865F2)
    e.title       = '↩️  Ticket Unclaimed'
    e.description = f'**{ctx.author.mention}** has unclaimed this ticket. It is now available for any eligible staff member to pick up.'
    await ctx.send(embed=e)


@bot.command(name='add')
@staff_only()
async def add_cmd(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply(embed=discord.Embed(description='**Usage:** `$add @user`\nGrants a user access to send messages in this ticket.', color=0x5865F2))
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description='This command can only be used inside a ticket channel.', color=0xED4245))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
    if not ticket:
        return await ctx.reply(embed=discord.Embed(description='No ticket found in this channel.', color=0xED4245))
    if not ticket.get('claimed_by'):
        return await ctx.reply(embed=discord.Embed(description='This ticket needs to be claimed before people can be added.', color=0xED4245))
    if ticket['claimed_by'] != ctx.author.id and not ctx.author.guild_permissions.administrator:
        return await ctx.reply(embed=discord.Embed(description='Only the claimer can add people to this ticket.', color=0xED4245))
    await ctx.channel.set_permissions(member, read_messages=True, send_messages=True)
    e = discord.Embed(color=0x57F287)
    e.title       = '✅  Member Added'
    e.description = f'{member.mention} has been granted access to this ticket.'
    e.set_footer(text=f'Added by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='remove')
@staff_only()
async def remove_cmd(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply(embed=discord.Embed(description='**Usage:** `$remove @user`\nRevokes a user\'s access to this ticket.', color=0x5865F2))
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description='This command can only be used inside a ticket channel.', color=0xED4245))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
    if not ticket or not await _can_manage(ctx, ticket):
        return await ctx.reply(embed=discord.Embed(description="You don't have permission to manage this ticket.", color=0xED4245))
    await ctx.channel.set_permissions(member, overwrite=None)
    e = discord.Embed(color=0xED4245)
    e.title       = '🚪  Member Removed'
    e.description = f'{member.mention}\'s access to this ticket has been revoked.'
    e.set_footer(text=f'Removed by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='rename')
@staff_only()
async def rename_cmd(ctx, *, new_name: str = None):
    if not new_name:
        return await ctx.reply(embed=discord.Embed(description='**Usage:** `$rename <name>`\nRenames the ticket channel.', color=0x5865F2))
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description='This command can only be used inside a ticket channel.', color=0xED4245))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
    if not ticket or not await _can_manage(ctx, ticket):
        return await ctx.reply(embed=discord.Embed(description="You don't have permission to rename this ticket.", color=0xED4245))
    safe     = re.sub(r'[^a-z0-9\-]', '-', new_name.lower())
    old_name = ctx.channel.name
    await ctx.channel.edit(name=f'ticket-{safe}')
    e = discord.Embed(color=0x57F287)
    e.title       = '✏️  Channel Renamed'
    e.description = f'`{old_name}` → `ticket-{safe}`'
    e.set_footer(text=f'Renamed by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='transfer')
@staff_only()
async def transfer_cmd(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply(embed=discord.Embed(description='**Usage:** `$transfer @user`\nTransfers your claim on this ticket to another staff member.', color=0x5865F2))
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description='This command can only be used inside a ticket channel.', color=0xED4245))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
        if not ticket:
            return await ctx.reply(embed=discord.Embed(description='No ticket found in this channel.', color=0xED4245))
        if not ticket['claimed_by']:
            return await ctx.reply(embed=discord.Embed(description="This ticket hasn't been claimed yet. Claim it first before transferring.", color=0xED4245))
        is_mm      = ticket['ticket_type'] == 'middleman'
        is_claimer = ticket['claimed_by'] == ctx.author.id
        is_admin   = ctx.author.guild_permissions.administrator
        if is_mm and not is_claimer:
            return await ctx.reply(embed=discord.Embed(description='Only the staff member who claimed this ticket can transfer it.', color=0xED4245))
        if not is_mm and not is_claimer and not is_admin:
            return await ctx.reply(embed=discord.Embed(description="You didn't claim this ticket.", color=0xED4245))
        old = ctx.guild.get_member(ticket['claimed_by'])
        if old:
            await ctx.channel.set_permissions(old, read_messages=True, send_messages=False)
        await ctx.channel.set_permissions(member, read_messages=True, send_messages=True)
        await c.execute("UPDATE tickets SET claimed_by=$1, status='claimed' WHERE ticket_id=$2", member.id, ticket['ticket_id'])
    e = discord.Embed(color=0x57F287)
    e.title       = '🔄  Ticket Transferred'
    e.description = f'This ticket has been transferred from **{ctx.author.mention}** to **{member.mention}**.'
    e.set_footer(text=f'Transferred by {ctx.author.display_name}')
    await ctx.send(embed=e)



@bot.command(name='locktic')
@staff_only()
async def locktic_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description='This command can only be used inside a ticket channel.', color=0xED4245))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
    if not ticket:
        return await ctx.reply(embed=discord.Embed(description='No ticket found in this channel.', color=0xED4245))
    if not ticket.get('claimed_by'):
        return await ctx.reply(embed=discord.Embed(description='This ticket must be claimed before it can be locked.', color=0xFEE75C))
    if ticket['claimed_by'] != ctx.author.id and not ctx.author.guild_permissions.administrator:
        return await ctx.reply(embed=discord.Embed(description='Only the staff member who claimed this ticket can lock it.', color=0xED4245))
    for key in ROLES.keys():
        r = ctx.guild.get_role(ROLES[key])
        if r:
            ow = ctx.channel.overwrites_for(r)
            ow.send_messages = False
            await ctx.channel.set_permissions(r, overwrite=ow)
    e = discord.Embed(color=0xED4245)
    e.title       = '🔒  Ticket Locked'
    e.description = 'This ticket has been locked. Only the claimer and the ticket creator can send messages here.\n\nRun `$unlocktic` to restore access.'
    e.set_footer(text=f'Locked by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='unlocktic')
@staff_only()
async def unlocktic_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description='This command can only be used inside a ticket channel.', color=0xED4245))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
    if not ticket:
        return await ctx.reply(embed=discord.Embed(description='No ticket found in this channel.', color=0xED4245))
    if not ticket.get('claimed_by'):
        return await ctx.reply(embed=discord.Embed(description='This ticket must be claimed before it can be unlocked.', color=0xFEE75C))
    if ticket['claimed_by'] != ctx.author.id and not ctx.author.guild_permissions.administrator:
        return await ctx.reply(embed=discord.Embed(description='Only the staff member who claimed this ticket can unlock it.', color=0xED4245))
    for key in ROLES.keys():
        r = ctx.guild.get_role(ROLES[key])
        if r:
            ow = ctx.channel.overwrites_for(r)
            ow.send_messages = True
            await ctx.channel.set_permissions(r, overwrite=ow)
    e = discord.Embed(color=0x57F287)
    e.title       = '🔓  Ticket Unlocked'
    e.description = 'This ticket has been unlocked. Everyone with access can now send messages again.'
    e.set_footer(text=f'Unlocked by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='proof')
@staff_only()
async def proof_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description='This command can only be used inside a ticket channel.', color=0xED4245))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
    if not ticket:
        return await ctx.reply(embed=discord.Embed(description='No ticket found in this channel.', color=0xED4245))
    proof_ch = ctx.guild.get_channel(PROOF_CHANNEL)
    if not proof_ch:
        return await ctx.reply(embed=discord.Embed(description='The proof channel could not be found. Please contact a server administrator.', color=0xED4245))
    opener = ctx.guild.get_member(ticket['user_id'])
    e = discord.Embed(title='✅  Trade Completed', color=0x57F287)
    e.add_field(name='⚖️  Middleman', value=ctx.author.mention,                              inline=True)
    e.add_field(name='📊  Tier',      value=TIER_LABEL.get(ticket.get('tier'), 'Unknown'),     inline=True)
    e.add_field(name='👤  Client',    value=opener.mention if opener else 'Unknown',           inline=True)
    if ticket.get('trade_details'):
        try:
            d = ticket['trade_details'] if isinstance(ticket['trade_details'], dict) else json.loads(ticket['trade_details'])
            e.add_field(name='🤝  Trading With', value=d.get('trader', '?'),    inline=False)
            e.add_field(name='📤  Gave',          value=d.get('giving', '?'),    inline=True)
            e.add_field(name='📥  Received',      value=d.get('receiving', '?'), inline=True)
            if d.get('tip'):
                e.add_field(name='💰  Tip', value=d['tip'], inline=True)
        except Exception:
            pass
    e.set_footer(text=f'ticket #{ticket["ticket_id"]}')
    await proof_ch.send(embed=e)
    e2 = discord.Embed(color=0x57F287)
    e2.title       = '✅  Proof Posted'
    e2.description = f'The trade proof has been successfully posted to {proof_ch.mention}.'
    e2.set_footer(text=f'Posted by {ctx.author.display_name}')
    await ctx.reply(embed=e2)


# ================================================================== channel perm commands

PERM_ALIASES = {
    'send':                     'send_messages',
    'send_messages':            'send_messages',
    'read':                     'read_messages',
    'view':                     'read_messages',
    'view_channel':             'read_messages',
    'read_messages':            'read_messages',
    'history':                  'read_message_history',
    'read_history':             'read_message_history',
    'read_message_history':     'read_message_history',
    'embed':                    'embed_links',
    'embeds':                   'embed_links',
    'embed_links':              'embed_links',
    'attach':                   'attach_files',
    'attach_files':             'attach_files',
    'react':                    'add_reactions',
    'add_reactions':            'add_reactions',
    'mentions':                 'mention_everyone',
    'mention_everyone':         'mention_everyone',
    'external':                 'use_external_emojis',
    'external_emojis':          'use_external_emojis',
    'use_external_emojis':      'use_external_emojis',
    'stickers':                 'use_external_stickers',
    'use_external_stickers':    'use_external_stickers',
    'pin':                      'manage_messages',
    'manage':                   'manage_messages',
    'manage_messages':          'manage_messages',
    'manage_perms':             'manage_permissions',
    'manage_permissions':       'manage_permissions',
    'webhooks':                 'manage_webhooks',
    'manage_webhooks':          'manage_webhooks',
    'slow':                     'manage_channels',
    'manage_channels':          'manage_channels',
    'tts':                      'send_tts_messages',
    'send_tts_messages':        'send_tts_messages',
    'polls':                    'send_polls',
    'send_polls':               'send_polls',
    'slash':                    'use_application_commands',
    'use_application_commands': 'use_application_commands',
    'threads':                  'create_public_threads',
    'create_public_threads':    'create_public_threads',
    'private_threads':          'create_private_threads',
    'create_private_threads':   'create_private_threads',
    'send_in_threads':          'send_messages_in_threads',
    'send_messages_in_threads': 'send_messages_in_threads',
    'voice':                    'connect',
    'connect':                  'connect',
    'speak':                    'speak',
    'talk':                     'speak',
    'video':                    'stream',
    'stream':                   'stream',
    'screen':                   'stream',
    'mute':                     'mute_members',
    'mute_members':             'mute_members',
    'deafen':                   'deafen_members',
    'deafen_members':           'deafen_members',
    'move':                     'move_members',
    'move_members':             'move_members',
    'vad':                      'use_voice_activation',
    'voice_activity':           'use_voice_activation',
    'use_voice_activation':     'use_voice_activation',
    'soundboard':               'use_soundboard',
    'use_soundboard':           'use_soundboard',
    'external_sounds':          'use_external_sounds',
    'use_external_sounds':      'use_external_sounds',
    'activities':               'use_embedded_activities',
    'embedded_activities':      'use_embedded_activities',
    'use_embedded_activities':  'use_embedded_activities',
    'priority':                 'priority_speaker',
    'priority_speaker':         'priority_speaker',
    'request_to_speak':         'request_to_speak',
    'stage':                    'request_to_speak',
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
        e = discord.Embed(title='⚙️  Channel Permission', color=0x5865F2)
        e.description = (
            '**Usage:** `$channelperm #channel @target <permission> <enable/disable>`\n\n'
            '**Example:** `$channelperm #general @members send disable`\n'
            '> Disables send messages for @members in #general.'
        )
        return await ctx.reply(embed=e)
    resolved_target = await resolve_target(ctx, target)
    if not resolved_target:
        return await ctx.reply(embed=discord.Embed(description=f'Could not find `{target}`. Please mention a role, user, or use `@everyone`.', color=0xED4245))
    resolved_perm   = resolve_perm(perm)
    resolved_toggle = resolve_toggle(toggle)
    if resolved_toggle is None:
        return await ctx.reply(embed=discord.Embed(description='The toggle must be either `enable` or `disable`.', color=0xED4245))
    ow = channel.overwrites_for(resolved_target)
    try:
        setattr(ow, resolved_perm, resolved_toggle)
    except AttributeError:
        return await ctx.reply(embed=discord.Embed(description=f'`{perm}` is not a recognized permission. Run `$help` and go to the Staff Tools page for the full list.', color=0xED4245))
    await channel.set_permissions(resolved_target, overwrite=ow)
    action = '✅ enabled' if resolved_toggle else '❌ disabled'
    name   = resolved_target.name if hasattr(resolved_target, 'name') else str(resolved_target)
    e = discord.Embed(
        title=f'{'✅' if resolved_toggle else '❌'}  Permission Updated',
        description=f'`{resolved_perm}` has been **{"enabled" if resolved_toggle else "disabled"}** for **{name}** in {channel.mention}.',
        color=0x57F287 if resolved_toggle else 0xED4245
    )
    e.set_footer(text=f'Updated by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='channelpermall')
@staff_only()
async def channelpermall_cmd(ctx, target: str = None, perm: str = None, toggle: str = None):
    if not target or not perm or not toggle:
        e = discord.Embed(title='⚙️  Channel Permission (All)', color=0x5865F2)
        e.description = (
            '**Usage:** `$channelpermall @target <permission> <enable/disable>`\n\n'
            '**Example:** `$channelpermall @members send disable`\n'
            '> Applies the permission change across every channel in the server.'
        )
        return await ctx.reply(embed=e)
    resolved_target = await resolve_target(ctx, target)
    if not resolved_target:
        return await ctx.reply(embed=discord.Embed(description=f'Could not find `{target}`. Please mention a role, user, or use `@everyone`.', color=0xED4245))
    resolved_perm   = resolve_perm(perm)
    resolved_toggle = resolve_toggle(toggle)
    if resolved_toggle is None:
        return await ctx.reply(embed=discord.Embed(description='The toggle must be either `enable` or `disable`.', color=0xED4245))

    channels = [c for c in ctx.guild.channels if isinstance(c, (discord.TextChannel, discord.VoiceChannel))]
    action   = 'enabled' if resolved_toggle else 'disabled'
    name     = resolved_target.name if hasattr(resolved_target, 'name') else str(resolved_target)

    msg = await ctx.reply(embed=discord.Embed(
        title='⏳  Working...',
        description=f'Applying `{resolved_perm}` → **{"enabled" if resolved_toggle else "disabled"}** for **{name}** across **{len(channels)}** channels.',
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

    result_title = f'{'✅' if resolved_toggle else '❌'}  Permission Updated'
    result_desc  = f'`{resolved_perm}` has been **{"enabled" if resolved_toggle else "disabled"}** for **{name}** across all channels.'
    if failed:
        result_desc += f'\n\n⚠️ **{failed}** channel(s) could not be updated — please check my permissions.'
    await msg.edit(embed=discord.Embed(
        title=result_title,
        description=result_desc,
        color=0x57F287 if resolved_toggle else 0xED4245
    ))


# ================================================================== setup commands

@bot.command(name='setup')
@owner_only()
async def setup_cmd(ctx):
    e = discord.Embed(color=TIER_COLOR['support'])
    e.set_author(name="Trial's Cross Trade Middleman Service", icon_url=ctx.guild.icon.url if ctx.guild.icon else None)
    e.title = '🎫  Need Help? Open a Ticket'
    e.description = (
        '> Select a category below that best fits your request.\n'  
        '\n'
        '🎭  **Support**\n'
        '╚► General questions, reports, appeals & anything else.\n'
        '\n'
        '⚖️  **Middleman**\n'
        '╚► Need a trusted MM for your trade? Pick your tier and\n'
        '    the right staff member will claim your ticket.\n'
        '\n'
        '🎁  **Claim Reward**\n'
        '╚► Hit an invite milestone or have a reward to claim?\n'
        '    Have your proof ready and we will get it sorted.'
    )
    e.set_footer(text='Tap a button below to get started  •  Trial\'s Cross Trade Middleman Service')
    await ctx.send(embed=e, view=TicketPanel())
    try:
        await ctx.message.delete()
    except Exception:
        pass


@bot.command(name='setcategory')
@owner_only()
async def setcategory_cmd(ctx, category: discord.CategoryChannel = None):
    if not category:
        return await ctx.reply(embed=discord.Embed(title='📁  Set Category', description='**Usage:** `$setcategory #category`\nSets the category where new ticket channels will be created.', color=0x5865F2))
    async with db.pool.acquire() as c:
        await c.execute(
            'INSERT INTO config (guild_id, ticket_category_id) VALUES ($1,$2) ON CONFLICT (guild_id) DO UPDATE SET ticket_category_id=$2',
            ctx.guild.id, category.id
        )
    e = discord.Embed(color=0x57F287)
    e.title       = '✅  Category Updated'
    e.description = f'New ticket channels will now be created under **{category.name}**.'
    e.set_footer(text=f'Updated by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='setlogs')
@owner_only()
async def setlogs_cmd(ctx, channel: discord.TextChannel = None):
    if not channel:
        return await ctx.reply(embed=discord.Embed(title='📋  Set Logs', description='**Usage:** `$setlogs #channel`\nSets the channel where ticket transcripts and audit logs are sent.', color=0x5865F2))
    async with db.pool.acquire() as c:
        await c.execute(
            'INSERT INTO config (guild_id, log_channel_id) VALUES ($1,$2) ON CONFLICT (guild_id) DO UPDATE SET log_channel_id=$2',
            ctx.guild.id, channel.id
        )
    e = discord.Embed(color=0x57F287)
    e.title       = '✅  Log Channel Updated'
    e.description = f'Ticket transcripts and audit logs will now be sent to {channel.mention}.'
    e.set_footer(text=f'Updated by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='config')
@owner_only()
async def config_cmd(ctx):
    async with db.pool.acquire() as c:
        cfg = await c.fetchrow('SELECT * FROM config WHERE guild_id=$1', ctx.guild.id)
    e = discord.Embed(title='⚙️  Bot Configuration', color=TIER_COLOR['support'])

    cat  = ctx.guild.get_channel(cfg['ticket_category_id']) if cfg and cfg.get('ticket_category_id') else None
    logs = ctx.guild.get_channel(cfg['log_channel_id'])     if cfg and cfg.get('log_channel_id')     else None
    e.add_field(name='📁  Category',      value=cat.mention  if cat  else 'Not Set', inline=True)
    e.add_field(name='📋  Log Channel',    value=logs.mention if logs else 'Not Set', inline=True)
    e.add_field(name='🎫  Total Tickets',  value=str(cfg['ticket_counter'] if cfg else 0), inline=True)
    e.add_field(name='🔒  Ticket Status',  value='🔒 Locked' if tickets_locked.get(ctx.guild.id) else '🟢 Open', inline=True)

    welcome_ch = ctx.guild.get_channel(WELCOME_CHANNEL)
    invite_ch  = ctx.guild.get_channel(INVITE_CHANNEL)
    verify_ch  = ctx.guild.get_channel(VERIFY_CHANNEL)
    proof_ch   = ctx.guild.get_channel(PROOF_CHANNEL)
    e.add_field(name='👋  Welcome',    value=welcome_ch.mention if welcome_ch else 'Not Found', inline=True)
    e.add_field(name='📨  Invite Log',  value=invite_ch.mention  if invite_ch  else 'Not Found', inline=True)
    e.add_field(name='✅  Verify',      value=verify_ch.mention  if verify_ch  else 'Not Found', inline=True)
    e.add_field(name='📸  Proof',       value=proof_ch.mention   if proof_ch   else 'Not Found', inline=True)

    unverified_r = ctx.guild.get_role(UNVERIFIED_ROLE)
    verified_r   = ctx.guild.get_role(VERIFIED_ROLE)
    member_r     = ctx.guild.get_role(MEMBER_ROLE)
    e.add_field(name='❓  Unverified Role', value=unverified_r.mention if unverified_r else 'Not Found', inline=True)
    e.add_field(name='✅  Verified Role',   value=verified_r.mention   if verified_r   else 'Not Found', inline=True)
    e.add_field(name='👤  Member Role',     value=member_r.mention     if member_r     else 'Not Found', inline=True)

    e.add_field(name='⏱️  Uptime',   value=fmt_uptime(datetime.now(timezone.utc) - BOT_START), inline=True)
    e.add_field(name='📡  Latency',  value=f'{round(bot.latency * 1000)}ms', inline=True)
    footer = 'Finish setup with $setcategory and $setlogs.' if not cfg else f'Requested by {ctx.author.display_name}'
    e.set_footer(text=footer)
    await ctx.reply(embed=e)


@bot.command(name='lock')
@owner_only()
async def lock_cmd(ctx):
    tickets_locked[ctx.guild.id] = True
    e = discord.Embed(color=0xED4245)
    e.title       = '🔒  Tickets Locked'
    e.title       = '🔒  Tickets Locked'
    e.description = 'New ticket creation has been disabled. No one can open a ticket until you run `$unlock`.'
    e.set_footer(text=f'locked by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='unlock')
@owner_only()
async def unlock_cmd(ctx):
    tickets_locked[ctx.guild.id] = False
    e = discord.Embed(color=0x57F287)
    e.title       = '🟢  Tickets Unlocked'
    e.title       = '🟢  Tickets Unlocked'
    e.description = 'Ticket creation has been re-enabled. Members can now open new tickets.'
    e.set_footer(text=f'unlocked by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='blacklist')
@owner_only()
async def blacklist_cmd(ctx, member: discord.Member = None, *, reason: str = 'no reason given'):
    if not member:
        return await ctx.reply(embed=discord.Embed(title='🚫  Blacklist', description='**Usage:** `$blacklist @user [reason]`\nPrevents a user from opening any tickets.', color=0x5865F2))
    async with db.pool.acquire() as c:
        await c.execute(
            'INSERT INTO blacklist (user_id, guild_id, reason, blacklisted_by) VALUES ($1,$2,$3,$4) '
            'ON CONFLICT (user_id, guild_id) DO UPDATE SET reason=$3, blacklisted_by=$4, created_at=NOW()',
            member.id, ctx.guild.id, reason, ctx.author.id
        )
    e = discord.Embed(color=0xED4245)
    e.title = '🚫  User Blacklisted'
    e.set_author(name=f'{member.display_name}', icon_url=member.display_avatar.url)
    e.add_field(name='🚫  User',    value=member.mention,     inline=True)
    e.add_field(name='👤  By',      value=ctx.author.mention, inline=True)
    e.add_field(name='📋  Reason',  value=reason,             inline=False)
    e.set_thumbnail(url=member.display_avatar.url)
    e.set_footer(text=f'blacklisted by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='unblacklist')
@owner_only()
async def unblacklist_cmd(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply(embed=discord.Embed(title='✅  Unblacklist', description='**Usage:** `$unblacklist @user`\nRemoves a user from the blacklist and restores their ability to open tickets.', color=0x5865F2))
    async with db.pool.acquire() as c:
        result = await c.execute(
            'DELETE FROM blacklist WHERE user_id=$1 AND guild_id=$2', member.id, ctx.guild.id
        )
    if result == 'DELETE 0':
        return await ctx.reply(embed=discord.Embed(description=f'{member.mention} is not currently blacklisted.', color=0xFEE75C))
    e = discord.Embed(color=0x57F287)
    e.title       = '✅  Unblacklisted'
    e.description = f'{member.mention} has been removed from the blacklist and can now open tickets again.'
    e.set_footer(text=f'Removed by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='blacklists')
@owner_only()
async def blacklists_cmd(ctx):
    async with db.pool.acquire() as c:
        rows = await c.fetch('SELECT * FROM blacklist WHERE guild_id=$1 ORDER BY created_at DESC', ctx.guild.id)
    if not rows:
        return await ctx.reply(embed=discord.Embed(title='🚫  Blacklist', description='No users are currently blacklisted.', color=0x57F287))
    lines = []
    for r in rows:
        m    = ctx.guild.get_member(r['user_id'])
        by   = ctx.guild.get_member(r['blacklisted_by'])
        date = r['created_at'].strftime('%b %d') if r.get('created_at') else '?'
        name = m.display_name if m else str(r['user_id'])
        lines.append(f"**{name}** — {r['reason']}  (by {by.display_name if by else '?'} on {date})")
    e = discord.Embed(title=f'🚫  Blacklist  ({len(rows)})', color=0xED4245)
    e.set_author(name=ctx.guild.name, icon_url=ctx.guild.icon.url if ctx.guild.icon else None)
    e.description = '\n'.join(lines)
    e.set_footer(text=f'Requested by {ctx.author.display_name}')
    await ctx.reply(embed=e)


# ================================================================== invite commands

@bot.command(name='invites')
async def invites_cmd(ctx, member: discord.Member = None):
    member = member or ctx.author
    verified_role = ctx.guild.get_role(VERIFIED_ROLE)

    # Pull every member this person has invited
    async with db.pool.acquire() as c:
        rows = await c.fetch(
            "SELECT user_id, is_rejoin FROM member_invites WHERE guild_id=$1 AND inviter_id=$2",
            ctx.guild.id, member.id
        )

    joins    = len(rows)
    left     = 0
    rejoins  = 0
    fake     = 0
    verified = 0

    now_utc = datetime.now(timezone.utc)

    for r in rows:
        invited_m = ctx.guild.get_member(r['user_id'])
        is_rejoin = r.get('is_rejoin', False)

        if is_rejoin:
            rejoins += 1
        elif invited_m is None:
            left += 1
        else:
            acc = invited_m.created_at if invited_m.created_at.tzinfo else invited_m.created_at.replace(tzinfo=timezone.utc)
            if (now_utc - acc).days < 3:
                fake += 1
            elif verified_role and verified_role in invited_m.roles:
                verified += 1

    real = joins - left - rejoins - fake

    e = discord.Embed(title='📨  Invite Log', color=0x5865F2)
    e.set_author(name=member.display_name, icon_url=member.display_avatar.url)
    e.set_thumbnail(url=member.display_avatar.url)
    e.description = (
        f'**{member.mention} has {real} real {"invite" if real == 1 else "invites"}**\n'
        f'\n'
        f'> 📥  Joins       **{joins}**\n'
        f'> 🚪  Left         **{left}**\n'
        f'> 🔄  Rejoins   **{rejoins}**\n'
        f'> 🤖  Fake        **{fake}**\n'
        f'> ✅  Verified   **{verified}**'
    )
    e.set_footer(text=f'Requested by {ctx.author.display_name}')
    await ctx.reply(embed=e)


async def build_lb_embed(guild, last_updated: str = None) -> discord.Embed:
    async with db.pool.acquire() as c:
        rows = await c.fetch(
            '''SELECT inviter_id, joins, leaves, fake, rejoins, verified
               FROM invite_stats
               WHERE guild_id=$1
               ORDER BY (joins - leaves - fake) DESC
               LIMIT 10''',
            guild.id
        )
    if not rows:
        e = discord.Embed(title='🏆  Invite Leaderboard', color=0x5865F2)
        e.set_author(name=guild.name, icon_url=guild.icon.url if guild.icon else None)
        e.description = 'No invite data has been recorded yet. Start inviting members!'
        return e

    lines  = []
    medals = {1: '🥇', 2: '🥈', 3: '🥉'}
    for i, row in enumerate(rows, 1):
        member = guild.get_member(row['inviter_id'])
        name   = member.mention if member else f'`{row["inviter_id"]}`'
        real   = row['joins'] - row['leaves'] - row['fake']
        word   = 'invite' if real == 1 else 'invites'
        medal  = medals.get(i, f'`{i}.`')
        lines.append(
            f'{medal} {name} — **{real}** {word}\n'
            f'🚪 {row["leaves"]} left  🤖 {row["fake"]} fake  🔄 {row["rejoins"]} rejoins  ✅ {row["verified"]} verified'
        )

    e  = discord.Embed(title='🏆  Invite Leaderboard', color=0x5865F2)
    e.set_author(name=guild.name, icon_url=guild.icon.url if guild.icon else None)
    e.description = '\n'.join(lines)
    e.set_footer(text='Live  ·  Refreshes every 30s')
    return e


class LiveLBView(View):
    def __init__(self, author_id: int):
        super().__init__(timeout=300)
        self.author_id = author_id
        self.stopped   = False
        self.task      = None
        self.message   = None

    @discord.ui.button(label='Stop', style=ButtonStyle.red, custom_id='lb_stop')
    async def stop_btn(self, interaction: discord.Interaction, _):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message(
                embed=discord.Embed(description='This leaderboard belongs to someone else.', color=0xED4245),
                ephemeral=True
            )
        self.stopped = True
        if self.task:
            self.task.cancel()
        self.stop()
        e = await build_lb_embed(interaction.guild)
        e.set_footer(text='stopped — run $lb again to restart')
        await interaction.response.edit_message(embed=e, view=None)

    async def on_timeout(self):
        self.stopped = True
        if self.task:
            self.task.cancel()
        try:
            e = await build_lb_embed(self.message.guild)
            e.set_footer(text='timed out — run $lb to restart')
            await self.message.edit(embed=e, view=None)
        except Exception:
            pass


async def live_lb_loop(view: LiveLBView, guild):
    try:
        while not view.stopped:
            await asyncio.sleep(30)
            if view.stopped:
                break
            e  = await build_lb_embed(guild)
            try:
                await view.message.edit(embed=e, view=view)
            except Exception:
                break
    except asyncio.CancelledError:
        pass



@bot.command(name='whoinvited')
async def whoinvited_cmd(ctx, member: discord.Member = None):
    member = member or ctx.author
    async with db.pool.acquire() as c:
        row = await c.fetchrow(
            "SELECT mi.inviter_id, mi.joined_at, mi.is_rejoin "
            "FROM member_invites mi "
            "WHERE mi.guild_id=$1 AND mi.user_id=$2",
            ctx.guild.id, member.id
        )
    if not row or not row['inviter_id']:
        e = discord.Embed(color=0xED4245)
        e.set_author(name=member.display_name, icon_url=member.display_avatar.url)
        e.set_thumbnail(url=member.display_avatar.url)
        e.description = f'No invite data found for {member.mention}.'
        e.set_footer(text=f'Requested by {ctx.author.display_name}')
        return await ctx.reply(embed=e)

    inviter    = ctx.guild.get_member(row['inviter_id'])
    joined_at  = row['joined_at']
    is_rejoin  = row.get('is_rejoin', False)
    verified_r = ctx.guild.get_role(VERIFIED_ROLE)
    is_verified = verified_r and verified_r in member.roles

    now_utc     = datetime.now(timezone.utc)
    acc_created = member.created_at if member.created_at.tzinfo else member.created_at.replace(tzinfo=timezone.utc)
    acc_age     = (now_utc - acc_created).days
    join_str    = joined_at.strftime('%b %d, %Y') if joined_at else 'Unknown'

    if is_rejoin:
        status = '🔄  Rejoin'
    elif acc_age < 3:
        status = '❌  Fake — account under 3 days old'
    elif is_verified:
        status = '✅  Verified'
    else:
        status = '👤  Unverified'

    inviter_str = inviter.mention if inviter else f'`{row["inviter_id"]}`'

    async with db.pool.acquire() as c:
        inv_stats = await c.fetchrow(
            'SELECT joins, leaves, fake, rejoins FROM invite_stats WHERE guild_id=$1 AND inviter_id=$2',
            ctx.guild.id, row['inviter_id']
        )
    real_invites = 0
    if inv_stats:
        real_invites = inv_stats['joins'] - inv_stats['leaves'] - inv_stats['fake'] - inv_stats['rejoins']

    e = discord.Embed(title='📨  Invite Lookup', color=0x5865F2)
    e.set_author(name=member.display_name, icon_url=member.display_avatar.url)
    e.set_thumbnail(url=member.display_avatar.url)
    e.description = (
        f'**{member.mention}** was invited by {inviter_str}\n'
        f'\n'
        f'> 📅  Joined          **{join_str}**\n'
        f'> 🏷️  Status           {status}\n'
        f'> 📨  Inviter has   **{real_invites}** real invite{"s" if real_invites != 1 else ""}'
    )
    e.set_footer(text=f'Requested by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='lb', aliases=['leaderboardinvites', 'lbi', 'invitelb'])
async def lb_cmd(ctx):
    view         = LiveLBView(author_id=ctx.author.id)
    e            = await build_lb_embed(ctx.guild)
    msg          = await ctx.reply(embed=e, view=view)
    view.message = msg
    view.task    = asyncio.create_task(live_lb_loop(view, ctx.guild))


@bot.command(name='invited')
async def invited_cmd(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply(embed=discord.Embed(title='👥  Invited', description='**Usage:** `$invited @user`\nShows everyone a user has invited with their current status.', color=0x5865F2))
    async with db.pool.acquire() as c:
        rows = await c.fetch(
            '''SELECT mi.user_id, mi.joined_at, mi.is_rejoin
               FROM member_invites mi
               WHERE mi.guild_id=$1 AND mi.inviter_id=$2
               ORDER BY mi.joined_at DESC''',
            ctx.guild.id, member.id
        )
    if not rows:
        e = discord.Embed(color=0x5865F2)
        e.set_author(name=member.display_name, icon_url=member.display_avatar.url)
        e.set_thumbnail(url=member.display_avatar.url)
        e.description = f'**{member.mention} has not invited anyone yet.**'
        e.set_footer(text=f'Requested by {ctx.author.display_name}')
        return await ctx.reply(embed=e)

    verified_role = ctx.guild.get_role(VERIFIED_ROLE)
    lines  = []
    counts = {'verified': 0, 'unverified': 0, 'left': 0, 'rejoin': 0}

    for r in rows:
        invited_member = ctx.guild.get_member(r['user_id'])
        if r.get('is_rejoin'):
            counts['rejoin'] += 1
            tag = '🔄'
        elif invited_member is None:
            counts['left'] += 1
            tag = '🚪'
        elif verified_role and verified_role in invited_member.roles:
            counts['verified'] += 1
            tag = '✅'
        else:
            counts['unverified'] += 1
            tag = '❌'

        name = invited_member.mention if invited_member else f'`{r["user_id"]}`'
        lines.append(f'{tag} {name}')

    total = len(rows)
    real  = total - counts['left'] - counts['rejoin'] - counts['unverified']

    # Split list into two columns
    half     = (len(lines) + 1) // 2
    col_left  = lines[:half]
    col_right = lines[half:]

    e = discord.Embed(title=f'👥  Invited by {member.display_name}', color=0x5865F2)
    e.set_author(name=member.display_name, icon_url=member.display_avatar.url)
    e.set_thumbnail(url=member.display_avatar.url)
    e.description = (
        f'{member.mention} has invited **{total}** member{"s" if total != 1 else ""}\n'
        f'\n'
        f'✅ {counts["verified"]}  ·  🚪 {counts["left"]}  ·  🔄 {counts["rejoin"]}  ·  ❌ {counts["unverified"]}'
    )
    if col_left:
        e.add_field(name='​', value='\n'.join(col_left[:15]),  inline=True)
    if col_right:
        e.add_field(name='​', value='\n'.join(col_right[:15]), inline=True)
    if total > 30:
        e.set_footer(text=f'Showing 30 of {total}  ·  Requested by {ctx.author.display_name}')
    else:
        e.set_footer(text=f'Requested by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='clearinvites')
async def clearinvites_cmd(ctx, target: str = None):
    if ctx.author.id != OWNER_ID and not ctx.author.guild_permissions.administrator:
        return await ctx.reply(embed=discord.Embed(description='You need to be the server owner or an administrator to use this command.', color=0xED4245))
    if not target:
        e = discord.Embed(title='🗑️  Clear Invites', color=0x5865F2)
        e.description = (
            '**Usage:**\n'
            '`$clearinvites all` — Reset every member\'s invite stats\n'
            '`$clearinvites @user` — Reset a specific member\'s stats'
        )
        return await ctx.reply(embed=e)

    if target.lower() == 'all':
        async with db.pool.acquire() as c:
            await c.execute('DELETE FROM invite_stats   WHERE guild_id=$1', ctx.guild.id)
            await c.execute('DELETE FROM member_invites WHERE guild_id=$1', ctx.guild.id)
            await c.execute('DELETE FROM member_left    WHERE guild_id=$1', ctx.guild.id)
        e = discord.Embed(color=0x57F287)
        e.title       = '✅  All Invite Stats Cleared'
        e.description = f'All invite stats across **{ctx.guild.name}** have been reset.'
        e.set_footer(text=f'Cleared by {ctx.author.display_name}')
        return await ctx.reply(embed=e)

    try:
        member = await commands.MemberConverter().convert(ctx, target)
    except Exception:
        return await ctx.reply(embed=discord.Embed(
            description=f'Could not find `{target}`. Please mention a member or use `all` to reset everyone.',
            color=0xED4245
        ))
    async with db.pool.acquire() as c:
        await c.execute(
            'DELETE FROM invite_stats WHERE guild_id=$1 AND inviter_id=$2',
            ctx.guild.id, member.id
        )
    e = discord.Embed(color=0x57F287)
    e.title       = '✅  Invite Stats Cleared'
    e.description = f'All invite stats have been reset for {member.mention}.'
    await ctx.reply(embed=e)


# ================================================================== utility commands

@bot.event
async def on_message_delete(message: discord.Message):
    if message.author.bot:
        return
    if len(snipe_cache) >= 500:
        snipe_cache.pop(next(iter(snipe_cache)), None)
    snipe_cache[message.channel.id] = {
        'content':     message.content or '[embed or attachment]',
        'author':      message.author,
        'avatar':      message.author.display_avatar.url,
        'attachments': [a.url for a in message.attachments],
    }


@bot.event
async def on_message_edit(before: discord.Message, after: discord.Message):
    if before.author.bot or before.content == after.content:
        return
    if len(esnipe_cache) >= 500:
        esnipe_cache.pop(next(iter(esnipe_cache)), None)
    esnipe_cache[before.channel.id] = {
        'before': before.content or '[no content]',
        'after':  after.content  or '[no content]',
        'author': before.author,
        'avatar': before.author.display_avatar.url,
    }


@bot.command(name='snipe', aliases=['sn'])
async def snipe_cmd(ctx):
    data = snipe_cache.get(ctx.channel.id)
    if not data:
        return await ctx.reply(embed=discord.Embed(title='👻  Snipe', description='There are no recently deleted messages in this channel.', color=0x5865F2))
    e = discord.Embed(title='👻  Deleted Message', color=0x5865F2)
    e.set_author(name=data['author'].display_name, icon_url=data['avatar'])
    e.description = data['content']
    if data.get('attachments'):
        e.add_field(name='📎  Attachments', value='\n'.join(data['attachments']), inline=False)
    e.set_footer(text=f'Sniped by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='esnipe', aliases=['es'])
async def esnipe_cmd(ctx):
    data = esnipe_cache.get(ctx.channel.id)
    if not data:
        return await ctx.reply(embed=discord.Embed(title='👻  Edit Snipe', description='There are no recently edited messages in this channel.', color=0x5865F2))
    e = discord.Embed(title='👻  Edited Message', color=0xFEE75C)
    e.set_author(name=data['author'].display_name, icon_url=data['avatar'])
    e.add_field(name='📤  Before', value=data['before'][:1024], inline=False)
    e.add_field(name='📥  After',  value=data['after'][:1024],  inline=False)
    e.set_footer(text=f'Sniped by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='ping')
async def ping_cmd(ctx):
    latency = round(bot.latency * 1000)
    bar = '█' * min(10, latency // 10) + '░' * max(0, 10 - latency // 10)
    e = discord.Embed(color=0x57F287 if latency < 150 else 0xFEE75C if latency < 300 else 0xED4245)
    e.title       = '🏓  Latency'
    e.description   = f'`{bar}` **{latency}ms**'
    e.set_footer(text=f'Uptime: {fmt_uptime(datetime.now(timezone.utc) - BOT_START)}')
    await ctx.reply(embed=e)


@bot.command(name='uptime')
async def uptime_cmd(ctx):
    delta = datetime.now(timezone.utc) - BOT_START
    e = discord.Embed(title='⏱️  Uptime', description=f'Online for **{fmt_uptime(delta)}** without a restart.', color=0x57F287)
    e.set_footer(text="Trial's Cross Trade Middleman Service Bot")
    await ctx.reply(embed=e)



@bot.command(name='slowmode', aliases=['slow'])
@staff_only()
async def slowmode_cmd(ctx, seconds: int = None):
    if seconds is None:
        return await ctx.reply(embed=discord.Embed(title='⚡  Slowmode', description='**Usage:** `$slowmode <seconds>`\nSet the slowmode delay for this channel. Use `0` to disable it.\n\n**Range:** 0 – 21600 seconds', color=0x5865F2))
    if not 0 <= seconds <= 21600:
        return await ctx.reply(embed=discord.Embed(description='Slowmode must be between **0** and **21600** seconds (6 hours).', color=0xED4245))
    await ctx.channel.edit(slowmode_delay=seconds)
    if seconds == 0:
        await ctx.reply(embed=discord.Embed(description=f'⚡ Slowmode disabled in {ctx.channel.mention}.', color=0x57F287))
    else:
        await ctx.reply(embed=discord.Embed(description=f'🐢 Slowmode set to **{seconds}s** in {ctx.channel.mention}.', color=0x57F287))


@bot.command(name='say')
@owner_only()
async def say_cmd(ctx, channel: discord.TextChannel = None, *, message: str = None):
    if not channel or not message:
        return await ctx.reply(embed=discord.Embed(title='📢  Say', description='**Usage:** `$say #channel <message>`\nMakes the bot send a message in the specified channel.', color=0x5865F2))
    try:
        await ctx.message.delete()
    except Exception:
        pass
    await channel.send(message)


@bot.command(name='userinfo', aliases=['ui', 'whois'])
async def userinfo_cmd(ctx, member: discord.Member = None):
    member    = member or ctx.author
    now       = datetime.now(timezone.utc)
    created   = member.created_at.replace(tzinfo=timezone.utc) if member.created_at.tzinfo is None else member.created_at
    joined    = member.joined_at.replace(tzinfo=timezone.utc)  if member.joined_at and member.joined_at.tzinfo is None else member.joined_at
    acc_age   = (now - created).days
    srv_age   = (now - joined).days if joined else 0
    roles     = [r.mention for r in reversed(member.roles) if r != ctx.guild.default_role]
    e = discord.Embed(color=member.color.value if member.color.value else 0x5865F2)
    e.set_author(name=str(member), icon_url=member.display_avatar.url)
    e.set_thumbnail(url=member.display_avatar.url)
    e.add_field(name='🪪  ID',            value=str(member.id),          inline=True)
    e.add_field(name='💬  Nickname',      value=member.nick or 'None',   inline=True)
    e.add_field(name='🤖  Bot',           value='Yes' if member.bot else 'No', inline=True)
    e.add_field(name='📅  Account Age',   value=f'{acc_age}d',           inline=True)
    e.add_field(name='📆  In Server',     value=f'{srv_age}d',           inline=True)
    e.add_field(name='🟢  Status',        value=str(member.status).title(), inline=True)
    e.add_field(
        name=f'🏷️  Roles ({len(roles)})',
        value=' '.join(roles[:15]) + (' ...' if len(roles) > 15 else '') if roles else 'none',
        inline=False
    )
    e.set_footer(text=f'Requested by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='serverinfo', aliases=['si', 'server'])
async def serverinfo_cmd(ctx):
    guild   = ctx.guild
    bots    = sum(1 for m in guild.members if m.bot)
    humans  = guild.member_count - bots
    online  = sum(1 for m in guild.members if m.status != discord.Status.offline and not m.bot)
    created = guild.created_at.replace(tzinfo=timezone.utc) if guild.created_at.tzinfo is None else guild.created_at
    age     = (datetime.now(timezone.utc) - created).days
    e = discord.Embed(title=f'🏠  {guild.name}', color=0x5865F2)
    if guild.icon:
        e.set_thumbnail(url=guild.icon.url)
    e.add_field(name='👑  Owner',        value=guild.owner.mention if guild.owner else 'Unknown', inline=True)
    e.add_field(name='🪪  Server ID',    value=str(guild.id),   inline=True)
    e.add_field(name='📅  Age',           value=f'{age}d',       inline=True)
    e.add_field(name='👥  Members',       value=f'{guild.member_count:,}', inline=True)
    e.add_field(name='🧑  Humans',        value=f'{humans:,}',   inline=True)
    e.add_field(name='🤖  Bots',          value=str(bots),       inline=True)
    e.add_field(name='🟢  Online',        value=str(online),     inline=True)
    e.add_field(name='💬  Channels',      value=str(len(guild.channels)), inline=True)
    e.add_field(name='🏷️  Roles',         value=str(len(guild.roles)),    inline=True)
    e.add_field(name='🚀  Boost Level',   value=str(guild.premium_tier),  inline=True)
    e.add_field(name='💎  Boosts',        value=str(guild.premium_subscription_count), inline=True)
    e.add_field(name='😄  Emojis',        value=f'{len(guild.emojis)}/{guild.emoji_limit}', inline=True)
    e.set_footer(text=f'Requested by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='membercount', aliases=['mc'])
async def membercount_cmd(ctx):
    guild  = ctx.guild
    total  = guild.member_count
    bots   = sum(1 for m in guild.members if m.bot)
    humans = total - bots
    online = sum(1 for m in guild.members if m.status != discord.Status.offline and not m.bot)
    e = discord.Embed(title=f'{guild.name}', color=0x5865F2)
    e.set_thumbnail(url=guild.icon.url if guild.icon else None)
    e.add_field(name='👥  Total',   value=f'{total:,}',   inline=True)
    e.add_field(name='🧑  Humans',  value=f'{humans:,}', inline=True)
    e.add_field(name='🤖  Bots',    value=str(bots),     inline=True)
    e.add_field(name='🟢  Online',  value=str(online),   inline=True)
    e.set_footer(text=f'Requested by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='newest', aliases=['nw'])
async def newest_cmd(ctx):
    members = sorted(
        [m for m in ctx.guild.members if not m.bot],
        key=lambda m: m.joined_at or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True
    )[:5]
    e = discord.Embed(title=f'🆕  Newest Members', color=0x57F287)
    e.set_author(name=ctx.guild.name, icon_url=ctx.guild.icon.url if ctx.guild.icon else None)
    lines = []
    for i, m in enumerate(members, 1):
        lines.append(f'**{i}.** {m.mention}')
    e.description = '\n'.join(lines)
    e.set_footer(text=f'Requested by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='oldest', aliases=['ol'])
async def oldest_cmd(ctx):
    members = sorted(
        [m for m in ctx.guild.members if not m.bot],
        key=lambda m: m.joined_at or datetime.now(timezone.utc)
    )[:5]
    e = discord.Embed(title=f'👴  Longest Standing Members', color=0xF1C40F)
    e.set_author(name=ctx.guild.name, icon_url=ctx.guild.icon.url if ctx.guild.icon else None)
    lines = []
    for i, m in enumerate(members, 1):
        lines.append(f'**{i}.** {m.mention}')
    e.description = '\n'.join(lines)
    e.set_footer(text=f'Requested by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='botlist', aliases=['bl'])
async def botlist_cmd(ctx):
    bots = sorted([m for m in ctx.guild.members if m.bot], key=lambda m: m.name.lower())
    if not bots:
        return await ctx.reply(embed=discord.Embed(description='There are no bots in this server.', color=0x5865F2))
    lines = [f'**{i}.** {b.mention}' for i, b in enumerate(bots, 1)]
    e = discord.Embed(title=f'🤖  Bots  ({len(bots)})', color=0x5865F2)
    e.set_author(name=ctx.guild.name, icon_url=ctx.guild.icon.url if ctx.guild.icon else None)
    e.description = '\n'.join(lines)
    e.set_footer(text=f'Requested by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='ticketstats', aliases=['ts', 'tstats'])
async def ticketstats_cmd(ctx):
    member = ctx.author
    async with db.pool.acquire() as c:
        row = await c.fetchrow(
            'SELECT claimed, closed, total_rating, rating_count FROM ticket_stats WHERE guild_id=$1 AND user_id=$2',
            ctx.guild.id, member.id
        )
    claimed      = row['claimed']      if row else 0
    closed       = row['closed']       if row else 0
    total_rating = row['total_rating'] if row else 0
    rating_count = row['rating_count'] if row else 0
    completion   = f'{round((closed / claimed) * 100)}%' if claimed > 0 else 'N/A'
    avg_rating   = f'{total_rating / rating_count:.1f} / 5.0  ({rating_count} rating{"s" if rating_count != 1 else ""})' if rating_count > 0 else 'no ratings yet'

    stars = ''
    if rating_count > 0:
        avg = total_rating / rating_count
        full  = int(avg)
        stars = '★' * full + '☆' * (5 - full)

    e = discord.Embed(color=0x5865F2)
    e.set_author(name=member.display_name, icon_url=member.display_avatar.url)
    e.set_thumbnail(url=member.display_avatar.url)
    e.add_field(name='✋  Claimed',      value=str(claimed),    inline=True)
    e.add_field(name='🔒  Closed',       value=str(closed),     inline=True)
    e.add_field(name='📊  Completion',   value=completion,      inline=True)
    e.add_field(name='⭐  Rating',       value=f'{stars}  {avg_rating}' if stars else avg_rating, inline=False)
    e.set_footer(text='Run $rateme inside a ticket to request a rating from the creator.')
    await ctx.reply(embed=e)



class RatingView(View):
    def __init__(self, claimer_id: int, guild_id: int, ticket_id: str, user_id: int):
        super().__init__(timeout=86400)  # 24h to respond
        self.claimer_id = claimer_id
        self.guild_id   = guild_id
        self.ticket_id  = ticket_id
        self.user_id    = user_id
        self.rated      = False

    async def _submit(self, interaction: discord.Interaction, rating: int):
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message(
                embed=discord.Embed(description='This rating request was not sent to you.', color=0xED4245),
                ephemeral=True
            )
        if self.rated:
            return await interaction.response.send_message(
                embed=discord.Embed(description='You have already submitted a rating for this ticket.', color=0xFEE75C),
                ephemeral=True
            )
        self.rated = True
        self.stop()
        async with db.pool.acquire() as c:
            existing = await c.fetchval(
                'SELECT rating FROM ticket_ratings WHERE guild_id=$1 AND ticket_id=$2',
                self.guild_id, self.ticket_id
            )
            if existing:
                await interaction.response.edit_message(
                    embed=discord.Embed(description='This ticket has already been rated.', color=0xFEE75C),
                    view=None
                )
                return
            await c.execute(
                'INSERT INTO ticket_ratings (guild_id, ticket_id, claimer_id, user_id, rating) VALUES ($1,$2,$3,$4,$5)',
                self.guild_id, self.ticket_id, self.claimer_id, self.user_id, rating
            )
            await c.execute(
                '''INSERT INTO ticket_stats (guild_id, user_id, total_rating, rating_count)
                   VALUES ($1, $2, $3, 1)
                   ON CONFLICT (guild_id, user_id) DO UPDATE
                   SET total_rating  = ticket_stats.total_rating  + $3,
                       rating_count  = ticket_stats.rating_count  + 1''',
                self.guild_id, self.claimer_id, rating
            )
        stars = '★' * rating + '☆' * (5 - rating)
        e = discord.Embed(color=0x57F287)
        e.title       = '⭐  Rating Submitted'
        e.description = f'Thanks for your feedback — it helps us improve.\n\n{stars}  **{rating} / 5**'
        e.set_footer(text="Trial's Cross Trade Middleman Service")
        await interaction.response.edit_message(embed=e, view=None)

    @discord.ui.button(label='1', style=ButtonStyle.red,    custom_id='rate_1')
    async def r1(self, i, _): await self._submit(i, 1)
    @discord.ui.button(label='2', style=ButtonStyle.red,    custom_id='rate_2')
    async def r2(self, i, _): await self._submit(i, 2)
    @discord.ui.button(label='3', style=ButtonStyle.gray,   custom_id='rate_3')
    async def r3(self, i, _): await self._submit(i, 3)
    @discord.ui.button(label='4', style=ButtonStyle.green,  custom_id='rate_4')
    async def r4(self, i, _): await self._submit(i, 4)
    @discord.ui.button(label='5', style=ButtonStyle.green,  custom_id='rate_5')
    async def r5(self, i, _): await self._submit(i, 5)


@bot.command(name='rateme')
@staff_only()
async def rateme_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description='This command can only be used inside a ticket channel.', color=0xED4245))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
    if not ticket:
        return await ctx.reply(embed=discord.Embed(description='No ticket found in this channel.', color=0xED4245))
    if not ticket.get('claimed_by'):
        return await ctx.reply(embed=discord.Embed(description='this ticket has not been claimed yet', color=0xED4245))
    if ticket['claimed_by'] != ctx.author.id:
        return await ctx.reply(embed=discord.Embed(description='only the claimer of this ticket can request a rating', color=0xED4245))
    creator = ctx.guild.get_member(ticket['user_id'])
    if not creator:
        return await ctx.reply(embed=discord.Embed(description="can't find the ticket creator — they may have left", color=0xED4245))
    if creator.id == ctx.author.id:
        return await ctx.reply(embed=discord.Embed(description='You cannot rate yourself.', color=0xED4245))
    # check already rated
    async with db.pool.acquire() as c:
        already = await c.fetchval(
            'SELECT rating FROM ticket_ratings WHERE guild_id=$1 AND ticket_id=$2',
            ctx.guild.id, ticket['ticket_id']
        )
    if already:
        return await ctx.reply(embed=discord.Embed(description='This ticket has already been rated.', color=0xFEE75C))

    view = RatingView(
        claimer_id=ctx.author.id,
        guild_id=ctx.guild.id,
        ticket_id=ticket['ticket_id'],
        user_id=creator.id
    )
    e = discord.Embed(color=0x5865F2)
    e.set_author(name="Trial's Cross Trade Middleman Service", icon_url=ctx.guild.icon.url if ctx.guild.icon else None)
    e.title       = '⭐  Rate Your Experience'
    e.description = (
        f'**{ctx.author.display_name}** handled your ticket **#{ticket["ticket_id"]}**.\n\n'
        f'How did it go? Tap a number below to leave a rating.\n'
        f'> **1** — Very poor  ·  **3** — Okay  ·  **5** — Excellent'
    )
    e.set_footer(text='You have 24 hours to respond  ·  Your rating is private')
    try:
        await creator.send(embed=e, view=view)
        resp = discord.Embed(description=f'✅ Rating request sent to {creator.mention}.', color=0x57F287)
        resp.set_footer(text="They have 24 hours to respond. It will appear in your stats automatically.")
        await ctx.reply(embed=resp)
    except discord.Forbidden:
        await ctx.reply(embed=discord.Embed(
            description=f"{creator.mention} has DMs disabled — can't send the rating request",
            color=0xFEE75C
        ))


@bot.command(name='activemm', aliases=['amm', 'onlinemm'])
async def activemm_cmd(ctx):
    guild      = ctx.guild
    tier_order = ['hightier', 'midtier', 'lowtier']
    tier_names = {'hightier': 'High Tier', 'midtier': 'Mid Tier', 'lowtier': 'Low Tier'}
    online_statuses = {discord.Status.online, discord.Status.idle, discord.Status.dnd}

    sections     = []
    total_online = 0
    total_mm     = 0

    for tier in tier_order:
        role = guild.get_role(ROLES[tier])
        if not role:
            continue
        members = [m for m in role.members if not m.bot]
        total_mm += len(members)
        online   = [m for m in members if m.status in online_statuses]
        offline  = [m for m in members if m.status not in online_statuses]
        total_online += len(online)

        lines = []
        for m in sorted(online,  key=lambda x: x.display_name.lower()):
            dot = '🟢' if m.status == discord.Status.online else ('🌙' if m.status == discord.Status.idle else '🔴')
            lines.append(f'{dot} {m.mention}')
        for m in sorted(offline, key=lambda x: x.display_name.lower()):
            lines.append(f'⚫ {m.mention}')
        if lines:
            sections.append(
                f'**{tier_names[tier]}** — {len(online)}/{len(members)} online\n'
                + '  '.join(lines)
            )

    if not sections:
        return await ctx.reply(embed=discord.Embed(title='⚖️  Active Middlemen', description='No middlemen are currently online.', color=0x5865F2))

    e = discord.Embed(
        title='⚖️  Active Middlemen',
        color=0x57F287 if total_online > 0 else 0x747F8D
    )
    e.description = '\n\n'.join(sections)
    e.set_footer(text=f'{total_online}/{total_mm} online  ·  Requested by {ctx.author.display_name}')
    await ctx.reply(embed=e)


# ================================================================== help command

HELP_PAGES = [
    # ─────────────────────────── PAGE 1 — TICKETS
    {
        'title': '🎫  Tickets',
        'color': 0x5865F2,
        'fields': [
            {
                'name':  '╔══ 📥  Opening a Ticket',
                'value': (
                    '╚► Use the ticket panel to open a ticket.\n'
                    '┃\n'
                    '┣ 🔵  **Support** — Questions, reports & appeals\n'
                    '┣ 🟣  **Middleman** — Secure trades, pick your tier\n'
                    '┗ 🟡  **Reward** — Claim giveaways & invite rewards'
                ),
            },
            {
                'name':  '╔══ 🔧  Managing a Ticket',
                'value': (
                    '╚► Staff commands inside a ticket.\n'
                    '┃\n'
                    '┣ `$claim` ————————— Claim the ticket as yours\n'
                    '┣ `$unclaim` —————— Drop your claim\n'
                    '┣ `$close` ————————— Close & save a transcript\n'
                    '┣ `$add @user` ———— Grant someone access\n'
                    '┣ `$remove @user` —— Revoke access\n'
                    '┣ `$rename <name>` — Rename the channel\n'
                    '┗ `$transfer @user` — Hand off your claim'
                ),
            },
            {
                'name':  '╔══ 🔒  Ticket Lock',
                'value': (
                    '╚► Restrict who can talk while a ticket is claimed.\n'
                    '┃\n'
                    '┣ `$locktic` ——— Lock to claimer + creator only\n'
                    '┗ `$unlocktic` — Restore access for all'
                ),
            },
            {
                'name':  '╔══ 📊  Stats & Proof',
                'value': (
                    '╚► Track performance & post proof.\n'
                    '┃\n'
                    '┣ `$ticketstats` — Your claimed / closed / rating  `$ts`\n'
                    '┣ `$rateme` ———— Send rating request to ticket creator\n'
                    '┗ `$proof` ———— Post completed trade proof'
                ),
            },
        ],
    },
    # ─────────────────────────── PAGE 2 — STAFF TOOLS
    {
        'title': '🛡️  Staff Tools',
        'color': 0xED4245,
        'fields': [
            {
                'name':  '╔══ 🔨  Moderation',
                'value': (
                    '╚► Control access and behavior.\n'
                    '┃\n'
                    '┣ `$blacklist @user [reason]` — Block from opening tickets\n'
                    '┣ `$unblacklist @user` ———— Remove from blacklist\n'
                    '┣ `$blacklists` ———————— View all blacklisted users\n'
                    '┣ `$slowmode <secs>` ———— Set slowmode, `0` = disable  `$slow`\n'
                    '┣ `$lock` ————————————— Disable ticket creation\n'
                    '┣ `$unlock` ——————————— Re-enable ticket creation\n'
                    '┣ `$say #channel <msg>` — Send a message as the bot\n'
                    '┣ `$botstats` ——————— Today\'s server activity  `$bstats`\n'
                    '┗ `$activity` ———————— Top 10 active members today'
                ),
            },
            {
                'name':  '╔══ ⚙️  Channel Permissions',
                'value': (
                    '╚► Edit what roles/users can do in channels.\n'
                    '┃\n'
                    '┣ `$channelperm #ch @target <perm> <on/off>`\n'
                    '┃   └ Set a perm in one specific channel\n'
                    '┗ `$channelpermall @target <perm> <on/off>`\n'
                    '    └ Set a perm across every channel at once'
                ),
            },
        ],
    },
    # ─────────────────────────── PAGE 3 — PERMISSIONS LIST
    {
        'title': '📋  Permission Names',
        'color': 0xED4245,
        'fields': [
            {
                'name':  '╔══ 💬  Text Permissions',
                'value': (
                    '╚► Pass any alias as `<perm>`.\n'
                    '┃\n'
                    '┣ `send` / `send_messages`\n'
                    '┣ `read` / `view` / `view_channel` / `read_messages`\n'
                    '┣ `history` / `read_history` / `read_message_history`\n'
                    '┣ `embed` / `embeds` / `embed_links`\n'
                    '┣ `attach` / `attach_files`\n'
                    '┣ `react` / `add_reactions`\n'
                    '┣ `mentions` / `mention_everyone`\n'
                    '┣ `external` / `external_emojis` / `use_external_emojis`\n'
                    '┣ `stickers` / `use_external_stickers`\n'
                    '┣ `tts` / `send_tts_messages`\n'
                    '┣ `polls` / `send_polls`\n'
                    '┣ `slash` / `use_application_commands`\n'
                    '┣ `threads` / `create_public_threads`\n'
                    '┣ `private_threads` / `create_private_threads`\n'
                    '┗ `send_in_threads` / `send_messages_in_threads`'
                ),
            },
            {
                'name':  '╔══ 🎙️  Voice Permissions',
                'value': (
                    '╚► Voice channel perms.\n'
                    '┃\n'
                    '┣ `voice` / `connect`\n'
                    '┣ `speak` / `talk`\n'
                    '┣ `stream` / `video` / `screen`\n'
                    '┣ `mute` / `mute_members`\n'
                    '┣ `deafen` / `deafen_members`\n'
                    '┣ `move` / `move_members`\n'
                    '┣ `vad` / `voice_activity` / `use_voice_activation`\n'
                    '┣ `soundboard` / `use_soundboard`\n'
                    '┣ `external_sounds` / `use_external_sounds`\n'
                    '┣ `activities` / `embedded_activities` / `use_embedded_activities`\n'
                    '┣ `priority` / `priority_speaker`\n'
                    '┗ `stage` / `request_to_speak`'
                ),
            },
            {
                'name':  '╔══ 🔧  Manage Permissions',
                'value': (
                    '╚► Admin-level channel perms.\n'
                    '┃\n'
                    '┣ `pin` / `manage` / `manage_messages`\n'
                    '┣ `slow` / `manage_channels`\n'
                    '┣ `webhooks` / `manage_webhooks`\n'
                    '┗ `manage_perms` / `manage_permissions`'
                ),
            },
        ],
    },
    # ─────────────────────────── PAGE 4 — UTILITY
    {
        'title': '🔧  Utility',
        'color': 0x57F287,
        'fields': [
            {
                'name':  '╔══ 🔍  Message Tools',
                'value': (
                    '╚► Recover deleted or edited messages.\n'
                    '┃\n'
                    '┣ `$snipe` — Last deleted message in this channel  `$sn`\n'
                    '┗ `$esnipe` — Last edited message, before & after  `$es`'
                ),
            },
            {
                'name':  '╔══ 👤  Member Info',
                'value': (
                    '╚► Look up members and server details.\n'
                    '┃\n'
                    '┣ `$userinfo [@user]` — Roles, age, join date, status  `$ui` `$whois`\n'
                    '┣ `$serverinfo` ——— Server stats overview  `$si`\n'
                    '┣ `$membercount` ——— Total, human, bot & online  `$mc`\n'
                    '┣ `$newest` ————— 5 most recently joined members  `$nw`\n'
                    '┣ `$oldest` ————— 5 longest standing members  `$ol`\n'
                    '┣ `$botlist` ———— All bots in the server  `$bl`\n'
                    '┗ `$activemm` ———— Online middlemen by tier  `$amm`'
                ),
            },
            {
                'name':  '╔══ 🤖  Bot',
                'value': (
                    '╚► Bot status.\n'
                    '┃\n'
                    '┣ `$ping` ——— Bot latency\n'
                    '┗ `$uptime` — How long the bot has been online'
                ),
            },
        ],
    },
    # ─────────────────────────── PAGE 5 — INVITE TRACKING
    {
        'title': '📨  Invite Tracking',
        'color': 0x5865F2,
        'fields': [
            {
                'name':  '╔══ 📈  How It Works',
                'value': (
                    '╚► Every join is tracked automatically.\n'
                    '┃\n'
                    '┣ 📥  **Invites** — Total people who joined via your link\n'
                    '┣ 🚪  **Left** — Joined but has since left\n'
                    '┣ 🔄  **Rejoins** — Left then rejoined within 7 days\n'
                    '┣ 🤖  **Fake** — Account under 3 days old at join\n'
                    '┗ ✅  **Verified** — Completed verification\n'
                    '\n'
                    '> **Real count** = Invites − Left − Rejoins − Fake'
                ),
            },
            {
                'name':  '╔══ 📋  Commands',
                'value': (
                    '╚► View and manage invite stats.\n'
                    '┃\n'
                    '┣ `$invites [@user]` ——— Your or someone\'s invite stats\n'
                    '┣ `$invited @user` ————— Everyone that user has invited\n'
                    '┣ `$whoinvited [@user]` — Who invited a specific member\n'
                    '┣ `$lb` ————————————— Live top-10 leaderboard  `$lbi` `$invitelb`\n'
                    '┣ `$createcustomlink` —— Personal invite link  `$ccl`\n'
                    '┣ `$clearinvites all` —— Reset all invite stats  *(owner)*\n'
                    '┗ `$clearinvites @user` — Reset one user\'s stats  *(owner)*'
                ),
            },
        ],
    },
    # ─────────────────────────── PAGE 6 — SETUP
    {
        'title': '⚙️  Server Setup',
        'color': 0xFEE75C,
        'fields': [
            {
                'name':  '╔══ 📁  Configuration  —  Owner Only',
                'value': (
                    '╚► Run once to configure the bot.\n'
                    '┃\n'
                    '┣ `$setup` ——————————————— Post the ticket panel\n'
                    '┣ `$setupverify` ————————— Post the verification panel\n'
                    '┣ `$setcategory #category` — Set where tickets are created\n'
                    '┣ `$setlogs #channel` ———— Set transcript & audit log channel\n'
                    '┗ `$config` ——————————————— View full config, channels & latency'
                ),
            },
        ],
    },
]
def make_help_embed(page: int) -> discord.Embed:
    p   = HELP_PAGES[page]
    e   = discord.Embed(title=p['title'], color=p.get('color', 0x5865F2))
    for field in p.get('fields', []):
        e.add_field(name=field['name'], value=field['value'], inline=False)
    e.set_footer(text=f"Trial's Cross Trade Middleman Service  ·  Page {page + 1} of {len(HELP_PAGES)}  ·  Prefix: $")
    return e


class HelpView(View):
    def __init__(self, author_id: int, page: int = 0):
        super().__init__(timeout=60)
        self.author_id = author_id
        self.page      = page
        self.message   = None
        self._update_buttons()

    def _update_buttons(self):
        self.prev_btn.disabled = self.page == 0
        self.next_btn.disabled = self.page == len(HELP_PAGES) - 1

    @discord.ui.button(label='◀  Back', style=ButtonStyle.blurple, custom_id='help_prev')
    async def prev_btn(self, interaction: discord.Interaction, _):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message(
                embed=discord.Embed(description='This help menu belongs to someone else.', color=0xED4245),
                ephemeral=True
            )
        self.page -= 1
        self._update_buttons()
        await interaction.response.edit_message(embed=make_help_embed(self.page), view=self)

    @discord.ui.button(label='Next  ▶', style=ButtonStyle.blurple, custom_id='help_next')
    async def next_btn(self, interaction: discord.Interaction, _):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message(
                embed=discord.Embed(description='This help menu belongs to someone else.', color=0xED4245),
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
            e.set_footer(text='session timed out  ·  run $help to open a new one')
            await self.message.edit(embed=e, view=self)
        except Exception:
            pass



@bot.command(name='createcustomlink', aliases=['ccl', 'custominvite'])
async def createcustomlink_cmd(ctx):
    member = ctx.author
    # Use the invite channel or welcome channel as the invite target
    channel = ctx.guild.get_channel(INVITE_CHANNEL) or ctx.guild.get_channel(WELCOME_CHANNEL) or ctx.channel
    try:
        invite = await channel.create_invite(max_age=0, max_uses=0, unique=True, reason=f'custom invite for {member.display_name}')
    except discord.Forbidden:
        return await ctx.reply(embed=discord.Embed(description='I don\'t have permission to create invites in that channel.', color=0xED4245))
    except Exception as ex:
        logger.error(f'createcustomlink: {ex}')
        return await ctx.reply(embed=discord.Embed(description='Something went wrong while creating the invite. Please try again.', color=0xED4245))
    async with db.pool.acquire() as c:
        await c.execute(
            'DELETE FROM custom_invites WHERE guild_id=$1 AND user_id=$2',
            ctx.guild.id, member.id
        )
        await c.execute(
            'INSERT INTO custom_invites (guild_id, code, user_id, created_by) VALUES ($1,$2,$3,$4)',
            ctx.guild.id, invite.code, member.id, ctx.author.id
        )
    invite_cache.setdefault(ctx.guild.id, {})[invite.code] = 0
    e = discord.Embed(color=0x57F287)
    e.set_author(name=ctx.guild.name, icon_url=ctx.guild.icon.url if ctx.guild.icon else None)
    e.description = (
        f'here\'s your custom invite link.\n\n'
        f'anyone who joins using it will count toward your invites.\n\n'
        f'`discord.gg/{invite.code}`'
    )
    e.set_footer(text='never expires  •  unlimited uses')
    try:
        await ctx.author.send(embed=e)
        await ctx.reply(embed=discord.Embed(title='✅  Invite Link Created', description='Your personal invite link has been sent to your DMs. Anyone who joins using it will count towards your invite stats.', color=0x57F287))
    except discord.Forbidden:
        await ctx.reply(embed=discord.Embed(description='Unable to send you a DM. Please make sure your DMs are open and try again.', color=0xFEE75C))



@bot.command(name='botstats', aliases=['bstats'])
@staff_only()
async def botstats_cmd(ctx):
    today        = datetime.now(timezone.utc).date()
    stats        = daily_stats.get(ctx.guild.id, {})
    cmds         = stats.get('commands', 0)
    tickets      = stats.get('tickets', 0)
    verifs       = stats.get('verifications', 0)
    active_users = len(message_counts.get(ctx.guild.id, {}))
    total_msgs   = sum(message_counts.get(ctx.guild.id, {}).values())

    # pull from DB for all-time ticket total and open tickets
    async with db.pool.acquire() as c:
        open_tickets  = await c.fetchval(
            "SELECT COUNT(*) FROM tickets WHERE guild_id=$1 AND status!='closed'", ctx.guild.id
        )
        total_tickets = await c.fetchval(
            'SELECT COUNT(*) FROM tickets WHERE guild_id=$1', ctx.guild.id
        )
        total_verifs  = await c.fetchval(
            'SELECT COUNT(*) FROM verifications WHERE guild_id=$1', ctx.guild.id
        )

    e = discord.Embed(title='📊  Bot Statistics', color=0x5865F2)
    e.add_field(
        name='📅  Today',
        value=(
            f'📟  Commands Used : **{cmds}**\n'
            f'🎫  Tickets Opened : **{tickets}**\n'
            f'✅  Verifications : **{verifs}**\n'
            f'💬  Messages Sent : **{total_msgs}**\n'
            f'👥  Active Members : **{active_users}**'
        ),
        inline=True
    )
    e.add_field(
        name='📈  All Time',
        value=(
            f'🎫  Total Tickets : **{total_tickets}**\n'
            f'🔓  Open Tickets : **{open_tickets}**\n'
            f'✅  Total Verifications : **{total_verifs}**'
        ),
        inline=True
    )
    e.set_footer(text=f'uptime: {fmt_uptime(datetime.now(timezone.utc) - BOT_START)}  •  {today.strftime("%B %d, %Y")}')
    await ctx.reply(embed=e)


@bot.command(name='activity')
@staff_only()
async def activity_cmd(ctx):
    counts = message_counts.get(ctx.guild.id, {})
    if not counts:
        return await ctx.reply(embed=discord.Embed(title='💬  Message Activity', description='No message activity has been tracked yet today.', color=0x5865F2))
    sorted_users = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:10]
    total = sum(counts.values())
    lines = []
    medals = ['🥇', '🥈', '🥉']
    for i, (uid, count) in enumerate(sorted_users):
        member = ctx.guild.get_member(uid)
        name   = member.display_name if member else f'unknown ({uid})'
        prefix = medals[i] if i < 3 else f'`{i+1}.`'
        bar    = '█' * min(int((count / sorted_users[0][1]) * 10), 10)
        lines.append(f'{prefix} **{name}** — `{count}` msgs  `{bar}`')
    e = discord.Embed(title='💬  Message Activity — Today', color=0x5865F2)
    e.description = '\n'.join(lines)
    e.set_footer(text=f'{total} total messages tracked across {len(counts)} members today')
    await ctx.reply(embed=e)


@bot.command(name='help')
async def help_cmd(ctx):
    view         = HelpView(author_id=ctx.author.id)
    view.message = await ctx.reply(embed=make_help_embed(0), view=view)
    try:
        await ctx.message.delete()
    except Exception:
        pass


# ================================================================== verification

def gen_captcha(length: int = 6) -> str:
    chars = string.ascii_uppercase + string.digits
    for c in 'O0I1':
        chars = chars.replace(c, '')
    return ''.join(random.choices(chars, k=length))


class VerifyView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Verify', style=ButtonStyle.green, emoji='✅', custom_id='btn_verify')
    async def verify(self, interaction: discord.Interaction, _):
        user          = interaction.user
        verified_role = interaction.guild.get_role(VERIFIED_ROLE)
        if verified_role and verified_role in user.roles:
            return await interaction.response.send_message(
                embed=discord.Embed(description='You are already verified.', color=0x57F287),
                ephemeral=True
            )
        code = gen_captcha()
        captchas[user.id] = code
        e = discord.Embed(title='🔐  verify', color=0x5865F2)
        e.description = f"here's your code — type it in the verify channel\n\n# `{code}`"
        e.set_footer(text="Trial's Cross Trade Middleman Service  ·  Do not share this code")
        await interaction.response.send_message(embed=e, ephemeral=True)


FONT_BOLD = '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf'
FONT_REG  = '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf'

def ordinal(n: int) -> str:
    s = {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')
    if 11 <= n % 100 <= 13:
        s = 'th'
    return f'{n}{s}'


def make_welcome_card(avatar_bytes: bytes, username: str, server_name: str, member_number: int) -> io.BytesIO:
    W, H  = 800, 200
    BG    = (15, 15, 20)
    ACC   = (255, 255, 255)
    SUB   = (180, 180, 180)
    RING  = (255, 255, 255)

    img  = Image.new('RGB', (W, H), BG)
    draw = ImageDraw.Draw(img)

    for x in range(W):
        v = int(20 * (x / W))
        draw.line([(x, 0), (x, H)], fill=(15 + v, 15 + v, 20 + v))

    AV_SIZE = 130
    AV_X    = 35
    AV_Y    = (H - AV_SIZE) // 2

    avatar_img  = Image.open(io.BytesIO(avatar_bytes)).convert('RGBA').resize((AV_SIZE, AV_SIZE))
    mask        = Image.new('L', (AV_SIZE, AV_SIZE), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, AV_SIZE - 1, AV_SIZE - 1), fill=255)
    avatar_circ = Image.new('RGBA', (AV_SIZE, AV_SIZE), (0, 0, 0, 0))
    avatar_circ.paste(avatar_img, mask=mask)

    RING_W   = 4
    ring_img = Image.new('RGBA', (AV_SIZE + RING_W * 2, AV_SIZE + RING_W * 2), (0, 0, 0, 0))
    ImageDraw.Draw(ring_img).ellipse(
        (0, 0, AV_SIZE + RING_W * 2 - 1, AV_SIZE + RING_W * 2 - 1), fill=RING
    )
    ring_mask = Image.new('L', ring_img.size, 0)
    ImageDraw.Draw(ring_mask).ellipse((0, 0, ring_img.size[0] - 1, ring_img.size[1] - 1), fill=255)
    img.paste(ring_img,    (AV_X - RING_W, AV_Y - RING_W), ring_mask)
    img.paste(avatar_circ, (AV_X, AV_Y), mask)

    TEXT_X   = AV_X + AV_SIZE + 30
    CENTER_Y = H // 2

    try:
        f_welcome = ImageFont.truetype(FONT_REG,  22)
        f_name    = ImageFont.truetype(FONT_BOLD, 36)
        f_sub     = ImageFont.truetype(FONT_REG,  20)
    except Exception:
        f_welcome = f_name = f_sub = ImageFont.load_default()

    draw.text((TEXT_X, CENTER_Y - 52), 'Welcome',                                         font=f_welcome, fill=SUB)
    draw.text((TEXT_X, CENTER_Y - 25), username,                                          font=f_name,    fill=ACC)
    draw.text((TEXT_X, CENTER_Y + 18), f'to {server_name}',                              font=f_sub,     fill=SUB)
    draw.text((TEXT_X, CENTER_Y + 44), f'you are the {ordinal(member_number)} member!',  font=f_sub,     fill=SUB)

    draw.rectangle([TEXT_X - 15, CENTER_Y - 55, TEXT_X - 10, CENTER_Y + 65], fill=(80, 80, 200))

    buf = io.BytesIO()
    img.save(buf, 'PNG')
    buf.seek(0)
    return buf


@bot.command(name='setupverify')
@owner_only()
async def setupverify_cmd(ctx):
    e = discord.Embed(color=0x5865F2)
    e.set_author(name="Trial's Cross Trade Middleman Service", icon_url=ctx.guild.icon.url if ctx.guild.icon else None)
    e.description = (
        "## 🔐  Server Verification\n\n"
        "**To access the server, you must complete the verification process.**\n\n"
        "1️⃣  Click the **Verify** button below.\n"
        "2️⃣  The bot will send you a unique verification code as an image.\n"
        "3️⃣  Type that exact code in the verification channel to confirm you are a real user.\n\n"
        "This system helps protect the server from bots, spam, and alt accounts.\n\n"
        "Once your code is confirmed, you will automatically receive full access to the server.\n"
        "-# If you experience any issues, please contact a staff member for assistance."
    )
    e.set_footer(text="Trial's Cross Trade Middleman Service  ·  Click the button below to get started")
    await ctx.send(embed=e, view=VerifyView())
    try:
        await ctx.message.delete()
    except Exception:
        pass



# ================================================================== events


@tasks.loop(seconds=30)
async def status_loop():
    idx  = status_loop.current_loop % len(STATUS_ROTATION)
    kind, text = STATUS_ROTATION[idx]
    if kind == 'watching':
        act = discord.Activity(type=discord.ActivityType.watching,  name=text)
    elif kind == 'playing':
        act = discord.Game(name=text)
    elif kind == 'listening':
        act = discord.Activity(type=discord.ActivityType.listening, name=text)
    else:
        act = discord.Game(name=text)
    await bot.change_presence(activity=act)


@tasks.loop(hours=24)
async def midnight_reset():
    daily_stats.clear()
    message_counts.clear()
    logger.info('daily stats reset')


@tasks.loop(minutes=5)
async def limiter_cleanup():
    """Prune stale rate limit entries every 5 minutes to prevent memory growth."""
    limiter.cleanup()


@bot.event
async def on_command(ctx):
    """
    Global anti-spam gate — fires before every $ command.
    Auto-suppresses users sending commands too fast.
    """
    if ctx.guild:
        daily_stats[ctx.guild.id]['commands'] += 1
    allowed, remaining = limiter.global_check(ctx.author.id)
    if not allowed:
        secs = int(remaining)
        try:
            await ctx.reply(
                embed=discord.Embed(
                    description=f'⛔ You are sending commands too fast. Please wait **{secs}s** before trying again.',
                    color=0xED4245
                ),
                delete_after=float(secs)
            )
        except Exception:
            pass
        raise commands.CheckFailure('global rate limit')


@bot.event
async def on_ready():
    global _bot_ready
    logger.info(f'logged in as {bot.user} in {len(bot.guilds)} guild(s)')
    if not status_loop.is_running():
        status_loop.start()
    if not limiter_cleanup.is_running():
        limiter_cleanup.start()
    if not midnight_reset.is_running():
        midnight_reset.start()

    if not _bot_ready:
        _bot_ready = True
        await bot.tree.sync()
        logger.info('slash commands synced')

        try:
            await db.connect()
        except Exception as ex:
            logger.error(f'db failed: {ex}')
            return

        bot.add_view(TicketPanel())
        bot.add_view(ControlView())
        bot.add_view(VerifyView())

        # cache invites on every ready (reconnect refreshes cache)
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
    # unwrap command invoke errors
    if isinstance(error, commands.CommandInvokeError):
        error = error.original

    if isinstance(error, commands.CommandNotFound):
        return  # silently swallow unknown commands

    if isinstance(error, commands.CheckFailure):
        # global rate limit already replied — don't double-reply
        if 'global rate limit' in str(error):
            return
        try:
            await ctx.reply(embed=discord.Embed(description='🚫 you don\'t have permission to use that', color=0xED4245))
        except Exception:
            pass

    elif isinstance(error, commands.MissingRequiredArgument):
        try:
            await ctx.reply(embed=discord.Embed(
                description=f'⚠️ missing argument: `{error.param.name}` — check `$help` for usage',
                color=0xFEE75C
            ))
        except Exception:
            pass

    elif isinstance(error, commands.CommandOnCooldown):
        try:
            await ctx.reply(embed=discord.Embed(
                description=f'You are sending commands too fast. Please wait **{error.retry_after:.1f}s** before trying again.',
                title='⏳  Slow Down',
                color=0xFEE75C
            ), delete_after=int(error.retry_after) + 1)
        except Exception:
            pass

    elif isinstance(error, commands.BotMissingPermissions):
        perms = ', '.join(error.missing_permissions)
        try:
            await ctx.reply(embed=discord.Embed(
                description=f'⚠️ i\'m missing permissions to do that: `{perms}`',
                color=0xED4245
            ))
        except Exception:
            pass

    elif isinstance(error, discord.HTTPException):
        if error.status == 429:
            # Discord is rate limiting us — log it and wait
            retry = getattr(error, 'retry_after', 1.0)
            logger.warning(f'Discord rate limit hit on {ctx.command} — retry after {retry:.2f}s')
            await asyncio.sleep(retry)
            try:
                await ctx.reply(embed=discord.Embed(
                    description='⏳ got rate limited by Discord — try again in a sec',
                    color=0xFEE75C
                ))
            except Exception:
                pass
        elif error.status == 403:
            try:
                await ctx.reply(embed=discord.Embed(
                    description='🚫 i don\'t have permission to do that here',
                    color=0xED4245
                ))
            except Exception:
                pass
        else:
            logger.error(f'{ctx.command}: HTTP {error.status} — {error.text}')

    else:
        logger.error(f'{ctx.command}: {type(error).__name__}: {error}')


@bot.event
async def on_member_join(member: discord.Member):
    guild = member.guild

    unverified_role = guild.get_role(UNVERIFIED_ROLE)
    if unverified_role:
        try:
            await member.add_roles(unverified_role, reason='joined — awaiting verification')
        except Exception as ex:
            logger.error(f'on_member_join verify: {ex}')

    welcome_ch = guild.get_channel(WELCOME_CHANNEL)
    if welcome_ch:
        try:
            av_bytes = await member.display_avatar.replace(size=256, format='png').read()
            card     = make_welcome_card(av_bytes, member.display_name, guild.name, guild.member_count)
            await welcome_ch.send(
                f'{member.mention} welcome to **{guild.name}**!',
                file=discord.File(card, filename='welcome.png')
            )
        except Exception as ex:
            logger.error(f'welcome send: {ex}')

    try:
        e = discord.Embed(color=0x57F287)
        e.set_author(name=f'Welcome to {guild.name}!', icon_url=guild.icon.url if guild.icon else None)
        e.description = (
            f'hey, welcome to **{guild.name}**!\n\n'
            f'make sure to check the rules and have fun'
        )
        e.set_thumbnail(url=member.display_avatar.url)
        await member.send(embed=e)
    except Exception:
        pass

    invite_ch = guild.get_channel(INVITE_CHANNEL)
    if not invite_ch:
        return

    if member.bot:
        try:
            await invite_ch.send(f'{member.mention} has joined **{guild.name}**, joined via unknown method (bot).')
        except Exception as ex:
            logger.error(f'invite log bot: {ex}')
        return

    inviter   = None
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
        # check if used code belongs to a custom invite (overrides inv.inviter)
        if used_code:
            async with db.pool.acquire() as c:
                ci = await c.fetchrow(
                    'SELECT user_id FROM custom_invites WHERE guild_id=$1 AND code=$2',
                    guild.id, used_code
                )
            if ci:
                inviter = guild.get_member(ci['user_id']) or await guild.fetch_member(ci['user_id'])
    except Exception as ex:
        logger.error(f'invite fetch: {ex}')

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
            await invite_ch.send(f'{member.mention} has joined **{guild.name}** via vanity link.')
        except Exception as ex:
            logger.error(f'invite log vanity: {ex}')
        return

    if inviter:
        try:
            now_utc        = datetime.now(timezone.utc)
            acc_created    = member.created_at if member.created_at.tzinfo else member.created_at.replace(tzinfo=timezone.utc)
            acc_age        = now_utc - acc_created
            is_new_account = acc_age.days < 3

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
                left_at = left_row['left_at'] if left_row['left_at'].tzinfo else left_row['left_at'].replace(tzinfo=timezone.utc)
                if (now_utc - left_at).days <= 7:
                    is_rejoin = True

            async with db.pool.acquire() as c:
                if is_rejoin:
                    await c.execute(
                        '''INSERT INTO invite_stats (guild_id, inviter_id, rejoins) VALUES ($1,$2,1)
                           ON CONFLICT (guild_id, inviter_id) DO UPDATE SET rejoins = invite_stats.rejoins + 1''',
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
                       ON CONFLICT (guild_id, user_id) DO UPDATE
                       SET inviter_id=$3, joined_at=NOW(), is_rejoin=$4''',
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
            real    = joins - leaves - fake
            word    = 'invite' if real == 1 else 'invites'

            if is_rejoin:
                note = ' (rejoin)'
            elif is_fake:
                note = ' (fake — account under 3 days old)'
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
            await invite_ch.send(f'{member.mention} has joined **{guild.name}**, join method unknown.')
        except Exception as ex:
            logger.error(f'invite log unknown: {ex}')


@bot.event
async def on_member_remove(member: discord.Member):
    try:
        async with db.pool.acquire() as c:
            await c.execute(
                '''INSERT INTO member_left (guild_id, user_id, left_at) VALUES ($1,$2,NOW())
                   ON CONFLICT (guild_id, user_id) DO UPDATE SET left_at=NOW()''',
                member.guild.id, member.id
            )
            inv_row = await c.fetchrow(
                'SELECT inviter_id, is_rejoin FROM member_invites WHERE guild_id=$1 AND user_id=$2',
                member.guild.id, member.id
            )
            if inv_row and not inv_row.get('is_rejoin'):
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
    if user_id in captchas and message.guild and message.channel.id == VERIFY_CHANNEL:
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

                try:
                    async with db.pool.acquire() as c:
                        daily_stats[message.guild.id]['verifications'] += 1
                        await c.execute(
                            'INSERT INTO verifications (guild_id, user_id) VALUES ($1,$2) ON CONFLICT DO NOTHING',
                            message.guild.id, message.author.id
                        )
                        inv_row = await c.fetchrow(
                            'SELECT inviter_id FROM member_invites WHERE guild_id=$1 AND user_id=$2',
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
                        description=f'✅ {message.author.mention} is now verified — welcome to the server!',
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
                    description=f'❌ Incorrect code, {message.author.mention}. Here is your new code: `{new_code}`',
                    color=0xED4245
                )
            )
            await asyncio.sleep(6)
            try:
                await wrong.delete()
            except Exception:
                pass
        return

    # ── message activity tracking ────────────────────────────────────
    if message.guild and not message.author.bot and not message.channel.name.startswith('ticket-'):
        global STATS_DATE
        today = datetime.now(timezone.utc).date()
        if today != STATS_DATE:
            STATS_DATE = today
            daily_stats.clear()
            message_counts.clear()
        message_counts[message.guild.id][message.author.id] += 1

    # ── ticket lock enforcement ──────────────────────────────────────
    # If the ticket is in a claimed state (any role has send_messages=False),
    # delete messages from anyone without an explicit individual allow overwrite.
    if (
        message.guild
        and message.channel.name.startswith('ticket-')
    ):
        member_ow = message.channel.overwrites_for(message.author)
        if member_ow.send_messages is not True:
            channel_is_locked = any(
                ow.send_messages is False
                for target, ow in message.channel.overwrites.items()
                if isinstance(target, discord.Role)
            )
            if channel_is_locked:
                try:
                    await message.delete()
                except discord.Forbidden:
                    pass
                except Exception:
                    pass
                return

    await bot.process_commands(message)


# ================================================================== web server (UptimeRobot / Render health)

async def web_server():
    async def handle(request):
        return aiohttp.web.Response(
            text=f'ok  •  {bot.user}  •  {len(bot.guilds)} guild(s)',
            content_type='text/plain'
        )

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
