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

MAX_OPEN       = 1
tickets_locked = {}
captchas       = {}
invite_cache   = {}   # guild_id -> {code: uses}
snipe_cache    = {}   # channel_id -> {content, author, avatar, attachments}
esnipe_cache   = {}   # channel_id -> {before, after, author, avatar}

STATUS_ROTATION = [
    ('watching',  'tickets'),
    ('watching',  'trades go down'),
    ("watching",  "Trial's Cross Trade"),
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
                '''CREATE TABLE IF NOT EXISTS giveaways (
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
                )''',
                '''CREATE TABLE IF NOT EXISTS giveaway_entries (
                    id          SERIAL PRIMARY KEY,
                    giveaway_id INT REFERENCES giveaways(id) ON DELETE CASCADE,
                    user_id     BIGINT
                )''',
                '''CREATE TABLE IF NOT EXISTS giveaway_last_winners (
                    guild_id   BIGINT,
                    prize      TEXT,
                    winner_ids BIGINT[],
                    PRIMARY KEY (guild_id, prize)
                )''',
                '''CREATE TABLE IF NOT EXISTS giveaway_bonus_roles (
                    guild_id BIGINT,
                    role_id  BIGINT,
                    entries  INT DEFAULT 1,
                    PRIMARY KEY (guild_id, role_id)
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
    e.set_author(
        name=f'{user.display_name}  •  {TIER_LABEL.get(tier, tier)}',
        icon_url=user.display_avatar.url
    )
    e.description = desc or "hey! someone will be with you shortly — sit tight and don't ping anyone"
    if extra_fields:
        for name, value, inline in extra_fields:
            e.add_field(name=name, value=value, inline=inline)
    e.set_footer(text=f'ticket #{ticket_id}')
    return e


async def pre_open_checks(interaction, guild, user):
    if tickets_locked.get(guild.id):
        await interaction.followup.send(
            embed=discord.Embed(description='tickets are closed rn, check back later', color=0xED4245),
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
        e = discord.Embed(title='you\'re blacklisted', color=0xED4245)
        e.add_field(name='**Reason**', value=reason,                       inline=False)
        e.add_field(name='**By**',     value=by.mention if by else 'staff', inline=True)
        e.add_field(name='**Date**',   value=date,                         inline=True)
        e.set_footer(text='if you think this is wrong, dm a staff member')
        await interaction.followup.send(embed=e, ephemeral=True)
        return False
    if real_open >= MAX_OPEN:
        await interaction.followup.send(
            embed=discord.Embed(
                description='you already have a ticket open — close it first',
                color=0xFEE75C
            ),
            ephemeral=True
        )
        return False
    if not cfg or not cfg['ticket_category_id']:
        await interaction.followup.send(
            embed=discord.Embed(
                description="tickets aren't fully set up yet — ping a staff member",
                color=0xFEE75C
            ),
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
    e = discord.Embed(title='Ticket Closed', color=0xED4245)
    e.add_field(name='**Ticket**',    value=f"#{ticket['ticket_id']}",         inline=True)
    e.add_field(name='**Type**',      value=f"{ticket['ticket_type']} / {ticket.get('tier', '-')}", inline=True)
    e.add_field(name='**Messages**',  value=str(msg_count),                    inline=True)
    e.add_field(name='**Opened by**', value=opener.mention if opener else '?', inline=True)
    e.add_field(name='**Closed by**', value=closer.mention,                    inline=True)
    if claimer:
        e.add_field(name='**Claimed by**', value=claimer.mention, inline=True)
    await lc.send(embed=e, file=file)


# ================================================================== nuclear random

def _nuke_seed(ref_list: list) -> int:
    """Cryptographically seeded random seed for giveaway winner selection."""
    parts = [
        os.urandom(64),
        str(_time.time_ns()).encode(),
        str(_time.perf_counter_ns()).encode(),
        secrets.token_bytes(64),
        struct.pack('>Q', id(ref_list)),
        struct.pack('>Q', id(_time.time)),
        hashlib.sha256(''.join(str(u) for u in ref_list).encode()).digest(),
    ]
    h = b''.join(parts)
    for _ in range(3):
        h = hashlib.sha512(h + os.urandom(32)).digest()
    return int.from_bytes(h, 'big')


def _pick_winners(pool: list, unique_users: list, num_win: int) -> list:
    """7-pass Fisher-Yates shuffle with nuclear seed, returns list of winner IDs."""
    weighted = list(pool)
    for _ in range(7):
        rng = random.Random(_nuke_seed(weighted))
        for i in range(len(weighted) - 1, 0, -1):
            j = rng.randint(0, i)
            weighted[i], weighted[j] = weighted[j], weighted[i]

    pick_rng = random.Random(_nuke_seed(weighted))
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
    return winners


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
                embed=discord.Embed(description='slow down a bit', color=0xFEE75C),
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
                    embed=discord.Embed(description='something went wrong, try again', color=0xED4245),
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
                embed=discord.Embed(description=f'opened — {channel.mention}', color=0x57F287),
                ephemeral=True
            )
            await send_log(guild, 'Ticket Opened',
                f'{user.mention} opened a {TIER_LABEL[self.tier]} ticket',
                TIER_COLOR[self.tier])
        except Exception as ex:
            logger.error(f'middleman open: {ex}')
            try:
                await interaction.followup.send(
                    embed=discord.Embed(description=f'something broke — {ex}', color=0xED4245),
                    ephemeral=True
                )
            except Exception:
                pass


class TierSelect(Select):
    def __init__(self):
        super().__init__(
            placeholder='pick your trade value range',
            options=[
                discord.SelectOption(label='Low Tier  •  100–900 RBX',   value='lowtier',  emoji='💎', description='trades under 900 rbx'),
                discord.SelectOption(label='Mid Tier  •  1K–3K RBX',     value='midtier',  emoji='💰', description='1k to 3k rbx range'),
                discord.SelectOption(label='High Tier  •  3.1K–5K+ RBX', value='hightier', emoji='💸', description='3.1k+ rbx, senior staff only'),
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
                embed=discord.Embed(description='slow down a bit', color=0xFEE75C),
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
                    embed=discord.Embed(description='something went wrong, try again later', color=0xED4245),
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
                ('**Claiming**', self.what.value,  False),
                ('**Proof**',    self.proof.value, False),
            ]
            e    = make_ticket_embed(user, 'reward', tid, fields)
            ping = user.mention
            if staff_r:
                ping += f' {staff_r.mention}'
            await channel.send(content=ping, embed=e, view=ControlView())
            await interaction.followup.send(
                embed=discord.Embed(description=f'opened — {channel.mention}', color=0x57F287),
                ephemeral=True
            )
        except Exception as ex:
            logger.error(f'reward open: {ex}')
            try:
                await interaction.followup.send(
                    embed=discord.Embed(description=f'something went wrong — {ex}', color=0xED4245),
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
                discord.SelectOption(label='Giveaway Prize',  value='giveaway', emoji='🎉', description='u won a giveaway'),
                discord.SelectOption(label='Invite Reward',   value='invite',   emoji='📨', description='invite reward'),
                discord.SelectOption(label='Event Reward',    value='event',    emoji='🏆', description='event reward'),
                discord.SelectOption(label='Bonus Reward',    value='bonus',    emoji='💰', description='bonus or special reward'),
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
        if not limiter.interaction_check(interaction.user.id):
            return await interaction.response.send_message(
                embed=discord.Embed(description='you\'re clicking too fast — slow down', color=0xFEE75C),
                ephemeral=True
            )
        if not limiter.check(interaction.user.id, 'open', 10):
            rem = limiter.remaining(interaction.user.id, 'open', 10)
            return await interaction.response.send_message(
                embed=discord.Embed(description=f'you opened a ticket recently — wait **{int(rem)}s**', color=0xFEE75C),
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
                    embed=discord.Embed(description='something went wrong, try again', color=0xED4245),
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
            e    = make_ticket_embed(user, 'support', tid)
            ping = user.mention
            if staff_r:
                ping += f' {staff_r.mention}'
            await channel.send(content=ping, embed=e, view=ControlView())
            await interaction.followup.send(
                embed=discord.Embed(description=f'opened — {channel.mention}', color=0x57F287),
                ephemeral=True
            )
        except Exception as ex:
            logger.error(f'support open: {ex}')
            try:
                await interaction.followup.send(
                    embed=discord.Embed(description=f'something went wrong — {ex}', color=0xED4245),
                    ephemeral=True
                )
            except Exception:
                pass

    @discord.ui.button(label='Middleman', style=ButtonStyle.success, emoji='⚖️', custom_id='btn_middleman')
    async def middleman(self, interaction: discord.Interaction, _):
        if tickets_locked.get(interaction.guild.id):
            return await interaction.response.send_message(
                embed=discord.Embed(description="tickets are closed right now — check back later", color=0xED4245),
                ephemeral=True
            )
        v = View(timeout=300)
        v.add_item(TierSelect())
        await interaction.response.send_message(
            embed=discord.Embed(description='pick a tier based on your trade value', color=TIER_COLOR['support']),
            view=v, ephemeral=True
        )

    @discord.ui.button(label='Claim Reward', style=ButtonStyle.secondary, emoji='🎁', custom_id='btn_reward')
    async def reward(self, interaction: discord.Interaction, _):
        if tickets_locked.get(interaction.guild.id):
            return await interaction.response.send_message(
                embed=discord.Embed(description="tickets are closed right now, check back later", color=0xED4245),
                ephemeral=True
            )
        v = View(timeout=300)
        v.add_item(RewardSelect())
        await interaction.response.send_message(
            embed=discord.Embed(description="what type of reward are you claiming?", color=TIER_COLOR['reward']),
            view=v, ephemeral=True
        )


class ControlView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Claim', style=ButtonStyle.green, emoji='✋', custom_id='btn_claim')
    async def claim(self, interaction: discord.Interaction, _):
        if not limiter.interaction_check(interaction.user.id):
            return await interaction.response.send_message(
                embed=discord.Embed(description='you\'re clicking too fast', color=0xFEE75C), ephemeral=True
            )
        if not limiter.check(interaction.user.id, 'claim', 2):
            rem = limiter.remaining(interaction.user.id, 'claim', 2)
            return await interaction.response.send_message(
                embed=discord.Embed(description=f'slow down — wait **{rem:.1f}s**', color=0xFEE75C), ephemeral=True
            )
        if not _is_staff(interaction.user):
            return await interaction.response.send_message(
                embed=discord.Embed(description='you need a staff role to claim tickets', color=0xED4245),
                ephemeral=True
            )
        async with db.pool.acquire() as c:
            ticket = await c.fetchrow(
                'SELECT * FROM tickets WHERE channel_id = $1', interaction.channel.id
            )
            if not ticket:
                return await interaction.response.send_message(
                    embed=discord.Embed(description='no ticket here', color=0xED4245), ephemeral=True
                )
            if ticket['claimed_by']:
                who = interaction.guild.get_member(ticket['claimed_by'])
                return await interaction.response.send_message(
                    embed=discord.Embed(
                        description=f"this ticket's already with {who.mention if who else 'someone'}",
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
        e.description = (
            f'{interaction.user.mention} has this one\n\n'
            f'`$add @user` to bring someone in\n'
            f'`$transfer @user` to hand it off\n'
            f'`$close` when you\'re done'
        )
        e.set_footer(text=f'claimed by {interaction.user.display_name}')
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
                embed=discord.Embed(description='you\'re clicking too fast', color=0xFEE75C), ephemeral=True
            )
        if not limiter.check(interaction.user.id, 'unclaim', 2):
            rem = limiter.remaining(interaction.user.id, 'unclaim', 2)
            return await interaction.response.send_message(
                embed=discord.Embed(description=f'slow down — wait **{rem:.1f}s**', color=0xFEE75C), ephemeral=True
            )
        async with db.pool.acquire() as c:
            ticket = await c.fetchrow(
                'SELECT * FROM tickets WHERE channel_id = $1', interaction.channel.id
            )
            if not ticket:
                return await interaction.response.send_message(
                    embed=discord.Embed(description='no ticket here', color=0xED4245), ephemeral=True
                )
            if not ticket['claimed_by']:
                return await interaction.response.send_message(
                    embed=discord.Embed(description="nobody's claimed this yet", color=0xED4245), ephemeral=True
                )
            is_mm      = ticket['ticket_type'] == 'middleman'
            is_claimer = ticket['claimed_by'] == interaction.user.id
            is_admin   = interaction.user.guild_permissions.administrator
            if is_mm and not is_claimer:
                return await interaction.response.send_message(
                    embed=discord.Embed(description='only the mm who claimed this can unclaim it', color=0xED4245),
                    ephemeral=True
                )
            if not is_mm and not is_claimer and not is_admin:
                return await interaction.response.send_message(
                    embed=discord.Embed(description="you didn't claim this", color=0xED4245),
                    ephemeral=True
                )
            await c.execute(
                "UPDATE tickets SET claimed_by=NULL, status='open' WHERE ticket_id=$1",
                ticket['ticket_id']
            )
        old = interaction.guild.get_member(ticket['claimed_by'])
        await claim_unlock(interaction.channel, old, ticket['ticket_type'])
        e = discord.Embed(color=0x5865F2)
        e.description = f'{interaction.user.mention} dropped this — up for grabs again'
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
                embed=discord.Embed(description="that's not your button", color=0xED4245),
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
        e.description = f'closing #{self.ticket["ticket_id"]} — saving transcript...'
        await interaction.message.edit(embed=e, view=None)
        await save_transcript(interaction.channel, self.ticket, interaction.user)
        await asyncio.sleep(0.5)
        await interaction.channel.delete()

    @discord.ui.button(label='Cancel', style=ButtonStyle.gray, emoji='✖️')
    async def cancel(self, interaction: discord.Interaction, _):
        if interaction.user.id != self.ctx.author.id:
            return await interaction.response.send_message(
                embed=discord.Embed(description="that's not yours to click", color=0xED4245),
                ephemeral=True
            )
        await interaction.response.defer()
        e = discord.Embed(color=0x5865F2)
        e.description = 'nevermind, ticket stays open'
        await interaction.message.edit(embed=e, view=None)

    async def on_timeout(self):
        try:
            e = discord.Embed(color=0x5865F2)
            e.description = 'close request expired — run `$close` again if you still need it'
            await self.msg.edit(embed=e, view=None)
        except Exception:
            pass


# ================================================================== ticket commands

@bot.command(name='close')
@staff_only()
async def close_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description='not a ticket channel', color=0xED4245))
    if not limiter.check(ctx.author.id, 'close', 3):
        rem = limiter.remaining(ctx.author.id, 'close', 3)
        return await ctx.reply(embed=discord.Embed(description=f'wait **{rem:.1f}s** before closing again', color=0xFEE75C))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
    if not ticket:
        return await ctx.reply(embed=discord.Embed(description="can't find this ticket", color=0xED4245))
    is_mm         = ticket['ticket_type'] == 'middleman'
    is_mm_claimed = is_mm and ticket.get('claimed_by')
    is_claimer    = ticket.get('claimed_by') == ctx.author.id
    is_admin      = ctx.author.guild_permissions.administrator
    is_staff      = _is_staff(ctx.author)
    if is_mm_claimed and not is_claimer:
        return await ctx.reply(embed=discord.Embed(description='only the mm who claimed this can close it', color=0xED4245))
    if not is_mm and ticket.get('claimed_by') and not is_claimer and not is_admin:
        return await ctx.reply(embed=discord.Embed(description='only the claimer or an admin can close this', color=0xED4245))
    if not is_mm and not ticket.get('claimed_by') and not is_staff:
        return await ctx.reply(embed=discord.Embed(description='you need a staff role for this', color=0xED4245))
    if is_mm and not ticket.get('claimed_by') and not is_staff:
        return await ctx.reply(embed=discord.Embed(description='you need a staff role to close tickets', color=0xED4245))
    e = discord.Embed(color=0xFEE75C)
    e.description = f'close ticket **#{ticket["ticket_id"]}**? this will delete the channel and save a transcript'
    view = CloseConfirm(ctx, ticket)
    view.msg = await ctx.send(embed=e, view=view)


@bot.command(name='claim')
@staff_only()
async def claim_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description="this isn't a ticket channel", color=0xED4245))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
        if not ticket:
            return await ctx.reply(embed=discord.Embed(description='no ticket found here', color=0xED4245))
        if ticket['claimed_by']:
            who = ctx.guild.get_member(ticket['claimed_by'])
            return await ctx.reply(embed=discord.Embed(
                description=f'already claimed by {who.mention if who else "someone"}',
                color=0xED4245
            ))
        if not await _can_manage(ctx, ticket):
            return await ctx.reply(embed=discord.Embed(description="you don't have the right role for this type", color=0xED4245))
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
    e.description = (
        f'{ctx.author.mention} has this one\n\n'
        f'`$add @user` to bring someone in\n'
        f'`$transfer @user` to hand it off\n'
        f'`$close` when you\'re done'
    )
    e.set_footer(text=f'claimed by {ctx.author.display_name}')
    await ctx.send(embed=e)


@bot.command(name='unclaim')
@staff_only()
async def unclaim_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description="this isn't a ticket channel", color=0xED4245))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
        if not ticket:
            return await ctx.reply(embed=discord.Embed(description='no ticket found here', color=0xED4245))
        if not ticket['claimed_by']:
            return await ctx.reply(embed=discord.Embed(description='nobody claimed this', color=0xED4245))
        is_mm      = ticket['ticket_type'] == 'middleman'
        is_claimer = ticket['claimed_by'] == ctx.author.id
        is_admin   = ctx.author.guild_permissions.administrator
        if is_mm and not is_claimer:
            return await ctx.reply(embed=discord.Embed(description='only the middleman who claimed this can unclaim it', color=0xED4245))
        if not is_mm and not is_claimer and not is_admin:
            return await ctx.reply(embed=discord.Embed(description="you didn't claim this", color=0xED4245))
        await c.execute(
            "UPDATE tickets SET claimed_by=NULL, status='open' WHERE ticket_id=$1",
            ticket['ticket_id']
        )
    old = ctx.guild.get_member(ticket['claimed_by'])
    await claim_unlock(ctx.channel, old, ticket['ticket_type'])
    e = discord.Embed(color=0x5865F2)
    e.description = f'{ctx.author.mention} dropped this — anyone eligible can grab it'
    await ctx.send(embed=e)


@bot.command(name='add')
@staff_only()
async def add_cmd(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply(embed=discord.Embed(description='usage: `$add @user`', color=0x5865F2))
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description="this isn't a ticket channel", color=0xED4245))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
    if not ticket:
        return await ctx.reply(embed=discord.Embed(description='no ticket found here', color=0xED4245))
    if not ticket.get('claimed_by'):
        return await ctx.reply(embed=discord.Embed(description='someone needs to claim this ticket first', color=0xED4245))
    if ticket['claimed_by'] != ctx.author.id and not ctx.author.guild_permissions.administrator:
        return await ctx.reply(embed=discord.Embed(description="only the claimer can add people", color=0xED4245))
    await ctx.channel.set_permissions(member, read_messages=True, send_messages=True)
    e = discord.Embed(color=0x57F287)
    e.description = f'{member.mention} can now see and talk in this ticket'
    e.set_footer(text=f'added by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='remove')
@staff_only()
async def remove_cmd(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply(embed=discord.Embed(description='usage: `$remove @user`', color=0x5865F2))
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description="this isn't a ticket channel", color=0xED4245))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
    if not ticket or not await _can_manage(ctx, ticket):
        return await ctx.reply(embed=discord.Embed(description="you can't manage this ticket", color=0xED4245))
    await ctx.channel.set_permissions(member, overwrite=None)
    e = discord.Embed(color=0x57F287)
    e.description = f'{member.mention} was removed from this ticket'
    e.set_footer(text=f'removed by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='rename')
@staff_only()
async def rename_cmd(ctx, *, new_name: str = None):
    if not new_name:
        return await ctx.reply(embed=discord.Embed(description='usage: `$rename new name`', color=0x5865F2))
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description="this isn't a ticket channel", color=0xED4245))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
    if not ticket or not await _can_manage(ctx, ticket):
        return await ctx.reply(embed=discord.Embed(description="you can't rename this", color=0xED4245))
    safe     = re.sub(r'[^a-z0-9\-]', '-', new_name.lower())
    old_name = ctx.channel.name
    await ctx.channel.edit(name=f'ticket-{safe}')
    e = discord.Embed(color=0x57F287)
    e.description = f'`{old_name}` → `ticket-{safe}`'
    e.set_footer(text=f'Renamed by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='transfer')
@staff_only()
async def transfer_cmd(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply(embed=discord.Embed(description='usage: `$transfer @user`', color=0x5865F2))
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description="this isn't a ticket channel", color=0xED4245))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
        if not ticket:
            return await ctx.reply(embed=discord.Embed(description='no ticket found here', color=0xED4245))
        if not ticket['claimed_by']:
            return await ctx.reply(embed=discord.Embed(description="nobody's claimed this yet", color=0xED4245))
        is_mm      = ticket['ticket_type'] == 'middleman'
        is_claimer = ticket['claimed_by'] == ctx.author.id
        is_admin   = ctx.author.guild_permissions.administrator
        if is_mm and not is_claimer:
            return await ctx.reply(embed=discord.Embed(description='only the mm who claimed this can transfer it', color=0xED4245))
        if not is_mm and not is_claimer and not is_admin:
            return await ctx.reply(embed=discord.Embed(description="you didn't claim this", color=0xED4245))
        old = ctx.guild.get_member(ticket['claimed_by'])
        if old:
            await ctx.channel.set_permissions(old, read_messages=True, send_messages=False)
        await ctx.channel.set_permissions(member, read_messages=True, send_messages=True)
        await c.execute("UPDATE tickets SET claimed_by=$1, status='claimed' WHERE ticket_id=$2", member.id, ticket['ticket_id'])
    e = discord.Embed(color=0x57F287)
    e.description = f'{ctx.author.mention} passed this to {member.mention}'
    e.set_footer(text=f'transferred by {ctx.author.display_name}')
    await ctx.send(embed=e)


@bot.command(name='proof')
@staff_only()
async def proof_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply(embed=discord.Embed(description="this isn't a ticket channel", color=0xED4245))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
    if not ticket:
        return await ctx.reply(embed=discord.Embed(description='no ticket found here', color=0xED4245))
    proof_ch = ctx.guild.get_channel(PROOF_CHANNEL)
    if not proof_ch:
        return await ctx.reply(embed=discord.Embed(description='proof channel not found, ping staff', color=0xED4245))
    opener = ctx.guild.get_member(ticket['user_id'])
    e = discord.Embed(title='trade completed ✓', color=0x57F287)
    e.add_field(name='**Middleman**', value=ctx.author.mention,                                inline=True)
    e.add_field(name='**Tier**',      value=TIER_LABEL.get(ticket.get('tier'), 'unknown'),     inline=True)
    e.add_field(name='**Client**',    value=opener.mention if opener else 'unknown',           inline=True)
    if ticket.get('trade_details'):
        try:
            d = ticket['trade_details'] if isinstance(ticket['trade_details'], dict) else json.loads(ticket['trade_details'])
            e.add_field(name='**Trading with**', value=d.get('trader', '?'),    inline=False)
            e.add_field(name='**Gave**',         value=d.get('giving', '?'),    inline=True)
            e.add_field(name='**Received**',     value=d.get('receiving', '?'), inline=True)
            if d.get('tip'):
                e.add_field(name='**Tip**', value=d['tip'], inline=True)
        except Exception:
            pass
    e.set_footer(text=f'ticket #{ticket["ticket_id"]}')
    await proof_ch.send(embed=e)
    e2 = discord.Embed(color=0x57F287)
    e2.description = f'proof posted in {proof_ch.mention}'
    e2.set_footer(text=f'posted by {ctx.author.display_name}')
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
        e = discord.Embed(title='channelperm usage', color=0x5865F2)
        e.description = (
            '`$channelperm #channel @target permission enable/disable`\n\n'
            '**example:** `$channelperm #general @members send disable`'
        )
        return await ctx.reply(embed=e)
    resolved_target = await resolve_target(ctx, target)
    if not resolved_target:
        return await ctx.reply(embed=discord.Embed(description=f"can't find `{target}`", color=0xED4245))
    resolved_perm   = resolve_perm(perm)
    resolved_toggle = resolve_toggle(toggle)
    if resolved_toggle is None:
        return await ctx.reply(embed=discord.Embed(description='toggle needs to be `enable` or `disable`', color=0xED4245))
    ow = channel.overwrites_for(resolved_target)
    try:
        setattr(ow, resolved_perm, resolved_toggle)
    except AttributeError:
        return await ctx.reply(embed=discord.Embed(description=f'`{perm}` isn\'t a valid perm', color=0xED4245))
    await channel.set_permissions(resolved_target, overwrite=ow)
    action = 'enabled' if resolved_toggle else 'disabled'
    name   = resolved_target.name if hasattr(resolved_target, 'name') else str(resolved_target)
    e = discord.Embed(
        description=f'`{resolved_perm}` {action} for **{name}** in {channel.mention}',
        color=0x57F287 if resolved_toggle else 0xED4245
    )
    e.set_footer(text=f'Set by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='channelpermall')
@staff_only()
async def channelpermall_cmd(ctx, target: str = None, perm: str = None, toggle: str = None):
    if not target or not perm or not toggle:
        e = discord.Embed(title='channelpermall usage', color=0x5865F2)
        e.description = (
            '`$channelpermall @target permission enable/disable`\n\n'
            '**example:** `$channelpermall @members send disable`'
        )
        return await ctx.reply(embed=e)
    resolved_target = await resolve_target(ctx, target)
    if not resolved_target:
        return await ctx.reply(embed=discord.Embed(description=f"couldn't find `{target}`", color=0xED4245))
    resolved_perm   = resolve_perm(perm)
    resolved_toggle = resolve_toggle(toggle)
    if resolved_toggle is None:
        return await ctx.reply(embed=discord.Embed(description='toggle must be `enable` or `disable`', color=0xED4245))

    channels = [c for c in ctx.guild.channels if isinstance(c, (discord.TextChannel, discord.VoiceChannel))]
    action   = 'enabled' if resolved_toggle else 'disabled'
    name     = resolved_target.name if hasattr(resolved_target, 'name') else str(resolved_target)

    msg = await ctx.reply(embed=discord.Embed(
        description=f'updating `{resolved_perm}` for **{name}** across {len(channels)} channels...',
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

    result = f'`{resolved_perm}` {action} for **{name}** in all channels'
    if failed:
        result += f'\n{failed} channel(s) failed — check bot permissions'
    await msg.edit(embed=discord.Embed(
        description=result,
        color=0x57F287 if resolved_toggle else 0xED4245
    ))


# ================================================================== setup commands

@bot.command(name='setup')
@owner_only()
async def setup_cmd(ctx):
    e = discord.Embed(color=TIER_COLOR['support'])
    e.set_author(name="Trial's Cross Trade  •  tickets")
    e.description = (
        '## Need help? open a ticket below\n\n'
        '**Support**\n'
        'general questions, reports, partnerships, anything really\n\n'
        '**Middleman**\n'
        'need a trusted mm for your trade? we got you\n'
        'pick your tier and a staff member will handle it\n\n'
        '**Claim Reward**\n'
        'won a giveaway or hit an invite milestone?\n'
        'have your proof ready and we\'ll sort it out'
    )
    e.set_footer(text="Trial's Cross Trade  •  tap a button to get started")
    await ctx.send(embed=e, view=TicketPanel())
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
            'INSERT INTO config (guild_id, ticket_category_id) VALUES ($1,$2) ON CONFLICT (guild_id) DO UPDATE SET ticket_category_id=$2',
            ctx.guild.id, category.id
        )
    e = discord.Embed(color=0x57F287)
    e.description = f'ticket category set to **{category.name}**'
    e.set_footer(text=f'set by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='setlogs')
@owner_only()
async def setlogs_cmd(ctx, channel: discord.TextChannel = None):
    if not channel:
        return await ctx.reply(embed=discord.Embed(description='usage: `$setlogs #channel`', color=0x5865F2))
    async with db.pool.acquire() as c:
        await c.execute(
            'INSERT INTO config (guild_id, log_channel_id) VALUES ($1,$2) ON CONFLICT (guild_id) DO UPDATE SET log_channel_id=$2',
            ctx.guild.id, channel.id
        )
    e = discord.Embed(color=0x57F287)
    e.description = f'logs going to {channel.mention} from now on'
    e.set_footer(text=f'set by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='config')
@owner_only()
async def config_cmd(ctx):
    async with db.pool.acquire() as c:
        cfg = await c.fetchrow('SELECT * FROM config WHERE guild_id=$1', ctx.guild.id)
    e = discord.Embed(title=f'config  •  {ctx.guild.name}', color=TIER_COLOR['support'])

    cat  = ctx.guild.get_channel(cfg['ticket_category_id']) if cfg and cfg.get('ticket_category_id') else None
    logs = ctx.guild.get_channel(cfg['log_channel_id'])     if cfg and cfg.get('log_channel_id')     else None
    e.add_field(name='**Category**',  value=cat.mention  if cat  else 'not set', inline=True)
    e.add_field(name='**Log Channel**', value=logs.mention if logs else 'not set', inline=True)
    e.add_field(name='**Total Tickets**', value=str(cfg['ticket_counter'] if cfg else 0), inline=True)
    e.add_field(name='**Status**',      value='🔒 locked' if tickets_locked.get(ctx.guild.id) else '🟢 open', inline=True)

    welcome_ch = ctx.guild.get_channel(WELCOME_CHANNEL)
    invite_ch  = ctx.guild.get_channel(INVITE_CHANNEL)
    verify_ch  = ctx.guild.get_channel(VERIFY_CHANNEL)
    proof_ch   = ctx.guild.get_channel(PROOF_CHANNEL)
    e.add_field(name='**Welcome Channel**', value=welcome_ch.mention if welcome_ch else 'not found', inline=True)
    e.add_field(name='**Invite Log**',      value=invite_ch.mention  if invite_ch  else 'not found', inline=True)
    e.add_field(name='**Verify Channel**',  value=verify_ch.mention  if verify_ch  else 'not found', inline=True)
    e.add_field(name='**Proof Channel**',   value=proof_ch.mention   if proof_ch   else 'not found', inline=True)

    unverified_r = ctx.guild.get_role(UNVERIFIED_ROLE)
    verified_r   = ctx.guild.get_role(VERIFIED_ROLE)
    member_r     = ctx.guild.get_role(MEMBER_ROLE)
    e.add_field(name='**Unverified Role**', value=unverified_r.mention if unverified_r else 'not found', inline=True)
    e.add_field(name='**Verified Role**',   value=verified_r.mention   if verified_r   else 'not found', inline=True)
    e.add_field(name='**Member Role**',     value=member_r.mention     if member_r     else 'not found', inline=True)

    e.add_field(name='**Uptime**',  value=fmt_uptime(datetime.now(timezone.utc) - BOT_START), inline=True)
    e.add_field(name='**Latency**', value=f'{round(bot.latency * 1000)}ms', inline=True)
    footer = 'finish setup with $setcategory and $setlogs' if not cfg else f'requested by {ctx.author.display_name}'
    e.set_footer(text=footer)
    await ctx.reply(embed=e)


@bot.command(name='lock')
@owner_only()
async def lock_cmd(ctx):
    tickets_locked[ctx.guild.id] = True
    e = discord.Embed(color=0xED4245)
    e.description = f'tickets are now **closed** — nobody can open new ones\nuse `$unlock` to reopen'
    e.set_footer(text=f'locked by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='unlock')
@owner_only()
async def unlock_cmd(ctx):
    tickets_locked[ctx.guild.id] = False
    e = discord.Embed(color=0x57F287)
    e.description = f"tickets are back open — people can create them again"
    e.set_footer(text=f'unlocked by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='blacklist')
@owner_only()
async def blacklist_cmd(ctx, member: discord.Member = None, *, reason: str = 'no reason given'):
    if not member:
        return await ctx.reply(embed=discord.Embed(description='usage: `$blacklist @user reason`', color=0x5865F2))
    async with db.pool.acquire() as c:
        await c.execute(
            'INSERT INTO blacklist (user_id, guild_id, reason, blacklisted_by) VALUES ($1,$2,$3,$4) '
            'ON CONFLICT (user_id, guild_id) DO UPDATE SET reason=$3, blacklisted_by=$4, created_at=NOW()',
            member.id, ctx.guild.id, reason, ctx.author.id
        )
    e = discord.Embed(color=0xED4245)
    e.set_author(name=f'{member.display_name} blacklisted', icon_url=member.display_avatar.url)
    e.add_field(name='**User**',   value=member.mention,     inline=True)
    e.add_field(name='**By**',     value=ctx.author.mention, inline=True)
    e.add_field(name='**Reason**', value=reason,             inline=False)
    e.set_thumbnail(url=member.display_avatar.url)
    e.set_footer(text=f'blacklisted by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='unblacklist')
@owner_only()
async def unblacklist_cmd(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply(embed=discord.Embed(description='usage: `$unblacklist @user`', color=0x5865F2))
    async with db.pool.acquire() as c:
        result = await c.execute(
            'DELETE FROM blacklist WHERE user_id=$1 AND guild_id=$2', member.id, ctx.guild.id
        )
    if result == 'DELETE 0':
        return await ctx.reply(embed=discord.Embed(description=f"{member.mention} isn't blacklisted", color=0xFEE75C))
    e = discord.Embed(color=0x57F287)
    e.description = f'{member.mention} is off the blacklist'
    e.set_footer(text=f'removed by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='blacklists')
@owner_only()
async def blacklists_cmd(ctx):
    async with db.pool.acquire() as c:
        rows = await c.fetch('SELECT * FROM blacklist WHERE guild_id=$1 ORDER BY created_at DESC', ctx.guild.id)
    if not rows:
        return await ctx.reply(embed=discord.Embed(description='nobody is blacklisted right now', color=0x57F287))
    lines = []
    for r in rows:
        m    = ctx.guild.get_member(r['user_id'])
        by   = ctx.guild.get_member(r['blacklisted_by'])
        date = r['created_at'].strftime('%b %d') if r.get('created_at') else '?'
        name = m.display_name if m else str(r['user_id'])
        lines.append(f"**{name}** — {r['reason']}  (by {by.display_name if by else '?'} on {date})")
    e = discord.Embed(title=f'blacklisted  •  {ctx.guild.name}  ({len(rows)})', color=0xED4245)
    e.description = '\n'.join(lines)
    e.set_footer(text=f'requested by {ctx.author.display_name}')
    await ctx.reply(embed=e)


# ================================================================== invite commands

@bot.command(name='invites')
async def invites_cmd(ctx, member: discord.Member = None):
    member = member or ctx.author
    async with db.pool.acquire() as c:
        row = await c.fetchrow(
            'SELECT * FROM invite_stats WHERE guild_id=$1 AND inviter_id=$2',
            ctx.guild.id, member.id
        )
    joins    = row['joins']    if row else 0
    leaves   = row['leaves']   if row else 0
    fake     = row['fake']     if row else 0
    rejoins  = row['rejoins']  if row else 0
    verified = row['verified'] if row else 0
    real     = joins - leaves - fake - rejoins
    word     = 'invite' if real == 1 else 'invites'

    e = discord.Embed(color=0x5865F2)
    e.set_author(name=f'invites  •  {member.display_name}', icon_url=member.display_avatar.url)
    e.description = (
        f'**{member.display_name}**  •  {real} real {word}\n\n'
        f'**Joins**    ╸  {joins}\n'
        f'**Left**     ╸  {leaves}\n'
        f'**Fake**     ╸  {fake}\n'
        f'**Rejoins**  ╸  {rejoins}  *(within 7 days)*\n'
        f'**Verified** ╸  {verified}'
    )
    e.set_thumbnail(url=member.display_avatar.url)
    e.set_footer(text=f'requested by {ctx.author.display_name}')
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
        e = discord.Embed(title=f'Invite Leaderboard  •  {guild.name}', color=0x5865F2)
        e.description = 'no invite data yet'
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
            f'**Left:** {row["leaves"]}  **Fake:** {row["fake"]}  '
            f'**Rejoins:** {row["rejoins"]}  **Verified:** {row["verified"]}'
        )

    e  = discord.Embed(title=f'Invite Leaderboard  •  {guild.name}', color=0x5865F2)
    e.description = '\n'.join(lines)
    ts = last_updated or datetime.now(timezone.utc).strftime('%H:%M:%S UTC')
    e.set_footer(text=f'updates every 30s  •  last refreshed {ts}')
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
                embed=discord.Embed(description="that's not your leaderboard", color=0xED4245),
                ephemeral=True
            )
        self.stopped = True
        if self.task:
            self.task.cancel()
        self.stop()
        e = await build_lb_embed(interaction.guild)
        e.set_footer(text='stopped  •  run $lb again to restart')
        await interaction.response.edit_message(embed=e, view=None)

    async def on_timeout(self):
        self.stopped = True
        if self.task:
            self.task.cancel()
        try:
            e = await build_lb_embed(self.message.guild)
            e.set_footer(text='timed out  •  run $lb to restart')
            await self.message.edit(embed=e, view=None)
        except Exception:
            pass


async def live_lb_loop(view: LiveLBView, guild):
    try:
        while not view.stopped:
            await asyncio.sleep(30)
            if view.stopped:
                break
            ts = datetime.now(timezone.utc).strftime('%H:%M:%S UTC')
            e  = await build_lb_embed(guild, ts)
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
            'SELECT inviter_id FROM member_invites WHERE guild_id=$1 AND user_id=$2',
            ctx.guild.id, member.id
        )
    if not row or not row['inviter_id']:
        return await ctx.reply(embed=discord.Embed(description=f'no invite data found for {member.mention}', color=0x5865F2))
    inviter = ctx.guild.get_member(row['inviter_id'])
    e = discord.Embed(color=0x5865F2)
    e.description = f'{member.mention} was invited by {inviter.mention if inviter else str(row["inviter_id"])}'
    e.set_footer(text=f'requested by {ctx.author.display_name}')
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
        return await ctx.reply(embed=discord.Embed(description='usage: `$invited @user`', color=0x5865F2))
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
        e.description = f"## {member.display_name} hasn't invited anyone yet"
        e.set_thumbnail(url=member.display_avatar.url)
        e.set_footer(text=f'requested by {ctx.author.display_name}')
        return await ctx.reply(embed=e)

    verified_role = ctx.guild.get_role(VERIFIED_ROLE)
    lines  = []
    counts = {'verified': 0, 'unverified': 0, 'left': 0, 'rejoin': 0}

    for r in rows:
        invited_member = ctx.guild.get_member(r['user_id'])
        date = r['joined_at'].strftime('%b %d, %Y') if r.get('joined_at') else '?'

        if r.get('is_rejoin'):
            counts['rejoin'] += 1
            tag = 'rejoin'
        elif invited_member is None:
            counts['left'] += 1
            tag = 'left'
        elif verified_role and verified_role in invited_member.roles:
            counts['verified'] += 1
            tag = 'verified'
        else:
            counts['unverified'] += 1
            tag = 'unverified'

        name = invited_member.mention if invited_member else f'`{r["user_id"]}`'
        lines.append(f'{tag}  {name}  •  {date}')

    total = len(rows)
    e = discord.Embed(color=0x5865F2)
    e.set_author(name=f'Invited by {member.display_name}', icon_url=member.display_avatar.url)
    e.description = (
        f'## {total} total invite{"s" if total != 1 else ""}\n\n'
        f'**Verified** :  {counts["verified"]}\n'
        f'**Unverified** :  {counts["unverified"]}\n'
        f'**Left** :  {counts["left"]}\n'
        f'**Rejoins** :  {counts["rejoin"]}\n\n'
        + '\n'.join(lines[:20])
        + (f'\n... and {total - 20} more' if total > 20 else '')
    )
    e.set_thumbnail(url=member.display_avatar.url)
    e.set_footer(text=f'requested by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='clearinvites')
@owner_only()
async def clearinvites_cmd(ctx, target: str = None):
    if not target:
        e = discord.Embed(title='clearinvites usage', color=0x5865F2)
        e.description = (
            '`$clearinvites all`      reset everyone\'s invites\n'
            '`$clearinvites @user`    reset one person\'s invites'
        )
        return await ctx.reply(embed=e)

    if target.lower() == 'all':
        async with db.pool.acquire() as c:
            await c.execute('DELETE FROM invite_stats   WHERE guild_id=$1', ctx.guild.id)
            await c.execute('DELETE FROM member_invites WHERE guild_id=$1', ctx.guild.id)
            await c.execute('DELETE FROM member_left    WHERE guild_id=$1', ctx.guild.id)
        e = discord.Embed(color=0x57F287)
        e.description = f'all invite stats cleared for **{ctx.guild.name}**'
        e.set_footer(text=f'Cleared by {ctx.author.display_name}')
        return await ctx.reply(embed=e)

    try:
        member = await commands.MemberConverter().convert(ctx, target)
    except Exception:
        return await ctx.reply(embed=discord.Embed(
            description=f"can't find `{target}` — use `$clearinvites all` or mention someone",
            color=0xED4245
        ))
    async with db.pool.acquire() as c:
        await c.execute(
            'DELETE FROM invite_stats WHERE guild_id=$1 AND inviter_id=$2',
            ctx.guild.id, member.id
        )
    e = discord.Embed(color=0x57F287)
    e.description = f'invite stats cleared for {member.mention}'
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
        return await ctx.reply(embed=discord.Embed(description='nothing to snipe in this channel', color=0x5865F2))
    e = discord.Embed(color=0x5865F2)
    e.set_author(name=data['author'].display_name, icon_url=data['avatar'])
    e.description = data['content']
    if data.get('attachments'):
        e.add_field(name='**Attachments**', value='\n'.join(data['attachments']), inline=False)
    e.set_footer(text=f'sniped by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='esnipe', aliases=['es'])
async def esnipe_cmd(ctx):
    data = esnipe_cache.get(ctx.channel.id)
    if not data:
        return await ctx.reply(embed=discord.Embed(description='no edited messages cached in this channel', color=0x5865F2))
    e = discord.Embed(color=0x5865F2)
    e.set_author(name=data['author'].display_name, icon_url=data['avatar'])
    e.add_field(name='**Before**', value=data['before'][:1024], inline=False)
    e.add_field(name='**After**',  value=data['after'][:1024],  inline=False)
    e.set_footer(text=f'sniped by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='ping')
async def ping_cmd(ctx):
    latency = round(bot.latency * 1000)
    bar = '█' * min(10, latency // 10) + '░' * max(0, 10 - latency // 10)
    e = discord.Embed(color=0x57F287 if latency < 150 else 0xFEE75C if latency < 300 else 0xED4245)
    e.description = f'`{bar}` **{latency}ms**'
    e.set_footer(text=f'up for {fmt_uptime(datetime.now(timezone.utc) - BOT_START)}')
    await ctx.reply(embed=e)


@bot.command(name='uptime')
async def uptime_cmd(ctx):
    delta = datetime.now(timezone.utc) - BOT_START
    e = discord.Embed(description=f'running for **{fmt_uptime(delta)}** without a restart', color=0x57F287)
    e.set_footer(text="Trial's Cross Trade")
    await ctx.reply(embed=e)



@bot.command(name='slowmode', aliases=['slow'])
@staff_only()
async def slowmode_cmd(ctx, seconds: int = None):
    if seconds is None:
        return await ctx.reply(embed=discord.Embed(description='usage: `$slowmode <seconds>` — use 0 to disable', color=0x5865F2))
    if not 0 <= seconds <= 21600:
        return await ctx.reply(embed=discord.Embed(description='slowmode must be between 0 and 21600 seconds', color=0xED4245))
    await ctx.channel.edit(slowmode_delay=seconds)
    if seconds == 0:
        await ctx.reply(embed=discord.Embed(description=f'slowmode off in {ctx.channel.mention}', color=0x57F287))
    else:
        await ctx.reply(embed=discord.Embed(description=f'slowmode set to **{seconds}s** in {ctx.channel.mention}', color=0x57F287))


@bot.command(name='say')
@owner_only()
async def say_cmd(ctx, channel: discord.TextChannel = None, *, message: str = None):
    if not channel or not message:
        return await ctx.reply(embed=discord.Embed(description='usage: `$say #channel your message`', color=0x5865F2))
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
    e.add_field(name='**ID**',          value=str(member.id),          inline=True)
    e.add_field(name='**Nickname**',    value=member.nick or 'none',   inline=True)
    e.add_field(name='**Bot**',         value='yes' if member.bot else 'no', inline=True)
    e.add_field(name='**Account Age**', value=f'{acc_age}d',           inline=True)
    e.add_field(name='**In Server**',   value=f'{srv_age}d',           inline=True)
    e.add_field(name='**Status**',      value=str(member.status).title(), inline=True)
    e.add_field(
        name=f'**Roles** ({len(roles)})',
        value=' '.join(roles[:15]) + (' ...' if len(roles) > 15 else '') if roles else 'none',
        inline=False
    )
    e.set_footer(text=f'requested by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='serverinfo', aliases=['si', 'server'])
async def serverinfo_cmd(ctx):
    guild   = ctx.guild
    bots    = sum(1 for m in guild.members if m.bot)
    humans  = guild.member_count - bots
    online  = sum(1 for m in guild.members if m.status != discord.Status.offline and not m.bot)
    created = guild.created_at.replace(tzinfo=timezone.utc) if guild.created_at.tzinfo is None else guild.created_at
    age     = (datetime.now(timezone.utc) - created).days
    e = discord.Embed(title=guild.name, color=0x5865F2)
    if guild.icon:
        e.set_thumbnail(url=guild.icon.url)
    e.add_field(name='**Owner**',       value=guild.owner.mention if guild.owner else 'unknown', inline=True)
    e.add_field(name='**Server ID**',   value=str(guild.id),   inline=True)
    e.add_field(name='**Server Age**',  value=f'{age}d',       inline=True)
    e.add_field(name='**Members**',     value=str(guild.member_count), inline=True)
    e.add_field(name='**Humans**',      value=str(humans),     inline=True)
    e.add_field(name='**Bots**',        value=str(bots),       inline=True)
    e.add_field(name='**Online**',      value=str(online),     inline=True)
    e.add_field(name='**Channels**',    value=str(len(guild.channels)), inline=True)
    e.add_field(name='**Roles**',       value=str(len(guild.roles)),    inline=True)
    e.add_field(name='**Boost Level**', value=str(guild.premium_tier),  inline=True)
    e.add_field(name='**Boosts**',      value=str(guild.premium_subscription_count), inline=True)
    e.add_field(name='**Emojis**',      value=f'{len(guild.emojis)}/{guild.emoji_limit}', inline=True)
    e.set_footer(text=f'requested by {ctx.author.display_name}')
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
    e.add_field(name='**Total**',   value=f'{total:,}',  inline=True)
    e.add_field(name='**Humans**',  value=str(humans), inline=True)
    e.add_field(name='**Bots**',    value=str(bots),   inline=True)
    e.add_field(name='**Online**',  value=str(online), inline=True)
    e.set_footer(text=f'requested by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='newest', aliases=['nw'])
async def newest_cmd(ctx):
    members = sorted(
        [m for m in ctx.guild.members if not m.bot],
        key=lambda m: m.joined_at or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True
    )[:5]
    e = discord.Embed(title=f'Newest Members  •  {ctx.guild.name}', color=0x57F287)
    lines = []
    for i, m in enumerate(members, 1):
        joined  = m.joined_at.strftime('%b %d, %Y') if m.joined_at  else '?'
        created = m.created_at.strftime('%b %d, %Y') if m.created_at else '?'
        lines.append(f'**{i}.** {m.mention}  •  joined {joined}  •  acc created {created}')
    e.description = '\n'.join(lines)
    e.set_footer(text=f'requested by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='oldest', aliases=['ol'])
async def oldest_cmd(ctx):
    members = sorted(
        [m for m in ctx.guild.members if not m.bot],
        key=lambda m: m.joined_at or datetime.now(timezone.utc)
    )[:5]
    e = discord.Embed(title=f'OGs  •  {ctx.guild.name}', color=0xF1C40F)
    lines = []
    for i, m in enumerate(members, 1):
        joined  = m.joined_at.strftime('%b %d, %Y') if m.joined_at  else '?'
        created = m.created_at.strftime('%b %d, %Y') if m.created_at else '?'
        lines.append(f'**{i}.** {m.mention}  •  joined {joined}  •  acc created {created}')
    e.description = '\n'.join(lines)
    e.set_footer(text=f'requested by {ctx.author.display_name}')
    await ctx.reply(embed=e)


@bot.command(name='botlist', aliases=['bl'])
async def botlist_cmd(ctx):
    bots = sorted([m for m in ctx.guild.members if m.bot], key=lambda m: m.name.lower())
    if not bots:
        return await ctx.reply(embed=discord.Embed(description='no bots in this server', color=0x5865F2))
    lines = [f'**{i}.** {b.mention}  •  `{b.name}`' for i, b in enumerate(bots, 1)]
    e = discord.Embed(title=f'bots in {ctx.guild.name}  ({len(bots)})', color=0x5865F2)
    e.description = '\n'.join(lines)
    e.set_footer(text=f'requested by {ctx.author.display_name}')
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
    e.set_author(name=f'ticket stats  •  {member.display_name}', icon_url=member.display_avatar.url)
    e.description = (
        f'## {member.display_name}\n\n'
        f'**Claimed**     ╸  {claimed}\n'
        f'**Closed**      ╸  {closed}\n'
        f'**Completion**  ╸  {completion}\n'
        f'**Rating**      ╸  {stars}  {avg_rating}'
    )
    e.set_thumbnail(url=member.display_avatar.url)
    e.set_footer(text='run $rateme inside a ticket to request a rating from the person you helped')
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
                embed=discord.Embed(description="this rating request isn't for you", color=0xED4245),
                ephemeral=True
            )
        if self.rated:
            return await interaction.response.send_message(
                embed=discord.Embed(description="you've already rated this ticket", color=0xFEE75C),
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
                    embed=discord.Embed(description='you already rated this ticket', color=0xFEE75C),
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
        e.description = f'got it, thanks\n\n**{stars}**  {rating}/5'
        e.set_footer(text="Trial's Cross Trade")
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
        return await ctx.reply(embed=discord.Embed(description="this isn't a ticket channel", color=0xED4245))
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
    if not ticket:
        return await ctx.reply(embed=discord.Embed(description='no ticket found here', color=0xED4245))
    if not ticket.get('claimed_by'):
        return await ctx.reply(embed=discord.Embed(description='this ticket has not been claimed yet', color=0xED4245))
    if ticket['claimed_by'] != ctx.author.id:
        return await ctx.reply(embed=discord.Embed(description='only the claimer of this ticket can request a rating', color=0xED4245))
    creator = ctx.guild.get_member(ticket['user_id'])
    if not creator:
        return await ctx.reply(embed=discord.Embed(description="can't find the ticket creator — they may have left", color=0xED4245))
    if creator.id == ctx.author.id:
        return await ctx.reply(embed=discord.Embed(description="you can't rate yourself", color=0xED4245))
    # check already rated
    async with db.pool.acquire() as c:
        already = await c.fetchval(
            'SELECT rating FROM ticket_ratings WHERE guild_id=$1 AND ticket_id=$2',
            ctx.guild.id, ticket['ticket_id']
        )
    if already:
        return await ctx.reply(embed=discord.Embed(description='this ticket has already been rated', color=0xFEE75C))

    view = RatingView(
        claimer_id=ctx.author.id,
        guild_id=ctx.guild.id,
        ticket_id=ticket['ticket_id'],
        user_id=creator.id
    )
    e = discord.Embed(color=0x5865F2)
    e.set_author(name="Trial's Cross Trade", icon_url=ctx.guild.icon.url if ctx.guild.icon else None)
    e.description = (
        f'**{ctx.author.display_name}** handled your ticket **#{ticket["ticket_id"]}**\n\n'
        f'how was your experience? tap a number — 1 is bad, 5 is great\n'
        f'takes two seconds and means a lot'
    )
    e.set_footer(text='expires in 24h  •  your rating is private')
    try:
        await creator.send(embed=e, view=view)
        resp = discord.Embed(description=f'sent a rating request to {creator.mention}', color=0x57F287)
        resp.set_footer(text='they have 24 hours — you\'ll get it in your stats automatically')
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
            dot = 'online' if m.status == discord.Status.online else ('idle' if m.status == discord.Status.idle else 'dnd')
            lines.append(f'{m.mention} [{dot}]')
        for m in sorted(offline, key=lambda x: x.display_name.lower()):
            lines.append(f'{m.mention} [offline]')
        if lines:
            sections.append(
                f'**{tier_names[tier]}** — {len(online)}/{len(members)} online\n'
                + '\n'.join(lines)
            )

    if not sections:
        return await ctx.reply(embed=discord.Embed(description='no middlemen found', color=0x5865F2))

    e = discord.Embed(
        title=f'Active Middlemen  •  {ctx.guild.name}',
        color=0x57F287 if total_online > 0 else 0x747F8D
    )
    e.description = '\n\n'.join(sections)
    e.set_footer(text=f'{total_online}/{total_mm} online  •  requested by {ctx.author.display_name}')
    await ctx.reply(embed=e)


# ================================================================== help command

HELP_PAGES = [
    {
        'title': 'Commands  •  Page 1 / 8',
        'name':  'Tickets  •  Staff and Middleman',
        'value': (
            '`$claim`                  claim a ticket\n'
            '`$unclaim`                drop your claim\n'
            '`$close`                  close and save transcript\n'
            '`$add @user`              let someone talk in ticket\n'
            '`$remove @user`           remove someone from ticket\n'
            '`$rename name`            rename the ticket channel\n'
            '`$transfer @user`         hand off your claim\n'
            '`$proof`                  post completed trade proof\n'
            '`$ticketstats`            your personal claimed / closed / rating stats  •  `$ts`\n'
        '`$rateme`                 send a rating request DM to the ticket creator'
        ),
    },
    {
        'title': 'Commands  •  Page 2 / 8',
        'name':  'Staff Tools',
        'value': (
            '`$channelperm #ch @target perm on/off`  — one channel\n'
            '`$channelpermall @target perm on/off`   — all channels\n'
                '`$slowmode <seconds>`                   — set slowmode, 0 disables  •  `$slow`\n\n'
            '**Text:** `send` `read` `embed` `attach` `react` `history` `mentions` `pin` `tts` `polls` `slash` `external` `stickers`\n'
            '**Voice:** `voice` `speak` `stream` `mute` `deafen` `move` `vad` `soundboard` `activities` `priority` `stage`\n'
            '**Manage:** `slow` `manage` `webhooks` `manage_perms` `threads` `private_threads` `send_in_threads`'
        ),
    },
    {
        'title': 'Commands  •  Page 3 / 8',
        'name':  'Utility  •  Everyone',
        'value': (
            '`$snipe` / `$sn`       last deleted message in channel\n'
            '`$esnipe` / `$es`      last edited message in channel\n'
            '`$ping`                bot latency\n'
            '`$uptime`              how long the bot has been online\n'
            '`$membercount` / `$mc` total, human, bot and online count\n'
            '`$newest` / `$nw`      5 most recently joined members\n'
            '`$oldest` / `$ol`      5 longest standing members\n'
            '`$botlist` / `$bl`     all bots in the server\n'
            '`$activemm` / `$amm`   online middlemen by tier'
        ),
    },
    {
        'title': 'Commands  •  Page 4 / 8',
        'name':  'User and Server Info',
        'value': (
            '`$userinfo [@user]`    full info — roles, age, status  •  `$ui` `$whois`\n'
            '`$serverinfo`          server overview — members, boosts, channels  •  `$si`'
        ),
    },
    {
        'title': 'Commands  •  Page 5 / 8',
        'name':  'Invites  •  Everyone',
        'value': (
            '`$invites [@user]`                view your or someone\'s invite stats\n'
            '`$invited @user`                  see everyone that user has invited\n'
            '`$whoinvited [@user]`             who invited a specific member\n'
            '`$lb` / `$lbi` / `$invitelb`      live invite leaderboard top 10\n'
            '`$clearinvites all`               reset all server invites  •  owner only\n'
            '`$clearinvites @user`             reset one user\'s invites  •  owner only'
        ),
    },
    {
        'title': 'Commands  •  Page 6 / 8',
        'name':  'Setup  •  Owner Only',
        'value': (
            '`$setup`               post ticket panel (support, mm, reward)\n'
            '`$setupverify`         post verification panel\n'
            '`$setcategory #cat`    set ticket category\n'
            '`$setlogs #ch`         set log channel\n'
            '`$config`              full bot config — channels, roles, uptime, latency\n'
            '`$lock` / `$unlock`    open or close all tickets\n'
            '`$say #ch message`     make the bot say something\n'
            '`$blacklist @user`     blacklist from tickets\n'
            '`$unblacklist @user`   remove from blacklist\n'
            '`$blacklists`          view all blacklisted users'
        ),
    },
    {
        'title': 'Commands  •  Page 7 / 8',
        'name':  'Giveaways  •  Slash Commands  •  GW Host role required',
        'value': (
            '`/giveaway create`         create a giveaway\n'
            '  `duration` `winners` `prize` `channel` `host` `image` `required_role`\n\n'
            '`/giveaway end`            end a giveaway early\n'
            '`/giveaway reroll`         reroll a new winner\n'
            '`/giveaway addentries`     add a bonus entry role\n'
            '`/giveaway removeentries`  remove a bonus entry role\n\n'
            '**Duration:** `10d` `2h` `30m` `90s`\n'
            '**Entries:** 1 base per user  •  bonus roles stack on top\n'
            '**Required role:** blocks entry if member doesn\'t have it'
        ),
    },
    {
        'title': 'Commands  •  Page 8 / 8',
        'name':  'Quick Reference  •  Aliases',
        'value': (
            '`$sn`   →  `$snipe`          `$es`  →  `$esnipe`\n'
            '`$mc`   →  `$membercount`    `$nw`  →  `$newest`\n'
            '`$ol`   →  `$oldest`         `$bl`  →  `$botlist`\n'
            '`$amm`  →  `$activemm`       `$ts`  →  `$ticketstats`\n'
            '`$ui`   →  `$userinfo`       `$si`  →  `$serverinfo`\n'
            '`$lb`   →  `$leaderboardinvites`\n'
            '`$slow` →  `$slowmode`\n\n'
            '**Prefix:** `$`  •  **Slash:** `/giveaway`\n'
            '**Support tickets** open for all  •  **MM tickets** require verification'
        ),
    },
]


def make_help_embed(page: int) -> discord.Embed:
    p = HELP_PAGES[page]
    e = discord.Embed(title=p['title'], color=TIER_COLOR['support'])
    e.add_field(name=p['name'], value=p['value'], inline=False)
    e.set_footer(text=f"Trial's Cross Trade  •  page {page + 1} of {len(HELP_PAGES)}")
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

    @discord.ui.button(label='Prev', style=ButtonStyle.gray, custom_id='help_prev')
    async def prev_btn(self, interaction: discord.Interaction, _):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message(
                embed=discord.Embed(description="that's not your help menu", color=0xED4245),
                ephemeral=True
            )
        self.page -= 1
        self._update_buttons()
        await interaction.response.edit_message(embed=make_help_embed(self.page), view=self)

    @discord.ui.button(label='Next', style=ButtonStyle.gray, custom_id='help_next')
    async def next_btn(self, interaction: discord.Interaction, _):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message(
                embed=discord.Embed(description="that's not your help menu", color=0xED4245),
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
            e.set_footer(text='timed out — run $help again')
            await self.message.edit(embed=e, view=self)
        except Exception:
            pass


@bot.command(name='help')
async def help_cmd(ctx):
    view         = HelpView(author_id=ctx.author.id)
    view.message = await ctx.reply(embed=make_help_embed(0), view=view)


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
                embed=discord.Embed(description="you're already verified", color=0x57F287),
                ephemeral=True
            )
        code = gen_captcha()
        captchas[user.id] = code
        e = discord.Embed(title='Captcha Verification', color=0x5865F2)
        e.description = f"here's your code:\n\n# `{code}`\n\ntype it in this channel"
        e.set_footer(text='case insensitive')
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
    e = discord.Embed(color=0x57F287)
    e.set_author(name="Trial's Cross Trade  •  Verification")
    e.description = (
        "welcome to **Trial's Cross Trade**\n\n"
        "before you can access the rest of the server you need to verify\n"
        "it takes under 10 seconds and keeps the server safe\n\n"
        "hit the button below, type the code you get — and you're in"
    )
    e.set_footer(text="Trial's Cross Trade  •  click below to get started")
    await ctx.send(embed=e, view=VerifyView())
    try:
        await ctx.message.delete()
    except Exception:
        pass


# ================================================================== giveaway system

def parse_duration(s: str) -> int:
    s     = s.strip().lower()
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
    if d > 0: return f'in {d}d {h}h'
    if h > 0: return f'in {h}h {m}m'
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
        self.giveaway_id       = giveaway_id
        self.enter_btn.custom_id = f'gw_enter:{giveaway_id}'
        self.parts_btn.custom_id = f'gw_parts:{giveaway_id}'

    @discord.ui.button(label='0', emoji='🎉', style=ButtonStyle.primary, custom_id='gw_enter:0')
    async def enter_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        gw_id = int(button.custom_id.split(':')[1])
        async with db.pool.acquire() as c:
            gw = await c.fetchrow('SELECT * FROM giveaways WHERE id=$1', gw_id)
            if not gw or gw['ended']:
                return await interaction.response.send_message(
                    embed=discord.Embed(description='giveaway already ended', color=0xED4245),
                    ephemeral=True
                )
            if gw['required_role_id']:
                if not interaction.user.get_role(gw['required_role_id']):
                    role = interaction.guild.get_role(gw['required_role_id'])
                    return await interaction.response.send_message(
                        embed=discord.Embed(
                            description=f'you need {role.mention if role else "the required role"} to enter',
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
                    embed=discord.Embed(description='left the giveaway', color=0xFEE75C),
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
        msg = f'you entered with **{total}** entr{"y" if total == 1 else "ies"}!'
        await interaction.followup.send(
            embed=discord.Embed(description=msg, color=0x57F287), ephemeral=True
        )

    @discord.ui.button(label='Participants', emoji='👥', style=ButtonStyle.secondary, custom_id='gw_parts:0')
    async def parts_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        gw_id = int(button.custom_id.split(':')[1])
        async with db.pool.acquire() as c:
            rows = await c.fetch(
                '''SELECT user_id, COUNT(*) AS entry_count
                   FROM giveaway_entries
                   WHERE giveaway_id=$1
                   GROUP BY user_id
                   ORDER BY entry_count DESC, user_id''',
                gw_id
            )
        if not rows:
            return await interaction.response.send_message(
                embed=discord.Embed(description='nobody entered yet', color=0x5865F2), ephemeral=True
            )
        lines = []
        for r in rows:
            n = r['entry_count']
            entry_str = f'{n} entr{"y" if n == 1 else "ies"}'
            lines.append(f'<@{r["user_id"]}> — **{entry_str}**')

        total    = len(lines)
        per_page = 15
        pages    = [lines[i:i + per_page] for i in range(0, len(lines), per_page)]

        def make_page(idx):
            e = discord.Embed(
                title=f'Participants — {total}  •  Page {idx+1}/{len(pages)}',
                color=0x5865F2
            )
            e.description = '\n'.join(pages[idx])
            return e

        class PartsView(View):
            def __init__(self):
                super().__init__(timeout=60)
                self.page    = 0
                self.message = None
                self._sync()

            def _sync(self):
                self.prev.disabled = self.page == 0
                self.nxt.disabled  = self.page == len(pages) - 1

            @discord.ui.button(label='Prev', style=ButtonStyle.gray)
            async def prev(self, inter: discord.Interaction, _):
                if inter.user.id != interaction.user.id:
                    return await inter.response.send_message('not your menu', ephemeral=True)
                self.page -= 1
                self._sync()
                await inter.response.edit_message(embed=make_page(self.page), view=self)

            @discord.ui.button(label='Next', style=ButtonStyle.gray)
            async def nxt(self, inter: discord.Interaction, _):
                if inter.user.id != interaction.user.id:
                    return await inter.response.send_message('not your menu', ephemeral=True)
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
            return await interaction.response.send_message(embed=make_page(0), ephemeral=True)

        pv          = PartsView()
        pv.message  = await interaction.response.send_message(embed=make_page(0), view=pv, ephemeral=True)


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
        e = discord.Embed(title=f'🎉 {gw["prize"]}', description='giveaway ended with no entries', color=0xED4245)
        await msg.edit(embed=e, view=None)
        await channel.send(embed=discord.Embed(description=f'**{gw["prize"]}** ended with no entries', color=0xED4245))
        return

    pool         = [r['user_id'] for r in entries]
    unique_users = list(set(pool))
    num_win      = min(gw['winners_count'], len(unique_users))

    async with db.pool.acquire() as c:
        last_row = await c.fetchrow(
            'SELECT winner_ids FROM giveaway_last_winners WHERE guild_id=$1 AND prize=$2',
            gw['guild_id'], gw['prize']
        )
    last_winners = set(last_row['winner_ids']) if last_row else set()
    eligible     = [u for u in unique_users if u not in last_winners]
    if len(eligible) < num_win:
        eligible = unique_users

    weighted = [u for u in pool if u in set(eligible)]
    if not weighted:
        weighted = list(pool)

    winners  = _pick_winners(weighted, eligible, num_win)

    async with db.pool.acquire() as c:
        await c.execute(
            '''INSERT INTO giveaway_last_winners (guild_id, prize, winner_ids) VALUES ($1,$2,$3)
               ON CONFLICT (guild_id, prize) DO UPDATE SET winner_ids=$3''',
            gw['guild_id'], gw['prize'], winners
        )

    mentions = ' '.join(f'<@{w}>' for w in winners)
    e = discord.Embed(title=f'🎉 {gw["prize"]}', color=0xF1C40F)
    e.description = f'**Winner{"s" if num_win > 1 else ""}** — {mentions}'
    e.set_footer(text='Giveaway ended')
    dead_view = GiveawayEntryView(giveaway_id)
    for item in dead_view.children:
        item.disabled = True
    await msg.edit(embed=e, view=dead_view)
    await channel.send(
        content=mentions,
        embed=discord.Embed(description=f'🎉 {mentions} won **{gw["prize"]}**!!', color=0xF1C40F)
    )


async def schedule_giveaway(giveaway_id: int, ends_at: datetime, guild: discord.Guild):
    delay = (ends_at - datetime.now(timezone.utc)).total_seconds()
    if delay > 0:
        await asyncio.sleep(delay)
    await end_giveaway(giveaway_id, guild)


# ================================================================== giveaway slash commands

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
    host: discord.Member         = None,
    image: str                   = None,
    required_role: discord.Role  = None,
):
    has_gw_role = interaction.user.get_role(GW_HOST_ROLE) is not None
    if not has_gw_role and not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message(
            embed=discord.Embed(description='you need the **Giveaway Host** role to make giveaways', color=0xED4245),
            ephemeral=True
        )
    secs = parse_duration(duration)
    if not secs:
        return await interaction.response.send_message(
            embed=discord.Embed(description='invalid duration — try something like `10d`, `2h`, `30m`', color=0xED4245),
            ephemeral=True
        )
    if winners < 1:
        return await interaction.response.send_message(
            embed=discord.Embed(description='gotta have at least 1 winner', color=0xED4245),
            ephemeral=True
        )
    channel = channel or interaction.channel
    host    = host    or interaction.user
    ends_at = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(seconds=secs)

    async with db.pool.acquire() as c:
        bonus_rows  = await c.fetch('SELECT role_id, entries FROM giveaway_bonus_roles WHERE guild_id=$1', interaction.guild_id)
        bonus_roles = [(r['role_id'], r['entries']) for r in bonus_rows]

    embed = await build_giveaway_embed(prize, winners, host, ends_at, required_role, image, bonus_roles, guild=interaction.guild)
    await interaction.response.send_message(
        embed=discord.Embed(description='posting...', color=0x5865F2), ephemeral=True
    )
    temp_view = GiveawayEntryView(0)
    msg       = await channel.send(embed=embed, view=temp_view)

    async with db.pool.acquire() as c:
        gw_id = await c.fetchval(
            '''INSERT INTO giveaways
               (guild_id, channel_id, message_id, prize, winners_count, host_id, required_role_id, image_url, ends_at, ended)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,FALSE)
               RETURNING id''',
            interaction.guild_id, channel.id, msg.id, prize, winners, host.id,
            required_role.id if required_role else None, image, ends_at
        )

    real_view = GiveawayEntryView(gw_id)
    await msg.edit(view=real_view)
    await interaction.edit_original_response(
        embed=discord.Embed(description=f'giveaway posted in {channel.mention}', color=0x57F287)
    )
    asyncio.create_task(schedule_giveaway(gw_id, ends_at, interaction.guild))


@giveaway_group.command(name='end', description='End a giveaway early')
@app_commands.describe(message_id='The message ID of the giveaway to end')
async def giveaway_end(interaction: discord.Interaction, message_id: str):
    if not interaction.user.guild_permissions.manage_guild and not _is_staff(interaction.user):
        return await interaction.response.send_message(
            embed=discord.Embed(description='staff only', color=0xED4245), ephemeral=True
        )
    try:
        mid = int(message_id)
    except ValueError:
        return await interaction.response.send_message(
            embed=discord.Embed(description='invalid message ID', color=0xED4245), ephemeral=True
        )
    async with db.pool.acquire() as c:
        gw = await c.fetchrow('SELECT * FROM giveaways WHERE message_id=$1 AND guild_id=$2', mid, interaction.guild_id)
    if not gw:
        return await interaction.response.send_message(
            embed=discord.Embed(description='giveaway not found', color=0xED4245), ephemeral=True
        )
    if gw['ended']:
        return await interaction.response.send_message(
            embed=discord.Embed(description='already ended', color=0xED4245), ephemeral=True
        )
    await interaction.response.send_message(
        embed=discord.Embed(description='ending it...', color=0xFEE75C), ephemeral=True
    )
    await end_giveaway(gw['id'], interaction.guild)
    await interaction.edit_original_response(
        embed=discord.Embed(description='giveaway ended', color=0x57F287)
    )


@giveaway_group.command(name='reroll', description='Reroll a giveaway winner')
@app_commands.describe(message_id='The message ID of the giveaway to reroll')
async def giveaway_reroll(interaction: discord.Interaction, message_id: str):
    if not interaction.user.guild_permissions.manage_guild and not _is_staff(interaction.user):
        return await interaction.response.send_message(
            embed=discord.Embed(description='staff only', color=0xED4245), ephemeral=True
        )
    try:
        mid = int(message_id)
    except ValueError:
        return await interaction.response.send_message(
            embed=discord.Embed(description='invalid message ID', color=0xED4245), ephemeral=True
        )
    async with db.pool.acquire() as c:
        gw = await c.fetchrow('SELECT * FROM giveaways WHERE message_id=$1 AND guild_id=$2', mid, interaction.guild_id)
        if not gw:
            return await interaction.response.send_message(
                embed=discord.Embed(description='giveaway not found', color=0xED4245), ephemeral=True
            )
        entries = await c.fetch('SELECT user_id FROM giveaway_entries WHERE giveaway_id=$1', gw['id'])
    if not entries:
        return await interaction.response.send_message(
            embed=discord.Embed(description='no entries to reroll from', color=0xED4245), ephemeral=True
        )
    pool         = [r['user_id'] for r in entries]
    unique_users = list(set(pool))
    num_win      = min(gw['winners_count'], len(unique_users))
    winners      = _pick_winners(pool, unique_users, num_win)
    mentions     = ' '.join(f'<@{w}>' for w in winners)
    channel      = interaction.guild.get_channel(gw['channel_id'])
    if channel:
        await channel.send(
            content=mentions,
            embed=discord.Embed(
                description=f'reroll!! {mentions} won **{gw["prize"]}**!!',
                color=0xF1C40F
            )
        )
    await interaction.response.send_message(
        embed=discord.Embed(description=f'rerolled — new winner: {mentions}', color=0x57F287),
        ephemeral=True
    )


@giveaway_group.command(name='addentries', description='Add a bonus entry role')
@app_commands.describe(role='The role to give bonus entries', entries='Number of bonus entries')
async def giveaway_addentries(interaction: discord.Interaction, role: discord.Role, entries: int):
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message(
            embed=discord.Embed(description='you need Manage Server for that', color=0xED4245), ephemeral=True
        )
    if entries < 1:
        return await interaction.response.send_message(
            embed=discord.Embed(description='gotta be at least 1 entry', color=0xED4245), ephemeral=True
        )
    async with db.pool.acquire() as c:
        await c.execute(
            '''INSERT INTO giveaway_bonus_roles (guild_id, role_id, entries) VALUES ($1,$2,$3)
               ON CONFLICT (guild_id, role_id) DO UPDATE SET entries=$3''',
            interaction.guild_id, role.id, entries
        )
    await interaction.response.send_message(
        embed=discord.Embed(
            description=f'{role.mention} now gives **{entries}** bonus entr{"y" if entries == 1 else "ies"}',
            color=0x57F287
        ),
        ephemeral=True
    )


@giveaway_group.command(name='removeentries', description='Remove a bonus entry role')
@app_commands.describe(role='The role to remove from bonus entries')
async def giveaway_removeentries(interaction: discord.Interaction, role: discord.Role):
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message(
            embed=discord.Embed(description='Manage Server required', color=0xED4245), ephemeral=True
        )
    async with db.pool.acquire() as c:
        result = await c.execute(
            'DELETE FROM giveaway_bonus_roles WHERE guild_id=$1 AND role_id=$2',
            interaction.guild_id, role.id
        )
    if result == 'DELETE 0':
        return await interaction.response.send_message(
            embed=discord.Embed(description=f"{role.mention} wasn't in the bonus list", color=0xFEE75C),
            ephemeral=True
        )
    await interaction.response.send_message(
        embed=discord.Embed(description=f'{role.mention} removed from bonus entries', color=0x57F287),
        ephemeral=True
    )


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
    allowed, remaining = limiter.global_check(ctx.author.id)
    if not allowed:
        secs = int(remaining)
        try:
            await ctx.reply(
                embed=discord.Embed(
                    description=f"slow down — you're sending commands too fast\ncooldown: **{secs}s**",
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

    if not _bot_ready:
        _bot_ready = True
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
        bot.add_view(VerifyView())

        try:
            async with db.pool.acquire() as c:
                active_gws = await c.fetch('SELECT id, guild_id, ends_at FROM giveaways WHERE ended=FALSE')
            for gw in active_gws:
                bot.add_view(GiveawayEntryView(gw['id']))
                guild = bot.get_guild(gw['guild_id'])
                if guild:
                    if gw['ends_at'] <= datetime.now(timezone.utc):
                        asyncio.create_task(end_giveaway(gw['id'], guild))
                    else:
                        asyncio.create_task(schedule_giveaway(gw['id'], gw['ends_at'], guild))
            logger.info(f'rescheduled {len(active_gws)} active giveaway(s)')
        except Exception as ex:
            logger.error(f'giveaway restore failed: {ex}')

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
            await ctx.reply(embed=discord.Embed(description='you don\'t have permission to use that', color=0xED4245))
        except Exception:
            pass

    elif isinstance(error, commands.MissingRequiredArgument):
        try:
            await ctx.reply(embed=discord.Embed(
                description=f'missing argument: `{error.param.name}`\ncheck `$help` for usage',
                color=0xFEE75C
            ))
        except Exception:
            pass

    elif isinstance(error, commands.CommandOnCooldown):
        try:
            await ctx.reply(embed=discord.Embed(
                description=f'slow down — wait **{error.retry_after:.1f}s**',
                color=0xFEE75C
            ), delete_after=int(error.retry_after) + 1)
        except Exception:
            pass

    elif isinstance(error, commands.BotMissingPermissions):
        perms = ', '.join(error.missing_permissions)
        try:
            await ctx.reply(embed=discord.Embed(
                description=f'i\'m missing permissions to do that: `{perms}`',
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
                    description='got rate limited by Discord — try again in a sec',
                    color=0xFEE75C
                ))
            except Exception:
                pass
        elif error.status == 403:
            try:
                await ctx.reply(embed=discord.Embed(
                    description='i don\'t have permission to do that here',
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
            is_new_account = acc_age.days < 5

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
                note = ' (fake — account too new)' if is_new_account else ' (fake)'
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
                        description=f'{message.author.mention} verified — welcome in!',
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
                    description=f'{message.author.mention} wrong code — new code is `{new_code}`',
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
