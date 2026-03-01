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

logging.basicConfig(level=logging.INFO, format='%(asctime)s  %(levelname)s  %(message)s')
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ config

OWNER_ID = 1029438856069656576

ROLES = {
    'staff':      1432081794647199895,
    'lowtier':    1453757017218093239,
    'midtier':    1434610759140118640,
    'hightier':   1453757157144137911,
    'unverified': 0,  # set via $setverify
    'verified':   0,  # set via $setverify
    'member':     0,  # set via $setverify
}

PROOF_CHANNEL = 1472695529883435091

# in-memory captcha store: user_id -> code
captchas: dict = {}
# verification config: guild_id -> {unverified, verified, member, channel}
verify_config: dict = {}

TIER_COLOR = {
    'lowtier':  0x57F287,
    'midtier':  0xFEE75C,
    'hightier': 0xED4245,
    'support':  0x5865F2,
    'reward':   0xF1C40F,
}

TIER_LABEL = {
    'lowtier':  'Low Tier   100 – 900 RBX',
    'midtier':  'Mid Tier   1K – 3K RBX',
    'hightier': 'High Tier  3.1K – 5K+ RBX',
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

MAX_OPEN = 1
tickets_locked: dict = {}


# ------------------------------------------------------------------ rate limiter

class RateLimiter:
    def __init__(self):
        self._buckets: dict = defaultdict(dict)

    def check(self, uid: int, action: str, cooldown: int) -> bool:
        now  = datetime.now(timezone.utc).timestamp()
        last = self._buckets[action].get(uid, 0)
        if now - last < cooldown:
            return False
        self._buckets[action][uid] = now
        return True

limiter = RateLimiter()


# ------------------------------------------------------------------ database

class Database:
    pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        url = os.getenv('DATABASE_URL', '')
        if not url:
            raise RuntimeError('DATABASE_URL is not set')
        if url.startswith('postgres://'):
            url = 'postgresql://' + url[len('postgres://'):]
        self.pool = await asyncpg.create_pool(url, min_size=2, max_size=10, command_timeout=15, max_inactive_connection_lifetime=300)
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
            for sql in [
                'ALTER TABLE config ADD COLUMN IF NOT EXISTS ticket_counter INT DEFAULT 0',
                'ALTER TABLE blacklist ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()',
                'ALTER TABLE config ADD COLUMN IF NOT EXISTS verify_unverified_id BIGINT',
                'ALTER TABLE config ADD COLUMN IF NOT EXISTS verify_verified_id BIGINT',
                'ALTER TABLE config ADD COLUMN IF NOT EXISTS verify_member_id BIGINT',
                'ALTER TABLE config ADD COLUMN IF NOT EXISTS verify_channel_id BIGINT',
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


# ------------------------------------------------------------------ bot

intents = discord.Intents.default()
intents.message_content = True
intents.members         = True
intents.guilds          = True

bot = commands.Bot(command_prefix='$', intents=intents, help_command=None)


# ------------------------------------------------------------------ checks

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
    if ctx.author.guild_permissions.administrator:
        return True
    ttype = ticket['ticket_type']
    tier  = ticket.get('tier')
    if ttype == 'support':
        r = ctx.guild.get_role(ROLES['staff'])
        if r and r in ctx.author.roles:
            return True
    elif ttype == 'middleman' and tier in ROLES:
        r = ctx.guild.get_role(ROLES[tier])
        if r and r in ctx.author.roles:
            return True
    if ticket.get('claimed_by') == ctx.author.id:
        return True
    return False


# ------------------------------------------------------------------ helpers

async def log(guild, title, desc=None, color=0x5865F2, fields=None):
    try:
        async with db.pool.acquire() as c:
            cfg = await c.fetchrow('SELECT log_channel_id FROM config WHERE guild_id = $1', guild.id)
        if not cfg or not cfg['log_channel_id']:
            return
        ch = guild.get_channel(cfg['log_channel_id'])
        if not ch:
            return
        e = discord.Embed(title=title, color=color, timestamp=datetime.now(timezone.utc))
        if desc:
            e.description = desc
        for k, v in (fields or {}).items():
            e.add_field(name=k, value=str(v), inline=True)
        await ch.send(embed=e)
    except Exception as ex:
        logger.error(f'log error: {ex}')

async def claim_lock(channel, claimer, creator=None, ticket_type='middleman'):
    # only lock down talk perms for middleman tickets
    # support and reward tickets stay open to all staff even when claimed
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
    # only need to restore perms if it was a middleman ticket
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

def ticket_embed(user, tier, ticket_id, extra_fields=None, desc=None):
    color = TIER_COLOR.get(tier, 0x5865F2)
    e = discord.Embed(color=color)
    e.set_author(name=f'{user.display_name}   {TIER_LABEL.get(tier, tier)}', icon_url=user.display_avatar.url)
    e.description = desc or 'a staff member will be with you shortly\n\nplease be patient and do not ping anyone'
    if extra_fields:
        for name, value, inline in extra_fields:
            e.add_field(name=name, value=value, inline=inline)
    e.set_footer(text=f'ticket {ticket_id}   {datetime.now(timezone.utc).strftime("%b %d, %Y")}')
    return e

async def pre_open_checks(interaction, guild, user):
    if tickets_locked.get(guild.id):
        await interaction.followup.send('tickets are closed right now, check back later', ephemeral=True)
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
        await interaction.followup.send(
            f"you're blacklisted from opening tickets\n\nreason: {reason}\nby: {by.mention if by else 'staff'} on {date}\n\nif you think this is wrong, dm a staff member",
            ephemeral=True
        )
        return False
    if real_open >= MAX_OPEN:
        await interaction.followup.send(
            f"you already have an open ticket — close it before making a new one",
            ephemeral=True
        )
        return False
    if not cfg or not cfg['ticket_category_id']:
        await interaction.followup.send("tickets aren't configured yet, ping a staff member", ephemeral=True)
        return False
    return cfg

async def send_transcript(channel, ticket, closer):
    async with db.pool.acquire() as c:
        cfg = await c.fetchrow('SELECT log_channel_id FROM config WHERE guild_id = $1', channel.guild.id)
    if not cfg or not cfg['log_channel_id']:
        return
    lc = channel.guild.get_channel(cfg['log_channel_id'])
    if not lc:
        return
    opener  = channel.guild.get_member(ticket['user_id'])
    claimer = channel.guild.get_member(ticket['claimed_by']) if ticket['claimed_by'] else None
    header = '\n'.join([
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
    e = discord.Embed(title='ticket closed', color=TIER_COLOR['hightier'])
    e.add_field(name='id',         value=ticket['ticket_id'],                  inline=True)
    e.add_field(name='opened by',  value=opener.mention if opener else '?',    inline=True)
    e.add_field(name='closed by',  value=closer.mention,                       inline=True)
    await lc.send(embed=e, file=file)


# ================================================================== UI


# ------------------------------------------------ middleman modal

class MiddlemanModal(Modal, title='Middleman Request'):
    def __init__(self, tier):
        super().__init__()
        self.tier = tier
        self.trader    = TextInput(label='Who are you trading with?',  placeholder='their username or ID', required=True)
        self.giving    = TextInput(label='What are you giving?',        placeholder='e.g. 1 garam',         style=discord.TextStyle.paragraph, required=True)
        self.receiving = TextInput(label='What are you receiving?',     placeholder='e.g. 500 Robux',       style=discord.TextStyle.paragraph, required=True)
        self.tip       = TextInput(label='Leaving a tip? (optional)',   placeholder='leave blank to skip',  required=False)
        self.add_item(self.trader)
        self.add_item(self.giving)
        self.add_item(self.receiving)
        self.add_item(self.tip)

    async def on_submit(self, interaction: discord.Interaction):
        if not limiter.check(interaction.user.id, 'open', 10):
            return await interaction.response.send_message('slow down a bit', ephemeral=True)
        try:
            await interaction.response.defer(ephemeral=True)
        except Exception:
            return
        guild, user = interaction.guild, interaction.user
        try:
            cfg = await pre_open_checks(interaction, guild, user)
        except Exception as ex:
            logger.error(f'pre_open_checks error: {ex}')
            try:
                await interaction.followup.send('something went wrong opening the ticket, try again', ephemeral=True)
            except Exception:
                pass
            return
        if not cfg:
            return
        try:
            num       = await db.next_num(guild.id)
            tid       = f'{num:04d}'
            slug      = TIER_SLUG.get(self.tier, self.tier)
            ch_name   = f'ticket-{slug}-{tid}-{user.name}'
            category  = guild.get_channel(cfg['ticket_category_id'])
            role_id   = ROLES.get(self.tier)
            staff_r   = guild.get_role(ROLES['staff'])
            tier_r    = guild.get_role(role_id) if role_id else None

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
                ('trading with', trade['trader'],    False),
                ('giving',       trade['giving'],    True),
                ('receiving',    trade['receiving'], True),
            ]
            if trade['tip']:
                fields.append(('tip', trade['tip'], True))
            e = ticket_embed(user, self.tier, tid, fields)
            ping = user.mention
            if tier_r:
                ping += f' {tier_r.mention}'
            await channel.send(content=ping, embed=e, view=ControlView())
            await interaction.followup.send(f'done — {channel.mention}', ephemeral=True)
            await log(guild, 'ticket opened', f'{user.mention} opened a {TIER_LABEL[self.tier]} ticket', TIER_COLOR[self.tier])
        except Exception as ex:
            logger.error(f'middleman open: {ex}')
            await interaction.followup.send(f'something went wrong — {ex}', ephemeral=True)


# ------------------------------------------------ tier select

class TierSelect(Select):
    def __init__(self):
        super().__init__(
            placeholder='pick your trade value',
            options=[
                discord.SelectOption(label='Low Tier   100 – 900 RBX',   value='lowtier',  emoji='💎', description='fast, simple trades under 900 robux'),
                discord.SelectOption(label='Mid Tier   1K – 3K RBX',     value='midtier',  emoji='💰', description='trades between 1,000 and 3,000 robux'),
                discord.SelectOption(label='High Tier  3.1K – 5K+ RBX',  value='hightier', emoji='💸', description='3,100 robux and up — senior staff only'),
            ]
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(MiddlemanModal(self.values[0]))


# ------------------------------------------------ reward modal

class RewardModal(Modal, title='Claim a Reward'):
    def __init__(self, reward_type):
        super().__init__()
        self.rtype  = reward_type
        self.what   = TextInput(label='What are you claiming?',  placeholder='describe the reward',                    required=True)
        self.proof  = TextInput(label='Proof',                   placeholder='message link, screenshot link, etc.',    style=discord.TextStyle.paragraph, required=True)
        self.add_item(self.what)
        self.add_item(self.proof)

    async def on_submit(self, interaction: discord.Interaction):
        if not limiter.check(interaction.user.id, 'open', 10):
            return await interaction.response.send_message('slow down a bit', ephemeral=True)
        try:
            await interaction.response.defer(ephemeral=True)
        except Exception:
            return
        guild, user = interaction.guild, interaction.user
        try:
            cfg = await pre_open_checks(interaction, guild, user)
        except Exception as ex:
            logger.error(f'pre_open_checks error: {ex}')
            try:
                await interaction.followup.send('something went wrong opening the ticket, try again', ephemeral=True)
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
                ('claiming', self.what.value,  False),
                ('proof',    self.proof.value, False),
            ]
            e = ticket_embed(user, 'reward', tid, fields)
            ping = user.mention
            if staff_r:
                ping += f' {staff_r.mention}'
            await channel.send(content=ping, embed=e, view=ControlView())
            await interaction.followup.send(f'done — {channel.mention}', ephemeral=True)
        except Exception as ex:
            logger.error(f'reward open: {ex}')
            await interaction.followup.send(f'something went wrong — {ex}', ephemeral=True)


# ------------------------------------------------ reward type select

class RewardSelect(Select):
    def __init__(self):
        super().__init__(
            placeholder='what kind of reward?',
            custom_id='reward_select',
            options=[
                discord.SelectOption(label='Giveaway Prize',  value='giveaway', emoji='🎉', description='you won a giveaway'),
                discord.SelectOption(label='Invite Reward',   value='invite',   emoji='📨', description='invite milestone reward'),
                discord.SelectOption(label='Event Reward',    value='event',    emoji='🏆', description='reward from a server event'),
                discord.SelectOption(label='Other',           value='other',    emoji='🎁', description='something else'),
            ]
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(RewardModal(self.values[0]))


# ------------------------------------------------ ticket panel

class TicketPanel(View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Support', style=ButtonStyle.primary, emoji='🎫', custom_id='btn_support')
    async def support(self, interaction: discord.Interaction, _):
        if not limiter.check(interaction.user.id, 'open', 10):
            return await interaction.response.send_message('slow down a bit', ephemeral=True)
        try:
            await interaction.response.defer(ephemeral=True)
        except Exception:
            return
        guild, user = interaction.guild, interaction.user
        try:
            cfg = await pre_open_checks(interaction, guild, user)
        except Exception as ex:
            logger.error(f'pre_open_checks error: {ex}')
            try:
                await interaction.followup.send('something went wrong opening the ticket, try again', ephemeral=True)
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
            e = ticket_embed(user, 'support', tid)
            ping = user.mention
            if staff_r:
                ping += f' {staff_r.mention}'
            await channel.send(content=ping, embed=e, view=ControlView())
            await interaction.followup.send(f'done — {channel.mention}', ephemeral=True)
        except Exception as ex:
            logger.error(f'support open: {ex}')
            await interaction.followup.send(f'something went wrong — {ex}', ephemeral=True)

    @discord.ui.button(label='Middleman', style=ButtonStyle.success, emoji='⚖️', custom_id='btn_middleman')
    async def middleman(self, interaction: discord.Interaction, _):
        if tickets_locked.get(interaction.guild.id):
            return await interaction.response.send_message('tickets are closed right now', ephemeral=True)
        v = View(timeout=300)
        v.add_item(TierSelect())
        await interaction.response.send_message(
            embed=discord.Embed(description='pick the range that matches your trade value', color=TIER_COLOR['support']),
            view=v, ephemeral=True
        )


# ------------------------------------------------ reward panel

class RewardPanel(View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Claim Reward', style=ButtonStyle.success, emoji='🎁', custom_id='btn_reward')
    async def claim(self, interaction: discord.Interaction, _):
        if tickets_locked.get(interaction.guild.id):
            return await interaction.response.send_message('claims are closed right now', ephemeral=True)
        v = View(timeout=300)
        v.add_item(RewardSelect())
        await interaction.response.send_message(
            embed=discord.Embed(description='what are you here to claim?', color=TIER_COLOR['reward']),
            view=v, ephemeral=True
        )


# ------------------------------------------------ ticket control (claim / unclaim)

class ControlView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Claim', style=ButtonStyle.green, emoji='✋', custom_id='btn_claim')
    async def claim(self, interaction: discord.Interaction, _):
        if not limiter.check(interaction.user.id, 'claim', 2):
            return await interaction.response.send_message('slow down', ephemeral=True)
        if not _is_staff(interaction.user):
            return await interaction.response.send_message("you need a staff role to claim tickets", ephemeral=True)
        async with db.pool.acquire() as c:
            ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', interaction.channel.id)
            if not ticket:
                return await interaction.response.send_message('no ticket found here', ephemeral=True)
            if ticket['claimed_by']:
                who = interaction.guild.get_member(ticket['claimed_by'])
                return await interaction.response.send_message(
                    f"already claimed by {who.mention if who else 'someone'}", ephemeral=True
                )
            await c.execute(
                "UPDATE tickets SET claimed_by = $1, status = 'claimed' WHERE ticket_id = $2",
                interaction.user.id, ticket['ticket_id']
            )
        creator = interaction.guild.get_member(ticket['user_id'])
        await claim_lock(interaction.channel, interaction.user, creator, ticket['ticket_type'])
        e = discord.Embed(color=TIER_COLOR['support'])
        e.description = f'claimed by {interaction.user.mention}\n\nuse `$add @user` to let someone else talk in here'
        await interaction.response.send_message(embed=e)
        await log(interaction.guild, 'ticket claimed', f"{interaction.user.mention} claimed ticket {ticket['ticket_id']}", TIER_COLOR['support'])

    @discord.ui.button(label='Unclaim', style=ButtonStyle.gray, emoji='↩️', custom_id='btn_unclaim')
    async def unclaim(self, interaction: discord.Interaction, _):
        if not limiter.check(interaction.user.id, 'unclaim', 2):
            return await interaction.response.send_message('slow down', ephemeral=True)
        async with db.pool.acquire() as c:
            ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', interaction.channel.id)
            if not ticket:
                return await interaction.response.send_message('no ticket found here', ephemeral=True)
            if not ticket['claimed_by']:
                return await interaction.response.send_message("this ticket hasn't been claimed", ephemeral=True)
            is_admin = interaction.user.guild_permissions.administrator
            if ticket['claimed_by'] != interaction.user.id and not is_admin:
                return await interaction.response.send_message("you didn't claim this", ephemeral=True)
            await c.execute(
                "UPDATE tickets SET claimed_by = NULL, status = 'open' WHERE ticket_id = $1",
                ticket['ticket_id']
            )
        old = interaction.guild.get_member(ticket['claimed_by'])
        await claim_unlock(interaction.channel, old, ticket['ticket_type'])
        e = discord.Embed(color=TIER_COLOR['support'])
        e.description = 'unclaimed — any staff member can pick this up now'
        await interaction.response.send_message(embed=e)


# ------------------------------------------------------------------ close confirmation

class CloseConfirm(View):
    def __init__(self, ctx, ticket):
        super().__init__(timeout=30)
        self.ctx    = ctx
        self.ticket = ticket
        self.msg    = None

    @discord.ui.button(label='close it', style=ButtonStyle.red, emoji='🔒')
    async def confirm(self, interaction: discord.Interaction, _):
        if interaction.user.id != self.ctx.author.id:
            return await interaction.response.send_message("that's not yours to click", ephemeral=True)
        await interaction.response.defer()
        async with db.pool.acquire() as c:
            await c.execute("UPDATE tickets SET status = 'closed' WHERE ticket_id = $1", self.ticket['ticket_id'])
        e = discord.Embed(color=TIER_COLOR['hightier'])
        e.description = f'closing — transcript saved\nclosed by {interaction.user.mention}'
        await interaction.message.edit(embed=e, view=None)
        await send_transcript(interaction.channel, self.ticket, interaction.user)
        await asyncio.sleep(0.5)
        await interaction.channel.delete()

    @discord.ui.button(label='cancel', style=ButtonStyle.gray)
    async def cancel(self, interaction: discord.Interaction, _):
        if interaction.user.id != self.ctx.author.id:
            return await interaction.response.send_message("that's not yours to click", ephemeral=True)
        await interaction.response.defer()
        e = discord.Embed(color=TIER_COLOR['support'])
        e.description = 'close cancelled'
        await interaction.message.edit(embed=e, view=None)

    async def on_timeout(self):
        try:
            e = discord.Embed(color=TIER_COLOR['support'])
            e.description = 'close timed out'
            await self.msg.edit(embed=e, view=None)
        except Exception:
            pass


# ================================================================== commands

@bot.command(name='close')
@staff_only()
async def close_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply("this isn't a ticket channel")
    if not limiter.check(ctx.author.id, 'close', 3):
        return await ctx.reply('wait a moment')
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
    if not ticket:
        return await ctx.reply("can't find this ticket")
    is_admin = ctx.author.guild_permissions.administrator
    if not is_admin and (not ticket.get('claimed_by') or ticket['claimed_by'] != ctx.author.id):
        return await ctx.reply("only the person who claimed this can close it")
    e = discord.Embed(color=TIER_COLOR['hightier'])
    e.description = 'are you sure you want to close this? the channel will be deleted and a transcript saved'
    view = CloseConfirm(ctx, ticket)
    view.msg = await ctx.send(embed=e, view=view)


@bot.command(name='claim')
@staff_only()
async def claim_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply("this isn't a ticket channel")
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
        if not ticket:
            return await ctx.reply("no ticket found here")
        if ticket['claimed_by']:
            who = ctx.guild.get_member(ticket['claimed_by'])
            return await ctx.reply(f"already claimed by {who.mention if who else 'someone'}")
        if not await _can_manage(ctx, ticket):
            return await ctx.reply("you don't have the right role for this ticket type")
        await c.execute(
            "UPDATE tickets SET claimed_by = $1, status = 'claimed' WHERE ticket_id = $2",
            ctx.author.id, ticket['ticket_id']
        )
    creator = ctx.guild.get_member(ticket['user_id'])
    await claim_lock(ctx.channel, ctx.author, creator, ticket['ticket_type'])
    e = discord.Embed(color=TIER_COLOR['support'])
    e.description = f'claimed by {ctx.author.mention}\n\nuse `$add @user` to let someone else talk in here'
    await ctx.send(embed=e)


@bot.command(name='unclaim')
@staff_only()
async def unclaim_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply("this isn't a ticket channel")
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
        if not ticket:
            return await ctx.reply("no ticket found here")
        if not ticket['claimed_by']:
            return await ctx.reply("this ticket isn't claimed")
        if ticket['claimed_by'] != ctx.author.id and not ctx.author.guild_permissions.administrator:
            return await ctx.reply("you didn't claim this")
        await c.execute(
            "UPDATE tickets SET claimed_by = NULL, status = 'open' WHERE ticket_id = $1",
            ticket['ticket_id']
        )
    old = ctx.guild.get_member(ticket['claimed_by'])
    await claim_unlock(ctx.channel, old, ticket['ticket_type'])
    e = discord.Embed(color=TIER_COLOR['support'])
    e.description = 'unclaimed — any staff member can pick this up now'
    await ctx.send(embed=e)


@bot.command(name='add')
@staff_only()
async def add_cmd(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply('usage: `$add @user`')
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply("this isn't a ticket channel")
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
    if not ticket:
        return await ctx.reply("no ticket found here")
    if not ticket.get('claimed_by'):
        return await ctx.reply("claim the ticket first before adding someone")
    if ticket['claimed_by'] != ctx.author.id:
        return await ctx.reply("only the claimer can add people")
    await ctx.channel.set_permissions(member, read_messages=True, send_messages=True)
    await ctx.reply(f'{member.mention} can now talk in here')


@bot.command(name='remove')
@staff_only()
async def remove_cmd(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply('usage: `$remove @user`')
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply("this isn't a ticket channel")
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
    if not ticket:
        return await ctx.reply("no ticket found here")
    if not await _can_manage(ctx, ticket):
        return await ctx.reply("you don't have permission to do that here")
    await ctx.channel.set_permissions(member, overwrite=None)
    await ctx.reply(f'{member.mention} removed')


@bot.command(name='rename')
@staff_only()
async def rename_cmd(ctx, *, new_name: str = None):
    if not new_name:
        return await ctx.reply('usage: `$rename new name`')
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply("this isn't a ticket channel")
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
    if not ticket or not await _can_manage(ctx, ticket):
        return await ctx.reply("you can't rename this")
    safe = re.sub(r'[^a-z0-9\-]', '-', new_name.lower())
    await ctx.channel.edit(name=f'ticket-{safe}')
    await ctx.reply(f'renamed to `ticket-{safe}`')


@bot.command(name='transfer')
@staff_only()
async def transfer_cmd(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply('usage: `$transfer @user`')
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply("this isn't a ticket channel")
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
        if not ticket:
            return await ctx.reply("no ticket found here")
        if not ticket['claimed_by']:
            return await ctx.reply("nobody has claimed this yet")
        if ticket['claimed_by'] != ctx.author.id and not ctx.author.guild_permissions.administrator:
            return await ctx.reply("you didn't claim this")
        old = ctx.guild.get_member(ticket['claimed_by'])
        if old:
            await ctx.channel.set_permissions(old, read_messages=True, send_messages=False)
        await ctx.channel.set_permissions(member, read_messages=True, send_messages=True)
        await c.execute('UPDATE tickets SET claimed_by = $1 WHERE ticket_id = $2', member.id, ticket['ticket_id'])
    await ctx.send(f'transferred from {ctx.author.mention} to {member.mention}')


@bot.command(name='proof')
@staff_only()
async def proof_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply("this isn't a ticket channel")
    async with db.pool.acquire() as c:
        ticket = await c.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
    if not ticket:
        return await ctx.reply("no ticket found here")
    proof_ch = ctx.guild.get_channel(PROOF_CHANNEL)
    if not proof_ch:
        return await ctx.reply("proof channel not found")
    opener = ctx.guild.get_member(ticket['user_id'])
    e = discord.Embed(title='trade completed', color=TIER_COLOR.get(ticket.get('tier'), TIER_COLOR['support']))
    e.add_field(name='middleman', value=ctx.author.mention,                                inline=True)
    e.add_field(name='tier',      value=TIER_LABEL.get(ticket.get('tier'), 'unknown'),     inline=True)
    e.add_field(name='client',    value=opener.mention if opener else 'unknown',           inline=True)
    if ticket.get('trade_details'):
        try:
            d = ticket['trade_details'] if isinstance(ticket['trade_details'], dict) else json.loads(ticket['trade_details'])
            e.add_field(name='trading with', value=d.get('trader', '?'),    inline=False)
            e.add_field(name='gave',         value=d.get('giving', '?'),    inline=True)
            e.add_field(name='received',     value=d.get('receiving', '?'), inline=True)
            if d.get('tip'):
                e.add_field(name='tip', value=d['tip'], inline=True)
        except Exception:
            pass
    e.set_footer(text=f"ticket {ticket['ticket_id']}")
    await proof_ch.send(embed=e)
    await ctx.reply(f'proof posted to {proof_ch.mention}')


# ------------------------------------------------------------------ channel perms

PERM_ALIASES = {
    'send':          'send_messages',
    'read':          'read_messages',
    'view':          'read_messages',
    'react':         'add_reactions',
    'reactions':     'add_reactions',
    'attach':        'attach_files',
    'files':         'attach_files',
    'embed':         'embed_links',
    'embeds':        'embed_links',
    'history':       'read_message_history',
    'mentions':      'mention_everyone',
    'pin':           'manage_messages',
    'manage':        'manage_messages',
    'threads':       'create_public_threads',
    'voice':         'connect',
    'speak':         'speak',
    'stream':        'stream',
    'slash':         'use_application_commands',
    'commands':      'use_application_commands',
    'external':      'use_external_emojis',
    'emojis':        'use_external_emojis',
}

def resolve_perm(perm: str) -> str:
    p = perm.lower().replace('-', '_').replace(' ', '_')
    return PERM_ALIASES.get(p, p)

def resolve_toggle(toggle: str) -> bool | None:
    if toggle.lower() in ('enable', 'on', 'true', 'allow', '1'):
        return True
    if toggle.lower() in ('disable', 'off', 'false', 'deny', '0'):
        return False
    return None

async def resolve_target(ctx, raw: str):
    # try member mention/id
    try:
        return await commands.MemberConverter().convert(ctx, raw)
    except Exception:
        pass
    # try role mention/id/name
    try:
        return await commands.RoleConverter().convert(ctx, raw)
    except Exception:
        pass
    # try @everyone
    if raw.lower() in ('everyone', '@everyone'):
        return ctx.guild.default_role
    return None


@bot.command(name='channelperm')
@staff_only()
async def channelperm_cmd(ctx, channel: discord.TextChannel = None, target: str = None, perm: str = None, toggle: str = None):
    if not channel or not target or not perm or not toggle:
        return await ctx.reply(
            'usage: `$channelperm #channel @user/role/everyone <permission> <enable/disable>`\n'
            'example: `$channelperm #general @members send enable`'
        )
    resolved_target = await resolve_target(ctx, target)
    if not resolved_target:
        return await ctx.reply(f"couldn't find `{target}`")
    resolved_perm = resolve_perm(perm)
    resolved_toggle = resolve_toggle(toggle)
    if resolved_toggle is None:
        return await ctx.reply('toggle must be `enable` or `disable`')
    ow = channel.overwrites_for(resolved_target)
    try:
        setattr(ow, resolved_perm, resolved_toggle)
    except AttributeError:
        return await ctx.reply(f"`{perm}` isn't a valid permission")
    await channel.set_permissions(resolved_target, overwrite=ow)
    action = 'enabled' if resolved_toggle else 'disabled'
    name = resolved_target.name if hasattr(resolved_target, 'name') else str(resolved_target)
    await ctx.reply(f'`{resolved_perm}` {action} for **{name}** in {channel.mention}')


@bot.command(name='channelpermall')
@staff_only()
async def channelpermall_cmd(ctx, target: str = None, perm: str = None, toggle: str = None):
    if not target or not perm or not toggle:
        return await ctx.reply(
            'usage: `$channelpermall @user/role/everyone <permission> <enable/disable>`\n'
            'example: `$channelpermall @members send disable`'
        )
    resolved_target = await resolve_target(ctx, target)
    if not resolved_target:
        return await ctx.reply(f"couldn't find `{target}`")
    resolved_perm = resolve_perm(perm)
    resolved_toggle = resolve_toggle(toggle)
    if resolved_toggle is None:
        return await ctx.reply('toggle must be `enable` or `disable`')

    channels = [c for c in ctx.guild.channels if isinstance(c, (discord.TextChannel, discord.VoiceChannel))]
    action   = 'enabled' if resolved_toggle else 'disabled'
    name     = resolved_target.name if hasattr(resolved_target, 'name') else str(resolved_target)

    msg = await ctx.reply(f'updating `{resolved_perm}` for **{name}** across {len(channels)} channels...')
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
        result += f' ({failed} failed — likely missing permissions)'
    await msg.edit(content=result)


# ------------------------------------------------------------------ owner commands

@bot.command(name='setup')
@owner_only()
async def setup_cmd(ctx):
    e = discord.Embed(color=TIER_COLOR['support'])
    e.set_author(name="Trial's Cross Trade  —  Middleman Service")
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
    e.set_author(name="Trial's Cross Trade  —  Reward Claims")
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
        return await ctx.reply('usage: `$setcategory #category`')
    async with db.pool.acquire() as c:
        await c.execute(
            'INSERT INTO config (guild_id, ticket_category_id) VALUES ($1,$2) ON CONFLICT (guild_id) DO UPDATE SET ticket_category_id = $2',
            ctx.guild.id, category.id
        )
    await ctx.reply(f'ticket category set to {category.mention}')


@bot.command(name='setlogs')
@owner_only()
async def setlogs_cmd(ctx, channel: discord.TextChannel = None):
    if not channel:
        return await ctx.reply('usage: `$setlogs #channel`')
    async with db.pool.acquire() as c:
        await c.execute(
            'INSERT INTO config (guild_id, log_channel_id) VALUES ($1,$2) ON CONFLICT (guild_id) DO UPDATE SET log_channel_id = $2',
            ctx.guild.id, channel.id
        )
    await ctx.reply(f'log channel set to {channel.mention}')


@bot.command(name='config')
@owner_only()
async def config_cmd(ctx):
    async with db.pool.acquire() as c:
        cfg = await c.fetchrow('SELECT * FROM config WHERE guild_id = $1', ctx.guild.id)
    e = discord.Embed(title='config', color=TIER_COLOR['support'])
    if cfg:
        cat   = ctx.guild.get_channel(cfg['ticket_category_id']) if cfg.get('ticket_category_id') else None
        logs  = ctx.guild.get_channel(cfg['log_channel_id'])     if cfg.get('log_channel_id')     else None
        proof = ctx.guild.get_channel(PROOF_CHANNEL)
        e.add_field(name='category',    value=cat.mention   if cat   else 'not set', inline=True)
        e.add_field(name='logs',        value=logs.mention  if logs  else 'not set', inline=True)
        e.add_field(name='proof',       value=proof.mention if proof else 'not set', inline=True)
        e.add_field(name='tickets made',value=str(cfg.get('ticket_counter', 0)),     inline=True)
        e.add_field(name='status',      value='locked' if tickets_locked.get(ctx.guild.id) else 'open', inline=True)
    else:
        e.description = 'nothing configured yet — run `$setcategory` and `$setlogs` to get started'
    await ctx.reply(embed=e)


@bot.command(name='lock')
@owner_only()
async def lock_cmd(ctx):
    tickets_locked[ctx.guild.id] = True
    await ctx.reply('tickets are locked — nobody can open new ones until you run `$unlock`')


@bot.command(name='unlock')
@owner_only()
async def unlock_cmd(ctx):
    tickets_locked[ctx.guild.id] = False
    await ctx.reply('tickets are open again')


@bot.command(name='blacklist')
@owner_only()
async def blacklist_cmd(ctx, member: discord.Member = None, *, reason: str = 'no reason given'):
    if not member:
        return await ctx.reply('usage: `$blacklist @user reason`')
    async with db.pool.acquire() as c:
        await c.execute(
            'INSERT INTO blacklist (user_id, guild_id, reason, blacklisted_by) VALUES ($1,$2,$3,$4) ON CONFLICT (user_id, guild_id) DO UPDATE SET reason = $3, blacklisted_by = $4, created_at = NOW()',
            member.id, ctx.guild.id, reason, ctx.author.id
        )
    await ctx.reply(f'{member.mention} blacklisted — {reason}')


@bot.command(name='unblacklist')
@owner_only()
async def unblacklist_cmd(ctx, member: discord.Member = None):
    if not member:
        return await ctx.reply('usage: `$unblacklist @user`')
    async with db.pool.acquire() as c:
        await c.execute('DELETE FROM blacklist WHERE user_id = $1 AND guild_id = $2', member.id, ctx.guild.id)
    await ctx.reply(f'{member.mention} removed from the blacklist')


@bot.command(name='blacklists')
@owner_only()
async def blacklists_cmd(ctx):
    async with db.pool.acquire() as c:
        rows = await c.fetch('SELECT * FROM blacklist WHERE guild_id = $1 ORDER BY created_at DESC', ctx.guild.id)
    if not rows:
        return await ctx.reply('nobody is blacklisted')
    lines = []
    for r in rows:
        m    = ctx.guild.get_member(r['user_id'])
        by   = ctx.guild.get_member(r['blacklisted_by'])
        date = r['created_at'].strftime('%b %d') if r.get('created_at') else '?'
        name = m.display_name if m else str(r['user_id'])
        lines.append(f"{name} — {r['reason']}  (by {by.display_name if by else '?'} on {date})")
    e = discord.Embed(title=f"blacklist  {len(rows)}", description='\n'.join(lines), color=TIER_COLOR['hightier'])
    await ctx.reply(embed=e)


@bot.command(name='help')
async def help_cmd(ctx):
    e = discord.Embed(title='commands', color=TIER_COLOR['support'])
    e.add_field(
        name='tickets   staff and middleman roles',
        value=(
            '`$claim`              claim a ticket\n'
            '`$unclaim`            drop your claim\n'
            '`$add @user`          let someone talk\n'
            '`$remove @user`       remove someone\n'
            '`$close`              close and save transcript\n'
            '`$rename name`        rename the channel\n'
            '`$transfer @user`     hand off your claim\n'
            '`$proof`              post trade completion proof'
        ),
        inline=False
    )
    e.add_field(
        name='giveaways   staff only',
        value=(
            '`$gstart <time> <Nw> <prize>`   start a giveaway\n'
            '`$gend <message id>`             end early\n'
            '`$greroll <message id>`          reroll winners'
        ),
        inline=False
    )
    e.add_field(
        name='setup   owner only',
        value=(
            '`$setup`              post the ticket panel\n'
            '`$setuprewards`       post the reward claim panel\n'
            '`$setupverify`        post the verify panel\n'
            '`$setverify`          configure verification roles\n'
            '`$setcategory`        set ticket category\n'
            '`$setlogs`            set log channel\n'
            '`$config`             view current config\n'
            '`$lock` / `$unlock`   open or close tickets\n'
            '`$blacklist @user`    block someone from tickets\n'
            '`$unblacklist @user`  remove block\n'
            '`$blacklists`         view all blocked users'
        ),
        inline=False
    )
    await ctx.reply(embed=e)


# ================================================================== GIVEAWAYS

giveaways: dict = {}

def parse_duration(s: str) -> int:
    total = 0
    for val, unit in re.findall(r'(\d+)([smhd])', s.lower()):
        val = int(val)
        if unit == 's': total += val
        elif unit == 'm': total += val * 60
        elif unit == 'h': total += val * 3600
        elif unit == 'd': total += val * 86400
    return total

def format_ends(seconds: int) -> str:
    ends_at = datetime.now(timezone.utc).timestamp() + seconds
    return f'<t:{int(ends_at)}:R>'

def format_duration(seconds: int) -> str:
    if seconds < 60:    return f'{seconds}s'
    if seconds < 3600:  return f'{seconds // 60}m'
    if seconds < 86400: return f'{seconds // 3600}h'
    return f'{seconds // 86400}d'

def build_giveaway_embed(gw: dict) -> discord.Embed:
    e = discord.Embed(title=gw['prize'], color=0xF1C40F)
    e.description = (
        f"react with 🎉 to enter\n\n"
        f"ends: <t:{int(gw['ends_at'])}:R>  (<t:{int(gw['ends_at'])}:f>)\n"
        f"hosted by <@{gw['host_id']}>"
    )
    e.add_field(name='Winners',  value=str(gw['winner_count']), inline=True)
    e.add_field(name='Entries',  value=str(len(gw['entries'])), inline=True)
    if gw.get('image_url'):
        e.set_image(url=gw['image_url'])
    e.set_footer(text=f"giveaway  •  {gw['guild_name']}")
    e.timestamp = datetime.fromtimestamp(gw['ends_at'], tz=timezone.utc)
    return e


# ---- slash command group

giveaway_group = discord.app_commands.Group(name='giveaway', description='giveaway commands')


@giveaway_group.command(name='start', description='start a giveaway')
@discord.app_commands.describe(
    prize    = 'what are you giving away?',
    duration = 'how long e.g. 1h 30m 2d (default 1h)',
    winners  = 'how many winners (default 1)',
    image    = 'optional image for the giveaway',
)
async def giveaway_start_slash(
    interaction: discord.Interaction,
    prize:    str,
    duration: str = '1h',
    winners:  int = 1,
    image:    discord.Attachment = None,
):
    if not _is_staff(interaction.user):
        return await interaction.response.send_message("you need a staff role to start giveaways", ephemeral=True)
    seconds = parse_duration(duration)
    if seconds <= 0:
        return await interaction.response.send_message('invalid duration — try `1h`, `30m`, `2d`', ephemeral=True)
    if winners < 1:
        return await interaction.response.send_message('winners must be at least 1', ephemeral=True)

    ends_at   = datetime.now(timezone.utc).timestamp() + seconds
    image_url = image.url if image else None

    gw = {
        'prize':        prize,
        'winner_count': winners,
        'ends_at':      ends_at,
        'host_id':      interaction.user.id,
        'guild_id':     interaction.guild.id,
        'guild_name':   interaction.guild.name,
        'channel_id':   interaction.channel.id,
        'entries':      set(),
        'ended':        False,
        'image_url':    image_url,
    }

    embed = build_giveaway_embed(gw)
    await interaction.response.send_message(embed=embed)
    msg = await interaction.original_response()
    await msg.add_reaction('🎉')
    gw['message_id'] = msg.id
    giveaways[msg.id] = gw
    bot.loop.create_task(_giveaway_timer(msg.id, interaction.channel.id, seconds))


@giveaway_group.command(name='end', description='end a giveaway early')
@discord.app_commands.describe(message_id='the message ID of the giveaway')
async def giveaway_end_slash(interaction: discord.Interaction, message_id: str):
    if not _is_staff(interaction.user):
        return await interaction.response.send_message("you need a staff role", ephemeral=True)
    gw = giveaways.get(int(message_id))
    if not gw:
        return await interaction.response.send_message("couldn't find that giveaway", ephemeral=True)
    await interaction.response.send_message('ending giveaway...', ephemeral=True)
    await _end_giveaway(int(message_id))


@giveaway_group.command(name='reroll', description='reroll winners of an ended giveaway')
@discord.app_commands.describe(message_id='the message ID of the giveaway')
async def giveaway_reroll_slash(interaction: discord.Interaction, message_id: str):
    if not _is_staff(interaction.user):
        return await interaction.response.send_message("you need a staff role", ephemeral=True)
    gw = giveaways.get(int(message_id))
    if not gw or not gw.get('ended'):
        return await interaction.response.send_message("that giveaway hasn't ended or doesn't exist", ephemeral=True)
    entries = list(gw['entries'])
    if not entries:
        return await interaction.response.send_message('no entries to reroll', ephemeral=True)
    count   = min(gw['winner_count'], len(entries))
    winners = random.sample(entries, count)
    mentions = ', '.join(f'<@{w}>' for w in winners)
    await interaction.response.send_message(f'🎉 rerolled! new winner(s): {mentions} — congrats!')


# ---- prefix commands

@bot.command(name='gstart')
@staff_only()
async def gstart_cmd(ctx, duration: str = '1h', winners: str = '1w', *, prize: str = None):
    if not prize:
        return await ctx.reply('usage: `$gstart <time> <Nw> <prize>`\nexample: `$gstart 1h 1w 500 Robux`')
    if not winners.endswith('w') or not winners[:-1].isdigit():
        return await ctx.reply('winners format: `1w`, `2w`, `3w`')
    seconds      = parse_duration(duration)
    winner_count = int(winners[:-1])
    if seconds <= 0:
        return await ctx.reply('invalid duration — try `1h`, `30m`, `2d`')

    image_url = None
    if ctx.message.attachments:
        image_url = ctx.message.attachments[0].url

    ends_at = datetime.now(timezone.utc).timestamp() + seconds
    gw = {
        'prize':        prize,
        'winner_count': winner_count,
        'ends_at':      ends_at,
        'host_id':      ctx.author.id,
        'guild_id':     ctx.guild.id,
        'guild_name':   ctx.guild.name,
        'channel_id':   ctx.channel.id,
        'entries':      set(),
        'ended':        False,
        'image_url':    image_url,
    }

    try:
        await ctx.message.delete()
    except Exception:
        pass

    embed = build_giveaway_embed(gw)
    msg   = await ctx.send(embed=embed)
    await msg.add_reaction('🎉')
    gw['message_id'] = msg.id
    giveaways[msg.id] = gw
    bot.loop.create_task(_giveaway_timer(msg.id, ctx.channel.id, seconds))


@bot.command(name='gend')
@staff_only()
async def gend_cmd(ctx, message_id: int = None):
    if not message_id:
        return await ctx.reply('usage: `$gend <message id>`')
    if not giveaways.get(message_id):
        return await ctx.reply("couldn't find that giveaway")
    await _end_giveaway(message_id)
    try: await ctx.message.delete()
    except: pass


@bot.command(name='greroll')
@staff_only()
async def greroll_cmd(ctx, message_id: int = None):
    if not message_id:
        return await ctx.reply('usage: `$greroll <message id>`')
    gw = giveaways.get(message_id)
    if not gw or not gw.get('ended'):
        return await ctx.reply("that giveaway hasn't ended or doesn't exist")
    entries = list(gw['entries'])
    if not entries:
        return await ctx.reply('no entries to reroll')
    count   = min(gw['winner_count'], len(entries))
    winners = random.sample(entries, count)
    mentions = ', '.join(f'<@{w}>' for w in winners)
    await ctx.send(f'🎉 rerolled! new winner(s): {mentions} — congrats!')


# ---- reaction handling

@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if payload.user_id == bot.user.id:
        return
    if str(payload.emoji) != '🎉':
        return
    gw = giveaways.get(payload.message_id)
    if not gw or gw.get('ended'):
        return
    gw['entries'].add(payload.user_id)
    await _update_giveaway_embed(payload.message_id)


@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    if payload.user_id == bot.user.id:
        return
    if str(payload.emoji) != '🎉':
        return
    gw = giveaways.get(payload.message_id)
    if not gw or gw.get('ended'):
        return
    gw['entries'].discard(payload.user_id)
    await _update_giveaway_embed(payload.message_id)


async def _update_giveaway_embed(message_id: int):
    gw = giveaways.get(message_id)
    if not gw:
        return
    try:
        ch  = bot.get_channel(gw['channel_id'])
        msg = await ch.fetch_message(message_id)
        embed = build_giveaway_embed(gw)
        await msg.edit(embed=embed)
    except Exception:
        pass


async def _giveaway_timer(message_id: int, channel_id: int, delay: int):
    await asyncio.sleep(delay)
    await _end_giveaway(message_id)


async def _end_giveaway(message_id: int):
    gw = giveaways.get(message_id)
    if not gw or gw.get('ended'):
        return
    gw['ended'] = True

    channel = bot.get_channel(gw['channel_id'])
    if not channel:
        return

    # remove bot entries from reaction list
    entries = list(gw['entries'] - {bot.user.id})
    count   = min(gw['winner_count'], len(entries))

    if entries and count > 0:
        winners  = random.sample(entries, count)
        mentions = ', '.join(f'<@{w}>' for w in winners)
    else:
        winners  = []
        mentions = 'nobody'

    try:
        msg = await channel.fetch_message(message_id)
        e   = discord.Embed(title=gw['prize'], color=0x5865F2)
        e.description = (
            f"giveaway ended\n\n"
            f"winner(s): {mentions}\n"
            f"hosted by <@{gw['host_id']}>"
        )
        e.add_field(name='Winners', value=str(gw['winner_count']), inline=True)
        e.add_field(name='Entries', value=str(len(entries)),        inline=True)
        if gw.get('image_url'):
            e.set_image(url=gw['image_url'])
        e.set_footer(text=f"ended  •  {gw['guild_name']}")
        e.timestamp = datetime.now(timezone.utc)
        await msg.edit(embed=e)
    except Exception:
        pass

    if winners:
        await channel.send(
            f"🎉 giveaway ended! congrats to {mentions} for winning **{gw['prize']}**!\n"
            f"use `$greroll {message_id}` to reroll"
        )
    else:
        await channel.send(f"giveaway for **{gw['prize']}** ended with no valid entries")



# ================================================================== VERIFICATION

def gen_captcha(length=6) -> str:
    chars = string.ascii_uppercase + string.digits
    chars = chars.replace('O', '').replace('0', '').replace('I', '').replace('1', '')
    return ''.join(random.choices(chars, k=length))


class VerifyView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Verify', style=ButtonStyle.green, emoji='✅', custom_id='btn_verify')
    async def verify(self, interaction: discord.Interaction, _):
        user = interaction.user
        cfg  = verify_config.get(interaction.guild.id)

        if not cfg:
            return await interaction.response.send_message(
                "verification isn't set up yet, ping a staff member", ephemeral=True
            )

        verified_role = interaction.guild.get_role(cfg['verified'])
        if verified_role and verified_role in user.roles:
            return await interaction.response.send_message(
                "you're already verified", ephemeral=True
            )

        code = gen_captcha()
        captchas[user.id] = code

        await interaction.response.send_message(
            f"your code is **`{code}`**\n\ntype it in this channel to verify",
            ephemeral=True
        )


@bot.event
async def on_member_join(member: discord.Member):
    cfg = verify_config.get(member.guild.id)
    if not cfg:
        return
    role = member.guild.get_role(cfg['unverified'])
    if role:
        try:
            await member.add_roles(role, reason='joined — awaiting verification')
        except Exception as ex:
            logger.error(f'on_member_join: {ex}')


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        await bot.process_commands(message)
        return

    # captcha check — only if user has a pending code
    user_id = message.author.id
    if user_id in captchas and message.guild:
        cfg = verify_config.get(message.guild.id)
        if cfg and message.channel.id == cfg.get('channel'):
            code = captchas[user_id]
            if message.content.strip().upper() == code:
                # correct
                try:
                    unverified = message.guild.get_role(cfg['unverified'])
                    verified   = message.guild.get_role(cfg['verified'])
                    member_r   = message.guild.get_role(cfg['member'])
                    if unverified and unverified in message.author.roles:
                        await message.author.remove_roles(unverified, reason='verified')
                    if verified:
                        await message.author.add_roles(verified, reason='verified')
                    if member_r:
                        await message.author.add_roles(member_r, reason='verified')
                    captchas.pop(user_id, None)
                    try:
                        await message.delete()
                    except Exception:
                        pass
                    confirm = await message.channel.send(
                        f"{message.author.mention} you're verified, welcome to the server!"
                    )
                    await asyncio.sleep(3)
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
                wrong_msg = await message.channel.send(
                    f"{message.author.mention} wrong code — your new code is **`{new_code}`**"
                )
                await asyncio.sleep(6)
                try:
                    await wrong_msg.delete()
                except Exception:
                    pass
            return

    await bot.process_commands(message)


@bot.command(name='setverify')
@owner_only()
async def setverify_cmd(ctx,
    unverified: discord.Role = None,
    verified:   discord.Role = None,
    member:     discord.Role = None,
    channel:    discord.TextChannel = None
):
    if not all([unverified, verified, member, channel]):
        return await ctx.reply('usage: `$setverify @unverified @verified @member #channel`')
    verify_config[ctx.guild.id] = {
        'unverified': unverified.id,
        'verified':   verified.id,
        'member':     member.id,
        'channel':    channel.id,
    }
    async with db.pool.acquire() as c:
        await c.execute(
            '''INSERT INTO config (guild_id, verify_unverified_id, verify_verified_id, verify_member_id, verify_channel_id)
               VALUES ($1,$2,$3,$4,$5)
               ON CONFLICT (guild_id) DO UPDATE SET
               verify_unverified_id=$2, verify_verified_id=$3, verify_member_id=$4, verify_channel_id=$5''',
            ctx.guild.id, unverified.id, verified.id, member.id, channel.id
        )
    await ctx.reply(
        f'verification set up\n'
        f'unverified role: {unverified.mention}\n'
        f'verified role: {verified.mention}\n'
        f'member role: {member.mention}\n'
        f'channel: {channel.mention}'
    )


@bot.command(name='setupverify')
@owner_only()
async def setupverify_cmd(ctx):
    e = discord.Embed(color=0x57F287)
    e.set_author(name="Trial's Cross Trade  —  Verification")
    e.description = (
        "Welcome to **Trial's Cross Trade!**\n\n"
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


# ================================================================== EVENTS

@bot.event
async def on_ready():
    logger.info(f'logged in as {bot.user}')
    try:
        await db.connect()
    except Exception as ex:
        logger.error(f'db failed: {ex}')
        return
    bot.add_view(TicketPanel())
    bot.add_view(ControlView())
    bot.add_view(RewardPanel())
    bot.add_view(VerifyView())
    bot.tree.add_command(giveaway_group)
    try:
        synced = await bot.tree.sync()
        logger.info(f'synced {len(synced)} slash command(s)')
    except Exception as ex:
        logger.error(f'slash sync failed: {ex}')
    # restore verify_config from DB
    try:
        async with db.pool.acquire() as c:
            rows = await c.fetch(
                'SELECT guild_id, verify_unverified_id, verify_verified_id, verify_member_id, verify_channel_id FROM config WHERE verify_channel_id IS NOT NULL'
            )
        for row in rows:
            verify_config[row['guild_id']] = {
                'unverified': row['verify_unverified_id'],
                'verified':   row['verify_verified_id'],
                'member':     row['verify_member_id'],
                'channel':    row['verify_channel_id'],
            }
        logger.info(f'loaded verify config for {len(rows)} guild(s)')
    except Exception as ex:
        logger.error(f'verify config load: {ex}')
    await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name='tickets'))
    logger.info(f'ready   {len(bot.guilds)} server(s)')


@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return
    if isinstance(error, commands.CheckFailure):
        await ctx.reply("you don't have permission to do that")
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.reply(f'missing argument: `{error.param.name}`')
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
