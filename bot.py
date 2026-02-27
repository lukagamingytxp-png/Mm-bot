# ============================================================
# Discord Bot - Full Rewrite
# Render + PostgreSQL + UptimeRobot compatible (free tier)
# ============================================================
# ENV VARS REQUIRED:
#   BOT_TOKEN      - your Discord bot token
#   DATABASE_URL   - PostgreSQL connection string (Render gives postgres://...)
#   PORT           - auto-set by Render (defaults to 8080)
# ============================================================

import asyncio
import io
import json
import logging
import os
import random
import re
import string
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional

import asyncpg
import discord
from aiohttp import web
from discord.ext import commands
from discord.ui import Button, Modal, Select, TextInput, View

# ==================== LOGGING ====================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger('Bot')

# ==================== CONSTANTS ====================

OWNER_ID = 1029438856069656576

HARDCODED_ROLES = {
    'lowtier':  1453757017218093239,
    'midtier':  1434610759140118640,
    'hightier': 1453757157144137911,
    'staff':    1432081794647199895,
    'jailed':   1468620489613377628,
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
    'warning':  0xFEE75C,
}

PERM_MAP = {
    'send messages':             'send_messages',
    'read messages':             'read_messages',
    'view channel':              'view_channel',
    'embed links':               'embed_links',
    'attach files':              'attach_files',
    'add reactions':             'add_reactions',
    'use external emojis':       'use_external_emojis',
    'use external stickers':     'use_external_stickers',
    'mention everyone':          'mention_everyone',
    'manage messages':           'manage_messages',
    'read message history':      'read_message_history',
    'send tts messages':         'send_tts_messages',
    'use application commands':  'use_application_commands',
    'manage threads':            'manage_threads',
    'create public threads':     'create_public_threads',
    'create private threads':    'create_private_threads',
    'send messages in threads':  'send_messages_in_threads',
    'connect':                   'connect',
    'speak':                     'speak',
    'stream':                    'stream',
    'use voice activation':      'use_voice_activation',
    'priority speaker':          'priority_speaker',
    'mute members':              'mute_members',
    'deafen members':            'deafen_members',
    'move members':              'move_members',
}

# ==================== IN-MEMORY STATE ====================

afk_users: dict       = {}
snipe_data: dict      = {}
edit_snipe_data: dict = {}
mod_perms: dict       = {}
admin_perms: dict     = {}
bot_start_time        = datetime.now(timezone.utc)

# ==================== RATE LIMITER ====================

class RateLimiter:
    def __init__(self):
        self._cooldowns: dict[str, float] = {}

    def check(self, user_id: int, command: str, seconds: int = 3) -> bool:
        key = f'{user_id}:{command}'
        now = datetime.now(timezone.utc).timestamp()
        if now - self._cooldowns.get(key, 0) < seconds:
            return False
        self._cooldowns[key] = now
        return True

rate_limiter = RateLimiter()

# ==================== HELPER: CHECK WHITELISTED ====================

def _check_whitelist(member: discord.Member, roles_store: dict, users_store: dict) -> bool:
    """Generic whitelist check — checks both role whitelist and user whitelist."""
    if not member:
        return False
    guild_id = member.guild.id
    if member.id in users_store.get(guild_id, []):
        return True
    role_ids = users_store.get(guild_id, [])  # kept for clarity, handled above
    for role in member.roles:
        if role.id in roles_store.get(guild_id, []):
            return True
    return False

# ==================== ANTI-LINK ====================

class AntiLink:
    _URL_PATTERN = re.compile(
        r'https?://|discord\.gg/|\.com\b|\.net\b|\.org\b|\.gg\b|\.io\b|\.xyz\b|\.ru\b|\.tk\b'
    )
    _INVITE_PATTERN = re.compile(
        r'(discord\.gg/|discord\.com/invite/|discordapp\.com/invite/)\S+'
    )

    def __init__(self):
        self.enabled:          dict = {}
        self.whitelisted_roles: dict = defaultdict(list)
        self.whitelisted_users: dict = defaultdict(list)
        self.whitelisted_urls:  dict = defaultdict(list)

    def is_link(self, content: str) -> bool:
        return bool(self._URL_PATTERN.search(content))

    def is_invite(self, content: str) -> bool:
        return bool(self._INVITE_PATTERN.search(content))

    def is_url_whitelisted(self, guild_id: int, content: str) -> bool:
        return any(u.lower() in content.lower() for u in self.whitelisted_urls.get(guild_id, []))

    def is_whitelisted(self, member: discord.Member) -> bool:
        return _check_whitelist(member, self.whitelisted_roles, self.whitelisted_users)

anti_link = AntiLink()

# ==================== ANTI-NUKE ====================

class AntiNuke:
    def __init__(self):
        self.enabled:           dict = {}
        self.whitelisted_roles: dict = defaultdict(list)
        self.whitelisted_users: dict = defaultdict(list)
        self.channel_deletes:   dict = defaultdict(list)  # (guild_id, user_id) -> [timestamps]
        self.bot_start_time          = datetime.now(timezone.utc)

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _to_utc(self, ts: datetime) -> datetime:
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)

    def is_recent(self, ts: datetime, window: float = 5.0) -> bool:
        return (self._now() - self._to_utc(ts)).total_seconds() < window

    def is_after_restart(self, ts: datetime) -> bool:
        return self._to_utc(ts) > self.bot_start_time

    def add_channel_delete(self, guild_id: int, user_id: int) -> int:
        """Track a channel delete and return total count in last 60s."""
        now = self._now()
        key = (guild_id, user_id)
        self.channel_deletes[key].append(now)
        self.channel_deletes[key] = [t for t in self.channel_deletes[key] if (now - t).total_seconds() < 60]
        return len(self.channel_deletes[key])

    def is_whitelisted(self, member: Optional[discord.Member]) -> bool:
        if not member:
            return False
        return _check_whitelist(member, self.whitelisted_roles, self.whitelisted_users)

    def audit_valid(self, entry, target_id: int) -> bool:
        if not self.is_recent(entry.created_at):
            return False
        if not self.is_after_restart(entry.created_at):
            return False
        if hasattr(entry.target, 'id') and entry.target.id != target_id:
            return False
        return True

anti_nuke = AntiNuke()


class AntiRaid:
    def __init__(self):
        self.enabled:          dict = {}
        self.join_tracking:    dict = defaultdict(list)
        self.whitelisted_roles: dict = defaultdict(list)
        self.whitelisted_users: dict = defaultdict(list)
        self.settings:         dict = defaultdict(lambda: {
            'max_joins':       10,
            'time_window':     10,
            'min_account_age': 7,
            'require_avatar':  True,
            'action':          'kick',
        })

    def add_join(self, guild_id: int, user_id: int):
        now = datetime.now(timezone.utc)
        self.join_tracking[guild_id].append((user_id, now))
        self.join_tracking[guild_id] = [
            (uid, t) for uid, t in self.join_tracking[guild_id]
            if (now - t).total_seconds() < 60
        ]

    def is_raid(self, guild_id: int) -> bool:
        if not self.enabled.get(guild_id):
            return False
        s = self.settings[guild_id]
        now = datetime.now(timezone.utc)
        recent = [(uid, t) for uid, t in self.join_tracking[guild_id]
                  if (now - t).total_seconds() < s['time_window']]
        return len(recent) >= s['max_joins']

    def check_member(self, member: discord.Member) -> tuple[bool, list]:
        guild_id = member.guild.id
        if not self.enabled.get(guild_id):
            return False, []
        s = self.settings[guild_id]
        reasons = []
        account_age = (datetime.now(timezone.utc) - member.created_at).days
        if account_age < s['min_account_age']:
            reasons.append(f'account only {account_age}d old')
        if s['require_avatar'] and member.avatar is None:
            reasons.append('no avatar')
        return bool(reasons), reasons

    def is_whitelisted(self, member: discord.Member) -> bool:
        return _check_whitelist(member, self.whitelisted_roles, self.whitelisted_users)

anti_raid = AntiRaid()

# ==================== ANTI-SPAM ====================

class AntiSpam:
    def __init__(self):
        self.enabled:           dict = {}
        self.messages:          dict = defaultdict(list)
        self.whitelisted_roles: dict = defaultdict(list)
        self.whitelisted_users: dict = defaultdict(list)
        self.settings:          dict = defaultdict(lambda: {
            'max_messages': 7,
            'time_window':  10,
            'max_mentions': 5,
            'max_emojis':   10,
            'action':       'timeout',
        })

    def add_message(self, guild_id: int, user_id: int):
        now = datetime.now(timezone.utc)
        key = (guild_id, user_id)
        self.messages[key].append(now)
        self.messages[key] = [t for t in self.messages[key] if (now - t).total_seconds() < 30]

    def is_spam(self, guild_id: int, user_id: int) -> bool:
        if not self.enabled.get(guild_id):
            return False
        s = self.settings[guild_id]
        now = datetime.now(timezone.utc)
        recent = [t for t in self.messages.get((guild_id, user_id), [])
                  if (now - t).total_seconds() < s['time_window']]
        return len(recent) >= s['max_messages']

    def get_timeout_duration(self, guild_id: int, user_id: int) -> timedelta:
        count = len(self.messages.get((guild_id, user_id), []))
        if count >= 20: return timedelta(hours=1)
        if count >= 15: return timedelta(minutes=30)
        if count >= 12: return timedelta(minutes=15)
        if count >= 10: return timedelta(minutes=10)
        return timedelta(minutes=5)

    def check_message(self, message: discord.Message) -> tuple[bool, list]:
        guild_id = message.guild.id
        if not self.enabled.get(guild_id):
            return False, []
        s = self.settings[guild_id]
        reasons = []
        mention_count = len(message.mentions) + len(message.role_mentions)
        if mention_count > s['max_mentions']:
            reasons.append(f'{mention_count} mentions')
        return bool(reasons), reasons

    def is_whitelisted(self, member: discord.Member) -> bool:
        return _check_whitelist(member, self.whitelisted_roles, self.whitelisted_users)

anti_spam = AntiSpam()

# ==================== ANTI-REACT ====================

class AntiReact:
    def __init__(self):
        self.enabled:           dict = {}
        self.reactions:         dict = defaultdict(list)
        self.whitelisted_roles: dict = defaultdict(list)
        self.whitelisted_users: dict = defaultdict(list)
        self.settings:          dict = defaultdict(lambda: {
            'max_reacts':  10,
            'time_window': 10,
            'action':      'timeout',
        })

    def add_reaction(self, guild_id: int, user_id: int, message_id: int):
        now = datetime.now(timezone.utc)
        key = (guild_id, user_id)
        self.reactions[key].append((now, message_id))
        self.reactions[key] = [(t, mid) for t, mid in self.reactions[key]
                               if (now - t).total_seconds() < 30]

    def is_react_spam(self, guild_id: int, user_id: int) -> bool:
        if not self.enabled.get(guild_id):
            return False
        s = self.settings[guild_id]
        now = datetime.now(timezone.utc)
        key = (guild_id, user_id)
        recent = [(t, mid) for t, mid in self.reactions.get(key, [])
                  if (now - t).total_seconds() < s['time_window']]
        unique_msgs = len(set(mid for _, mid in recent))
        return len(recent) >= s['max_reacts'] and unique_msgs >= 3

    def get_recent_message_ids(self, guild_id: int, user_id: int) -> list:
        s = self.settings[guild_id]
        now = datetime.now(timezone.utc)
        recent = [(t, mid) for t, mid in self.reactions.get((guild_id, user_id), [])
                  if (now - t).total_seconds() < s['time_window']]
        return list(set(mid for _, mid in recent))

    def is_whitelisted(self, member: discord.Member) -> bool:
        return _check_whitelist(member, self.whitelisted_roles, self.whitelisted_users)

anti_react = AntiReact()

# ==================== ANTI-GHOST-PING ====================

class AntiGhostPing:
    """Detects when someone mass-mentions users then deletes the message."""
    def __init__(self):
        self.enabled:           dict = {}
        self.whitelisted_roles: dict = defaultdict(list)
        self.whitelisted_users: dict = defaultdict(list)
        self.settings:          dict = defaultdict(lambda: {
            'min_mentions': 2,   # at least N mentions to trigger
            'action':       'warn',
        })

    def is_whitelisted(self, member: discord.Member) -> bool:
        return _check_whitelist(member, self.whitelisted_roles, self.whitelisted_users)

anti_ghost_ping = AntiGhostPing()

# ==================== ANTI-CAPS ====================

class AntiCaps:
    """Deletes messages that are mostly capital letters."""
    def __init__(self):
        self.enabled:           dict = {}
        self.whitelisted_roles: dict = defaultdict(list)
        self.whitelisted_users: dict = defaultdict(list)
        self.settings:          dict = defaultdict(lambda: {
            'min_length':   10,    # ignore short messages
            'caps_percent': 70,    # % caps to trigger (0-100)
        })

    def is_caps_spam(self, guild_id: int, content: str) -> bool:
        if not self.enabled.get(guild_id):
            return False
        s = self.settings[guild_id]
        letters = [c for c in content if c.isalpha()]
        if len(letters) < s['min_length']:
            return False
        caps = sum(1 for c in letters if c.isupper())
        return (caps / len(letters) * 100) >= s['caps_percent']

    def is_whitelisted(self, member: discord.Member) -> bool:
        return _check_whitelist(member, self.whitelisted_roles, self.whitelisted_users)

anti_caps = AntiCaps()

# ==================== ANTI-DUPLICATE ====================

class AntiDuplicate:
    """Detects the same message sent repeatedly."""
    def __init__(self):
        self.enabled:           dict = {}
        self.recent:            dict = defaultdict(list)
        self.whitelisted_roles: dict = defaultdict(list)
        self.whitelisted_users: dict = defaultdict(list)
        self.settings:          dict = defaultdict(lambda: {
            'max_dupes':   3,     # trigger after N identical messages
            'time_window': 15,    # seconds
        })

    def check(self, guild_id: int, user_id: int, content: str) -> bool:
        if not self.enabled.get(guild_id):
            return False
        if not content or len(content) < 5:
            return False
        s = self.settings[guild_id]
        now = datetime.now(timezone.utc)
        key = (guild_id, user_id)
        self.recent[key].append((now, content.lower()))
        self.recent[key] = [(t, c) for t, c in self.recent[key]
                            if (now - t).total_seconds() < s['time_window']]
        dupes = sum(1 for _, c in self.recent[key] if c == content.lower())
        return dupes >= s['max_dupes']

    def is_whitelisted(self, member: discord.Member) -> bool:
        return _check_whitelist(member, self.whitelisted_roles, self.whitelisted_users)

anti_duplicate = AntiDuplicate()

# ==================== ANTI-EMOJI ====================

class AntiEmoji:
    """Detects emoji spam in a single message."""
    def __init__(self):
        self.enabled:           dict = {}
        self.whitelisted_roles: dict = defaultdict(list)
        self.whitelisted_users: dict = defaultdict(list)
        self.settings:          dict = defaultdict(lambda: {
            'max_emojis': 10,
        })
        self._emoji_pattern = re.compile(
            r'<a?:[a-zA-Z0-9_]+:[0-9]+>|'
            r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF'
            r'\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF'
            r'\U00002702-\U000027B0\U000024C2-\U0001F251]+'
        )

    def count_emojis(self, content: str) -> int:
        return len(self._emoji_pattern.findall(content))

    def is_emoji_spam(self, guild_id: int, content: str) -> bool:
        if not self.enabled.get(guild_id):
            return False
        return self.count_emojis(content) > self.settings[guild_id]['max_emojis']

    def is_whitelisted(self, member: discord.Member) -> bool:
        return _check_whitelist(member, self.whitelisted_roles, self.whitelisted_users)

anti_emoji = AntiEmoji()

# ==================== ANTI-INVITE ====================

class AntiInvite:
    """Blocks Discord server invite links specifically."""
    _PATTERN = re.compile(
        r'(discord\.gg/|discord\.com/invite/|discordapp\.com/invite/)[a-zA-Z0-9\-]+'
    )

    def __init__(self):
        self.enabled:           dict = {}
        self.whitelisted_roles: dict = defaultdict(list)
        self.whitelisted_users: dict = defaultdict(list)
        self.whitelisted_guilds: dict = defaultdict(list)  # whitelisted invite codes/guild IDs

    def is_invite(self, content: str) -> bool:
        return bool(self._PATTERN.search(content))

    def is_whitelisted(self, member: discord.Member) -> bool:
        return _check_whitelist(member, self.whitelisted_roles, self.whitelisted_users)

anti_invite = AntiInvite()

# ==================== AUTOMOD ====================

class AutoMod:
    _DEFAULT_WORDS = [
        'nigger', 'nigga', 'faggot', 'fag', 'retard', 'retarded',
        'cunt', 'kys', 'kms', 'rape', 'molest', 'pedo', 'pedophile',
        'tranny', 'dyke', 'chink', 'spic', 'wetback', 'gook',
        'beaner', 'cracker', 'honkey',
    ]

    def __init__(self):
        self.enabled:    dict = {}
        self.bad_words:  dict = defaultdict(lambda: list(self._DEFAULT_WORDS))

    def _is_bypassed(self, message: discord.Message) -> bool:
        if message.author.guild_permissions.administrator:
            return True
        staff_role = message.guild.get_role(HARDCODED_ROLES.get('staff'))
        if staff_role and staff_role in message.author.roles:
            return True
        return False

    def check_message(self, message: discord.Message) -> tuple[bool, Optional[str]]:
        if not self.enabled.get(message.guild.id):
            return False, None
        if self._is_bypassed(message):
            return False, None
        content_lower = message.content.lower()
        for word in self.bad_words[message.guild.id]:
            if re.search(rf'\b{re.escape(word)}\b', content_lower):
                return True, word
        return False, None

    def add_word(self, guild_id: int, word: str) -> bool:
        word = word.lower().strip()
        if word not in self.bad_words[guild_id]:
            self.bad_words[guild_id].append(word)
            return True
        return False

    def remove_word(self, guild_id: int, word: str) -> bool:
        word = word.lower().strip()
        if word in self.bad_words[guild_id]:
            self.bad_words[guild_id].remove(word)
            return True
        return False

automod = AutoMod()

# ==================== LOCKDOWN ====================

class Lockdown:
    def __init__(self):
        self.locked_channels: dict = defaultdict(list)

    def is_locked(self, guild_id: int) -> bool:
        return bool(self.locked_channels.get(guild_id))

lockdown = Lockdown()

# ==================== GIVEAWAY ====================

class GiveawaySystem:
    def __init__(self):
        self.active: dict = {}          # message_id -> giveaway_data
        self.last_winners: dict = {}    # giveaway_id -> set of user_ids who won last draw

    async def create(self, ctx: commands.Context, duration: timedelta, winners: int, prize: str) -> int:
        end_time = datetime.now(timezone.utc) + duration
        embed = discord.Embed(title='🎉 GIVEAWAY 🎉', color=0xF1C40F)
        embed.add_field(name='Prize',        value=prize,                                inline=False)
        embed.add_field(name='Winners',      value=str(winners),                         inline=True)
        embed.add_field(name='Ends',         value=f'<t:{int(end_time.timestamp())}:R>', inline=True)
        embed.add_field(name='How to Enter', value='React with 🎉',                     inline=False)
        embed.set_footer(text=f'Hosted by {ctx.author.display_name}')
        msg = await ctx.send(embed=embed)
        await msg.add_reaction('🎉')
        self.active[msg.id] = {
            'message_id': msg.id,
            'channel_id': ctx.channel.id,
            'guild_id':   ctx.guild.id,
            'prize':      prize,
            'winners':    winners,
            'host':       ctx.author.id,
            'end_time':   end_time,
            'ended':      False,
        }
        return msg.id

    async def end(self, bot: commands.Bot, giveaway_id: int, reroll: bool = False):
        if giveaway_id not in self.active:
            return None, 'Giveaway not found'
        data = self.active[giveaway_id]
        try:
            guild   = bot.get_guild(data['guild_id'])
            channel = guild.get_channel(data['channel_id'])
            message = await channel.fetch_message(data['message_id'])
            reaction = next((r for r in message.reactions if str(r.emoji) == '🎉'), None)
            if not reaction:
                return None, 'No entries found'
            # Get all entrants, shuffle them properly
            users = [u async for u in reaction.users() if not u.bot]
            if not users:
                return None, 'No valid entries'
            random.shuffle(users)
            # Filter out last winners to prevent back-to-back wins (if enough people)
            last = self.last_winners.get(giveaway_id, set())
            eligible = [u for u in users if u.id not in last]
            # Fall back to everyone if not enough eligible
            pool = eligible if len(eligible) >= data['winners'] else users
            count   = min(data['winners'], len(pool))
            winners = random.sample(pool, count)
            # Track these winners so they can't win back-to-back
            self.last_winners[giveaway_id] = {w.id for w in winners}
            mentions = ' '.join(w.mention for w in winners)
            embed = discord.Embed(title='🎉 GIVEAWAY ENDED 🎉', color=0x2ECC71)
            embed.add_field(name='Prize',   value=data['prize'], inline=False)
            embed.add_field(name='Winners', value=mentions,      inline=False)
            host = guild.get_member(data['host'])
            if host:
                embed.set_footer(text=f'Hosted by {host.display_name}')
            await message.edit(embed=embed)
            await channel.send(f'🎉 Congrats {mentions}! You won **{data["prize"]}**!')
            if not reroll:
                data['ended'] = True
            return winners, None
        except Exception as e:
            return None, str(e)

giveaway = GiveawaySystem()

# ==================== DATABASE ====================

class Database:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        url = os.getenv('DATABASE_URL', '')
        if not url:
            raise RuntimeError('DATABASE_URL environment variable not set')
        # Render gives postgres://, asyncpg needs postgresql://
        if url.startswith('postgres://'):
            url = url.replace('postgres://', 'postgresql://', 1)
        self.pool = await asyncpg.create_pool(url, min_size=1, max_size=5, command_timeout=30)
        await self._create_tables()
        logger.info('Database connected and tables ready')

    async def _create_tables(self):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS config (
                    guild_id           BIGINT PRIMARY KEY,
                    ticket_category_id BIGINT,
                    log_channel_id     BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS tickets (
                    ticket_id    TEXT PRIMARY KEY,
                    guild_id     BIGINT,
                    channel_id   BIGINT,
                    user_id      BIGINT,
                    ticket_type  TEXT,
                    tier         TEXT,
                    claimed_by   BIGINT,
                    status       TEXT DEFAULT 'open',
                    trade_details JSONB,
                    created_at   TIMESTAMP DEFAULT NOW()
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS blacklist (
                    user_id         BIGINT PRIMARY KEY,
                    guild_id        BIGINT,
                    reason          TEXT,
                    blacklisted_by  BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS jailed_users (
                    user_id    BIGINT PRIMARY KEY,
                    guild_id   BIGINT,
                    saved_roles JSONB,
                    reason     TEXT,
                    jailed_by  BIGINT,
                    jailed_at  TIMESTAMP DEFAULT NOW()
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS warnings (
                    id        SERIAL PRIMARY KEY,
                    guild_id  BIGINT,
                    user_id   BIGINT,
                    reason    TEXT,
                    warned_by BIGINT,
                    warned_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS ps_links (
                    user_id   BIGINT,
                    game_key  TEXT,
                    game_name TEXT,
                    link      TEXT,
                    PRIMARY KEY (user_id, game_key)
                )
            ''')

    async def close(self):
        if self.pool:
            await self.pool.close()

db = Database()

# ==================== BOT SETUP ====================

intents = discord.Intents.default()
intents.message_content = True
intents.members         = True
intents.guilds          = True
intents.bans            = True
intents.integrations    = True
intents.reactions       = True

bot = commands.Bot(command_prefix='$', intents=intents, help_command=None)

# ==================== HELPERS ====================

def parse_duration(s: str) -> Optional[timedelta]:
    """Parse strings like 10s, 5m, 2h, 1d — also supports 1h30m."""
    units = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}
    total = 0
    for amount, unit in re.findall(r'(\d+)([smhd])', s.lower()):
        total += int(amount) * units[unit]
    return timedelta(seconds=total) if total else None

def generate_ticket_id() -> str:
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))

def is_owner():
    async def predicate(ctx):
        return ctx.author.id == OWNER_ID
    return commands.check(predicate)

def has_admin_perms():
    async def predicate(ctx):
        if ctx.author.id == OWNER_ID:
            return True
        role_id = admin_perms.get(ctx.guild.id)
        if not role_id:
            return False
        role = ctx.guild.get_role(role_id)
        return bool(role and any(r >= role for r in ctx.author.roles))
    return commands.check(predicate)

def has_mod_perms():
    async def predicate(ctx):
        if ctx.author.id == OWNER_ID:
            return True
        admin_id = admin_perms.get(ctx.guild.id)
        if admin_id:
            admin_role = ctx.guild.get_role(admin_id)
            if admin_role and any(r >= admin_role for r in ctx.author.roles):
                return True
        mod_id = mod_perms.get(ctx.guild.id)
        if not mod_id:
            return False
        mod_role = ctx.guild.get_role(mod_id)
        return bool(mod_role and any(r >= mod_role for r in ctx.author.roles))
    return commands.check(predicate)

def has_ticket_staff_perms():
    """Staff role, any middleman tier role, admin, or owner can use ticket commands."""
    async def predicate(ctx):
        if ctx.author.id == OWNER_ID:
            return True
        if ctx.author.guild_permissions.administrator:
            return True
        # Check staff role
        staff_role = ctx.guild.get_role(HARDCODED_ROLES['staff'])
        if staff_role and staff_role in ctx.author.roles:
            return True
        # Check any middleman tier role
        for tier_key in ['lowtier', 'midtier', 'hightier']:
            role = ctx.guild.get_role(HARDCODED_ROLES[tier_key])
            if role and role in ctx.author.roles:
                return True
        return False
    return commands.check(predicate)

async def send_log(
    guild: discord.Guild,
    title: str,
    description: str = None,
    color: int = 0x5865F2,
    fields: dict = None,
    user: discord.Member = None,
):
    try:
        async with db.pool.acquire() as conn:
            cfg = await conn.fetchrow('SELECT log_channel_id FROM config WHERE guild_id = $1', guild.id)
        if not cfg or not cfg['log_channel_id']:
            return
        channel = guild.get_channel(cfg['log_channel_id'])
        if not channel:
            return
        embed = discord.Embed(title=title, color=color, timestamp=datetime.now(timezone.utc))
        if description:
            embed.description = description
        if user:
            embed.set_author(name=str(user), icon_url=user.display_avatar.url)
        for name, value in (fields or {}).items():
            embed.add_field(name=name, value=str(value), inline=True)
        await channel.send(embed=embed)
    except Exception as e:
        logger.error(f'send_log error: {e}')

async def ticket_has_permission(ctx, ticket) -> bool:
    """Check if ctx.author can manage a ticket."""
    if ctx.author.id == OWNER_ID:
        return True
    if ticket['ticket_type'] == 'support':
        role = ctx.guild.get_role(HARDCODED_ROLES['staff'])
        if role and role in ctx.author.roles:
            return True
    elif ticket['ticket_type'] == 'middleman':
        tier = ticket.get('tier')
        if tier in HARDCODED_ROLES:
            role = ctx.guild.get_role(HARDCODED_ROLES[tier])
            if role and role in ctx.author.roles:
                return True
    if ticket.get('claimed_by') and ctx.author.id == ticket['claimed_by']:
        return True
    return False

async def nuke_punish(guild: discord.Guild, member: discord.Member, reason: str):
    """Strip all roles, give shame role, ping owner in logs."""
    # Strip all roles
    try:
        roles_to_remove = [r for r in member.roles if r.id != guild.id and not r.managed]
        if roles_to_remove:
            await member.remove_roles(*roles_to_remove, reason=f'anti-nuke: {reason}')
    except Exception as e:
        logger.error(f'nuke_punish strip: {e}')
    # Give or create "dumbass tried to nuke" role
    try:
        shame_role = discord.utils.get(guild.roles, name='dumbass tried to nuke')
        if not shame_role:
            shame_role = await guild.create_role(
                name='dumbass tried to nuke',
                color=discord.Color.dark_red(),
                reason='anti-nuke shame role'
            )
        await member.add_roles(shame_role, reason=f'anti-nuke: {reason}')
    except Exception as e:
        logger.error(f'nuke_punish shame role: {e}')

# ==================== TICKET UI ====================

class MiddlemanModal(Modal, title='Middleman Request'):
    def __init__(self, tier: str):
        super().__init__()
        self.tier = tier
        self.trader    = TextInput(label='Trading with', placeholder='@username or ID', required=True)
        self.giving    = TextInput(label='You give', placeholder='e.g., 1 garam', style=discord.TextStyle.paragraph, required=True)
        self.receiving = TextInput(label='You receive', placeholder='e.g., 296 Robux', style=discord.TextStyle.paragraph, required=True)
        self.tip       = TextInput(label='Tip (optional)', placeholder='Optional', required=False)
        self.add_item(self.trader)
        self.add_item(self.giving)
        self.add_item(self.receiving)
        self.add_item(self.tip)

    async def on_submit(self, interaction: discord.Interaction):
        if not rate_limiter.check(interaction.user.id, 'ticket', 10):
            return await interaction.response.send_message('⏱️ Wait 10 seconds', ephemeral=True)
        await interaction.response.defer(ephemeral=True)
        try:
            guild, user = interaction.guild, interaction.user
            async with db.pool.acquire() as conn:
                bl = await conn.fetchrow('SELECT 1 FROM blacklist WHERE user_id=$1 AND guild_id=$2', user.id, guild.id)
                if bl:
                    return await interaction.followup.send('❌ You are blacklisted from tickets', ephemeral=True)
                cfg = await conn.fetchrow('SELECT * FROM config WHERE guild_id=$1', guild.id)
            if not cfg or not cfg['ticket_category_id']:
                return await interaction.followup.send('❌ Not configured — ask an admin to run `$setcategory`', ephemeral=True)
            category = guild.get_channel(cfg['ticket_category_id'])
            if not category:
                return await interaction.followup.send('❌ Ticket category not found', ephemeral=True)
            tier_short = {'lowtier': '100-900rbx', 'midtier': '1k-3k-rbx', 'hightier': '3k-5k-rbx'}.get(self.tier, self.tier)
            ticket_id  = generate_ticket_id()
            overwrites = {
                guild.default_role: discord.PermissionOverwrite(read_messages=False),
                user:               discord.PermissionOverwrite(read_messages=True, send_messages=True),
                guild.me:           discord.PermissionOverwrite(read_messages=True, send_messages=True, manage_channels=True),
            }
            role_id = HARDCODED_ROLES.get(self.tier)
            if role_id:
                role = guild.get_role(role_id)
                if role:
                    overwrites[role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
            channel = await category.create_text_channel(
                name=f'ticket-{tier_short}-{user.name}', overwrites=overwrites
            )
            trade_details = {
                'trader':    self.trader.value,
                'giving':    self.giving.value,
                'receiving': self.receiving.value,
                'tip':       self.tip.value or 'None',
            }
            async with db.pool.acquire() as conn:
                await conn.execute(
                    'INSERT INTO tickets (ticket_id,guild_id,channel_id,user_id,ticket_type,tier,trade_details) VALUES ($1,$2,$3,$4,$5,$6,$7)',
                    ticket_id, guild.id, channel.id, user.id, 'middleman', self.tier, json.dumps(trade_details)
                )
            tier_names = {'lowtier': '💎 100–900 RBX', 'midtier': '💰 1K–3K RBX', 'hightier': '💸 3.1K–5K+ RBX'}
            embed = discord.Embed(
                title='⚖️ Middleman Request',
                description=(
                    f'**Tier:** {tier_names.get(self.tier)}\n\n'
                    'A middleman will claim this shortly.\n\n'
                    '📋 **Guidelines:**\n'
                    '• Be patient and respectful\n'
                    '• Provide all necessary info\n'
                    "• Don't spam or ping staff\n"
                    '• Wait for staff to claim'
                ),
                color=COLORS.get(self.tier, COLORS['support']),
            )
            embed.set_author(name=user.display_name, icon_url=user.display_avatar.url)
            embed.add_field(name='Trading With', value=trade_details['trader'],    inline=False)
            embed.add_field(name='Giving',       value=trade_details['giving'],    inline=True)
            embed.add_field(name='Receiving',    value=trade_details['receiving'], inline=True)
            if trade_details['tip'] != 'None':
                embed.add_field(name='Tip', value=trade_details['tip'], inline=False)
            embed.set_footer(text=f'ID: {ticket_id}')
            ping = user.mention
            if role_id and (tr := guild.get_role(role_id)):
                ping += f' {tr.mention}'
            await channel.send(content=ping, embed=embed, view=TicketControlView())
            # Log
            if cfg.get('log_channel_id'):
                lc = guild.get_channel(cfg['log_channel_id'])
                if lc:
                    le = discord.Embed(title='📂 Ticket Opened', color=COLORS['success'])
                    le.add_field(name='ID',   value=f'`{ticket_id}`', inline=True)
                    le.add_field(name='User', value=user.mention,     inline=True)
                    le.add_field(name='Tier', value=tier_names.get(self.tier), inline=True)
                    await lc.send(embed=le)
            await interaction.followup.send(
                embed=discord.Embed(title='✅ Ticket Created', description=channel.mention, color=COLORS['success']),
                ephemeral=True
            )
        except Exception as e:
            logger.error(f'Ticket create error: {e}')
            await interaction.followup.send(f'❌ Error: {e}', ephemeral=True)


class MiddlemanTierSelect(Select):
    def __init__(self):
        super().__init__(
            placeholder='Select item value range',
            options=[
                discord.SelectOption(label='Low Tier | 100–900 RBX',    value='lowtier',  emoji='💎', description='Items valued between 100 and 900 robux. Quick & easy trades.'),
                discord.SelectOption(label='Mid Tier | 1K–3K RBX',      value='midtier',  emoji='💰', description='Items valued between 1,000 and 3,000 robux. Mid value middleman.'),
                discord.SelectOption(label='High Tier | 3.1K–5K+ RBX',  value='hightier', emoji='💸', description='Items valued 3,100 robux and above. High value, trusted staff only.'),
            ]
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(MiddlemanModal(self.values[0]))


class TicketPanelView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Support', style=discord.ButtonStyle.primary, emoji='🎫', custom_id='support_btn')
    async def support_button(self, interaction: discord.Interaction, _):
        if not rate_limiter.check(interaction.user.id, 'ticket', 10):
            return await interaction.response.send_message('⏱️ Wait 10 seconds', ephemeral=True)
        await interaction.response.defer(ephemeral=True)
        try:
            guild, user = interaction.guild, interaction.user
            async with db.pool.acquire() as conn:
                bl  = await conn.fetchrow('SELECT 1 FROM blacklist WHERE user_id=$1 AND guild_id=$2', user.id, guild.id)
                cfg = await conn.fetchrow('SELECT * FROM config WHERE guild_id=$1', guild.id)
            if bl:
                return await interaction.followup.send('❌ You are blacklisted from tickets', ephemeral=True)
            if not cfg or not cfg['ticket_category_id']:
                return await interaction.followup.send('❌ Tickets not configured', ephemeral=True)
            category = guild.get_channel(cfg['ticket_category_id'])
            ticket_id = generate_ticket_id()
            overwrites = {
                guild.default_role: discord.PermissionOverwrite(read_messages=False),
                user:               discord.PermissionOverwrite(read_messages=True, send_messages=True),
                guild.me:           discord.PermissionOverwrite(read_messages=True, send_messages=True, manage_channels=True),
            }
            staff = guild.get_role(HARDCODED_ROLES['staff'])
            if staff:
                overwrites[staff] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
            channel = await category.create_text_channel(name=f'ticket-support-{user.name}', overwrites=overwrites)
            async with db.pool.acquire() as conn:
                await conn.execute(
                    'INSERT INTO tickets (ticket_id,guild_id,channel_id,user_id,ticket_type,tier) VALUES ($1,$2,$3,$4,$5,$6)',
                    ticket_id, guild.id, channel.id, user.id, 'support', 'support'
                )
            embed = discord.Embed(title='🎫 Support Ticket', description='Staff will assist you shortly', color=COLORS['support'])
            embed.set_author(name=user.display_name, icon_url=user.display_avatar.url)
            embed.set_footer(text=f'ID: {ticket_id}')
            ping = user.mention
            if staff:
                ping += f' {staff.mention}'
            await channel.send(content=ping, embed=embed, view=TicketControlView())
            await interaction.followup.send(
                embed=discord.Embed(title='✅ Ticket Created', description=channel.mention, color=COLORS['success']),
                ephemeral=True
            )
        except Exception as e:
            logger.error(f'Support ticket error: {e}')
            await interaction.followup.send(f'❌ Error: {e}', ephemeral=True)

    @discord.ui.button(label='Middleman', style=discord.ButtonStyle.success, emoji='⚖️', custom_id='middleman_btn')
    async def middleman_button(self, interaction: discord.Interaction, _):
        view = View(timeout=300)
        view.add_item(MiddlemanTierSelect())
        await interaction.response.send_message(
            embed=discord.Embed(title='⚖️ Select Trade Value', description='Choose your tier below', color=COLORS['support']),
            view=view, ephemeral=True
        )


class TicketControlView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Claim', style=discord.ButtonStyle.green, custom_id='claim_ticket', emoji='✋')
    async def claim_button(self, interaction: discord.Interaction, _):
        if not rate_limiter.check(interaction.user.id, 'claim', 2):
            return await interaction.response.send_message('⏱️ Wait', ephemeral=True)
        try:
            async with db.pool.acquire() as conn:
                ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', interaction.channel.id)
                if not ticket:
                    return await interaction.response.send_message('❌ Not a ticket channel', ephemeral=True)
                if ticket['claimed_by']:
                    return await interaction.response.send_message('❌ Already claimed', ephemeral=True)
                await conn.execute(
                    "UPDATE tickets SET claimed_by=$1, status='claimed' WHERE ticket_id=$2",
                    interaction.user.id, ticket['ticket_id']
                )
            embed = discord.Embed(title='✋ Claimed', description=f'By {interaction.user.mention}', color=COLORS['success'])
            await interaction.response.send_message(embed=embed)
        except Exception as e:
            await interaction.response.send_message(f'❌ Error: {e}', ephemeral=True)

    @discord.ui.button(label='Unclaim', style=discord.ButtonStyle.gray, custom_id='unclaim_ticket', emoji='↩️')
    async def unclaim_button(self, interaction: discord.Interaction, _):
        if not rate_limiter.check(interaction.user.id, 'unclaim', 2):
            return await interaction.response.send_message('⏱️ Wait', ephemeral=True)
        try:
            async with db.pool.acquire() as conn:
                ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', interaction.channel.id)
                if not ticket:
                    return await interaction.response.send_message('❌ Not a ticket channel', ephemeral=True)
                if not ticket['claimed_by']:
                    return await interaction.response.send_message('❌ Not claimed', ephemeral=True)
                if ticket['claimed_by'] != interaction.user.id and interaction.user.id != OWNER_ID:
                    return await interaction.response.send_message('❌ Only the claimer can unclaim', ephemeral=True)
                await conn.execute(
                    "UPDATE tickets SET claimed_by=NULL, status='open' WHERE ticket_id=$1",
                    ticket['ticket_id']
                )
            await interaction.response.send_message(
                embed=discord.Embed(title='↩️ Unclaimed', description='Ticket is now available', color=COLORS['support'])
            )
        except Exception as e:
            await interaction.response.send_message(f'❌ Error: {e}', ephemeral=True)

# ==================== TICKET COMMANDS ====================

@bot.command(name='close')
@has_ticket_staff_perms()
async def close_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply('❌ Not a ticket channel')
    if not rate_limiter.check(ctx.author.id, 'close', 3):
        return await ctx.reply('⏱️ Wait 3 seconds')
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
            if not ticket:
                return await ctx.reply('❌ Ticket not found in database')
            if not await ticket_has_permission(ctx, ticket):
                return await ctx.reply("❌ You don't have permission to close this ticket")
            await conn.execute("UPDATE tickets SET status='closed' WHERE ticket_id=$1", ticket['ticket_id'])
        embed = discord.Embed(title='🔒 Closing Ticket', description=f'Closed by {ctx.author.mention}', color=COLORS['error'])
        await ctx.send(embed=embed)
        # Transcript
        async with db.pool.acquire() as conn:
            cfg = await conn.fetchrow('SELECT log_channel_id FROM config WHERE guild_id=$1', ctx.guild.id)
        if cfg and cfg.get('log_channel_id'):
            lc = ctx.guild.get_channel(cfg['log_channel_id'])
            if lc:
                opener  = ctx.guild.get_member(ticket['user_id'])
                claimer = ctx.guild.get_member(ticket['claimed_by']) if ticket['claimed_by'] else None
                header  = (
                    f"TRANSCRIPT\n{'='*50}\n"
                    f"ID: {ticket['ticket_id']}\n"
                    f"Opened by: {opener.name if opener else ticket['user_id']}\n"
                    f"Claimed by: {claimer.name if claimer else 'Unclaimed'}\n"
                    f"Closed by: {ctx.author.name}\n"
                    f"{'='*50}\n\n"
                )
                msgs = []
                async for m in ctx.channel.history(limit=500, oldest_first=True):
                    content = m.content or '[embed/attachment]'
                    msgs.append(f'[{m.created_at.strftime("%H:%M:%S")}] {m.author.name}: {content}')
                transcript = header + '\n'.join(msgs)
                file = discord.File(
                    fp=io.BytesIO(transcript.encode('utf-8')),
                    filename=f"transcript-{ticket['ticket_id']}.txt"
                )
                le = discord.Embed(title='🔒 Ticket Closed', color=COLORS['error'])
                le.add_field(name='ID',       value=ticket['ticket_id'],              inline=True)
                le.add_field(name='Opened by', value=opener.mention if opener else 'Unknown', inline=True)
                le.add_field(name='Closed by', value=ctx.author.mention,             inline=True)
                await lc.send(embed=le, file=file)
        await asyncio.sleep(0.5)
        await ctx.channel.delete()
    except Exception as e:
        logger.error(f'close_cmd error: {e}')
        await ctx.reply(f'❌ Error: {e}')


@bot.command(name='claim')
@has_ticket_staff_perms()
async def claim_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply('❌ Not a ticket channel')
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
            if not ticket:   return await ctx.reply('❌ Ticket not found')
            if ticket['claimed_by']: return await ctx.reply('❌ Already claimed')
            if not await ticket_has_permission(ctx, ticket):
                return await ctx.reply("❌ You don't have the required role")
            await conn.execute(
                "UPDATE tickets SET claimed_by=$1, status='claimed' WHERE ticket_id=$2",
                ctx.author.id, ticket['ticket_id']
            )
        await ctx.send(embed=discord.Embed(title='✋ Ticket Claimed', description=f'By {ctx.author.mention}', color=COLORS['success']))
    except Exception as e:
        await ctx.reply(f'❌ Error: {e}')


@bot.command(name='unclaim')
@has_ticket_staff_perms()
async def unclaim_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply('❌ Not a ticket channel')
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
            if not ticket:             return await ctx.reply('❌ Ticket not found')
            if not ticket['claimed_by']: return await ctx.reply('❌ Not claimed')
            if ticket['claimed_by'] != ctx.author.id and ctx.author.id != OWNER_ID:
                return await ctx.reply("❌ You didn't claim this ticket")
            await conn.execute(
                "UPDATE tickets SET claimed_by=NULL, status='open' WHERE ticket_id=$1",
                ticket['ticket_id']
            )
        await ctx.send(embed=discord.Embed(title='↩️ Unclaimed', description='Ticket is open again', color=COLORS['support']))
    except Exception as e:
        await ctx.reply(f'❌ Error: {e}')


@bot.command(name='add')
@has_ticket_staff_perms()
async def add_cmd(ctx, member: discord.Member = None):
    if not member:  return await ctx.reply('❌ Usage: `$add @User`')
    if not ctx.channel.name.startswith('ticket-'): return await ctx.reply('❌ Not a ticket channel')
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
            if not ticket: return await ctx.reply('❌ Ticket not found')
            if not await ticket_has_permission(ctx, ticket): return await ctx.reply("❌ Permission denied")
        await ctx.channel.set_permissions(member, read_messages=True, send_messages=True)
        await ctx.reply(embed=discord.Embed(title='✅ User Added', description=f'{member.mention} added to ticket', color=COLORS['success']))
    except Exception as e:
        await ctx.reply(f'❌ Error: {e}')


@bot.command(name='remove')
@has_ticket_staff_perms()
async def remove_cmd(ctx, member: discord.Member = None):
    if not member:  return await ctx.reply('❌ Usage: `$remove @User`')
    if not ctx.channel.name.startswith('ticket-'): return await ctx.reply('❌ Not a ticket channel')
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
            if not ticket: return await ctx.reply('❌ Ticket not found')
            if not await ticket_has_permission(ctx, ticket): return await ctx.reply("❌ Permission denied")
        await ctx.channel.set_permissions(member, overwrite=None)
        await ctx.reply(embed=discord.Embed(title='❌ User Removed', description=f'{member.mention} removed from ticket', color=COLORS['error']))
    except Exception as e:
        await ctx.reply(f'❌ Error: {e}')


@bot.command(name='rename')
@has_ticket_staff_perms()
async def rename_cmd(ctx, *, new_name: str = None):
    if not new_name: return await ctx.reply('❌ Usage: `$rename <name>`')
    if not ctx.channel.name.startswith('ticket-'): return await ctx.reply('❌ Not a ticket channel')
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
            if not ticket: return await ctx.reply('❌ Ticket not found')
            if not await ticket_has_permission(ctx, ticket): return await ctx.reply("❌ Permission denied")
        safe_name = re.sub(r'[^a-z0-9\-]', '-', new_name.lower())
        await ctx.channel.edit(name=f'ticket-{safe_name}')
        await ctx.reply(embed=discord.Embed(title='✏️ Renamed', description=f'Now: `ticket-{safe_name}`', color=COLORS['support']))
    except Exception as e:
        await ctx.reply(f'❌ Error: {e}')


@bot.command(name='transfer')
@has_ticket_staff_perms()
async def transfer_cmd(ctx, member: discord.Member = None):
    if not member: return await ctx.reply('❌ Usage: `$transfer @User`')
    if not ctx.channel.name.startswith('ticket-'): return await ctx.reply('❌ Not a ticket channel')
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
            if not ticket:             return await ctx.reply('❌ Ticket not found')
            if not ticket['claimed_by']: return await ctx.reply('❌ Must be claimed first')
            if not await ticket_has_permission(ctx, ticket): return await ctx.reply("❌ Permission denied")
            # Verify target has appropriate role
            ctx_copy = ctx
            ctx_copy.author = member
            if not await ticket_has_permission(ctx_copy, ticket):
                return await ctx.reply(f"❌ {member.mention} doesn't have the required role")
            old = ctx.guild.get_member(ticket['claimed_by'])
            if old and old.id != OWNER_ID:
                await ctx.channel.set_permissions(old, send_messages=False, read_messages=True)
            await conn.execute('UPDATE tickets SET claimed_by=$1 WHERE ticket_id=$2', member.id, ticket['ticket_id'])
        embed = discord.Embed(title='🔄 Transferred', color=COLORS['support'])
        embed.add_field(name='From', value=ctx.author.mention, inline=True)
        embed.add_field(name='To',   value=member.mention,     inline=True)
        await ctx.send(content=member.mention, embed=embed)
    except Exception as e:
        await ctx.reply(f'❌ Error: {e}')


@bot.command(name='proof')
@has_ticket_staff_perms()
async def proof_cmd(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply('❌ Only usable in ticket channels')
    try:
        async with db.pool.acquire() as conn:
            ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', ctx.channel.id)
            if not ticket: return await ctx.reply('❌ Ticket not found')
        proof_ch = ctx.guild.get_channel(HARDCODED_CHANNELS['proof'])
        if not proof_ch: return await ctx.reply('❌ Proof channel not found')
        opener = ctx.guild.get_member(ticket['user_id'])
        tier_names = {'lowtier': '0-150M', 'midtier': '150-500M', 'hightier': '500M+'}
        embed = discord.Embed(title='✅ Trade Completed', color=COLORS['success'])
        embed.add_field(name='Middleman',  value=ctx.author.mention,              inline=True)
        embed.add_field(name='Tier',       value=tier_names.get(ticket.get('tier'), 'Support'), inline=True)
        embed.add_field(name='Requester',  value=opener.mention if opener else 'Unknown',      inline=True)
        if ticket.get('trade_details'):
            try:
                d = ticket['trade_details'] if isinstance(ticket['trade_details'], dict) else json.loads(ticket['trade_details'])
                embed.add_field(name='Trader',    value=d.get('trader', 'Unknown'), inline=False)
                embed.add_field(name='Giving',    value=d.get('giving', 'N/A'),     inline=True)
                embed.add_field(name='Receiving', value=d.get('receiving', 'N/A'), inline=True)
                if d.get('tip') and d['tip'] != 'None':
                    embed.add_field(name='Tip', value=d['tip'], inline=False)
            except Exception:
                pass
        embed.set_footer(text=f'ID: {ticket["ticket_id"]}')
        await proof_ch.send(embed=embed)
        await ctx.reply(embed=discord.Embed(title='✅ Proof Sent', description=f'Posted to {proof_ch.mention}', color=COLORS['success']))
    except Exception as e:
        await ctx.reply(f'❌ Error: {e}')

# ==================== SETUP COMMANDS ====================

@bot.command(name='setup')
@commands.has_permissions(administrator=True)
async def setup_cmd(ctx):
    embed = discord.Embed(
        title='🎟️ Ticket Center | Support & Middleman',
        description=(
            '🛠️ **Support**\n'
            '• General support\n'
            '• Giveaway & event prizes\n'
            '• Partnership requests\n\n'
            '⚖️ **Middleman**\n'
            '• Secure & verified trading\n'
            '• Trusted middleman services\n'
            '• Trades protected by trusted staff'
        ),
        color=COLORS['support']
    )
    await ctx.send(embed=embed, view=TicketPanelView())
    try: await ctx.message.delete()
    except: pass


@bot.command(name='setcategory')
@commands.has_permissions(administrator=True)
async def setcategory_cmd(ctx, category: discord.CategoryChannel = None):
    if not category: return await ctx.reply('❌ Usage: `$setcategory #Tickets`')
    async with db.pool.acquire() as conn:
        await conn.execute(
            'INSERT INTO config (guild_id, ticket_category_id) VALUES ($1,$2) ON CONFLICT (guild_id) DO UPDATE SET ticket_category_id=$2',
            ctx.guild.id, category.id
        )
    await ctx.reply(embed=discord.Embed(title='✅ Category Set', description=category.mention, color=COLORS['success']))


@bot.command(name='setlogs')
@commands.has_permissions(administrator=True)
async def setlogs_cmd(ctx, channel: discord.TextChannel = None):
    if not channel: return await ctx.reply('❌ Usage: `$setlogs #logs`')
    async with db.pool.acquire() as conn:
        await conn.execute(
            'INSERT INTO config (guild_id, log_channel_id) VALUES ($1,$2) ON CONFLICT (guild_id) DO UPDATE SET log_channel_id=$2',
            ctx.guild.id, channel.id
        )
    await ctx.reply(embed=discord.Embed(title='✅ Logs Set', description=channel.mention, color=COLORS['success']))


@bot.command(name='config')
@commands.has_permissions(administrator=True)
async def config_cmd(ctx):
    async with db.pool.acquire() as conn:
        cfg = await conn.fetchrow('SELECT * FROM config WHERE guild_id=$1', ctx.guild.id)
    embed = discord.Embed(title='⚙️ Bot Configuration', color=COLORS['support'])
    if cfg:
        cat = ctx.guild.get_channel(cfg['ticket_category_id']) if cfg.get('ticket_category_id') else None
        log = ctx.guild.get_channel(cfg['log_channel_id'])     if cfg.get('log_channel_id')     else None
        embed.add_field(name='Ticket Category', value=cat.mention if cat else '❌ Not set', inline=True)
        embed.add_field(name='Log Channel',     value=log.mention if log else '❌ Not set', inline=True)
    else:
        embed.description = '❌ Not configured yet. Use `$setcategory` and `$setlogs`'
    proof_ch = ctx.guild.get_channel(HARDCODED_CHANNELS['proof'])
    embed.add_field(name='Proof Channel', value=proof_ch.mention if proof_ch else '❌ Not found', inline=True)
    ar = ctx.guild.get_role(admin_perms.get(ctx.guild.id)) if admin_perms.get(ctx.guild.id) else None
    mr = ctx.guild.get_role(mod_perms.get(ctx.guild.id))   if mod_perms.get(ctx.guild.id)   else None
    embed.add_field(name='Admin Role', value=ar.mention if ar else '❌ Not set', inline=True)
    embed.add_field(name='Mod Role',   value=mr.mention if mr else '❌ Not set', inline=True)
    await ctx.reply(embed=embed)

# ==================== PERM SETUP ====================

@bot.group(name='adminperms', aliases=['ap'], invoke_without_command=True)
@is_owner()
async def adminperms_group(ctx):
    await ctx.reply('Usage: `$adminperms set @role` | `$adminperms show`')

@adminperms_group.command(name='set')
@is_owner()
async def adminperms_set(ctx, role: discord.Role = None):
    if not role: return await ctx.reply('❌ Usage: `$adminperms set @Admin`')
    admin_perms[ctx.guild.id] = role.id
    embed = discord.Embed(title='✅ Admin Role Set', color=COLORS['success'])
    embed.description = f'{role.mention} can now use: `ban` `kick` `role` `slowmode` `lock` `hide` `purge`'
    await ctx.reply(embed=embed)

@adminperms_group.command(name='show')
@is_owner()
async def adminperms_show(ctx):
    rid = admin_perms.get(ctx.guild.id)
    if not rid: return await ctx.reply('❌ No admin role set')
    role = ctx.guild.get_role(rid)
    await ctx.reply(embed=discord.Embed(title='⚙️ Admin Role', description=role.mention if role else '❌ Role missing', color=COLORS['support']))


@bot.group(name='modperms', aliases=['mp'], invoke_without_command=True)
@is_owner()
async def modperms_group(ctx):
    await ctx.reply('Usage: `$modperms set @role` | `$modperms show`')

@modperms_group.command(name='set')
@is_owner()
async def modperms_set(ctx, role: discord.Role = None):
    if not role: return await ctx.reply('❌ Usage: `$modperms set @Mod`')
    mod_perms[ctx.guild.id] = role.id
    embed = discord.Embed(title='✅ Mod Role Set', color=COLORS['success'])
    embed.description = f'{role.mention} can now use: `mute` `unmute` `warn` `warnings` `clearwarnings` `nick`'
    await ctx.reply(embed=embed)

@modperms_group.command(name='show')
@is_owner()
async def modperms_show(ctx):
    rid = mod_perms.get(ctx.guild.id)
    if not rid: return await ctx.reply('❌ No mod role set')
    role = ctx.guild.get_role(rid)
    await ctx.reply(embed=discord.Embed(title='⚙️ Mod Role', description=role.mention if role else '❌ Role missing', color=COLORS['support']))

# ==================== JAIL ====================

@bot.command(name='jail')
@commands.has_permissions(administrator=True)
async def jail_cmd(ctx, member: discord.Member = None, *, reason: str = 'No reason'):
    if not member: return await ctx.reply('❌ Usage: `$jail @User <reason>`')
    if member.bot:             return await ctx.reply('❌ Cannot jail bots')
    if member.id == ctx.author.id: return await ctx.reply('❌ Cannot jail yourself')
    try:
        async with db.pool.acquire() as conn:
            if await conn.fetchrow('SELECT 1 FROM jailed_users WHERE user_id=$1', member.id):
                return await ctx.reply('❌ Already jailed')
        role_ids = [r.id for r in member.roles if r.id != ctx.guild.id]
        for r in member.roles:
            if r.id != ctx.guild.id:
                try: await member.remove_roles(r)
                except: pass
        jailed_role = ctx.guild.get_role(HARDCODED_ROLES['jailed'])
        if jailed_role: await member.add_roles(jailed_role)
        async with db.pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO jailed_users (user_id,guild_id,saved_roles,reason,jailed_by) VALUES ($1,$2,$3,$4,$5)',
                member.id, ctx.guild.id, json.dumps(role_ids), reason, ctx.author.id
            )
        embed = discord.Embed(title='🚔 User Jailed', color=COLORS['error'])
        embed.add_field(name='User',        value=member.mention,    inline=True)
        embed.add_field(name='Reason',      value=reason,            inline=True)
        embed.add_field(name='Roles Saved', value=str(len(role_ids)), inline=True)
        await ctx.reply(embed=embed)
        await send_log(ctx.guild, '🚔 User Jailed', user=ctx.author, color=COLORS['error'],
                       fields={'User': str(member), 'Reason': reason})
    except Exception as e:
        await ctx.reply(f'❌ Error: {e}')


@bot.command(name='unjail')
@commands.has_permissions(administrator=True)
async def unjail_cmd(ctx, member: discord.Member = None):
    if not member: return await ctx.reply('❌ Usage: `$unjail @User`')
    try:
        async with db.pool.acquire() as conn:
            data = await conn.fetchrow('SELECT * FROM jailed_users WHERE user_id=$1', member.id)
            if not data: return await ctx.reply('❌ User is not jailed')
        jailed_role = ctx.guild.get_role(HARDCODED_ROLES['jailed'])
        if jailed_role and jailed_role in member.roles:
            await member.remove_roles(jailed_role)
        saved = data['saved_roles'] if isinstance(data['saved_roles'], list) else json.loads(data['saved_roles'])
        restored = 0
        for rid in saved:
            r = ctx.guild.get_role(rid)
            if r:
                try: await member.add_roles(r); restored += 1
                except: pass
        async with db.pool.acquire() as conn:
            await conn.execute('DELETE FROM jailed_users WHERE user_id=$1', member.id)
        embed = discord.Embed(title='✅ User Unjailed', color=COLORS['success'])
        embed.add_field(name='User',            value=member.mention,                   inline=True)
        embed.add_field(name='Roles Restored',  value=f'{restored}/{len(saved)}',       inline=True)
        await ctx.reply(embed=embed)
    except Exception as e:
        await ctx.reply(f'❌ Error: {e}')


@bot.command(name='jailed')
@commands.has_permissions(administrator=True)
async def jailed_cmd(ctx):
    async with db.pool.acquire() as conn:
        rows = await conn.fetch('SELECT * FROM jailed_users WHERE guild_id=$1', ctx.guild.id)
    if not rows: return await ctx.reply('✅ No jailed users')
    embed = discord.Embed(title='🚔 Jailed Users', color=COLORS['error'])
    for row in rows:
        m = ctx.guild.get_member(row['user_id'])
        embed.add_field(name=m.mention if m else f"ID: {row['user_id']}", value=f"Reason: {row['reason']}", inline=False)
    await ctx.reply(embed=embed)

# ==================== BLACKLIST ====================

@bot.command(name='blacklist')
@commands.has_permissions(administrator=True)
async def blacklist_cmd(ctx, member: discord.Member = None, *, reason: str = 'No reason'):
    if not member: return await ctx.reply('❌ Usage: `$blacklist @User <reason>`')
    async with db.pool.acquire() as conn:
        await conn.execute(
            'INSERT INTO blacklist (user_id,guild_id,reason,blacklisted_by) VALUES ($1,$2,$3,$4) ON CONFLICT (user_id) DO UPDATE SET reason=$3, blacklisted_by=$4',
            member.id, ctx.guild.id, reason, ctx.author.id
        )
    embed = discord.Embed(title='🚫 Blacklisted', color=COLORS['error'])
    embed.add_field(name='User',   value=member.mention, inline=True)
    embed.add_field(name='Reason', value=reason,         inline=True)
    await ctx.reply(embed=embed)


@bot.command(name='unblacklist')
@commands.has_permissions(administrator=True)
async def unblacklist_cmd(ctx, member: discord.Member = None):
    if not member: return await ctx.reply('❌ Usage: `$unblacklist @User`')
    async with db.pool.acquire() as conn:
        await conn.execute('DELETE FROM blacklist WHERE user_id=$1', member.id)
    await ctx.reply(embed=discord.Embed(title='✅ Unblacklisted', description=f'{member.mention} can now create tickets', color=COLORS['success']))


@bot.command(name='blacklists')
@commands.has_permissions(administrator=True)
async def blacklists_cmd(ctx):
    async with db.pool.acquire() as conn:
        rows = await conn.fetch('SELECT * FROM blacklist WHERE guild_id=$1', ctx.guild.id)
    if not rows: return await ctx.reply('✅ No blacklisted users')
    embed = discord.Embed(title='🚫 Blacklisted Users', color=COLORS['error'])
    for row in rows:
        m = ctx.guild.get_member(row['user_id'])
        embed.add_field(name=m.mention if m else f"ID: {row['user_id']}", value=row['reason'], inline=False)
    await ctx.reply(embed=embed)

# ==================== MODERATION COMMANDS ====================

@bot.command(name='ban', aliases=['b'])
@has_admin_perms()
async def ban_cmd(ctx, member: discord.Member = None, *, reason: str = 'No reason'):
    if not member: return await ctx.reply('❌ Usage: `$ban @User <reason>`')
    if member.id == ctx.author.id: return await ctx.reply('❌ Cannot ban yourself')
    if member.top_role >= ctx.author.top_role and ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ Cannot ban someone with an equal or higher role')
    if member.top_role >= ctx.guild.me.top_role:
        return await ctx.reply("❌ My role is too low to ban that user")
    try:
        try:
            await member.send(embed=discord.Embed(title=f'🔨 Banned from {ctx.guild.name}', description=f'**Reason:** {reason}', color=COLORS['error']))
        except: pass
        await member.ban(reason=f'{reason} | by {ctx.author}', delete_message_days=0)
        embed = discord.Embed(title='🔨 Banned', color=COLORS['error'])
        embed.add_field(name='User',      value=f'{member} ({member.id})', inline=True)
        embed.add_field(name='Reason',    value=reason,                     inline=True)
        embed.add_field(name='Banned By', value=ctx.author.mention,         inline=True)
        await ctx.reply(embed=embed)
        await send_log(ctx.guild, '🔨 User Banned', user=ctx.author, color=COLORS['error'],
                       fields={'User': str(member), 'Reason': reason})
    except Exception as e:
        await ctx.reply(f'❌ Error: {e}')


@bot.command(name='unban', aliases=['ub'])
@has_admin_perms()
async def unban_cmd(ctx, user_id: int = None, *, reason: str = 'No reason'):
    if not user_id: return await ctx.reply('❌ Usage: `$unban <user_id>`')
    try:
        user = await bot.fetch_user(user_id)
        await ctx.guild.unban(user, reason=f'{reason} | by {ctx.author}')
        embed = discord.Embed(title='✅ Unbanned', color=COLORS['success'])
        embed.add_field(name='User',   value=f'{user} ({user.id})', inline=True)
        embed.add_field(name='Reason', value=reason,                inline=True)
        await ctx.reply(embed=embed)
    except discord.NotFound:
        await ctx.reply('❌ User not found or not banned')
    except Exception as e:
        await ctx.reply(f'❌ Error: {e}')


@bot.command(name='hackban', aliases=['hb'])
@has_admin_perms()
async def hackban_cmd(ctx, user_id: int = None, *, reason: str = 'No reason'):
    if not user_id: return await ctx.reply('❌ Usage: `$hackban <user_id> <reason>`')
    try:
        user = await bot.fetch_user(user_id)
        await ctx.guild.ban(user, reason=f'{reason} | hackban by {ctx.author}', delete_message_days=0)
        embed = discord.Embed(title='🔨 Hackbanned', color=COLORS['error'])
        embed.add_field(name='User',   value=f'{user} ({user.id})', inline=True)
        embed.add_field(name='Reason', value=reason,                inline=True)
        await ctx.reply(embed=embed)
    except discord.NotFound:
        await ctx.reply('❌ User not found')
    except Exception as e:
        await ctx.reply(f'❌ Error: {e}')


@bot.command(name='unhackban', aliases=['uhb'])
@has_admin_perms()
async def unhackban_cmd(ctx, user_id: int = None):
    if not user_id: return await ctx.reply('❌ Usage: `$unhackban <user_id>`')
    try:
        user = await bot.fetch_user(user_id)
        await ctx.guild.unban(user, reason=f'Unhackbanned by {ctx.author}')
        await ctx.reply(embed=discord.Embed(title='✅ Unhackbanned', description=f'{user} ({user.id})', color=COLORS['success']))
    except discord.NotFound:
        await ctx.reply('❌ User not found or not banned')
    except Exception as e:
        await ctx.reply(f'❌ Error: {e}')


@bot.command(name='kick', aliases=['k'])
@has_admin_perms()
async def kick_cmd(ctx, member: discord.Member = None, *, reason: str = 'No reason'):
    if not member: return await ctx.reply('❌ Usage: `$kick @User <reason>`')
    if member.id == ctx.author.id: return await ctx.reply('❌ Cannot kick yourself')
    if member.top_role >= ctx.author.top_role and ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ Cannot kick someone with an equal or higher role')
    try:
        try:
            await member.send(embed=discord.Embed(title=f'👢 Kicked from {ctx.guild.name}', description=f'**Reason:** {reason}', color=COLORS['error']))
        except: pass
        await member.kick(reason=f'{reason} | by {ctx.author}')
        embed = discord.Embed(title='👢 Kicked', color=COLORS['error'])
        embed.add_field(name='User',      value=f'{member} ({member.id})', inline=True)
        embed.add_field(name='Reason',    value=reason,                     inline=True)
        embed.add_field(name='Kicked By', value=ctx.author.mention,         inline=True)
        await ctx.reply(embed=embed)
    except Exception as e:
        await ctx.reply(f'❌ Error: {e}')


@bot.command(name='mute', aliases=['m'])
@has_mod_perms()
async def mute_cmd(ctx, member: discord.Member = None, duration: str = None, *, reason: str = 'No reason'):
    if not member:   return await ctx.reply('❌ Usage: `$mute @User <time> <reason>` — e.g., `$mute @User 10m Spamming`')
    if not duration: return await ctx.reply('❌ Missing duration. Examples: `10s` `5m` `1h` `1d`')
    if member.id == ctx.author.id: return await ctx.reply('❌ Cannot mute yourself')
    if member.top_role >= ctx.author.top_role and ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ Cannot mute someone with an equal or higher role')
    delta = parse_duration(duration)
    if not delta: return await ctx.reply('❌ Invalid duration. Examples: `10s` `5m` `1h` `1d`')
    if delta > timedelta(days=28): return await ctx.reply('❌ Max timeout is 28 days')
    try:
        await member.timeout(delta, reason=f'{reason} | by {ctx.author}')
        embed = discord.Embed(title='🔇 Muted', color=COLORS['error'])
        embed.add_field(name='User',     value=member.mention, inline=True)
        embed.add_field(name='Duration', value=duration,       inline=True)
        embed.add_field(name='Reason',   value=reason,         inline=True)
        embed.add_field(name='By',       value=ctx.author.mention, inline=True)
        await ctx.reply(embed=embed)
    except Exception as e:
        await ctx.reply(f'❌ Error: {e}')


@bot.command(name='unmute', aliases=['um'])
@has_mod_perms()
async def unmute_cmd(ctx, member: discord.Member = None):
    if not member: return await ctx.reply('❌ Usage: `$unmute @User`')
    try:
        await member.timeout(None, reason=f'Unmuted by {ctx.author}')
        await ctx.reply(embed=discord.Embed(title='🔊 Unmuted', description=member.mention, color=COLORS['success']))
    except Exception as e:
        await ctx.reply(f'❌ Error: {e}')


@bot.command(name='warn', aliases=['w'])
@has_mod_perms()
async def warn_cmd(ctx, member: discord.Member = None, *, reason: str = 'No reason'):
    if not member: return await ctx.reply('❌ Usage: `$warn @User <reason>`')
    if member.bot: return await ctx.reply('❌ Cannot warn bots')
    try:
        async with db.pool.acquire() as conn:
            await conn.execute('INSERT INTO warnings (guild_id,user_id,reason,warned_by) VALUES ($1,$2,$3,$4)',
                               ctx.guild.id, member.id, reason, ctx.author.id)
            count = await conn.fetchval('SELECT COUNT(*) FROM warnings WHERE guild_id=$1 AND user_id=$2', ctx.guild.id, member.id)
        try:
            await member.send(embed=discord.Embed(
                title=f'⚠️ Warning in {ctx.guild.name}',
                description=f'**Reason:** {reason}\n**Total warnings:** {count}',
                color=COLORS['error']
            ))
        except: pass
        embed = discord.Embed(title='⚠️ Warning Issued', color=COLORS['error'])
        embed.add_field(name='User',     value=member.mention,      inline=True)
        embed.add_field(name='Reason',   value=reason,              inline=True)
        embed.add_field(name='Total',    value=str(count),          inline=True)
        embed.add_field(name='Warned By', value=ctx.author.mention, inline=True)
        await ctx.reply(embed=embed)
    except Exception as e:
        await ctx.reply(f'❌ Error: {e}')


@bot.command(name='warnings', aliases=['ws'])
@has_mod_perms()
async def warnings_cmd(ctx, member: discord.Member = None):
    if not member: return await ctx.reply('❌ Usage: `$warnings @User`')
    async with db.pool.acquire() as conn:
        rows = await conn.fetch('SELECT * FROM warnings WHERE guild_id=$1 AND user_id=$2 ORDER BY warned_at DESC', ctx.guild.id, member.id)
    if not rows: return await ctx.reply(f'✅ {member.mention} has no warnings')
    embed = discord.Embed(title=f'⚠️ Warnings — {member.display_name}', color=COLORS['error'])
    embed.set_thumbnail(url=member.display_avatar.url)
    for i, row in enumerate(rows[:10], 1):
        warner = ctx.guild.get_member(row['warned_by'])
        embed.add_field(name=f'#{i}', value=f"**Reason:** {row['reason']}\n**By:** {warner.mention if warner else 'Unknown'}", inline=False)
    embed.set_footer(text=f'Total: {len(rows)}')
    await ctx.reply(embed=embed)


@bot.command(name='clearwarnings', aliases=['cw'])
@has_mod_perms()
async def clearwarnings_cmd(ctx, member: discord.Member = None):
    if not member: return await ctx.reply('❌ Usage: `$clearwarnings @User`')
    async with db.pool.acquire() as conn:
        count = await conn.fetchval('SELECT COUNT(*) FROM warnings WHERE guild_id=$1 AND user_id=$2', ctx.guild.id, member.id)
        await conn.execute('DELETE FROM warnings WHERE guild_id=$1 AND user_id=$2', ctx.guild.id, member.id)
    await ctx.reply(embed=discord.Embed(title='✅ Warnings Cleared', description=f'Cleared **{count}** warning(s) for {member.mention}', color=COLORS['success']))


@bot.command(name='role', aliases=['r'])
@has_admin_perms()
async def role_cmd(ctx, member: discord.Member = None, *, role_name: str = None):
    if not member or not role_name:
        return await ctx.reply('❌ Usage: `$role @User rolename`')
    role = discord.utils.find(lambda r: r.name.lower() == role_name.lower(), ctx.guild.roles)
    if not role: return await ctx.reply(f'❌ Role `{role_name}` not found')
    if role >= ctx.author.top_role and ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ Cannot manage a role equal to or above your own')
    if role >= ctx.guild.me.top_role:
        return await ctx.reply("❌ That role is above my highest role")
    if role in member.roles:
        await member.remove_roles(role)
        embed = discord.Embed(title='➖ Role Removed', color=COLORS['error'])
    else:
        await member.add_roles(role)
        embed = discord.Embed(title='➕ Role Added', color=COLORS['success'])
    embed.add_field(name='User', value=member.mention, inline=True)
    embed.add_field(name='Role', value=role.mention,   inline=True)
    await ctx.reply(embed=embed)


@bot.command(name='nick', aliases=['n'])
@has_mod_perms()
async def nick_cmd(ctx, member: discord.Member = None, *, nickname: str = None):
    if not member: return await ctx.reply('❌ Usage: `$nick @User <name>` or `$nick @User reset`')
    try:
        new_nick = None if (not nickname or nickname.lower() == 'reset') else nickname
        await member.edit(nick=new_nick)
        if new_nick:
            embed = discord.Embed(title='✏️ Nickname Changed', color=COLORS['success'])
            embed.add_field(name='User', value=member.mention, inline=True)
            embed.add_field(name='Nick', value=new_nick,       inline=True)
        else:
            embed = discord.Embed(title='✏️ Nickname Reset', description=member.mention, color=COLORS['success'])
        await ctx.reply(embed=embed)
    except Exception as e:
        await ctx.reply(f'❌ Error: {e}')


@bot.command(name='slowmode', aliases=['sm'])
@has_admin_perms()
async def slowmode_cmd(ctx, seconds: int = None):
    if seconds is None: return await ctx.reply('❌ Usage: `$slowmode <seconds>` (0 to disable)')
    if not (0 <= seconds <= 21600): return await ctx.reply('❌ Must be 0–21600 seconds')
    await ctx.channel.edit(slowmode_delay=seconds)
    if seconds:
        await ctx.reply(embed=discord.Embed(title=f'🐢 Slowmode: {seconds}s', description=ctx.channel.mention, color=COLORS['support']))
    else:
        await ctx.reply(embed=discord.Embed(title='✅ Slowmode Disabled', description=ctx.channel.mention, color=COLORS['success']))


@bot.command(name='lock', aliases=['lk'])
@has_admin_perms()
async def lock_cmd(ctx):
    await ctx.channel.set_permissions(ctx.guild.default_role, send_messages=False)
    for rid in [HARDCODED_ROLES['staff'], admin_perms.get(ctx.guild.id), mod_perms.get(ctx.guild.id)]:
        if rid:
            role = ctx.guild.get_role(rid)
            if role: await ctx.channel.set_permissions(role, send_messages=True)
    await ctx.send(embed=discord.Embed(title='🔒 Channel Locked', description=ctx.channel.mention, color=COLORS['error']))


@bot.command(name='unlock', aliases=['ulk'])
@has_admin_perms()
async def unlock_cmd(ctx):
    await ctx.channel.set_permissions(ctx.guild.default_role, send_messages=None)
    await ctx.send(embed=discord.Embed(title='🔓 Channel Unlocked', description=ctx.channel.mention, color=COLORS['success']))


@bot.command(name='hide', aliases=['hd'])
@has_admin_perms()
async def hide_cmd(ctx):
    await ctx.channel.set_permissions(ctx.guild.default_role, view_channel=False)
    await ctx.send(embed=discord.Embed(title='🙈 Channel Hidden', description=ctx.channel.mention, color=COLORS['error']))


@bot.command(name='unhide', aliases=['uhd'])
@has_admin_perms()
async def unhide_cmd(ctx):
    await ctx.channel.set_permissions(ctx.guild.default_role, view_channel=None)
    await ctx.send(embed=discord.Embed(title='👁️ Channel Visible', description=ctx.channel.mention, color=COLORS['success']))


@bot.command(name='purge')
@has_admin_perms()
async def purge_cmd(ctx, amount: int = None, member: discord.Member = None):
    if not amount: return await ctx.reply('❌ Usage: `$purge <amount> [@user]`')
    if not (1 <= amount <= 100): return await ctx.reply('❌ Amount must be 1–100')
    try: await ctx.message.delete()
    except: pass
    check = (lambda m: m.author.id == member.id) if member else None
    deleted = await ctx.channel.purge(limit=amount, check=check)
    msg = await ctx.send(embed=discord.Embed(
        title='🧹 Purged',
        description=f'Deleted **{len(deleted)}** messages' + (f' from {member.mention}' if member else ''),
        color=COLORS['success']
    ))
    await asyncio.sleep(3)
    try: await msg.delete()
    except: pass


@bot.command(name='clear')
@commands.has_permissions(manage_messages=True)
async def clear_cmd(ctx):
    deleted = 0
    async for message in ctx.channel.history(limit=200):
        if message.author == bot.user:
            try: await message.delete(); deleted += 1; await asyncio.sleep(0.3)
            except: pass
    msg = await ctx.send(embed=discord.Embed(title='🧹 Cleared', description=f'Deleted {deleted} bot messages', color=COLORS['success']))
    await asyncio.sleep(3)
    try: await msg.delete()
    except: pass
    try: await ctx.message.delete()
    except: pass

# ==================== LOCKDOWN / CHANNELPERM ====================

@bot.command(name='lockdown')
@is_owner()
async def lockdown_cmd(ctx):
    if lockdown.is_locked(ctx.guild.id): return await ctx.reply('❌ Already locked down')
    staff_role = ctx.guild.get_role(HARDCODED_ROLES['staff'])
    mod_role   = ctx.guild.get_role(mod_perms.get(ctx.guild.id)) if mod_perms.get(ctx.guild.id) else None
    admin_role = ctx.guild.get_role(admin_perms.get(ctx.guild.id)) if admin_perms.get(ctx.guild.id) else None
    locked = 0
    for ch in ctx.guild.text_channels:
        try:
            await ch.set_permissions(ctx.guild.default_role, send_messages=False)
            for r in [staff_role, mod_role, admin_role]:
                if r: await ch.set_permissions(r, send_messages=True)
            lockdown.locked_channels[ctx.guild.id].append(ch.id)
            locked += 1
        except: pass
    await ctx.reply(embed=discord.Embed(
        title='🔒 Server Locked Down',
        description=f'**{locked}** channels locked\nUse `$unlockdown` to restore',
        color=COLORS['error']
    ))


@bot.command(name='unlockdown')
@is_owner()
async def unlockdown_cmd(ctx):
    if not lockdown.is_locked(ctx.guild.id): return await ctx.reply('❌ Not locked down')
    unlocked = 0
    for cid in lockdown.locked_channels[ctx.guild.id]:
        ch = ctx.guild.get_channel(cid)
        if ch:
            try: await ch.set_permissions(ctx.guild.default_role, send_messages=None); unlocked += 1
            except: pass
    lockdown.locked_channels[ctx.guild.id] = []
    await ctx.reply(embed=discord.Embed(title='🔓 Server Unlocked', description=f'**{unlocked}** channels restored', color=COLORS['success']))


@bot.command(name='channelperm')
@is_owner()
async def channelperm_cmd(ctx, channel: discord.TextChannel = None, target_input: str = None, action: str = None, *, permission_name: str = None):
    if not all([channel, target_input, action, permission_name]):
        return await ctx.reply('❌ Usage: `$channelperm #channel @Target enable/disable <permission>`')
    target = await _resolve_target(ctx, target_input)
    if not target: return await ctx.reply('❌ Invalid role/member/everyone')
    if action.lower() not in ['enable', 'disable']:
        return await ctx.reply('❌ Action must be `enable` or `disable`')
    perm_key = permission_name.lower().strip()
    if perm_key not in PERM_MAP:
        return await ctx.reply(f'❌ Unknown permission: `{permission_name}`')
    perm_value = action.lower() == 'enable'
    try:
        ow = channel.overwrites_for(target)
        setattr(ow, PERM_MAP[perm_key], perm_value)
        await channel.set_permissions(target, overwrite=ow)
        embed = discord.Embed(title=f'{"✅" if perm_value else "🚫"} Permission {"Enabled" if perm_value else "Disabled"}', color=COLORS['success'] if perm_value else COLORS['error'])
        embed.add_field(name='Channel',    value=channel.mention,  inline=True)
        embed.add_field(name='Target',     value=getattr(target, 'mention', str(target)), inline=True)
        embed.add_field(name='Permission', value=permission_name,  inline=True)
        await ctx.reply(embed=embed)
    except Exception as e:
        await ctx.reply(f'❌ Error: {e}')


@bot.command(name='channelpermall')
@is_owner()
async def channelpermall_cmd(ctx, target_input: str = None, action: str = None, *, permission_name: str = None):
    if not all([target_input, action, permission_name]):
        return await ctx.reply('❌ Usage: `$channelpermall @Target enable/disable <permission>`')
    target = await _resolve_target(ctx, target_input)
    if not target: return await ctx.reply('❌ Invalid role/member/everyone')
    if action.lower() not in ['enable', 'disable']:
        return await ctx.reply('❌ Action must be `enable` or `disable`')
    perm_key = permission_name.lower().strip()
    if perm_key not in PERM_MAP: return await ctx.reply(f'❌ Unknown permission: `{permission_name}`')
    perm_value = action.lower() == 'enable'
    updated = failed = 0
    msg = await ctx.reply('⏳ Updating all channels...')
    for ch in ctx.guild.channels:
        try:
            ow = ch.overwrites_for(target)
            setattr(ow, PERM_MAP[perm_key], perm_value)
            await ch.set_permissions(target, overwrite=ow)
            updated += 1
        except: failed += 1
    embed = discord.Embed(title=f'{"✅" if perm_value else "🚫"} Updated', color=COLORS['success'] if perm_value else COLORS['error'])
    embed.add_field(name='Permission', value=permission_name,         inline=True)
    embed.add_field(name='Updated',    value=f'{updated}/{updated+failed}', inline=True)
    await msg.edit(content=None, embed=embed)


async def _resolve_target(ctx, target_input: str):
    if target_input.lower() in ['everyone', '@everyone']:
        return ctx.guild.default_role
    try: return await commands.RoleConverter().convert(ctx, target_input)
    except: pass
    try: return await commands.MemberConverter().convert(ctx, target_input)
    except: pass
    return None

# ==================== WHITELIST COMMANDS ====================

_PROTECTION_MAP = {
    'anti-link':   (anti_link,      'anti-link'),
    'anti-spam':   (anti_spam,      'anti-spam'),
    'anti-nuke':   (anti_nuke,      'anti-nuke'),
    'anti-raid':   (anti_raid,      'anti-raid'),
    'anti-react':  (anti_react,     'anti-react'),
    'anti-caps':   (anti_caps,      'anti-caps'),
    'anti-dupe':   (anti_duplicate, 'anti-dupe'),
    'anti-emoji':  (anti_emoji,     'anti-emoji'),
    'anti-invite': (anti_invite,    'anti-invite'),
    'anti-ghost':  (anti_ghost_ping,'anti-ghost'),
}

@bot.command(name='whitelist')
@is_owner()
async def whitelist_cmd(ctx, protection: str = None, *, target: str = None):
    if not protection or not target:
        return await ctx.reply('❌ Usage: `$whitelist <protection> @Role/@Member`\nOptions: `anti-link` `anti-spam` `anti-nuke` `anti-raid` `anti-react` `anti-caps` `anti-dupe` `anti-emoji` `anti-invite` `anti-ghost`')
    key = protection.lower()
    if key not in _PROTECTION_MAP:
        return await ctx.reply(f'❌ Invalid protection. Options: {", ".join(f"`{k}`" for k in _PROTECTION_MAP)}')
    obj, label = _PROTECTION_MAP[key]
    role = None
    try:   role = await commands.RoleConverter().convert(ctx, target)
    except: pass
    member = None
    if not role:
        try:   member = await commands.MemberConverter().convert(ctx, target)
        except: return await ctx.reply('❌ Invalid role or member')
    t = role or member
    store = obj.whitelisted_roles if role else obj.whitelisted_users
    if t.id in store[ctx.guild.id]:
        return await ctx.reply(f'❌ Already whitelisted for {label}')
    store[ctx.guild.id].append(t.id)
    await ctx.reply(embed=discord.Embed(
        title=f'✅ Whitelisted — {label}',
        description=f'{t.mention} is exempt from {label}',
        color=COLORS['success']
    ))


@bot.command(name='unwhitelist')
@is_owner()
async def unwhitelist_cmd(ctx, protection: str = None, *, target: str = None):
    if not protection or not target:
        return await ctx.reply('❌ Usage: `$unwhitelist <protection> @Role/@Member`')
    key = protection.lower()
    if key not in _PROTECTION_MAP:
        return await ctx.reply(f'❌ Invalid protection')
    obj, label = _PROTECTION_MAP[key]
    role = None
    try:   role = await commands.RoleConverter().convert(ctx, target)
    except: pass
    member = None
    if not role:
        try:   member = await commands.MemberConverter().convert(ctx, target)
        except: return await ctx.reply('❌ Invalid role or member')
    t = role or member
    store = obj.whitelisted_roles if role else obj.whitelisted_users
    if t.id not in store[ctx.guild.id]:
        return await ctx.reply(f'❌ Not whitelisted for {label}')
    store[ctx.guild.id].remove(t.id)
    await ctx.reply(embed=discord.Embed(title=f'✅ Removed — {label}', description=f'{t.mention} removed', color=COLORS['success']))


@bot.command(name='whitelisted')
@is_owner()
async def whitelisted_cmd(ctx, protection: str = None):
    if not protection:
        return await ctx.reply(f'❌ Usage: `$whitelisted <protection>`\nOptions: {", ".join(f"`{k}`" for k in _PROTECTION_MAP)}')
    key = protection.lower()
    if key not in _PROTECTION_MAP:
        return await ctx.reply('❌ Invalid protection')
    obj, label = _PROTECTION_MAP[key]
    roles   = obj.whitelisted_roles.get(ctx.guild.id, [])
    users   = obj.whitelisted_users.get(ctx.guild.id, [])
    if not roles and not users:
        return await ctx.reply(f'No whitelisted entries for {label}')
    embed = discord.Embed(title=f'📋 {label} Whitelist', color=COLORS['support'])
    if roles:
        role_text = '\n'.join(r.mention for rid in roles if (r := ctx.guild.get_role(rid)))
        if role_text: embed.add_field(name='Roles',   value=role_text, inline=False)
    if users:
        user_text = '\n'.join(m.mention for uid in users if (m := ctx.guild.get_member(uid)))
        if user_text: embed.add_field(name='Members', value=user_text, inline=False)
    await ctx.reply(embed=embed)

# ==================== ANTI-NUKE COMMANDS ====================

@bot.group(name='anti-nuke', invoke_without_command=True)
@is_owner()
async def anti_nuke_group(ctx):
    embed = discord.Embed(title='🛡️ anti-nuke', color=COLORS['support'])
    embed.add_field(name='control',   value='`$anti-nuke enable/disable/status`', inline=False)
    embed.add_field(name='whitelist', value='`$anti-nuke whitelist/unwhitelist @role`', inline=False)
    embed.add_field(name='protects',  value='mass bans, kicks, channel/role deletes, unauthorized bots/webhooks/integrations', inline=False)
    await ctx.reply(embed=embed)

@anti_nuke_group.command(name='enable')
@is_owner()
async def anti_nuke_enable(ctx):
    if anti_nuke.enabled.get(ctx.guild.id): return await ctx.reply('❌ Already enabled')
    anti_nuke.enabled[ctx.guild.id] = True
    await ctx.reply(embed=discord.Embed(title='✅ Anti-Nuke Enabled', color=COLORS['success']))

@anti_nuke_group.command(name='disable')
@is_owner()
async def anti_nuke_disable(ctx):
    if not anti_nuke.enabled.get(ctx.guild.id): return await ctx.reply('❌ Already disabled')
    anti_nuke.enabled[ctx.guild.id] = False
    await ctx.reply(embed=discord.Embed(title='✅ Anti-Nuke Disabled', color=COLORS['support']))

@anti_nuke_group.command(name='whitelist')
@is_owner()
async def anti_nuke_whitelist(ctx, role: discord.Role = None):
    if not role: return await ctx.reply('❌ Specify a role')
    if role.id in anti_nuke.whitelisted_roles[ctx.guild.id]: return await ctx.reply('❌ Already whitelisted')
    anti_nuke.whitelisted_roles[ctx.guild.id].append(role.id)
    await ctx.reply(embed=discord.Embed(title='✅ Whitelisted', description=f'{role.mention} bypasses anti-nuke', color=COLORS['success']))

@anti_nuke_group.command(name='unwhitelist')
@is_owner()
async def anti_nuke_unwhitelist(ctx, role: discord.Role = None):
    if not role: return await ctx.reply('❌ Specify a role')
    if role.id not in anti_nuke.whitelisted_roles[ctx.guild.id]: return await ctx.reply('❌ Not whitelisted')
    anti_nuke.whitelisted_roles[ctx.guild.id].remove(role.id)
    await ctx.reply(embed=discord.Embed(title='✅ Removed', color=COLORS['success']))

@anti_nuke_group.command(name='status')
@is_owner()
async def anti_nuke_status(ctx):
    enabled = anti_nuke.enabled.get(ctx.guild.id, False)
    embed = discord.Embed(title='🛡️ Anti-Nuke Status', color=COLORS['success'] if enabled else COLORS['error'])
    embed.add_field(name='Status',      value='✅ Enabled' if enabled else '❌ Disabled', inline=True)
    embed.add_field(name='Whitelisted', value=str(len(anti_nuke.whitelisted_roles.get(ctx.guild.id, []))), inline=True)
    await ctx.reply(embed=embed)

# ==================== ANTI-RAID COMMANDS ====================

@bot.group(name='anti-raid', invoke_without_command=True)
@is_owner()
async def anti_raid_group(ctx):
    embed = discord.Embed(title='🛡️ anti-raid', color=COLORS['support'])
    embed.add_field(name='control',  value='`$anti-raid enable/disable/status`',      inline=False)
    embed.add_field(name='settings', value='`$anti-raid action <kick/ban>`\n`$anti-raid accountage <days>`\n`$anti-raid avatar <on/off>`', inline=False)
    await ctx.reply(embed=embed)

@anti_raid_group.command(name='enable')
@is_owner()
async def anti_raid_enable(ctx):
    if anti_raid.enabled.get(ctx.guild.id): return await ctx.reply('❌ Already enabled')
    anti_raid.enabled[ctx.guild.id] = True
    await ctx.reply(embed=discord.Embed(title='✅ Anti-Raid Enabled', color=COLORS['success']))

@anti_raid_group.command(name='disable')
@is_owner()
async def anti_raid_disable(ctx):
    if not anti_raid.enabled.get(ctx.guild.id): return await ctx.reply('❌ Already disabled')
    anti_raid.enabled[ctx.guild.id] = False
    await ctx.reply(embed=discord.Embed(title='✅ Anti-Raid Disabled', color=COLORS['support']))

@anti_raid_group.command(name='action')
@is_owner()
async def anti_raid_action(ctx, action: str = None):
    if action not in ['kick', 'ban']:
        return await ctx.reply('❌ Action must be `kick` or `ban`')
    anti_raid.settings[ctx.guild.id]['action'] = action
    await ctx.reply(embed=discord.Embed(title='✅ Updated', description=f'Action: `{action}`', color=COLORS['success']))

@anti_raid_group.command(name='accountage')
@is_owner()
async def anti_raid_accountage(ctx, days: int = None):
    if days is None or days < 0: return await ctx.reply('❌ Usage: `$anti-raid accountage <days>`')
    anti_raid.settings[ctx.guild.id]['min_account_age'] = days
    await ctx.reply(embed=discord.Embed(title='✅ Updated', description=f'Min account age: `{days}` days', color=COLORS['success']))

@anti_raid_group.command(name='avatar')
@is_owner()
async def anti_raid_avatar(ctx, value: str = None):
    if value not in ['on', 'off']:
        return await ctx.reply('❌ Usage: `$anti-raid avatar <on/off>`')
    anti_raid.settings[ctx.guild.id]['require_avatar'] = (value == 'on')
    await ctx.reply(embed=discord.Embed(title='✅ Updated', description=f'Require avatar: `{value}`', color=COLORS['success']))

@anti_raid_group.command(name='status')
@is_owner()
async def anti_raid_status(ctx):
    enabled = anti_raid.enabled.get(ctx.guild.id, False)
    s = anti_raid.settings[ctx.guild.id]
    embed = discord.Embed(title='🛡️ Anti-Raid Status', color=COLORS['success'] if enabled else COLORS['error'])
    embed.add_field(name='Status',       value='✅ Enabled' if enabled else '❌ Disabled', inline=True)
    embed.add_field(name='Action',       value=s['action'],          inline=True)
    embed.add_field(name='Min Age',      value=f"{s['min_account_age']}d", inline=True)
    embed.add_field(name='Max Joins',    value=f"{s['max_joins']}/{s['time_window']}s", inline=True)
    embed.add_field(name='Req. Avatar',  value='Yes' if s['require_avatar'] else 'No', inline=True)
    await ctx.reply(embed=embed)

# ==================== ANTI-SPAM COMMANDS ====================

@bot.group(name='anti-spam', invoke_without_command=True)
@is_owner()
async def anti_spam_group(ctx):
    embed = discord.Embed(title='🛡️ anti-spam', color=COLORS['support'])
    embed.add_field(name='control',  value='`$anti-spam enable/disable/status`',   inline=False)
    embed.add_field(name='settings', value='`$anti-spam threshold <msgs> <secs>`', inline=False)
    embed.add_field(name='detects',  value='7+ msgs in 10s, mass mentions, escalating timeouts', inline=False)
    await ctx.reply(embed=embed)

@anti_spam_group.command(name='enable')
@is_owner()
async def anti_spam_enable(ctx):
    if anti_spam.enabled.get(ctx.guild.id): return await ctx.reply('❌ Already enabled')
    anti_spam.enabled[ctx.guild.id] = True
    await ctx.reply(embed=discord.Embed(title='✅ Anti-Spam Enabled', color=COLORS['success']))

@anti_spam_group.command(name='disable')
@is_owner()
async def anti_spam_disable(ctx):
    if not anti_spam.enabled.get(ctx.guild.id): return await ctx.reply('❌ Already disabled')
    anti_spam.enabled[ctx.guild.id] = False
    await ctx.reply(embed=discord.Embed(title='✅ Anti-Spam Disabled', color=COLORS['support']))

@anti_spam_group.command(name='threshold')
@is_owner()
async def anti_spam_threshold(ctx, max_msgs: int = None, window_secs: int = None):
    if not max_msgs or not window_secs:
        return await ctx.reply('❌ Usage: `$anti-spam threshold <max_messages> <window_seconds>`')
    anti_spam.settings[ctx.guild.id]['max_messages'] = max_msgs
    anti_spam.settings[ctx.guild.id]['time_window']  = window_secs
    await ctx.reply(embed=discord.Embed(title='✅ Updated', description=f'Triggers at `{max_msgs}` messages in `{window_secs}` seconds', color=COLORS['success']))

@anti_spam_group.command(name='status')
@is_owner()
async def anti_spam_status(ctx):
    enabled = anti_spam.enabled.get(ctx.guild.id, False)
    s = anti_spam.settings[ctx.guild.id]
    embed = discord.Embed(title='🛡️ Anti-Spam Status', color=COLORS['success'] if enabled else COLORS['error'])
    embed.add_field(name='Status',       value='✅ Enabled' if enabled else '❌ Disabled', inline=True)
    embed.add_field(name='Threshold',    value=f"{s['max_messages']} msgs / {s['time_window']}s", inline=True)
    embed.add_field(name='Max Mentions', value=str(s['max_mentions']), inline=True)
    await ctx.reply(embed=embed)

# ==================== ANTI-LINK COMMANDS ====================

@bot.group(name='anti-link', invoke_without_command=True)
@is_owner()
async def anti_link_group(ctx):
    embed = discord.Embed(title='🛡️ anti-link', color=COLORS['support'])
    embed.add_field(name='control',   value='`$anti-link enable/disable/status`',      inline=False)
    embed.add_field(name='whitelist', value='`$anti-link role @role`\n`$anti-link unrole @role`\n`$anti-link url <url>`', inline=False)
    await ctx.reply(embed=embed)

@anti_link_group.command(name='enable')
@is_owner()
async def anti_link_enable(ctx):
    if anti_link.enabled.get(ctx.guild.id): return await ctx.reply('❌ Already enabled')
    anti_link.enabled[ctx.guild.id] = True
    await ctx.reply(embed=discord.Embed(title='✅ Anti-Link Enabled', color=COLORS['success']))

@anti_link_group.command(name='disable')
@is_owner()
async def anti_link_disable(ctx):
    if not anti_link.enabled.get(ctx.guild.id): return await ctx.reply('❌ Already disabled')
    anti_link.enabled[ctx.guild.id] = False
    await ctx.reply(embed=discord.Embed(title='✅ Anti-Link Disabled', color=COLORS['support']))

@anti_link_group.command(name='role')
@is_owner()
async def anti_link_role(ctx, role: discord.Role = None):
    if not role: return await ctx.reply('❌ Specify a role')
    if role.id in anti_link.whitelisted_roles[ctx.guild.id]: return await ctx.reply('❌ Already whitelisted')
    anti_link.whitelisted_roles[ctx.guild.id].append(role.id)
    await ctx.reply(embed=discord.Embed(title='✅ Whitelisted', description=f'{role.mention} can post links', color=COLORS['success']))

@anti_link_group.command(name='unrole')
@is_owner()
async def anti_link_unrole(ctx, role: discord.Role = None):
    if not role: return await ctx.reply('❌ Specify a role')
    if role.id not in anti_link.whitelisted_roles[ctx.guild.id]: return await ctx.reply('❌ Not whitelisted')
    anti_link.whitelisted_roles[ctx.guild.id].remove(role.id)
    await ctx.reply(embed=discord.Embed(title='✅ Removed', color=COLORS['success']))

@anti_link_group.command(name='url')
@is_owner()
async def anti_link_url(ctx, *, url: str = None):
    if not url: return await ctx.reply('❌ Usage: `$anti-link url <url>`')
    anti_link.whitelisted_urls[ctx.guild.id].append(url.lower())
    await ctx.reply(embed=discord.Embed(title='✅ URL Whitelisted', description=f'`{url}` is allowed', color=COLORS['success']))

@anti_link_group.command(name='status')
@is_owner()
async def anti_link_status(ctx):
    enabled = anti_link.enabled.get(ctx.guild.id, False)
    embed = discord.Embed(title='🔗 Anti-Link Status', color=COLORS['success'] if enabled else COLORS['error'])
    embed.add_field(name='Status',      value='✅ Enabled' if enabled else '❌ Disabled', inline=True)
    embed.add_field(name='WL Roles',    value=str(len(anti_link.whitelisted_roles.get(ctx.guild.id, []))), inline=True)
    embed.add_field(name='WL URLs',     value=str(len(anti_link.whitelisted_urls.get(ctx.guild.id, []))),  inline=True)
    await ctx.reply(embed=embed)

# ==================== ANTI-REACT COMMANDS ====================

@bot.group(name='anti-react', invoke_without_command=True)
@is_owner()
async def anti_react_group(ctx):
    embed = discord.Embed(title='🛡️ anti-react', color=COLORS['support'])
    embed.add_field(name='control',  value='`$anti-react enable/disable/status`',            inline=False)
    embed.add_field(name='settings', value='`$anti-react action <warn/timeout/kick>`', inline=False)
    await ctx.reply(embed=embed)

@anti_react_group.command(name='enable')
@is_owner()
async def anti_react_enable(ctx):
    if anti_react.enabled.get(ctx.guild.id): return await ctx.reply('❌ Already enabled')
    anti_react.enabled[ctx.guild.id] = True
    await ctx.reply(embed=discord.Embed(title='✅ Anti-React Enabled', color=COLORS['success']))

@anti_react_group.command(name='disable')
@is_owner()
async def anti_react_disable(ctx):
    if not anti_react.enabled.get(ctx.guild.id): return await ctx.reply('❌ Already disabled')
    anti_react.enabled[ctx.guild.id] = False
    await ctx.reply(embed=discord.Embed(title='✅ Anti-React Disabled', color=COLORS['support']))

@anti_react_group.command(name='action')
@is_owner()
async def anti_react_action(ctx, action: str = None):
    if action not in ['warn', 'timeout', 'kick']:
        return await ctx.reply('❌ Action: `warn` `timeout` `kick`')
    anti_react.settings[ctx.guild.id]['action'] = action
    await ctx.reply(embed=discord.Embed(title='✅ Updated', description=f'Action: `{action}`', color=COLORS['success']))

@anti_react_group.command(name='status')
@is_owner()
async def anti_react_status(ctx):
    enabled = anti_react.enabled.get(ctx.guild.id, False)
    s = anti_react.settings[ctx.guild.id]
    embed = discord.Embed(title='🛡️ Anti-React Status', color=COLORS['success'] if enabled else COLORS['error'])
    embed.add_field(name='Status', value='✅ Enabled' if enabled else '❌ Disabled', inline=True)
    embed.add_field(name='Action', value=s['action'],                                inline=True)
    embed.add_field(name='Threshold', value=f"{s['max_reacts']} reacts / {s['time_window']}s", inline=True)
    await ctx.reply(embed=embed)

# ==================== ANTI-GHOST-PING COMMANDS ====================

@bot.group(name='anti-ghost', invoke_without_command=True)
@is_owner()
async def anti_ghost_group(ctx):
    embed = discord.Embed(title='👻 anti-ghost-ping', color=COLORS['support'])
    embed.add_field(name='control',  value='`$anti-ghost enable/disable/status`',   inline=False)
    embed.add_field(name='settings', value='`$anti-ghost action <warn/timeout/kick>`\n`$anti-ghost minpings <number>`', inline=False)
    embed.add_field(name='detects',  value='Messages with mentions that are immediately deleted', inline=False)
    await ctx.reply(embed=embed)

@anti_ghost_group.command(name='enable')
@is_owner()
async def anti_ghost_enable(ctx):
    if anti_ghost_ping.enabled.get(ctx.guild.id): return await ctx.reply('❌ Already enabled')
    anti_ghost_ping.enabled[ctx.guild.id] = True
    await ctx.reply(embed=discord.Embed(title='✅ Anti-Ghost-Ping Enabled', color=COLORS['success']))

@anti_ghost_group.command(name='disable')
@is_owner()
async def anti_ghost_disable(ctx):
    if not anti_ghost_ping.enabled.get(ctx.guild.id): return await ctx.reply('❌ Already disabled')
    anti_ghost_ping.enabled[ctx.guild.id] = False
    await ctx.reply(embed=discord.Embed(title='✅ Anti-Ghost-Ping Disabled', color=COLORS['support']))

@anti_ghost_group.command(name='action')
@is_owner()
async def anti_ghost_action(ctx, action: str = None):
    if action not in ['warn', 'timeout', 'kick']:
        return await ctx.reply('❌ Action: `warn` `timeout` `kick`')
    anti_ghost_ping.settings[ctx.guild.id]['action'] = action
    await ctx.reply(embed=discord.Embed(title='✅ Updated', description=f'Action: `{action}`', color=COLORS['success']))

@anti_ghost_group.command(name='minpings')
@is_owner()
async def anti_ghost_minpings(ctx, n: int = None):
    if not n or n < 1: return await ctx.reply('❌ Usage: `$anti-ghost minpings <number>`')
    anti_ghost_ping.settings[ctx.guild.id]['min_mentions'] = n
    await ctx.reply(embed=discord.Embed(title='✅ Updated', description=f'Min mentions to trigger: `{n}`', color=COLORS['success']))

@anti_ghost_group.command(name='status')
@is_owner()
async def anti_ghost_status(ctx):
    enabled = anti_ghost_ping.enabled.get(ctx.guild.id, False)
    s = anti_ghost_ping.settings[ctx.guild.id]
    embed = discord.Embed(title='👻 Anti-Ghost-Ping Status', color=COLORS['success'] if enabled else COLORS['error'])
    embed.add_field(name='Status',     value='✅ Enabled' if enabled else '❌ Disabled', inline=True)
    embed.add_field(name='Action',     value=s['action'],          inline=True)
    embed.add_field(name='Min Pings',  value=str(s['min_mentions']), inline=True)
    await ctx.reply(embed=embed)

# ==================== ANTI-CAPS COMMANDS ====================

@bot.group(name='anti-caps', invoke_without_command=True)
@is_owner()
async def anti_caps_group(ctx):
    embed = discord.Embed(title='🔠 anti-caps', color=COLORS['support'])
    embed.add_field(name='control',  value='`$anti-caps enable/disable/status`',    inline=False)
    embed.add_field(name='settings', value='`$anti-caps percent <0-100>`\n`$anti-caps minlength <chars>`', inline=False)
    await ctx.reply(embed=embed)

@anti_caps_group.command(name='enable')
@is_owner()
async def anti_caps_enable(ctx):
    if anti_caps.enabled.get(ctx.guild.id): return await ctx.reply('❌ Already enabled')
    anti_caps.enabled[ctx.guild.id] = True
    await ctx.reply(embed=discord.Embed(title='✅ Anti-Caps Enabled', color=COLORS['success']))

@anti_caps_group.command(name='disable')
@is_owner()
async def anti_caps_disable(ctx):
    if not anti_caps.enabled.get(ctx.guild.id): return await ctx.reply('❌ Already disabled')
    anti_caps.enabled[ctx.guild.id] = False
    await ctx.reply(embed=discord.Embed(title='✅ Anti-Caps Disabled', color=COLORS['support']))

@anti_caps_group.command(name='percent')
@is_owner()
async def anti_caps_percent(ctx, pct: int = None):
    if pct is None or not (10 <= pct <= 100): return await ctx.reply('❌ Usage: `$anti-caps percent <10-100>`')
    anti_caps.settings[ctx.guild.id]['caps_percent'] = pct
    await ctx.reply(embed=discord.Embed(title='✅ Updated', description=f'Triggers at `{pct}%` capitals', color=COLORS['success']))

@anti_caps_group.command(name='status')
@is_owner()
async def anti_caps_status(ctx):
    enabled = anti_caps.enabled.get(ctx.guild.id, False)
    s = anti_caps.settings[ctx.guild.id]
    embed = discord.Embed(title='🔠 Anti-Caps Status', color=COLORS['success'] if enabled else COLORS['error'])
    embed.add_field(name='Status',     value='✅ Enabled' if enabled else '❌ Disabled',   inline=True)
    embed.add_field(name='Threshold',  value=f"{s['caps_percent']}% caps",                inline=True)
    embed.add_field(name='Min Length', value=f"{s['min_length']} letters",                inline=True)
    await ctx.reply(embed=embed)

# ==================== ANTI-DUPLICATE COMMANDS ====================

@bot.group(name='anti-dupe', invoke_without_command=True)
@is_owner()
async def anti_dupe_group(ctx):
    embed = discord.Embed(title='📋 anti-duplicate', color=COLORS['support'])
    embed.add_field(name='control',  value='`$anti-dupe enable/disable/status`',  inline=False)
    embed.add_field(name='settings', value='`$anti-dupe threshold <count> <secs>`', inline=False)
    await ctx.reply(embed=embed)

@anti_dupe_group.command(name='enable')
@is_owner()
async def anti_dupe_enable(ctx):
    if anti_duplicate.enabled.get(ctx.guild.id): return await ctx.reply('❌ Already enabled')
    anti_duplicate.enabled[ctx.guild.id] = True
    await ctx.reply(embed=discord.Embed(title='✅ Anti-Duplicate Enabled', color=COLORS['success']))

@anti_dupe_group.command(name='disable')
@is_owner()
async def anti_dupe_disable(ctx):
    if not anti_duplicate.enabled.get(ctx.guild.id): return await ctx.reply('❌ Already disabled')
    anti_duplicate.enabled[ctx.guild.id] = False
    await ctx.reply(embed=discord.Embed(title='✅ Anti-Duplicate Disabled', color=COLORS['support']))

@anti_dupe_group.command(name='threshold')
@is_owner()
async def anti_dupe_threshold(ctx, count: int = None, secs: int = None):
    if not count or not secs: return await ctx.reply('❌ Usage: `$anti-dupe threshold <count> <seconds>`')
    anti_duplicate.settings[ctx.guild.id]['max_dupes']   = count
    anti_duplicate.settings[ctx.guild.id]['time_window'] = secs
    await ctx.reply(embed=discord.Embed(title='✅ Updated', description=f'Triggers at `{count}` duplicate messages in `{secs}s`', color=COLORS['success']))

@anti_dupe_group.command(name='status')
@is_owner()
async def anti_dupe_status(ctx):
    enabled = anti_duplicate.enabled.get(ctx.guild.id, False)
    s = anti_duplicate.settings[ctx.guild.id]
    embed = discord.Embed(title='📋 Anti-Duplicate Status', color=COLORS['success'] if enabled else COLORS['error'])
    embed.add_field(name='Status',    value='✅ Enabled' if enabled else '❌ Disabled', inline=True)
    embed.add_field(name='Threshold', value=f"{s['max_dupes']}x in {s['time_window']}s", inline=True)
    await ctx.reply(embed=embed)

# ==================== ANTI-EMOJI COMMANDS ====================

@bot.group(name='anti-emoji', invoke_without_command=True)
@is_owner()
async def anti_emoji_group(ctx):
    embed = discord.Embed(title='😀 anti-emoji', color=COLORS['support'])
    embed.add_field(name='control',  value='`$anti-emoji enable/disable/status`',    inline=False)
    embed.add_field(name='settings', value='`$anti-emoji max <number>`',              inline=False)
    await ctx.reply(embed=embed)

@anti_emoji_group.command(name='enable')
@is_owner()
async def anti_emoji_enable(ctx):
    if anti_emoji.enabled.get(ctx.guild.id): return await ctx.reply('❌ Already enabled')
    anti_emoji.enabled[ctx.guild.id] = True
    await ctx.reply(embed=discord.Embed(title='✅ Anti-Emoji Enabled', color=COLORS['success']))

@anti_emoji_group.command(name='disable')
@is_owner()
async def anti_emoji_disable(ctx):
    if not anti_emoji.enabled.get(ctx.guild.id): return await ctx.reply('❌ Already disabled')
    anti_emoji.enabled[ctx.guild.id] = False
    await ctx.reply(embed=discord.Embed(title='✅ Anti-Emoji Disabled', color=COLORS['support']))

@anti_emoji_group.command(name='max')
@is_owner()
async def anti_emoji_max(ctx, n: int = None):
    if not n or n < 1: return await ctx.reply('❌ Usage: `$anti-emoji max <number>`')
    anti_emoji.settings[ctx.guild.id]['max_emojis'] = n
    await ctx.reply(embed=discord.Embed(title='✅ Updated', description=f'Max emojis per message: `{n}`', color=COLORS['success']))

@anti_emoji_group.command(name='status')
@is_owner()
async def anti_emoji_status(ctx):
    enabled = anti_emoji.enabled.get(ctx.guild.id, False)
    embed = discord.Embed(title='😀 Anti-Emoji Status', color=COLORS['success'] if enabled else COLORS['error'])
    embed.add_field(name='Status', value='✅ Enabled' if enabled else '❌ Disabled', inline=True)
    embed.add_field(name='Max Emojis', value=str(anti_emoji.settings[ctx.guild.id]['max_emojis']), inline=True)
    await ctx.reply(embed=embed)

# ==================== ANTI-INVITE COMMANDS ====================

@bot.group(name='anti-invite', invoke_without_command=True)
@is_owner()
async def anti_invite_group(ctx):
    embed = discord.Embed(title='📨 anti-invite', color=COLORS['support'])
    embed.add_field(name='control',  value='`$anti-invite enable/disable/status`', inline=False)
    embed.add_field(name='detects',  value='Discord server invite links (discord.gg/...)', inline=False)
    await ctx.reply(embed=embed)

@anti_invite_group.command(name='enable')
@is_owner()
async def anti_invite_enable(ctx):
    if anti_invite.enabled.get(ctx.guild.id): return await ctx.reply('❌ Already enabled')
    anti_invite.enabled[ctx.guild.id] = True
    await ctx.reply(embed=discord.Embed(title='✅ Anti-Invite Enabled', color=COLORS['success']))

@anti_invite_group.command(name='disable')
@is_owner()
async def anti_invite_disable(ctx):
    if not anti_invite.enabled.get(ctx.guild.id): return await ctx.reply('❌ Already disabled')
    anti_invite.enabled[ctx.guild.id] = False
    await ctx.reply(embed=discord.Embed(title='✅ Anti-Invite Disabled', color=COLORS['support']))

@anti_invite_group.command(name='status')
@is_owner()
async def anti_invite_status(ctx):
    enabled = anti_invite.enabled.get(ctx.guild.id, False)
    embed = discord.Embed(title='📨 Anti-Invite Status', color=COLORS['success'] if enabled else COLORS['error'])
    embed.add_field(name='Status', value='✅ Enabled' if enabled else '❌ Disabled', inline=True)
    await ctx.reply(embed=embed)

# ==================== AUTOMOD COMMANDS ====================

@bot.group(name='automod', invoke_without_command=True)
@is_owner()
async def automod_group(ctx):
    embed = discord.Embed(title='🤖 automod', color=COLORS['support'])
    embed.add_field(name='control', value='`$automod enable/disable/status`',         inline=False)
    embed.add_field(name='words',   value='`$automod add <word>` `$automod remove <word>` `$automod list`', inline=False)
    await ctx.reply(embed=embed)

@automod_group.command(name='enable')
@is_owner()
async def automod_enable(ctx):
    if automod.enabled.get(ctx.guild.id): return await ctx.reply('❌ Already enabled')
    automod.enabled[ctx.guild.id] = True
    await ctx.reply(embed=discord.Embed(title='✅ AutoMod Enabled', description='Filters bad words — staff bypass active', color=COLORS['success']))

@automod_group.command(name='disable')
@is_owner()
async def automod_disable(ctx):
    if not automod.enabled.get(ctx.guild.id): return await ctx.reply('❌ Already disabled')
    automod.enabled[ctx.guild.id] = False
    await ctx.reply(embed=discord.Embed(title='✅ AutoMod Disabled', color=COLORS['support']))

@automod_group.command(name='add')
@is_owner()
async def automod_add(ctx, *, word: str = None):
    if not word: return await ctx.reply('❌ Usage: `$automod add <word>`')
    if automod.add_word(ctx.guild.id, word):
        await ctx.reply(embed=discord.Embed(title='✅ Word Added', description=f'Added ||{word}|| to filter', color=COLORS['success']))
    else:
        await ctx.reply('❌ Already in filter')

@automod_group.command(name='remove')
@is_owner()
async def automod_remove(ctx, *, word: str = None):
    if not word: return await ctx.reply('❌ Usage: `$automod remove <word>`')
    if automod.remove_word(ctx.guild.id, word):
        await ctx.reply(embed=discord.Embed(title='✅ Word Removed', color=COLORS['success']))
    else:
        await ctx.reply('❌ Not in filter')

@automod_group.command(name='list')
@is_owner()
async def automod_list(ctx):
    words = automod.bad_words.get(ctx.guild.id, [])
    if not words: return await ctx.reply('No words in filter')
    spoilered = ' '.join(f'||{w}||' for w in words[:30])
    embed = discord.Embed(title='🤖 AutoMod Filter', description=spoilered, color=COLORS['support'])
    if len(words) > 30:
        embed.set_footer(text=f'+{len(words)-30} more words')
    await ctx.reply(embed=embed)

@automod_group.command(name='status')
@is_owner()
async def automod_status(ctx):
    enabled = automod.enabled.get(ctx.guild.id, False)
    embed = discord.Embed(title='🤖 AutoMod Status', color=COLORS['success'] if enabled else COLORS['error'])
    embed.add_field(name='Status',       value='✅ Enabled' if enabled else '❌ Disabled', inline=True)
    embed.add_field(name='Words',        value=str(len(automod.bad_words.get(ctx.guild.id, []))), inline=True)
    embed.add_field(name='Staff Bypass', value='Yes', inline=True)
    await ctx.reply(embed=embed)

# ==================== GIVEAWAY COMMANDS ====================

@bot.group(name='giveaway', aliases=['g'], invoke_without_command=True)
async def giveaway_group(ctx):
    embed = discord.Embed(title='🎉 Giveaway System', color=COLORS['support'])
    embed.add_field(name='Start',  value='`$giveaway start <time> <winners> <prize>`\nExample: `$g start 1h 3 Nitro`', inline=False)
    embed.add_field(name='Manage', value='`$giveaway end <msg_id>`\n`$giveaway reroll <msg_id>`\n`$giveaway list`', inline=False)
    await ctx.reply(embed=embed)

@giveaway_group.command(name='start', aliases=['create'])
@commands.has_permissions(manage_guild=True)
async def giveaway_start(ctx, duration: str, winners: int, *, prize: str):
    if not (1 <= winners <= 20): return await ctx.reply('❌ Winners must be 1–20')
    dur = parse_duration(duration)
    if not dur:           return await ctx.reply('❌ Invalid duration. Examples: `1h` `30m` `1d` `2h30m`')
    if dur.total_seconds() < 60:       return await ctx.reply('❌ Minimum duration is 1 minute')
    if dur.total_seconds() > 604800:   return await ctx.reply('❌ Maximum duration is 7 days')
    try: await ctx.message.delete()
    except: pass
    gid = await giveaway.create(ctx, dur, winners, prize)

    async def auto_end():
        await asyncio.sleep(dur.total_seconds())
        await giveaway.end(bot, gid)

    asyncio.create_task(auto_end())

@giveaway_group.command(name='end')
@commands.has_permissions(manage_guild=True)
async def giveaway_end(ctx, message_id: int):
    winners, err = await giveaway.end(bot, message_id)
    if err:     return await ctx.reply(f'❌ {err}')
    await ctx.reply(f'✅ Giveaway ended — {len(winners)} winner(s) picked')

@giveaway_group.command(name='reroll')
@commands.has_permissions(manage_guild=True)
async def giveaway_reroll(ctx, message_id: int):
    winners, err = await giveaway.end(bot, message_id, reroll=True)
    if err:     return await ctx.reply(f'❌ {err}')
    await ctx.send(f'🎉 **REROLL** — New winner(s): {" ".join(w.mention for w in winners)}!')

@giveaway_group.command(name='list')
async def giveaway_list(ctx):
    active = [g for g in giveaway.active.values() if not g['ended'] and g['guild_id'] == ctx.guild.id]
    if not active: return await ctx.reply('No active giveaways')
    embed = discord.Embed(title='🎉 Active Giveaways', color=COLORS['support'])
    for g in active[:10]:
        ch = ctx.guild.get_channel(g['channel_id'])
        embed.add_field(
            name=g['prize'],
            value=f"Channel: {ch.mention if ch else 'Unknown'}\nEnds: <t:{int(g['end_time'].timestamp())}:R>\nWinners: {g['winners']}",
            inline=False
        )
    await ctx.reply(embed=embed)

# ==================== UTILITY COMMANDS ====================

@bot.command(name='afk')
async def afk_cmd(ctx, *, reason: str = 'AFK'):
    original = ctx.author.display_name
    if original.startswith('[AFK] '):
        original = original[6:]
    afk_users[ctx.author.id] = {
        'reason':        reason,
        'time':          datetime.now(timezone.utc),
        'original_nick': original,
    }
    try:
        new_nick = f'[AFK] {original}'[:32]
        await ctx.author.edit(nick=new_nick, reason='AFK')
    except: pass
    await ctx.reply(f'✅ AFK set: {reason}', delete_after=5)


@bot.command(name='afkoff')
async def afkoff_cmd(ctx):
    if ctx.author.id not in afk_users:
        return await ctx.reply('❌ You are not AFK', delete_after=5)
    original = afk_users.pop(ctx.author.id).get('original_nick')
    try:
        if ctx.author.display_name.startswith('[AFK]'):
            await ctx.author.edit(nick=original if original else None, reason='AFK removed')
    except: pass
    await ctx.reply('✅ Welcome back!', delete_after=5)


@bot.command(name='snipe', aliases=['sn'])
async def snipe_cmd(ctx):
    data = snipe_data.get(ctx.channel.id)
    if not data:
        return await ctx.reply(embed=discord.Embed(title='🔍 Nothing to Snipe', description='No recently deleted messages', color=COLORS['support']))
    embed = discord.Embed(title='🔍 Sniped Message', description=data['content'], color=COLORS['support'])
    embed.set_author(name=data['author'], icon_url=data['avatar'])
    embed.set_footer(text=f"Deleted at {data.get('time', 'unknown')}")
    await ctx.reply(embed=embed)


@bot.command(name='editsnipe', aliases=['es'])
async def editsnipe_cmd(ctx):
    data = edit_snipe_data.get(ctx.channel.id)
    if not data:
        return await ctx.reply(embed=discord.Embed(title='🔍 Nothing to Snipe', description='No recently edited messages', color=COLORS['support']))
    embed = discord.Embed(title='✏️ Edit Sniped', color=COLORS['support'])
    embed.add_field(name='Before', value=data['before'] or '*(empty)*', inline=False)
    embed.add_field(name='After',  value=data['after']  or '*(empty)*', inline=False)
    embed.set_author(name=data['author'], icon_url=data['avatar'])
    await ctx.reply(embed=embed)


@bot.command(name='avatar', aliases=['av'])
async def avatar_cmd(ctx, member: discord.Member = None):
    member = member or ctx.author
    embed = discord.Embed(title=f"🖼️ {member.display_name}'s Avatar", color=COLORS['support'])
    embed.set_image(url=member.display_avatar.url)
    await ctx.reply(embed=embed)


@bot.command(name='banner', aliases=['bn'])
async def banner_cmd(ctx, member: discord.Member = None):
    member = member or ctx.author
    user = await bot.fetch_user(member.id)
    if not user.banner:
        return await ctx.reply(embed=discord.Embed(title='❌ No Banner', description=f'{member.mention} has no banner', color=COLORS['error']))
    embed = discord.Embed(title=f"🖼️ {member.display_name}'s Banner", color=COLORS['support'])
    embed.set_image(url=user.banner.url)
    await ctx.reply(embed=embed)


@bot.command(name='userinfo', aliases=['ui'])
async def userinfo_cmd(ctx, member: discord.Member = None):
    member  = member or ctx.author
    now     = datetime.now(timezone.utc)
    created = member.created_at.strftime('%b %d, %Y')
    joined  = member.joined_at.strftime('%b %d, %Y') if member.joined_at else 'Unknown'
    acc_age = (now - member.created_at).days
    join_age = (now - member.joined_at).days if member.joined_at else 0
    roles   = [r.mention for r in member.roles if r.id != ctx.guild.id]
    embed = discord.Embed(title=f'👤 {member.display_name}', color=member.color if member.color.value else COLORS['support'])
    embed.set_thumbnail(url=member.display_avatar.url)
    embed.add_field(name='Username',       value=str(member),            inline=True)
    embed.add_field(name='ID',             value=str(member.id),         inline=True)
    embed.add_field(name='Bot',            value='Yes' if member.bot else 'No', inline=True)
    embed.add_field(name='Account Created', value=f'{created}\n{acc_age}d ago', inline=True)
    embed.add_field(name='Joined Server',  value=f'{joined}\n{join_age}d ago',  inline=True)
    embed.add_field(name='Top Role',       value=member.top_role.mention, inline=True)
    embed.add_field(name=f'Roles ({len(roles)})', value=' '.join(roles[-10:]) or 'None', inline=False)
    await ctx.reply(embed=embed)


@bot.command(name='serverinfo', aliases=['si'])
async def serverinfo_cmd(ctx):
    g = ctx.guild
    bots   = sum(1 for m in g.members if m.bot)
    humans = g.member_count - bots
    embed = discord.Embed(title=g.name, color=COLORS['support'])
    if g.icon: embed.set_thumbnail(url=g.icon.url)
    embed.add_field(name='Owner',   value=g.owner.mention if g.owner else 'Unknown', inline=True)
    embed.add_field(name='ID',      value=str(g.id),              inline=True)
    embed.add_field(name='Created', value=g.created_at.strftime('%b %d, %Y'), inline=True)
    embed.add_field(name='Members', value=f'{g.member_count} ({humans} humans, {bots} bots)', inline=True)
    embed.add_field(name='Channels', value=f'{len(g.text_channels)} text, {len(g.voice_channels)} voice', inline=True)
    embed.add_field(name='Roles',   value=str(len(g.roles)), inline=True)
    embed.add_field(name='Boost',   value=f'Level {g.premium_tier} ({g.premium_subscription_count} boosts)', inline=True)
    embed.add_field(name='Emojis',  value=f'{len(g.emojis)}/{g.emoji_limit}', inline=True)
    await ctx.reply(embed=embed)


@bot.command(name='roleinfo', aliases=['ri'])
async def roleinfo_cmd(ctx, role: discord.Role = None):
    if not role: return await ctx.reply('❌ Usage: `$roleinfo @Role`')
    count = sum(1 for m in ctx.guild.members if role in m.roles)
    embed = discord.Embed(title=role.name, color=role.color if role.color.value else COLORS['support'])
    embed.add_field(name='ID',          value=str(role.id),   inline=True)
    embed.add_field(name='Color',       value=str(role.color), inline=True)
    embed.add_field(name='Members',     value=str(count),      inline=True)
    embed.add_field(name='Created',     value=role.created_at.strftime('%b %d, %Y'), inline=True)
    embed.add_field(name='Position',    value=str(role.position), inline=True)
    embed.add_field(name='Mentionable', value='Yes' if role.mentionable else 'No', inline=True)
    embed.add_field(name='Hoisted',     value='Yes' if role.hoist else 'No', inline=True)
    embed.add_field(name='Managed',     value='Yes' if role.managed else 'No', inline=True)
    await ctx.reply(embed=embed)


@bot.command(name='membercount', aliases=['mc'])
async def membercount_cmd(ctx):
    bots   = sum(1 for m in ctx.guild.members if m.bot)
    humans = ctx.guild.member_count - bots
    embed = discord.Embed(title=f'{ctx.guild.name} — Members', color=COLORS['support'])
    embed.add_field(name='Total',  value=str(ctx.guild.member_count), inline=True)
    embed.add_field(name='Humans', value=str(humans),                 inline=True)
    embed.add_field(name='Bots',   value=str(bots),                   inline=True)
    await ctx.reply(embed=embed)


@bot.command(name='botinfo', aliases=['bi'])
async def botinfo_cmd(ctx):
    uptime  = datetime.now(timezone.utc) - bot_start_time
    h, rem  = divmod(int(uptime.total_seconds()), 3600)
    m, s    = divmod(rem, 60)
    latency = round(bot.latency * 1000)
    embed = discord.Embed(title='🤖 Bot Info', color=COLORS['support'])
    embed.set_thumbnail(url=bot.user.display_avatar.url)
    embed.add_field(name='Ping',    value=f'{latency}ms', inline=True)
    embed.add_field(name='Uptime',  value=f'{h}h {m}m {s}s', inline=True)
    embed.add_field(name='Servers', value=str(len(bot.guilds)), inline=True)
    embed.add_field(name='Users',   value=str(len(set(bot.get_all_members()))), inline=True)
    await ctx.reply(embed=embed)


@bot.command(name='ping')
async def ping_cmd(ctx):
    latency = round(bot.latency * 1000)
    color = COLORS['success'] if latency < 200 else COLORS['error']
    await ctx.reply(embed=discord.Embed(title='🏓 Pong', description=f'**{latency}ms**', color=color))


@bot.command(name='say')
@is_owner()
async def say_cmd(ctx, channel: discord.TextChannel = None, *, message: str = None):
    """Send a plain message as the bot to a channel"""
    if not message:
        return await ctx.reply('❌ Usage: `$say [#channel] <message>`')
    target = channel or ctx.channel
    try:
        await ctx.message.delete()
    except: pass
    await target.send(message)


@bot.command(name='embedsay', aliases=['esay'])
@is_owner()
async def embedsay_cmd(ctx, channel: discord.TextChannel = None, *, text: str = None):
    """Send a formatted embed as the bot. Format: title | description | color(hex optional)"""
    if not text:
        return await ctx.reply('❌ Usage: `$embedsay [#channel] <title> | <description> | <#hexcolor optional>`\nExample: `$embedsay #general Announcement | Server is live! | #57F287`')
    parts = [p.strip() for p in text.split('|')]
    title = parts[0] if len(parts) > 0 else None
    description = parts[1] if len(parts) > 1 else None
    color = COLORS['support']
    if len(parts) > 2:
        hex_str = parts[2].strip().lstrip('#')
        try:
            color = int(hex_str, 16)
        except ValueError:
            pass
    if not title and not description:
        return await ctx.reply('❌ Provide at least a title or description')
    target = channel or ctx.channel
    embed = discord.Embed(color=color)
    if title:
        embed.title = title
    if description:
        embed.description = description
    try:
        await ctx.message.delete()
    except: pass
    await target.send(embed=embed)




class HelpView(View):
    def __init__(self, pages: list, author_id: int):
        super().__init__(timeout=180)
        self.pages      = pages
        self.page       = 0
        self.author_id  = author_id
        self._update_buttons()

    def _update_buttons(self):
        self.children[0].disabled = (self.page == 0)
        self.children[1].disabled = (self.page == len(self.pages) - 1)

    @discord.ui.button(label='◀ Prev', style=discord.ButtonStyle.secondary)
    async def prev_btn(self, interaction: discord.Interaction, _):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message('❌ Not your help menu', ephemeral=True)
        self.page -= 1
        self._update_buttons()
        await interaction.response.edit_message(embed=self.pages[self.page], view=self)

    @discord.ui.button(label='Next ▶', style=discord.ButtonStyle.secondary)
    async def next_btn(self, interaction: discord.Interaction, _):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message('❌ Not your help menu', ephemeral=True)
        self.page += 1
        self._update_buttons()
        await interaction.response.edit_message(embed=self.pages[self.page], view=self)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True



@bot.command(name='help')
async def help_cmd(ctx):
    pages = []

    e1 = discord.Embed(title='🎫 Ticket Commands', description='**Requires:** Staff or Middleman role', color=COLORS['support'])
    e1.add_field(name='Manage',    value='`$close` — close & transcript ticket\n`$claim` — claim a ticket\n`$unclaim` — release your claim', inline=False)
    e1.add_field(name='Users',     value='`$add @user` — add user to ticket\n`$remove @user` — remove user from ticket', inline=False)
    e1.add_field(name='Edit',      value='`$rename <n>` — rename ticket channel\n`$transfer @user` — transfer claim to another staff', inline=False)
    e1.add_field(name='Proof',     value='`$proof` — post trade completion to proof channel', inline=False)
    e1.set_footer(text='Page 1/12  •  Use ◀ ▶ to navigate')
    pages.append(e1)

    e2 = discord.Embed(title='🛠️ Utility Commands', description='**Usable by:** Everyone', color=COLORS['support'])
    e2.add_field(name='💬 AFK',    value='`$afk <reason>` — go AFK (adds [AFK] to name)\n`$afkoff` — manually remove AFK', inline=False)
    e2.add_field(name='📊 Info',   value='`$userinfo [@user]` / `$ui`\n`$serverinfo` / `$si`\n`$roleinfo @role` / `$ri`\n`$membercount` / `$mc`\n`$botinfo` / `$bi`\n`$ping`', inline=False)
    e2.add_field(name='🖼️ Media',  value='`$avatar [@user]` / `$av`\n`$banner [@user]` / `$bn`', inline=False)
    e2.add_field(name='👀 Snipe',  value='`$snipe` / `$sn` — last deleted message\n`$editsnipe` / `$es` — last edited message', inline=False)
    e2.set_footer(text='Page 2/12')
    pages.append(e2)

    e3 = discord.Embed(title='⚙️ Setup Commands', description='**Requires:** Administrator', color=COLORS['support'])
    e3.add_field(name='🎫 Tickets',   value='`$setup` — post ticket panel\n`$setcategory #category`\n`$setlogs #channel`\n`$config` — view server config', inline=False)
    e3.add_field(name='🚔 Jail',      value='`$jail @user <reason>`\n`$unjail @user`\n`$jailed` — list all jailed users', inline=False)
    e3.add_field(name='🚫 Blacklist', value='`$blacklist @user <reason>`\n`$unblacklist @user`\n`$blacklists` — list all blacklisted', inline=False)
    e3.add_field(name='🧹 Cleanup',   value='`$clear <amount>`\n`$purge <amount> [@user]`', inline=False)
    e3.set_footer(text='Page 3/12')
    pages.append(e3)

    e4 = discord.Embed(title='🛡️ Moderation', description='**Requires:** Mod role', color=COLORS['support'])
    e4.add_field(name='🔇 Mute',    value='`$mute @user <time> <reason>` / `$m` (10s/5m/1h/2d)\n`$unmute @user` / `$um`', inline=False)
    e4.add_field(name='⚠️ Warn',    value='`$warn @user <reason>` / `$w`\n`$warnings @user` / `$ws`\n`$clearwarnings @user` / `$cw`', inline=False)
    e4.add_field(name='✏️ Nick',    value='`$nick @user <n>` / `$n`\n`$nick @user reset` — reset nickname', inline=False)
    e4.set_footer(text='Page 4/12')
    pages.append(e4)

    e5 = discord.Embed(title='🔨 Admin Commands', description='**Requires:** Admin role', color=COLORS['support'])
    e5.add_field(name='🚪 Ban/Kick', value='`$ban @user <reason>` / `$b`\n`$unban <id>` / `$ub`\n`$hackban <id> <reason>` / `$hb`\n`$unhackban <id>` / `$uhb`\n`$kick @user <reason>` / `$k`', inline=False)
    e5.add_field(name='🎭 Role',     value='`$role @user <rolename>` / `$r` — add or remove role', inline=False)
    e5.add_field(name='🔒 Channel',  value='`$slowmode <secs>` / `$sm`\n`$lock` / `$lk` — lock channel\n`$unlock` / `$ulk`\n`$hide` / `$hd`\n`$unhide` / `$uhd`', inline=False)
    e5.add_field(name='📢 Say',      value='`$say [#channel] <message>` — send plain message as bot\n`$embedsay [#channel] <title> | <desc> | <#color>` / `$esay`', inline=False)
    e5.set_footer(text='Page 5/12')
    pages.append(e5)

    e6 = discord.Embed(title='👑 Owner Commands', description='**Requires:** Owner', color=COLORS['support'])
    e6.add_field(name='Permission Roles', value='`$adminperms set @role` / `$ap set`\n`$adminperms show`\n`$modperms set @role` / `$mp set`\n`$modperms show`', inline=False)
    e6.add_field(name='Lockdown',         value='`$lockdown` — lock every channel\n`$unlockdown` — unlock everything', inline=False)
    e6.add_field(name='Channel Perms',    value='`$channelperm #ch @target enable/disable <perm>`\n`$channelpermall @target enable/disable <perm>`', inline=False)
    e6.set_footer(text='Page 6/12')
    pages.append(e6)

    e7 = discord.Embed(title='🎉 Giveaway Commands', description='**Requires:** Admin role', color=COLORS['support'])
    e7.add_field(name='Start',  value='`$giveaway start <time> <winners> <prize>` / `$g start`\nExample: `$g start 1h 3 1000 Robux`', inline=False)
    e7.add_field(name='Manage', value='`$giveaway end <msg_id>` / `$g end`\n`$giveaway reroll <msg_id>` / `$g reroll`\n`$giveaway list` / `$g list`', inline=False)
    e7.set_footer(text='Page 7/12')
    pages.append(e7)

    e8 = discord.Embed(title='🛡️ Anti-Nuke & Anti-Raid', description='**Requires:** Owner', color=COLORS['support'])
    e8.add_field(name='🔒 Anti-Nuke', value='`$anti-nuke enable` / `disable` / `status`\nDetects:\n• Unauthorized ban → strip & shame the banner (staff exempt)\n• Unauthorized kick → strip & shame the kicker (staff exempt)\n• 3+ channel/category deletes → strip & shame the deleter\n• Unauthorized webhook → **deleted**, creator stripped & shamed\n• Unauthorized bot added → **banned**, adder stripped & shamed\nPings owner on every trigger', inline=False)
    e8.add_field(name='🚨 Anti-Raid', value='`$anti-raid enable` / `disable` / `status`\n`$anti-raid action <kick/ban>`\n`$anti-raid accountage <days>`\n`$anti-raid avatar <on/off>`', inline=False)
    e8.set_footer(text='Page 8/12')
    pages.append(e8)

    e9 = discord.Embed(title='🛡️ Anti-Spam & Anti-Link', description='**Requires:** Owner', color=COLORS['support'])
    e9.add_field(name='💬 Anti-Spam',   value='`$anti-spam enable` / `disable` / `status`\n`$anti-spam threshold <msgs> <secs>`\nAuto-escalating timeouts for floods & mass mentions', inline=False)
    e9.add_field(name='🔗 Anti-Link',   value='`$anti-link enable` / `disable` / `status`\n`$anti-link role @role` — whitelist role\n`$anti-link url <url>` — whitelist URL', inline=False)
    e9.add_field(name='📨 Anti-Invite', value='`$anti-invite enable` / `disable` / `status`\nBlocks discord.gg invite links', inline=False)
    e9.set_footer(text='Page 9/12')
    pages.append(e9)

    e10 = discord.Embed(title='🛡️ Anti-React, Anti-Ghost & Anti-Caps', description='**Requires:** Owner', color=COLORS['support'])
    e10.add_field(name='😤 Anti-React',      value='`$anti-react enable` / `disable` / `status`\n`$anti-react action <warn/timeout/kick>`\nDetects reaction spam across multiple messages', inline=False)
    e10.add_field(name='👻 Anti-Ghost-Ping', value='`$anti-ghost enable` / `disable` / `status`\n`$anti-ghost action <warn/timeout/kick>`\n`$anti-ghost minpings <n>`\nDetects deleted messages with mentions', inline=False)
    e10.add_field(name='🔠 Anti-Caps',       value='`$anti-caps enable` / `disable` / `status`\n`$anti-caps percent <0-100>`\nDeletes messages that are mostly capitals', inline=False)
    e10.set_footer(text='Page 10/12')
    pages.append(e10)

    e11 = discord.Embed(title='🛡️ Anti-Dupe, Anti-Emoji & AutoMod', description='**Requires:** Owner', color=COLORS['support'])
    e11.add_field(name='📋 Anti-Duplicate', value='`$anti-dupe enable` / `disable` / `status`\n`$anti-dupe threshold <count> <secs>`\nBlocks repeated identical messages', inline=False)
    e11.add_field(name='😀 Anti-Emoji',     value='`$anti-emoji enable` / `disable` / `status`\n`$anti-emoji max <number>`\nBlocks too many emojis per message', inline=False)
    e11.add_field(name='🤖 AutoMod',        value='`$automod enable` / `disable` / `status`\n`$automod add <word>` / `remove <word>` / `list`\nFilters bad words, staff bypass active', inline=False)
    e11.set_footer(text='Page 11/12')
    pages.append(e11)

    e12 = discord.Embed(title='⚪ Whitelist System', description='**Requires:** Owner — exempt roles/users from anti-systems', color=COLORS['support'])
    e12.add_field(name='Commands', value='`$whitelist <system> @role/@user`\n`$unwhitelist <system> @role/@user`\n`$whitelisted <system>`', inline=False)
    e12.add_field(name='Systems',  value='`anti-link` `anti-spam` `anti-nuke` `anti-raid`\n`anti-react` `anti-caps` `anti-dupe` `anti-emoji`\n`anti-invite` `anti-ghost`', inline=False)
    e12.add_field(name='Example',  value='`$whitelist anti-link @Staff`\n`$whitelist anti-spam @John`', inline=False)
    e12.set_footer(text='Page 12/12')
    pages.append(e12)

    await ctx.reply(embed=pages[0], view=HelpView(pages, ctx.author.id))


# ==================== EVENTS ====================

@bot.event
async def on_ready():
    global bot_start_time
    bot_start_time = datetime.now(timezone.utc)
    anti_nuke.bot_start_time = bot_start_time
    logger.info(f'Logged in as {bot.user} (ID: {bot.user.id})')
    try:
        await db.connect()
    except Exception as e:
        logger.error(f'Database connection failed: {e}')
        return
    bot.add_view(TicketPanelView())
    bot.add_view(TicketControlView())
    await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name='$help'))
    logger.info(f'Ready — serving {len(bot.guilds)} guild(s)')


@bot.event
async def on_message_delete(message: discord.Message):
    if message.author.bot:
        return
    # Anti-ghost-ping
    if message.guild and anti_ghost_ping.enabled.get(message.guild.id):
        mentions = len(message.mentions) + len(message.role_mentions)
        threshold = anti_ghost_ping.settings[message.guild.id]['min_mentions']
        if mentions >= threshold and not anti_ghost_ping.is_whitelisted(message.author):
            action = anti_ghost_ping.settings[message.guild.id]['action']
            try:
                await message.channel.send(
                    f'👻 {message.author.mention} ghost-pinged {mentions} user(s)',
                    delete_after=8
                )
                if action == 'timeout':
                    await message.author.timeout(timedelta(minutes=5), reason='anti-ghost-ping')
                elif action == 'kick':
                    await message.author.kick(reason='anti-ghost-ping')
                await send_log(
                    message.guild, '👻 ANTI-GHOST-PING',
                    f'{message.author.mention} ghost-pinged {mentions} user(s)',
                    COLORS['error'],
                    {'Channel': message.channel.mention, 'Mentions': str(mentions)}
                )
            except Exception as e:
                logger.error(f'anti-ghost-ping: {e}')
    # Snipe data
    if message.content:
        snipe_data[message.channel.id] = {
            'content': message.content[:1000],
            'author':  str(message.author),
            'avatar':  message.author.display_avatar.url,
            'time':    datetime.now(timezone.utc).strftime('%H:%M:%S'),
        }


@bot.event
async def on_message_edit(before: discord.Message, after: discord.Message):
    if before.author.bot:
        return
    if before.content != after.content:
        edit_snipe_data[before.channel.id] = {
            'before': before.content[:500],
            'after':  after.content[:500],
            'author': str(before.author),
            'avatar': before.author.display_avatar.url,
        }


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or not message.guild:
        return

    author = message.author

    # --- Admin bypass: skip all anti-systems for admin role and above (anti-nuke still applies separately via events) ---
    def _is_admin_or_above(member: discord.Member) -> bool:
        if member.id == OWNER_ID: return True
        if member.guild_permissions.administrator: return True
        admin_id = admin_perms.get(member.guild.id)
        if admin_id:
            admin_role = member.guild.get_role(admin_id)
            if admin_role and any(r >= admin_role for r in member.roles):
                return True
        return False

    author_is_admin = _is_admin_or_above(author)

    # --- AutoMod bad-word filter (runs first, returns early) ---
    if not author_is_admin and automod.enabled.get(message.guild.id):
        has_bad, _ = automod.check_message(message)
        if has_bad:
            try:
                await message.delete()
                await message.channel.send(f'{author.mention} watch your language', delete_after=5)
                await send_log(message.guild, '🚨 AUTOMOD', f'deleted message from {author.mention}',
                               COLORS['error'], {'Channel': message.channel.mention})
            except: pass
            return

    # --- AFK: auto-remove on message ---
    if author.id in afk_users:
        original = afk_users.pop(author.id).get('original_nick')
        try:
            if author.display_name.startswith('[AFK]'):
                restore_to = original if original else author.display_name.replace('[AFK] ', '', 1)
                await author.edit(nick=restore_to if restore_to != author.name else None)
            msg = await message.channel.send(
                embed=discord.Embed(description=f'Welcome back {author.mention}, AFK removed', color=COLORS['success'])
            )
            await asyncio.sleep(5)
            await msg.delete()
        except: pass

    # --- AFK: notify on mention ---
    for mentioned in message.mentions:
        if mentioned.id in afk_users:
            info = afk_users[mentioned.id]
            try:
                await message.channel.send(
                    embed=discord.Embed(description=f'{mentioned.mention} is AFK: {info["reason"]}', color=COLORS['support']),
                    delete_after=8
                )
            except: pass

    # --- Anti-emoji ---
    if not author_is_admin and anti_emoji.enabled.get(message.guild.id) and not anti_emoji.is_whitelisted(author):
        if anti_emoji.is_emoji_spam(message.guild.id, message.content):
            try:
                await message.delete()
                await message.channel.send(f'{author.mention} too many emojis', delete_after=4)
            except: pass
            return

    # --- Anti-caps ---
    if not author_is_admin and anti_caps.enabled.get(message.guild.id) and not anti_caps.is_whitelisted(author):
        if anti_caps.is_caps_spam(message.guild.id, message.content):
            try:
                await message.delete()
                await message.channel.send(f'{author.mention} please stop using all caps', delete_after=4)
                await send_log(message.guild, '🔠 ANTI-CAPS', f'deleted caps message from {author.mention}',
                               COLORS['warning'], {'Channel': message.channel.mention})
            except: pass
            return

    # --- Anti-duplicate ---
    if not author_is_admin and anti_duplicate.enabled.get(message.guild.id) and not anti_duplicate.is_whitelisted(author):
        if anti_duplicate.check(message.guild.id, author.id, message.content):
            try:
                await message.delete()
                await message.channel.send(f'{author.mention} stop sending duplicate messages', delete_after=4)
            except: pass
            return


    # --- Ticket filter: strict talk permissions ---
    if message.channel.name.startswith('ticket-'):
        try:
            async with db.pool.acquire() as conn:
                ticket = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id=$1', message.channel.id)
            if ticket and author.id != OWNER_ID:
                claimed = ticket.get('claimed_by')

                if not claimed:
                    # Unclaimed: only staff/tier role can talk — creator cannot
                    has_role_perm = False
                    if ticket['ticket_type'] == 'support':
                        staff_role = message.guild.get_role(HARDCODED_ROLES['staff'])
                        if staff_role and staff_role in author.roles:
                            has_role_perm = True
                    elif ticket['ticket_type'] == 'middleman':
                        tier_role = message.guild.get_role(HARDCODED_ROLES.get(ticket.get('tier', '')))
                        if tier_role and tier_role in author.roles:
                            has_role_perm = True
                    if not has_role_perm:
                        try:
                            await message.delete()
                            if author.id == ticket['user_id']:
                                await message.channel.send(f'{author.mention} wait for a staff member to claim your ticket before talking', delete_after=4)
                            else:
                                await message.channel.send(f'{author.mention} you cannot talk in this ticket', delete_after=3)
                        except: pass
                        return
                else:
                    # Claimed: ONLY claimer and explicitly $add'd users — creator still cannot talk unless added
                    ow = message.channel.overwrites_for(author)
                    if author.id != claimed and ow.send_messages is not True:
                        try:
                            await message.delete()
                            await message.channel.send(f'{author.mention} only the claimer and added users can talk in this ticket', delete_after=4)
                        except: pass
                        return
        except Exception as e:
            logger.error(f'Ticket filter: {e}')

    # --- Anti-invite (before anti-link to give specific message) ---
    if not author_is_admin and anti_invite.enabled.get(message.guild.id) and not anti_invite.is_whitelisted(author):
        if anti_invite.is_invite(message.content):
            try:
                await message.delete()
                await message.channel.send(f'{author.mention} server invites are not allowed here', delete_after=4)
                await send_log(message.guild, '📨 ANTI-INVITE', f'deleted invite from {author.mention}',
                               COLORS['error'], {'Channel': message.channel.mention})
            except: pass
            return

    # --- Anti-link ---
    if not author_is_admin and anti_link.enabled.get(message.guild.id) and not anti_link.is_whitelisted(author):
        if anti_link.is_link(message.content) and not anti_link.is_url_whitelisted(message.guild.id, message.content):
            try:
                await message.delete()
                await message.channel.send(f'{author.mention} links are not allowed', delete_after=4)
                await send_log(message.guild, '🔗 ANTI-LINK', f'deleted link from {author.mention}',
                               COLORS['error'], {'Channel': message.channel.mention})
            except: pass
            return

    # --- Anti-spam ---
    if not author_is_admin and anti_spam.enabled.get(message.guild.id) and not anti_spam.is_whitelisted(author):
        anti_spam.add_message(message.guild.id, author.id)
        if anti_spam.is_spam(message.guild.id, author.id):
            try:
                await message.delete()
                dur  = anti_spam.get_timeout_duration(message.guild.id, author.id)
                mins = max(1, int(dur.total_seconds() / 60))
                await author.timeout(dur, reason='anti-spam: message flood')
                await message.channel.send(f'{author.mention} timed out for {mins}min for spamming', delete_after=5)
                await send_log(message.guild, '🚨 ANTI-SPAM', f'timed out {author.mention}',
                               COLORS['error'], {'Duration': f'{mins}min', 'Channel': message.channel.mention})
            except Exception as e:
                logger.error(f'anti-spam: {e}')
            return
        is_bad, reasons = anti_spam.check_message(message)
        if is_bad:
            try:
                await message.delete()
                await message.channel.send(f'{author.mention} {", ".join(reasons)}', delete_after=4)
            except: pass
            return

    await bot.process_commands(message)


@bot.event
async def on_member_join(member: discord.Member):
    if not anti_raid.enabled.get(member.guild.id):
        return
    try:
        anti_raid.add_join(member.guild.id, member.id)
        if anti_raid.is_raid(member.guild.id):
            try:
                await member.kick(reason='anti-raid: join flood')
                await send_log(member.guild, '🚨 ANTI-RAID: Join Flood', f'kicked {member.mention}',
                               COLORS['error'], {'Total Joins': str(len(anti_raid.join_tracking[member.guild.id]))})
            except Exception as e:
                logger.error(f'anti-raid kick: {e}')
            return
        is_sus, reasons = anti_raid.check_member(member)
        if is_sus and not anti_raid.is_whitelisted(member):
            action = anti_raid.settings[member.guild.id]['action']
            try:
                if action == 'kick':   await member.kick(reason=f"anti-raid: {', '.join(reasons)}")
                elif action == 'ban':  await member.ban( reason=f"anti-raid: {', '.join(reasons)}", delete_message_days=0)
                await send_log(member.guild, f'🚨 ANTI-RAID: {action.title()}',
                               f'{action} {member.mention}', COLORS['error'],
                               {'Reasons': ', '.join(reasons)})
            except Exception as e:
                logger.error(f'anti-raid action: {e}')
    except Exception as e:
        logger.error(f'on_member_join: {e}')


@bot.event
async def on_member_ban(guild: discord.Guild, user: discord.User):
    """Strip whoever issued the ban unless they are staff/owner."""
    if not anti_nuke.enabled.get(guild.id): return
    try:
        async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.ban):
            if not anti_nuke.audit_valid(entry, user.id): return
            banner = entry.user
            if banner.id in (OWNER_ID, bot.user.id): return
            banner_member = guild.get_member(banner.id)
            if not banner_member: return
            if anti_nuke.is_whitelisted(banner_member): return
            staff_role = guild.get_role(HARDCODED_ROLES['staff'])
            if staff_role and staff_role in banner_member.roles: return
            await nuke_punish(guild, banner_member, 'unauthorized ban')
            await send_log(guild, '🚨 ANTI-NUKE: Unauthorized Ban',
                f'<@{OWNER_ID}> — {banner_member.mention} stripped & shamed for banning {user}',
                COLORS['error'], {'Perpetrator': str(banner), 'Banned User': str(user)})
            break
    except Exception as e:
        logger.error(f'on_member_ban: {e}')


@bot.event
async def on_member_remove(member: discord.Member):
    """Strip whoever issued the kick unless they are staff/owner."""
    if not anti_nuke.enabled.get(member.guild.id): return
    try:
        await asyncio.sleep(0.5)
        async for entry in member.guild.audit_logs(limit=1, action=discord.AuditLogAction.kick):
            if not anti_nuke.audit_valid(entry, member.id): return
            kicker = entry.user
            if kicker.id in (OWNER_ID, bot.user.id): return
            kicker_member = member.guild.get_member(kicker.id)
            if not kicker_member: return
            if anti_nuke.is_whitelisted(kicker_member): return
            staff_role = member.guild.get_role(HARDCODED_ROLES['staff'])
            if staff_role and staff_role in kicker_member.roles: return
            await nuke_punish(member.guild, kicker_member, 'unauthorized kick')
            await send_log(member.guild, '🚨 ANTI-NUKE: Unauthorized Kick',
                f'<@{OWNER_ID}> — {kicker_member.mention} stripped & shamed for kicking {member}',
                COLORS['error'], {'Perpetrator': str(kicker), 'Kicked User': str(member)})
            break
    except Exception as e:
        logger.error(f'on_member_remove: {e}')


@bot.event
async def on_guild_channel_delete(channel: discord.abc.GuildChannel):
    """Strip whoever deleted 3+ channels/categories."""
    if not anti_nuke.enabled.get(channel.guild.id): return
    try:
        async for entry in channel.guild.audit_logs(limit=1, action=discord.AuditLogAction.channel_delete):
            if not anti_nuke.audit_valid(entry, channel.id): return
            deleter = entry.user
            if deleter.id in (OWNER_ID, bot.user.id): return
            deleter_member = channel.guild.get_member(deleter.id)
            if not deleter_member: return
            if anti_nuke.is_whitelisted(deleter_member): return
            staff_role = channel.guild.get_role(HARDCODED_ROLES['staff'])
            if staff_role and staff_role in deleter_member.roles: return
            count = anti_nuke.add_channel_delete(channel.guild.id, deleter.id)
            if count >= 3:
                await nuke_punish(channel.guild, deleter_member, 'mass channel deletion')
                await send_log(channel.guild, '🚨 ANTI-NUKE: Mass Channel Delete',
                    f'<@{OWNER_ID}> — {deleter_member.mention} stripped & shamed for deleting {count} channels',
                    COLORS['error'], {'Perpetrator': str(deleter), 'Channel': channel.name, 'Count': str(count)})
            break
    except Exception as e:
        logger.error(f'on_guild_channel_delete: {e}')


@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    """Detect unauthorized bot — kick it and strip whoever added it."""
    if not anti_nuke.enabled.get(after.guild.id): return
    if len(after.roles) <= len(before.roles): return
    new_roles = set(after.roles) - set(before.roles)
    for role in new_roles:
        if not role.managed: continue
        try:
            async for entry in after.guild.audit_logs(limit=5, action=discord.AuditLogAction.bot_add):
                if not anti_nuke.is_recent(entry.created_at): continue
                if not anti_nuke.is_after_restart(entry.created_at): continue
                if not (hasattr(entry.target, 'id') and entry.target.id == after.id): continue
                inviter = entry.user
                if inviter.id == OWNER_ID: return
                inviter_member = after.guild.get_member(inviter.id)
                if anti_nuke.is_whitelisted(inviter_member): return
                # Ban/kick the unauthorized bot
                try:
                    await after.guild.ban(after, reason='anti-nuke: unauthorized bot', delete_message_days=0)
                except:
                    try: await after.kick(reason='anti-nuke: unauthorized bot')
                    except: pass
                # Strip and shame whoever added it
                if inviter_member:
                    await nuke_punish(after.guild, inviter_member, 'added unauthorized bot')
                await send_log(after.guild, '🚨 ANTI-NUKE: Unauthorized Bot Added',
                    f'<@{OWNER_ID}> — Bot {after} banned. {inviter_member.mention if inviter_member else inviter} stripped & shamed.',
                    COLORS['error'], {'Bot': str(after), 'Added By': str(inviter)})
                break
        except Exception as e:
            logger.error(f'on_member_update anti-nuke: {e}')


@bot.event
async def on_webhooks_update(channel: discord.TextChannel):
    """Delete unauthorized webhook and strip whoever created it."""
    if not anti_nuke.enabled.get(channel.guild.id): return
    try:
        async for entry in channel.guild.audit_logs(limit=1, action=discord.AuditLogAction.webhook_create):
            if not anti_nuke.is_recent(entry.created_at): return
            if not anti_nuke.is_after_restart(entry.created_at): return
            creator = entry.user
            if creator.id in (OWNER_ID, bot.user.id): return
            creator_member = channel.guild.get_member(creator.id)
            if anti_nuke.is_whitelisted(creator_member): return
            # Delete the webhook
            try:
                webhooks = await channel.webhooks()
                for wh in webhooks:
                    try: await wh.delete(reason='anti-nuke: unauthorized webhook')
                    except: pass
            except: pass
            # Strip and shame whoever created it
            if creator_member:
                await nuke_punish(channel.guild, creator_member, 'unauthorized webhook creation')
            await send_log(channel.guild, '🚨 ANTI-NUKE: Unauthorized Webhook',
                f'<@{OWNER_ID}> — Webhook deleted. {creator_member.mention if creator_member else creator} stripped & shamed.',
                COLORS['error'], {'Channel': channel.mention, 'Created By': str(creator)})
            break
    except Exception as e:
        logger.error(f'on_webhooks_update: {e}')




@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if not payload.guild_id: return
    guild = bot.get_guild(payload.guild_id)
    if not guild or not anti_react.enabled.get(guild.id): return
    member = guild.get_member(payload.user_id)
    if not member or member.bot or anti_react.is_whitelisted(member): return
    # Admin role and above bypass anti-react
    admin_id = admin_perms.get(guild.id)
    if member.guild_permissions.administrator: return
    if admin_id:
        admin_role = guild.get_role(admin_id)
        if admin_role and any(r >= admin_role for r in member.roles): return
    anti_react.add_reaction(guild.id, member.id, payload.message_id)
    if not anti_react.is_react_spam(guild.id, member.id): return
    action    = anti_react.settings[guild.id]['action']
    msg_ids   = anti_react.get_recent_message_ids(guild.id, member.id)
    channel   = guild.get_channel(payload.channel_id)
    try:
        if channel:
            for mid in msg_ids:
                try:
                    m = await channel.fetch_message(mid)
                    await m.clear_reactions()
                except: pass
        if action == 'warn':
            async with db.pool.acquire() as conn:
                await conn.execute('INSERT INTO warnings (guild_id,user_id,reason,warned_by) VALUES ($1,$2,$3,$4)',
                                   guild.id, member.id, 'reaction spam', bot.user.id)
            if channel:
                await channel.send(f'{member.mention} warned for reaction spam', delete_after=5)
        elif action == 'timeout':
            await member.timeout(timedelta(minutes=5), reason='anti-react: reaction spam')
            if channel:
                await channel.send(f'{member.mention} timed out for reaction spam', delete_after=5)
        elif action == 'kick':
            await member.kick(reason='anti-react: reaction spam')
        await send_log(guild, '🚨 ANTI-REACT', f'{action} {member.mention}', COLORS['error'],
                       {'Action': action, 'Messages': str(len(msg_ids))})
    except Exception as e:
        logger.error(f'anti-react: {e}')


# ==================== ERROR HANDLER ====================

@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError):
    if isinstance(error, commands.CommandNotFound):
        return
    if isinstance(error, commands.MissingPermissions):
        perms = ', '.join(error.missing_permissions).replace('_', ' ')
        return await ctx.reply(f'❌ You need **{perms}** permission')
    if isinstance(error, commands.BotMissingPermissions):
        perms = ', '.join(error.missing_permissions).replace('_', ' ')
        return await ctx.reply(f'❌ I need **{perms}** permission')
    if isinstance(error, commands.CheckFailure):
        return await ctx.reply('❌ You do not have permission to use this command')
    if isinstance(error, commands.MissingRequiredArgument):
        return await ctx.reply(f'❌ Missing argument: **{error.param.name}** — use `$help` for usage')
    if isinstance(error, commands.BadArgument):
        return await ctx.reply('❌ Invalid argument — use `$help` for usage')
    if isinstance(error, commands.UserNotFound):
        return await ctx.reply('❌ User not found')
    if isinstance(error, commands.MemberNotFound):
        return await ctx.reply('❌ Member not found')
    if isinstance(error, commands.ChannelNotFound):
        return await ctx.reply('❌ Channel not found')
    if isinstance(error, commands.RoleNotFound):
        return await ctx.reply('❌ Role not found')
    if isinstance(error, commands.CommandOnCooldown):
        return await ctx.reply(f'⏱️ Slow down — try again in {error.retry_after:.1f}s')
    # Unwrap unexpected errors
    err = getattr(error, 'original', error)
    logger.error(f'Unhandled error in {ctx.command}: {err}')

# ==================== WEB SERVER (Render + UptimeRobot) ====================

async def start_web_server():
    """
    Lightweight HTTP server for:
    - Render free tier keep-alive
    - UptimeRobot health checks (ping /health or / every 5 minutes)
    """
    async def health(request):
        uptime = (datetime.now(timezone.utc) - bot_start_time)
        h, rem = divmod(int(uptime.total_seconds()), 3600)
        m, s   = divmod(rem, 60)
        return web.Response(
            text=f'OK — uptime: {h}h {m}m {s}s — guilds: {len(bot.guilds)}',
            status=200
        )

    async def root(request):
        return web.Response(text='Bot is running', status=200)

    app = web.Application()
    app.router.add_get('/',       root)
    app.router.add_get('/health', health)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv('PORT', 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f'Web server started on port {port}')

# ==================== MAIN ====================

async def main():
    await start_web_server()
    token = os.getenv('BOT_TOKEN') or os.getenv('DISCORD_TOKEN')
    if not token:
        raise RuntimeError('BOT_TOKEN environment variable is not set')
    try:
        async with bot:
            await bot.start(token)
    finally:
        await db.close()

if __name__ == '__main__':
    asyncio.run(main())
