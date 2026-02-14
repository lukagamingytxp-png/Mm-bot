"""
╔══════════════════════════════════════════════════════════════╗
║           Professional Discord Ticket System Bot              ║
║         Optimized for Render Free Tier + PostgreSQL          ║
╔══════════════════════════════════════════════════════════════╗
"""

import discord
from discord.ext import commands, tasks
from discord.ui import Button, View, Select, Modal, TextInput
import os
import asyncpg
from datetime import datetime, timedelta
from aiohttp import web
import logging
import random
import string
from typing import Optional, Dict, List, Tuple
import asyncio
from collections import defaultdict
import json
import sys
import io

# ═══════════════════════════════════════════════════════════════
#                        LOGGING SETUP
# ═══════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('TicketBot')

# ═══════════════════════════════════════════════════════════════
#                     CONFIGURATION & CONSTANTS
# ═══════════════════════════════════════════════════════════════

class Config:
    """Centralized configuration management"""
    
    # Role IDs (hardcoded for your server)
    ROLES = {
        'lowtier': 1434610759140118640,
        'midtier': 1453757157144137911,
        'hightier': 1453757225267892276,
        'support': 1432081794647199895
    }
    
    # Default embed colors
    COLORS = {
        'lowtier': 0x57F287,    # Green
        'midtier': 0xFEE75C,    # Yellow
        'hightier': 0xED4245,   # Red
        'support': 0x5865F2,    # Blurple
        'success': 0x00D26A,    # Success Green
        'error': 0xFF4757,      # Error Red
        'warning': 0xFFB900,    # Warning Orange
        'info': 0x3498DB,       # Info Blue
        'primary': 0x5865F2     # Discord Blurple
    }
    
    # Rate limiting
    RATE_LIMIT_COOLDOWN = 3
    RATE_LIMIT_CLEANUP_INTERVAL = 300
    
    # Ticket settings
    TICKET_ID_LENGTH = 6
    MAX_TICKET_NAME_LENGTH = 50
    
    # Web server settings
    WEB_PORT = int(os.getenv('PORT', 8080))
    
    # Database settings
    DB_MIN_CONNECTIONS = 2
    DB_MAX_CONNECTIONS = 10
    DB_COMMAND_TIMEOUT = 30

# ═══════════════════════════════════════════════════════════════
#                       RATE LIMITER
# ═══════════════════════════════════════════════════════════════

class RateLimiter:
    def __init__(self):
        self.cooldowns: Dict[str, float] = defaultdict(float)
    
    def check_cooldown(self, user_id: int, command: str, cooldown: int = Config.RATE_LIMIT_COOLDOWN) -> bool:
        key = f"{user_id}:{command}"
        now = datetime.utcnow().timestamp()
        
        if key in self.cooldowns:
            time_passed = now - self.cooldowns[key]
            if time_passed < cooldown:
                return False
        
        self.cooldowns[key] = now
        return True
    
    async def cleanup_old_entries(self):
        while True:
            try:
                await asyncio.sleep(Config.RATE_LIMIT_CLEANUP_INTERVAL)
                now = datetime.utcnow().timestamp()
                expired_keys = [k for k, v in self.cooldowns.items() if now - v > 3600]
                for key in expired_keys:
                    del self.cooldowns[key]
                if expired_keys:
                    logger.debug(f"Cleaned up {len(expired_keys)} expired cooldown entries")
            except Exception as e:
                logger.error(f"Error in cooldown cleanup: {e}")

rate_limiter = RateLimiter()

# ═══════════════════════════════════════════════════════════════
#                     DATABASE MANAGER
# ═══════════════════════════════════════════════════════════════

class Database:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
    
    async def connect(self):
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            raise Exception("❌ DATABASE_URL environment variable not set")
        
        if database_url.startswith('postgres://'):
            database_url = database_url.replace('postgres://', 'postgresql://', 1)
        
        for attempt in range(1, 6):
            try:
                logger.info(f"🔌 Connecting to database (attempt {attempt}/5)...")
                self.pool = await asyncpg.create_pool(
                    database_url,
                    min_size=Config.DB_MIN_CONNECTIONS,
                    max_size=Config.DB_MAX_CONNECTIONS,
                    command_timeout=Config.DB_COMMAND_TIMEOUT
                )
                await self.create_tables()
                logger.info("✅ Database connected successfully")
                return
            except Exception as e:
                logger.error(f"❌ Database connection failed (attempt {attempt}): {e}")
                if attempt < 5:
                    await asyncio.sleep(2 ** attempt)
                else:
                    raise Exception("Failed to connect to database after multiple attempts")
    
    async def create_tables(self):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS config (
                    guild_id BIGINT PRIMARY KEY,
                    ticket_category_id BIGINT,
                    log_channel_id BIGINT,
                    proof_channel_id BIGINT,
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS tier_colors (
                    guild_id BIGINT,
                    tier TEXT,
                    color INT,
                    PRIMARY KEY (guild_id, tier)
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS tickets (
                    ticket_id TEXT PRIMARY KEY,
                    guild_id BIGINT NOT NULL,
                    channel_id BIGINT NOT NULL,
                    user_id BIGINT NOT NULL,
                    ticket_type TEXT NOT NULL,
                    tier TEXT,
                    claimed_by BIGINT,
                    status TEXT DEFAULT 'open',
                    trade_details JSONB,
                    created_at TIMESTAMP DEFAULT NOW(),
                    closed_at TIMESTAMP
                )
            ''')
            
            await conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_tickets_guild_status 
                ON tickets(guild_id, status)
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS stats (
                    user_id BIGINT PRIMARY KEY,
                    tickets_claimed INT DEFAULT 0,
                    tickets_closed INT DEFAULT 0,
                    last_activity TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS blacklist (
                    user_id BIGINT PRIMARY KEY,
                    guild_id BIGINT NOT NULL,
                    reason TEXT,
                    blacklisted_by BIGINT,
                    blacklisted_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS ps_links (
                    user_id BIGINT,
                    game_key TEXT,
                    game_name TEXT,
                    link TEXT,
                    roblox_username TEXT,
                    last_updated TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (user_id, game_key)
                )
            ''')
            
            logger.info("📊 Database schema initialized")
    
    async def get_config(self, guild_id: int) -> Optional[Dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM config WHERE guild_id = $1', guild_id)
            return dict(row) if row else None
    
    async def set_config(self, guild_id: int, **kwargs):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO config (guild_id, ticket_category_id, log_channel_id, proof_channel_id, updated_at)
                VALUES ($1, $2, $3, $4, NOW())
                ON CONFLICT (guild_id) DO UPDATE SET
                    ticket_category_id = COALESCE($2, config.ticket_category_id),
                    log_channel_id = COALESCE($3, config.log_channel_id),
                    proof_channel_id = COALESCE($4, config.proof_channel_id),
                    updated_at = NOW()
            ''', guild_id, kwargs.get('ticket_category_id'), 
            kwargs.get('log_channel_id'), kwargs.get('proof_channel_id'))
    
    async def set_tier_color(self, guild_id: int, tier: str, color: int):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO tier_colors (guild_id, tier, color)
                VALUES ($1, $2, $3)
                ON CONFLICT (guild_id, tier) DO UPDATE SET color = $3
            ''', guild_id, tier, color)
    
    async def get_tier_color(self, guild_id: int, tier: str) -> int:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT color FROM tier_colors WHERE guild_id = $1 AND tier = $2',
                guild_id, tier
            )
            if row:
                return row['color']
            return Config.COLORS.get(tier, Config.COLORS['primary'])
    
    async def create_ticket(self, ticket_id: str, guild_id: int, channel_id: int, 
                          user_id: int, ticket_type: str, tier: str = None, 
                          trade_details: Dict = None):
        async with self.pool.acquire() as conn:
            trade_details_json = json.dumps(trade_details) if trade_details else None
            await conn.execute('''
                INSERT INTO tickets (ticket_id, guild_id, channel_id, user_id, 
                                   ticket_type, tier, trade_details)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            ''', ticket_id, guild_id, channel_id, user_id, ticket_type, tier, trade_details_json)
    
    async def get_ticket(self, ticket_id: str) -> Optional[Dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM tickets WHERE ticket_id = $1', ticket_id)
            return dict(row) if row else None
    
    async def get_ticket_by_channel(self, channel_id: int) -> Optional[Dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM tickets WHERE channel_id = $1', channel_id)
            return dict(row) if row else None
    
    async def claim_ticket(self, ticket_id: str, user_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE tickets SET claimed_by = $2, status = 'claimed' 
                WHERE ticket_id = $1
            ''', ticket_id, user_id)
            
            await conn.execute('''
                INSERT INTO stats (user_id, tickets_claimed, last_activity)
                VALUES ($1, 1, NOW())
                ON CONFLICT (user_id) DO UPDATE SET
                    tickets_claimed = stats.tickets_claimed + 1,
                    last_activity = NOW()
            ''', user_id)
    
    async def unclaim_ticket(self, ticket_id: str):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT claimed_by FROM tickets WHERE ticket_id = $1', ticket_id)
            
            if row and row['claimed_by']:
                await conn.execute('''
                    UPDATE stats SET tickets_claimed = tickets_claimed - 1 
                    WHERE user_id = $1 AND tickets_claimed > 0
                ''', row['claimed_by'])
            
            await conn.execute('''
                UPDATE tickets SET claimed_by = NULL, status = 'open' 
                WHERE ticket_id = $1
            ''', ticket_id)
    
    async def close_ticket(self, ticket_id: str):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT claimed_by FROM tickets WHERE ticket_id = $1', ticket_id)
            
            await conn.execute('''
                UPDATE tickets SET status = 'closed', closed_at = NOW() 
                WHERE ticket_id = $1
            ''', ticket_id)
            
            if row and row['claimed_by']:
                await conn.execute('''
                    UPDATE stats SET 
                        tickets_closed = tickets_closed + 1,
                        last_activity = NOW()
                    WHERE user_id = $1
                ''', row['claimed_by'])
    
    async def get_user_stats(self, user_id: int) -> Dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM stats WHERE user_id = $1', user_id)
            if row:
                return dict(row)
            return {'tickets_claimed': 0, 'tickets_closed': 0}
    
    async def get_leaderboard(self, guild_id: int, limit: int = 10) -> List[Dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT s.user_id, s.tickets_claimed, s.tickets_closed
                FROM stats s
                INNER JOIN tickets t ON s.user_id = t.claimed_by
                WHERE t.guild_id = $1
                GROUP BY s.user_id, s.tickets_claimed, s.tickets_closed
                ORDER BY s.tickets_closed DESC, s.tickets_claimed DESC
                LIMIT $2
            ''', guild_id, limit)
            return [dict(row) for row in rows]
    
    async def blacklist_user(self, user_id: int, guild_id: int, reason: str, blacklisted_by: int):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO blacklist (user_id, guild_id, reason, blacklisted_by)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (user_id) DO UPDATE SET
                    reason = $3,
                    blacklisted_by = $4,
                    blacklisted_at = NOW()
            ''', user_id, guild_id, reason, blacklisted_by)
    
    async def unblacklist_user(self, user_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('DELETE FROM blacklist WHERE user_id = $1', user_id)
    
    async def is_blacklisted(self, user_id: int, guild_id: int) -> Optional[Dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('''
                SELECT * FROM blacklist 
                WHERE user_id = $1 AND guild_id = $2
            ''', user_id, guild_id)
            return dict(row) if row else None
    
    async def get_all_blacklisted(self, guild_id: int) -> List[Dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT * FROM blacklist 
                WHERE guild_id = $1 
                ORDER BY blacklisted_at DESC
            ''', guild_id)
            return [dict(row) for row in rows]
    
    async def set_ps_link(self, user_id: int, game_key: str, game_name: str, 
                         link: str, roblox_username: str = None):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO ps_links (user_id, game_key, game_name, link, roblox_username, last_updated)
                VALUES ($1, $2, $3, $4, $5, NOW())
                ON CONFLICT (user_id, game_key) DO UPDATE SET
                    game_name = $3,
                    link = $4,
                    roblox_username = $5,
                    last_updated = NOW()
            ''', user_id, game_key, game_name, link, roblox_username)
    
    async def get_ps_link(self, user_id: int, game_key: str) -> Optional[Dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('''
                SELECT * FROM ps_links 
                WHERE user_id = $1 AND game_key = $2
            ''', user_id, game_key)
            return dict(row) if row else None
    
    async def get_all_ps_links(self, user_id: int) -> List[Dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT * FROM ps_links 
                WHERE user_id = $1 
                ORDER BY last_updated DESC
            ''', user_id)
            return [dict(row) for row in rows]
    
    async def delete_ps_link(self, user_id: int, game_key: str):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                DELETE FROM ps_links 
                WHERE user_id = $1 AND game_key = $2
            ''', user_id, game_key)
    
    async def close(self):
        if self.pool:
            await self.pool.close()
            logger.info("🔌 Database connection closed")

db = Database()

# ═══════════════════════════════════════════════════════════════
#                       UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════

async def generate_ticket_id() -> str:
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=Config.TICKET_ID_LENGTH))

def create_embed(title: str, description: str = None, color: int = Config.COLORS['primary']) -> discord.Embed:
    embed = discord.Embed(title=title, description=description, color=color, timestamp=datetime.utcnow())
    return embed

def create_success_embed(message: str) -> discord.Embed:
    return create_embed("✅ Success", message, Config.COLORS['success'])

def create_error_embed(message: str) -> discord.Embed:
    return create_embed("❌ Error", message, Config.COLORS['error'])

def create_warning_embed(message: str) -> discord.Embed:
    return create_embed("⚠️ Warning", message, Config.COLORS['warning'])

def create_info_embed(message: str) -> discord.Embed:
    return create_embed("ℹ️ Information", message, Config.COLORS['info'])

def has_staff_role(member: discord.Member) -> bool:
    """Check if user has any staff role (lowtier, midtier, hightier, support)"""
    user_roles = [role.id for role in member.roles]
    allowed_roles = list(Config.ROLES.values())
    return any(role_id in allowed_roles for role_id in user_roles)

# ═══════════════════════════════════════════════════════════════
#                    TICKET CREATION LOGIC
# ═══════════════════════════════════════════════════════════════

async def create_ticket(guild: discord.Guild, user: discord.Member, 
                       ticket_type: str, tier: str = None, 
                       trade_details: Dict = None) -> Optional[discord.TextChannel]:
    try:
        # Check blacklist
        blacklist_data = await db.is_blacklisted(user.id, guild.id)
        if blacklist_data:
            raise Exception(f"You are blacklisted: {blacklist_data['reason']}")
        
        # Get configuration
        config = await db.get_config(guild.id)
        if not config or not config['ticket_category_id']:
            raise Exception("Ticket system not configured. Contact an administrator.")
        
        category = guild.get_channel(config['ticket_category_id'])
        if not category:
            raise Exception("Ticket category not found. Contact an administrator.")
        
        # Generate ticket ID
        ticket_id = await generate_ticket_id()
        
        # Determine channel name
        if ticket_type == 'middleman':
            channel_name = f'ticket-mm-{user.name}-{ticket_id}'
        else:
            channel_name = f'ticket-{ticket_type}-{user.name}-{ticket_id}'
        
        # Create channel with permissions
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(read_messages=False),
            user: discord.PermissionOverwrite(
                read_messages=True,
                send_messages=True,
                attach_files=True,
                embed_links=True,
                read_message_history=True
            ),
            guild.me: discord.PermissionOverwrite(
                read_messages=True,
                send_messages=True,
                manage_channels=True,
                manage_messages=True,
                embed_links=True,
                attach_files=True
            )
        }
        
        # Add tier role permissions for middleman tickets
        if tier and ticket_type == 'middleman':
            role_id = Config.ROLES.get(tier)
            if role_id:
                role = guild.get_role(role_id)
                if role:
                    overwrites[role] = discord.PermissionOverwrite(
                        read_messages=True,
                        send_messages=True,
                        attach_files=True,
                        embed_links=True,
                        read_message_history=True
                    )
        
        # Add support role permissions for support tickets
        if ticket_type == 'support':
            role_id = Config.ROLES.get('support')
            if role_id:
                role = guild.get_role(role_id)
                if role:
                    overwrites[role] = discord.PermissionOverwrite(
                        read_messages=True,
                        send_messages=True,
                        attach_files=True,
                        embed_links=True,
                        read_message_history=True
                    )
        
        # Create the channel
        channel = await category.create_text_channel(
            name=channel_name,
            overwrites=overwrites,
            reason=f"Ticket created by {user}"
        )
        
        # Save to database
        await db.create_ticket(
            ticket_id=ticket_id,
            guild_id=guild.id,
            channel_id=channel.id,
            user_id=user.id,
            ticket_type=ticket_type,
            tier=tier,
            trade_details=trade_details
        )
        
        # Get embed color
        color = await db.get_tier_color(guild.id, tier if tier else ticket_type)
        
        # Create welcome embed
        if ticket_type == 'middleman':
            embed_title = "Middleman Ticket"
            embed_desc = f"{user.mention}\n\nA middleman will assist you shortly"
        else:
            embed_title = "Support Ticket"
            embed_desc = f"{user.mention}\n\nOur team will help you shortly"
        
        embed = discord.Embed(
            title=embed_title,
            description=embed_desc,
            color=color,
            timestamp=datetime.utcnow()
        )
        
        if ticket_type == 'middleman' and trade_details:
            embed.add_field(
                name='Trading With',
                value=trade_details.get('trader', 'N/A'),
                inline=False
            )
            embed.add_field(
                name='You Give',
                value=trade_details.get('giving', 'N/A'),
                inline=True
            )
            embed.add_field(
                name='You Receive',
                value=trade_details.get('receiving', 'N/A'),
                inline=True
            )
            if trade_details.get('tip') and trade_details['tip'].lower() != 'none':
                embed.add_field(
                    name='Tip',
                    value=trade_details['tip'],
                    inline=False
                )
        
        embed.set_footer(text=f'Ticket ID: {ticket_id}')
        
        # Prepare ping message
        ping_msg = user.mention
        if tier and ticket_type == 'middleman':
            role_id = Config.ROLES.get(tier)
            if role_id:
                tier_role = guild.get_role(role_id)
                if tier_role:
                    ping_msg += f" {tier_role.mention}"
        elif ticket_type == 'support':
            role_id = Config.ROLES.get('support')
            if role_id:
                support_role = guild.get_role(role_id)
                if support_role:
                    ping_msg += f" {support_role.mention}"
        
        # Send welcome message with control buttons
        await channel.send(
            content=ping_msg,
            embed=embed,
            view=TicketControlView()
        )
        
        # Log the ticket creation
        if config and config.get('log_channel_id'):
            log_channel = guild.get_channel(config['log_channel_id'])
            if log_channel:
                log_embed = discord.Embed(title='Ticket Opened', color=0x57F287)
                log_embed.add_field(name='ID', value=f"`{ticket_id}`", inline=True)
                log_embed.add_field(name='Type', value=ticket_type.title(), inline=True)
                log_embed.add_field(name='User', value=user.mention, inline=True)
                log_embed.add_field(name='Channel', value=channel.mention, inline=True)
                
                if tier:
                    tier_names = {'lowtier': 'Low Value', 'midtier': 'Mid Value', 'hightier': 'High Value'}
                    log_embed.add_field(name='Tier', value=tier_names.get(tier, tier), inline=True)
                
                if trade_details:
                    trade_text = f"**Trading with:** {trade_details.get('trader', 'N/A')}\n"
                    trade_text += f"**Giving:** {trade_details.get('giving', 'N/A')}\n"
                    trade_text += f"**Receiving:** {trade_details.get('receiving', 'N/A')}"
                    if trade_details.get('tip') and trade_details['tip'].lower() != 'none':
                        trade_text += f"\n**Tip:** {trade_details['tip']}"
                    log_embed.add_field(name='Trade Details', value=trade_text, inline=False)
                
                await log_channel.send(embed=log_embed)
        
        logger.info(f"✅ Ticket {ticket_id} created by {user} in {guild.name}")
        return channel
        
    except Exception as e:
        logger.error(f"Error creating ticket: {e}")
        raise

# ═══════════════════════════════════════════════════════════════
#                    DISCORD UI COMPONENTS
# ═══════════════════════════════════════════════════════════════

class MiddlemanModal(Modal, title='Middleman Request'):
    def __init__(self, tier, interaction_msg):
        super().__init__()
        self.tier = tier
        self.interaction_msg = interaction_msg
        
        self.trader = TextInput(
            label='Trading with',
            placeholder='@username or ID',
            required=True,
            max_length=100
        )
        self.add_item(self.trader)
        
        self.giving = TextInput(
            label='You give',
            placeholder='e.g., 1 garam',
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=500
        )
        self.add_item(self.giving)
        
        self.receiving = TextInput(
            label='You receive',
            placeholder='e.g., 296 Robux',
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=500
        )
        self.add_item(self.receiving)
        
        self.tip = TextInput(
            label='Tip (optional)',
            placeholder='Optional',
            required=False,
            max_length=200
        )
        self.add_item(self.tip)
    
    async def on_submit(self, interaction: discord.Interaction):
        if not rate_limiter.check_cooldown(interaction.user.id, 'ticket_create', 10):
            return await interaction.response.send_message('Wait 10 seconds', ephemeral=True)
        
        await interaction.response.defer(ephemeral=True)
        
        try:
            trade_details = {
                'trader': self.trader.value,
                'giving': self.giving.value,
                'receiving': self.receiving.value,
                'tip': self.tip.value or 'None'
            }
            
            channel = await create_ticket(
                interaction.guild,
                interaction.user,
                'middleman',
                self.tier,
                trade_details
            )
            
            # Extract ticket ID from channel name
            ticket_id = channel.name.split('-')[-1]
            
            embed = discord.Embed(
                title='Ticket Created',
                description=f'#{ticket_id}\n{channel.mention}',
                color=0x57F287
            )
            await self.interaction_msg.edit(embed=embed, view=None)
            
        except Exception as e:
            await interaction.followup.send(f'{str(e)}', ephemeral=True)

class MiddlemanTierSelect(Select):
    def __init__(self, interaction_msg):
        self.interaction_msg = interaction_msg
        super().__init__(
            placeholder='Select trade value',
            custom_id='mm_tier_select',
            options=[
                discord.SelectOption(
                    label='Low Value',
                    value='lowtier',
                    emoji='🟢',
                    description='- Only for low valued stuff'
                ),
                discord.SelectOption(
                    label='Mid Value',
                    value='midtier',
                    emoji='🟡',
                    description='- Only for mid valued stuff'
                ),
                discord.SelectOption(
                    label='High Value',
                    value='hightier',
                    emoji='🔴',
                    description='- Only for high valued stuff'
                )
            ]
        )
    
    async def callback(self, interaction):
        modal = MiddlemanModal(self.values[0], self.interaction_msg)
        await interaction.response.send_modal(modal)

class TicketPanelView(View):
    """Main ticket panel with 2 buttons: Support and Middleman"""
    
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(
        label='Support',
        style=discord.ButtonStyle.primary,
        emoji='🎫',
        custom_id='support_btn'
    )
    async def support_button(self, interaction: discord.Interaction, button: Button):
        if not rate_limiter.check_cooldown(interaction.user.id, 'ticket_create', 10):
            return await interaction.response.send_message('Wait 10 seconds', ephemeral=True)
        
        await interaction.response.defer(ephemeral=True)
        
        try:
            channel = await create_ticket(interaction.guild, interaction.user, 'support')
            
            # Extract ticket ID from channel name
            ticket_id = channel.name.split('-')[-1]
            
            embed = discord.Embed(
                title='Ticket Created',
                description=f'#{ticket_id}\n{channel.mention}',
                color=0x57F287
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            
        except Exception as e:
            await interaction.followup.send(f'{str(e)}', ephemeral=True)
    
    @discord.ui.button(
        label='Middleman',
        style=discord.ButtonStyle.success,
        emoji='⚖️',
        custom_id='middleman_btn'
    )
    async def middleman_button(self, interaction: discord.Interaction, button: Button):
        embed = discord.Embed(
            title='Select Trade Value',
            description='Choose tier',
            color=0xFEE75C
        )
        
        view = View(timeout=300)
        select = MiddlemanTierSelect(None)
        view.add_item(select)
        
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        msg = await interaction.original_response()
        select.interaction_msg = msg

class TicketControlView(View):
    """Control buttons for ticket management"""
    
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(
        label='Claim',
        style=discord.ButtonStyle.green,
        custom_id='claim_ticket',
        emoji='✋'
    )
    async def claim_button(self, interaction: discord.Interaction, button: Button):
        if not rate_limiter.check_cooldown(interaction.user.id, 'claim', 2):
            return await interaction.response.send_message('Wait', ephemeral=True)
        
        try:
            ticket = await db.get_ticket_by_channel(interaction.channel.id)
            if not ticket:
                return await interaction.response.send_message('Not a ticket', ephemeral=True)
            
            if ticket.get('claimed_by'):
                return await interaction.response.send_message('Already claimed', ephemeral=True)
            
            await db.claim_ticket(ticket['ticket_id'], interaction.user.id)
            
            embed = discord.Embed(
                title='Claimed',
                description=f'By {interaction.user.mention}',
                color=0x57F287
            )
            await interaction.response.send_message(embed=embed)
            
            # Log the claim
            config = await db.get_config(interaction.guild.id)
            if config and config.get('log_channel_id'):
                log_channel = interaction.guild.get_channel(config['log_channel_id'])
                if log_channel:
                    log_embed = discord.Embed(
                        title='Ticket Claimed',
                        description=f"{interaction.user.mention} claimed {interaction.channel.mention}",
                        color=0x57F287
                    )
                    log_embed.add_field(name='ID', value=f"`{ticket['ticket_id']}`", inline=True)
                    await log_channel.send(embed=log_embed)
                    
        except Exception as e:
            await interaction.response.send_message(f'{str(e)}', ephemeral=True)
    
    @discord.ui.button(
        label='Unclaim',
        style=discord.ButtonStyle.gray,
        custom_id='unclaim_ticket',
        emoji='↩️'
    )
    async def unclaim_button(self, interaction: discord.Interaction, button: Button):
        if not rate_limiter.check_cooldown(interaction.user.id, 'unclaim', 2):
            return await interaction.response.send_message('Wait', ephemeral=True)
        
        ticket = await db.get_ticket_by_channel(interaction.channel.id)
        if not ticket:
            return await interaction.response.send_message('Not a ticket', ephemeral=True)
        
        if not ticket.get('claimed_by'):
            return await interaction.response.send_message('Not claimed', ephemeral=True)
        
        # Only claimer can unclaim
        if ticket['claimed_by'] != interaction.user.id:
            return await interaction.response.send_message('Only claimer can unclaim', ephemeral=True)
        
        try:
            await db.unclaim_ticket(ticket['ticket_id'])
            
            embed = discord.Embed(
                title='Unclaimed',
                description='Ticket available',
                color=0x5865F2
            )
            await interaction.response.send_message(embed=embed)
            
        except Exception as e:
            await interaction.response.send_message(f'{str(e)}', ephemeral=True)
    
    @discord.ui.button(
        label='Close',
        style=discord.ButtonStyle.red,
        custom_id='close_ticket',
        emoji='🔒'
    )
    async def close_button(self, interaction: discord.Interaction, button: Button):
        if not rate_limiter.check_cooldown(interaction.user.id, 'close', 3):
            return await interaction.response.send_message('Wait', ephemeral=True)
        
        ticket = await db.get_ticket_by_channel(interaction.channel.id)
        if not ticket:
            return await interaction.response.send_message('Not a ticket', ephemeral=True)
        
        # Only claimer can close
        if ticket.get('claimed_by') and ticket['claimed_by'] != interaction.user.id:
            return await interaction.response.send_message('Only claimer can close', ephemeral=True)
        
        await interaction.response.send_message('Closing...', ephemeral=True)
        
        try:
            await db.close_ticket(ticket['ticket_id'])
            
            embed = discord.Embed(
                title='Closing Ticket',
                description=f'Closed by {interaction.user.mention}\n\nDeleting in 5 seconds',
                color=0xED4245
            )
            await interaction.channel.send(embed=embed)
            
            # Log the closure with transcript
            config = await db.get_config(interaction.guild.id)
            if config and config.get('log_channel_id'):
                log_channel = interaction.guild.get_channel(config['log_channel_id'])
                if log_channel:
                    opener = interaction.guild.get_member(ticket['user_id'])
                    claimer = interaction.guild.get_member(ticket['claimed_by']) if ticket.get('claimed_by') else None
                    
                    # Create transcript
                    transcript = f"TICKET TRANSCRIPT\n{'='*60}\n"
                    transcript += f"Ticket ID: {ticket['ticket_id']}\n"
                    transcript += f"Type: {ticket['ticket_type'].title()}\n"
                    transcript += f"Opened by: {opener.name if opener else 'Unknown'}\n"
                    transcript += f"Claimed by: {claimer.name if claimer else 'Unclaimed'}\n"
                    transcript += f"Closed by: {interaction.user.name}\n"
                    transcript += f"Created: {ticket['created_at'].strftime('%Y-%m-%d %H:%M:%S')}\n"
                    transcript += f"{'='*60}\n\n"
                    
                    messages = []
                    async for msg in interaction.channel.history(limit=100, oldest_first=True):
                        timestamp = msg.created_at.strftime('%H:%M:%S')
                        content = msg.content if msg.content else '[No content]'
                        messages.append(f"[{timestamp}] {msg.author.name}: {content}")
                    transcript += '\n'.join(messages)
                    
                    transcript_file = discord.File(
                        fp=io.BytesIO(transcript.encode('utf-8')),
                        filename=f"transcript-{ticket['ticket_id']}.txt"
                    )
                    
                    log_embed = discord.Embed(title='Ticket Closed', color=0xED4245)
                    log_embed.add_field(name='ID', value=ticket['ticket_id'], inline=True)
                    log_embed.add_field(name='Type', value=ticket['ticket_type'].title(), inline=True)
                    log_embed.add_field(name='Opened', value=opener.mention if opener else 'Unknown', inline=True)
                    log_embed.add_field(name='Claimed', value=claimer.mention if claimer else 'Unclaimed', inline=True)
                    log_embed.add_field(name='Closed', value=interaction.user.mention, inline=True)
                    
                    await log_channel.send(embed=log_embed, file=transcript_file)
            
            await asyncio.sleep(5)
            await interaction.channel.delete()
            
        except Exception as e:
            logger.error(f"Error closing ticket: {e}")

# ═══════════════════════════════════════════════════════════════
#                        BOT INITIALIZATION
# ═══════════════════════════════════════════════════════════════

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.guilds = True

bot = commands.Bot(
    command_prefix='$',
    intents=intents,
    help_command=None,
    case_insensitive=True
)

# ═══════════════════════════════════════════════════════════════
#                       BOT COMMANDS
# ═══════════════════════════════════════════════════════════════

@bot.command(name='setup')
@commands.has_permissions(administrator=True)
async def setup(ctx):
    """Create the ticket panel"""
    embed = discord.Embed(
        title="🎫 Support Ticket System",
        description=(
            "Welcome to our support system! Choose the appropriate button below to create a ticket.\n\n"
            "**🎧 Support** - For general help and questions\n"
            "**⚖️ Middleman** - For safe trades (select your tier)\n\n"
            "Our team will assist you as soon as possible!"
        ),
        color=Config.COLORS['primary']
    )
    
    embed.set_thumbnail(url=ctx.guild.icon.url if ctx.guild.icon else None)
    embed.set_footer(text="Click a button below to get started")
    
    await ctx.send(embed=embed, view=TicketPanelView())
    
    try:
        await ctx.message.delete()
    except:
        pass

@bot.command(name='setcategory')
@commands.has_permissions(administrator=True)
async def setcategory(ctx, category: discord.CategoryChannel):
    """Set the category for tickets"""
    await db.set_config(ctx.guild.id, ticket_category_id=category.id)
    embed = create_success_embed(f"Ticket category set to: {category.name}")
    await ctx.reply(embed=embed)

@bot.command(name='setlogs')
@commands.has_permissions(administrator=True)
async def setlogs(ctx, channel: discord.TextChannel):
    """Set the log channel"""
    await db.set_config(ctx.guild.id, log_channel_id=channel.id)
    embed = create_success_embed(f"Log channel set to: {channel.mention}")
    await ctx.reply(embed=embed)

@bot.command(name='setproof')
@commands.has_permissions(administrator=True)
async def setproof(ctx, channel: discord.TextChannel):
    """Set the proof channel"""
    await db.set_config(ctx.guild.id, proof_channel_id=channel.id)
    embed = create_success_embed(f"Proof channel set to: {channel.mention}")
    await ctx.reply(embed=embed)

@bot.command(name='setcolor')
@commands.has_permissions(administrator=True)
async def setcolor(ctx, tier: str, color: str):
    """Set custom color for a tier"""
    tier = tier.lower()
    if tier not in ['lowtier', 'midtier', 'hightier', 'support']:
        embed = create_error_embed("Invalid tier. Use: lowtier, midtier, hightier, or support")
        return await ctx.reply(embed=embed)
    
    try:
        color_hex = color.replace('#', '')
        color_int = int(color_hex, 16)
        
        await db.set_tier_color(ctx.guild.id, tier, color_int)
        
        embed = create_success_embed(f"Color for {tier} set to {color}")
        embed.color = color_int
        await ctx.reply(embed=embed)
        
    except ValueError:
        embed = create_error_embed("Invalid color format. Use hex format: #FF0000 or FF0000")
        await ctx.reply(embed=embed)

@bot.command(name='config')
@commands.has_permissions(administrator=True)
async def config(ctx):
    """View current configuration"""
    config_data = await db.get_config(ctx.guild.id)
    
    embed = discord.Embed(
        title="⚙️ Server Configuration",
        color=Config.COLORS['info'],
        timestamp=datetime.utcnow()
    )
    
    if not config_data:
        embed.description = "No configuration found. Use `$setup` to get started."
    else:
        category = ctx.guild.get_channel(config_data.get('ticket_category_id'))
        log_channel = ctx.guild.get_channel(config_data.get('log_channel_id'))
        proof_channel = ctx.guild.get_channel(config_data.get('proof_channel_id'))
        
        embed.add_field(
            name="📁 Ticket Category",
            value=category.name if category else "Not set",
            inline=False
        )
        embed.add_field(
            name="📝 Log Channel",
            value=log_channel.mention if log_channel else "Not set",
            inline=True
        )
        embed.add_field(
            name="📸 Proof Channel",
            value=proof_channel.mention if proof_channel else "Not set",
            inline=True
        )
    
    await ctx.reply(embed=embed)

@bot.command(name='claim')
async def claim_cmd(ctx):
    """Claim a ticket"""
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply('Not a ticket channel')
    
    if not rate_limiter.check_cooldown(ctx.author.id, 'claim_cmd', 2):
        return await ctx.reply('Wait')
    
    try:
        ticket = await db.get_ticket_by_channel(ctx.channel.id)
        if not ticket:
            return await ctx.reply('Ticket not found')
        
        if ticket.get('claimed_by'):
            return await ctx.reply('Already claimed')
        
        await db.claim_ticket(ticket['ticket_id'], ctx.author.id)
        embed = discord.Embed(title='Claimed', description=f'By {ctx.author.mention}', color=0x57F287)
        await ctx.reply(embed=embed)
        
    except Exception as e:
        await ctx.reply(f'{str(e)}')

@bot.command(name='unclaim')
async def unclaim_cmd(ctx):
    """Unclaim a ticket - ONLY CLAIMER CAN USE"""
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply('Not a ticket channel')
    
    if not rate_limiter.check_cooldown(ctx.author.id, 'unclaim_cmd', 2):
        return await ctx.reply('Wait')
    
    ticket = await db.get_ticket_by_channel(ctx.channel.id)
    if not ticket:
        return await ctx.reply('Not found')
    
    # Only claimer can unclaim
    if ticket['claimed_by'] != ctx.author.id:
        return await ctx.reply('Only claimer can unclaim')
    
    try:
        await db.unclaim_ticket(ticket['ticket_id'])
        embed = discord.Embed(title='Unclaimed', description='Ticket available', color=0x5865F2)
        await ctx.reply(embed=embed)
    except Exception as e:
        await ctx.reply(f'{str(e)}')

@bot.command(name='close')
async def close_cmd(ctx):
    """Close a ticket - ONLY CLAIMER CAN USE"""
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply('Not a ticket channel')
    
    if not rate_limiter.check_cooldown(ctx.author.id, 'close_cmd', 3):
        return await ctx.reply('Wait 3 seconds')
    
    ticket = await db.get_ticket_by_channel(ctx.channel.id)
    if not ticket:
        return await ctx.reply('Ticket not found')
    
    # Only claimer can close
    if ticket.get('claimed_by') and ticket['claimed_by'] != ctx.author.id:
        return await ctx.reply('Only claimer can close')
    
    try:
        await db.close_ticket(ticket['ticket_id'])
        
        embed = discord.Embed(
            title='Closing Ticket',
            description=f'Closed by {ctx.author.mention}\n\nDeleting in 5 seconds',
            color=0xED4245
        )
        await ctx.send(embed=embed)
        
        # Log with transcript (same as button handler)
        config = await db.get_config(ctx.guild.id)
        if config and config.get('log_channel_id'):
            log_channel = ctx.guild.get_channel(config['log_channel_id'])
            if log_channel:
                opener = ctx.guild.get_member(ticket['user_id'])
                claimer = ctx.guild.get_member(ticket['claimed_by']) if ticket.get('claimed_by') else None
                
                transcript = f"TICKET TRANSCRIPT\n{'='*60}\n"
                transcript += f"Ticket ID: {ticket['ticket_id']}\n"
                transcript += f"Type: {ticket['ticket_type'].title()}\n"
                transcript += f"Opened by: {opener.name if opener else 'Unknown'}\n"
                transcript += f"Claimed by: {claimer.name if claimer else 'Unclaimed'}\n"
                transcript += f"Closed by: {ctx.author.name}\n"
                transcript += f"Created: {ticket['created_at'].strftime('%Y-%m-%d %H:%M:%S')}\n"
                transcript += f"{'='*60}\n\n"
                
                messages = []
                async for msg in ctx.channel.history(limit=100, oldest_first=True):
                    timestamp = msg.created_at.strftime('%H:%M:%S')
                    content = msg.content if msg.content else '[No content]'
                    messages.append(f"[{timestamp}] {msg.author.name}: {content}")
                transcript += '\n'.join(messages)
                
                transcript_file = discord.File(
                    fp=io.BytesIO(transcript.encode('utf-8')),
                    filename=f"transcript-{ticket['ticket_id']}.txt"
                )
                
                log_embed = discord.Embed(title='Ticket Closed', color=0xED4245)
                log_embed.add_field(name='ID', value=ticket['ticket_id'], inline=True)
                log_embed.add_field(name='Type', value=ticket['ticket_type'].title(), inline=True)
                log_embed.add_field(name='Opened', value=opener.mention if opener else 'Unknown', inline=True)
                log_embed.add_field(name='Claimed', value=claimer.mention if claimer else 'Unclaimed', inline=True)
                log_embed.add_field(name='Closed', value=ctx.author.mention, inline=True)
                
                await log_channel.send(embed=log_embed, file=transcript_file)
        
        await asyncio.sleep(5)
        await ctx.channel.delete()
        
    except Exception as e:
        await ctx.reply(f'{str(e)}')

@bot.command(name='add')
async def add_user(ctx, member: discord.Member):
    """Add user to ticket - ONLY CLAIMER CAN USE"""
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply('Not a ticket channel')
    
    ticket = await db.get_ticket_by_channel(ctx.channel.id)
    if not ticket:
        return await ctx.reply('Ticket not found')
    
    # Only claimer can add users (unless admin)
    if ticket.get('claimed_by') and ticket['claimed_by'] != ctx.author.id and not ctx.author.guild_permissions.administrator:
        return await ctx.reply('Only claimer can add users')
    
    await ctx.channel.set_permissions(member, read_messages=True, send_messages=True)
    embed = discord.Embed(title='User Added', description=f'{member.mention} added', color=0x57F287)
    await ctx.reply(embed=embed)

@bot.command(name='remove')
async def remove_user(ctx, member: discord.Member):
    """Remove user from ticket - ONLY CLAIMER CAN USE"""
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply('Not a ticket channel')
    
    ticket = await db.get_ticket_by_channel(ctx.channel.id)
    if not ticket:
        return await ctx.reply('Ticket not found')
    
    # Only claimer can remove users (unless admin)
    if ticket.get('claimed_by') and ticket['claimed_by'] != ctx.author.id and not ctx.author.guild_permissions.administrator:
        return await ctx.reply('Only claimer can remove users')
    
    await ctx.channel.set_permissions(member, overwrite=None)
    embed = discord.Embed(title='User Removed', description=f'{member.mention} removed', color=0xED4245)
    await ctx.reply(embed=embed)

@bot.command(name='rename')
async def rename(ctx, *, new_name: str):
    """Rename ticket channel"""
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply('Not a ticket channel')
    
    # Check if user is staff
    if not has_staff_role(ctx.author) and not ctx.author.guild_permissions.administrator:
        return await ctx.reply('Only staff can rename tickets')
    
    if len(new_name) > Config.MAX_TICKET_NAME_LENGTH:
        return await ctx.reply(f'Name too long. Maximum {Config.MAX_TICKET_NAME_LENGTH} characters.')
    
    new_name = new_name.lower().replace(' ', '-')
    new_name = ''.join(c for c in new_name if c.isalnum() or c == '-')
    
    await ctx.channel.edit(name=new_name)
    embed = create_success_embed(f"Channel renamed to: {new_name}")
    await ctx.reply(embed=embed)

@bot.command(name='stats')
async def stats(ctx, member: discord.Member = None):
    """View user statistics"""
    target = member or ctx.author
    stats_data = await db.get_user_stats(target.id)
    
    embed = discord.Embed(
        title=f"📊 Statistics for {target.display_name}",
        color=Config.COLORS['info'],
        timestamp=datetime.utcnow()
    )
    
    embed.set_thumbnail(url=target.display_avatar.url)
    
    embed.add_field(
        name="🎫 Tickets Claimed",
        value=f"```{stats_data['tickets_claimed']}```",
        inline=True
    )
    
    embed.add_field(
        name="✅ Tickets Closed",
        value=f"```{stats_data['tickets_closed']}```",
        inline=True
    )
    
    if stats_data['tickets_claimed'] > 0:
        success_rate = (stats_data['tickets_closed'] / stats_data['tickets_claimed']) * 100
        embed.add_field(
            name="📈 Success Rate",
            value=f"```{success_rate:.1f}%```",
            inline=True
        )
    
    embed.set_footer(text=f"Requested by {ctx.author.name}", icon_url=ctx.author.display_avatar.url)
    
    await ctx.reply(embed=embed)

@bot.command(name='leaderboard', aliases=['lb', 'top'])
async def leaderboard(ctx):
    """View top performers"""
    leaders = await db.get_leaderboard(ctx.guild.id, limit=10)
    
    if not leaders:
        embed = create_info_embed("No statistics available yet.")
        return await ctx.reply(embed=embed)
    
    embed = discord.Embed(
        title="🏆 Leaderboard",
        description="Top middlemen/support staff",
        color=Config.COLORS['warning'],
        timestamp=datetime.utcnow()
    )
    
    for i, leader in enumerate(leaders, 1):
        member = ctx.guild.get_member(leader['user_id'])
        if member:
            embed.add_field(
                name=f"{i}. {member.display_name}",
                value=f"Claimed: {leader['tickets_claimed']} | Closed: {leader['tickets_closed']}",
                inline=False
            )
    
    await ctx.reply(embed=embed)

@bot.command(name='blacklist')
@commands.has_permissions(administrator=True)
async def blacklist(ctx, member: discord.Member, *, reason: str = "No reason provided"):
    """Blacklist a user"""
    await db.blacklist_user(member.id, ctx.guild.id, reason, ctx.author.id)
    
    embed = create_success_embed(f"{member.mention} has been blacklisted.\n**Reason:** {reason}")
    await ctx.reply(embed=embed)
    
    config = await db.get_config(ctx.guild.id)
    if config and config.get('log_channel_id'):
        log_channel = ctx.guild.get_channel(config['log_channel_id'])
        if log_channel:
            log_embed = discord.Embed(title="🚫 User Blacklisted", color=Config.COLORS['error'], timestamp=datetime.utcnow())
            log_embed.add_field(name="User", value=member.mention, inline=True)
            log_embed.add_field(name="By", value=ctx.author.mention, inline=True)
            log_embed.add_field(name="Reason", value=reason, inline=False)
            await log_channel.send(embed=log_embed)

@bot.command(name='unblacklist')
@commands.has_permissions(administrator=True)
async def unblacklist(ctx, member: discord.Member):
    """Remove user from blacklist"""
    await db.unblacklist_user(member.id)
    embed = create_success_embed(f"{member.mention} has been removed from the blacklist.")
    await ctx.reply(embed=embed)

@bot.command(name='blacklists')
@commands.has_permissions(administrator=True)
async def blacklists(ctx):
    """View all blacklisted users"""
    blacklisted = await db.get_all_blacklisted(ctx.guild.id)
    
    if not blacklisted:
        embed = create_info_embed("No blacklisted users.")
        return await ctx.reply(embed=embed)
    
    embed = discord.Embed(
        title="🚫 Blacklisted Users",
        color=Config.COLORS['error'],
        timestamp=datetime.utcnow()
    )
    
    for entry in blacklisted[:10]:
        member = ctx.guild.get_member(entry['user_id'])
        name = member.display_name if member else f"User ID: {entry['user_id']}"
        
        embed.add_field(
            name=name,
            value=f"**Reason:** {entry['reason']}\n**By:** <@{entry['blacklisted_by']}>\n**Date:** {entry['blacklisted_at'].strftime('%Y-%m-%d')}",
            inline=False
        )
    
    if len(blacklisted) > 10:
        embed.set_footer(text=f"Showing 10 of {len(blacklisted)} blacklisted users")
    
    await ctx.reply(embed=embed)

@bot.command(name='setps')
async def setps(ctx, game_key: str, link: str, roblox_username: str = None):
    """Save private server link - ONLY STAFF/MIDDLEMAN"""
    # Check if user has staff role
    if not has_staff_role(ctx.author) and not ctx.author.guild_permissions.administrator:
        embed = create_error_embed("Only middleman/staff can use this command.")
        return await ctx.reply(embed=embed)
    
    if not (link.startswith('http://') or link.startswith('https://')):
        embed = create_error_embed("Please provide a valid URL starting with http:// or https://")
        return await ctx.reply(embed=embed)
    
    game_name = game_key.replace('_', ' ').title()
    
    await db.set_ps_link(ctx.author.id, game_key, game_name, link, roblox_username)
    
    embed = create_success_embed(
        f"Private server link saved!\n**Game:** {game_name}\n**Key:** `{game_key}`"
    )
    
    if roblox_username:
        embed.add_field(name="Roblox Username", value=roblox_username, inline=False)
    
    await ctx.reply(embed=embed)

@bot.command(name='ps')
async def ps(ctx, game_key: str):
    """Send private server link - ONLY STAFF/MIDDLEMAN"""
    # Check if user has staff role
    if not has_staff_role(ctx.author) and not ctx.author.guild_permissions.administrator:
        embed = create_error_embed("Only middleman/staff can use this command.")
        return await ctx.reply(embed=embed)
    
    ps_data = await db.get_ps_link(ctx.author.id, game_key)
    
    if not ps_data:
        embed = create_error_embed(
            f"No private server found for `{game_key}`.\nUse `$setps {game_key} <link>` to add one."
        )
        return await ctx.reply(embed=embed)
    
    embed = discord.Embed(
        title=f"🎮 {ps_data['game_name']}",
        description=f"[Click here to join]({ps_data['link']})",
        color=Config.COLORS['success'],
        timestamp=datetime.utcnow()
    )
    
    if ps_data.get('roblox_username'):
        embed.add_field(name="Roblox Username", value=ps_data['roblox_username'], inline=False)
    
    embed.set_footer(text=f"Shared by {ctx.author.name}", icon_url=ctx.author.display_avatar.url)
    
    await ctx.send(embed=embed)

@bot.command(name='pslist')
async def pslist(ctx):
    """View all your PS links"""
    ps_links = await db.get_all_ps_links(ctx.author.id)
    
    if not ps_links:
        embed = create_info_embed(
            "You haven't saved any private server links yet.\nUse `$setps <game> <link>` to add one."
        )
        return await ctx.reply(embed=embed)
    
    embed = discord.Embed(
        title=f"🎮 Your Private Servers",
        color=Config.COLORS['info'],
        timestamp=datetime.utcnow()
    )
    
    for ps in ps_links[:10]:
        value = f"**Key:** `{ps['game_key']}`\n"
        if ps.get('roblox_username'):
            value += f"**Username:** {ps['roblox_username']}\n"
        value += f"**Link:** [Click here]({ps['link']})"
        
        embed.add_field(name=ps['game_name'], value=value, inline=False)
    
    if len(ps_links) > 10:
        embed.set_footer(text=f"Showing 10 of {len(ps_links)} private servers")
    
    await ctx.reply(embed=embed)

@bot.command(name='removeps')
async def removeps(ctx, game_key: str):
    """Remove a PS link"""
    ps_data = await db.get_ps_link(ctx.author.id, game_key)
    
    if not ps_data:
        embed = create_error_embed(f"No private server found for `{game_key}`.")
        return await ctx.reply(embed=embed)
    
    await db.delete_ps_link(ctx.author.id, game_key)
    embed = create_success_embed(f"Removed private server for **{ps_data['game_name']}**")
    await ctx.reply(embed=embed)

@bot.command(name='clear')
@commands.has_permissions(manage_messages=True)
async def clear(ctx):
    """Clear bot messages"""
    deleted = 0
    async for message in ctx.channel.history(limit=100):
        if message.author == bot.user:
            try:
                await message.delete()
                deleted += 1
                await asyncio.sleep(0.5)
            except:
                pass
    
    embed = create_success_embed(f"Deleted {deleted} bot messages")
    msg = await ctx.send(embed=embed)
    await asyncio.sleep(3)
    await msg.delete()
    try:
        await ctx.message.delete()
    except:
        pass

@bot.command(name='ping')
async def ping(ctx):
    """Check bot latency"""
    latency = round(bot.latency * 1000)
    
    if latency < 200:
        color = Config.COLORS['success']
        status = "Excellent"
    elif latency < 500:
        color = Config.COLORS['warning']
        status = "Good"
    else:
        color = Config.COLORS['error']
        status = "Poor"
    
    embed = discord.Embed(
        title="🏓 Pong!",
        description=f"**Latency:** {latency}ms\n**Status:** {status}",
        color=color
    )
    
    await ctx.reply(embed=embed)

@bot.command(name='help')
async def help_command(ctx):
    """Show help menu"""
    pages = []
    
    page1 = discord.Embed(title="🎫 Ticket Commands", description="Commands for managing tickets", color=Config.COLORS['primary'])
    page1.add_field(name="$close", value="Close ticket (only claimer)\nUsage: `$close`", inline=False)
    page1.add_field(name="$claim", value="Claim a ticket\nUsage: `$claim`", inline=False)
    page1.add_field(name="$unclaim", value="Unclaim ticket (only claimer)\nUsage: `$unclaim`", inline=False)
    page1.add_field(name="$add", value="Add user to ticket (only claimer)\nUsage: `$add @user`", inline=False)
    page1.add_field(name="$remove", value="Remove user (only claimer)\nUsage: `$remove @user`", inline=False)
    page1.add_field(name="$rename", value="Rename ticket (staff only)\nUsage: `$rename <new-name>`", inline=False)
    page1.set_footer(text="Page 1/5 • Use arrows to navigate")
    pages.append(page1)
    
    page2 = discord.Embed(title="📊 Statistics Commands", description="View statistics", color=Config.COLORS['primary'])
    page2.add_field(name="$stats", value="View stats\nUsage: `$stats` or `$stats @user`", inline=False)
    page2.add_field(name="$leaderboard", value="View top performers\nUsage: `$leaderboard` or `$lb`", inline=False)
    page2.add_field(name="$ping", value="Check bot latency\nUsage: `$ping`", inline=False)
    page2.set_footer(text="Page 2/5 • Use arrows to navigate")
    pages.append(page2)
    
    page3 = discord.Embed(title="🎮 Private Server Commands", description="For middlemen/staff only", color=Config.COLORS['primary'])
    page3.add_field(name="$setps", value="Save PS link (staff only)\nUsage: `$setps <game> <link> [username]`", inline=False)
    page3.add_field(name="$ps", value="Send PS link (staff only)\nUsage: `$ps <game>`", inline=False)
    page3.add_field(name="$pslist", value="View all your PS links\nUsage: `$pslist`", inline=False)
    page3.add_field(name="$removeps", value="Remove a PS link\nUsage: `$removeps <game>`", inline=False)
    page3.set_footer(text="Page 3/5 • Use arrows to navigate")
    pages.append(page3)
    
    if ctx.author.guild_permissions.administrator:
        page4 = discord.Embed(title="⚙️ Admin Setup", description="Server configuration", color=Config.COLORS['primary'])
        page4.add_field(name="$setup", value="Create ticket panel\nUsage: `$setup`", inline=False)
        page4.add_field(name="$setcategory", value="Set ticket category\nUsage: `$setcategory <category>`", inline=False)
        page4.add_field(name="$setlogs", value="Set log channel\nUsage: `$setlogs #channel`", inline=False)
        page4.add_field(name="$setproof", value="Set proof channel\nUsage: `$setproof #channel`", inline=False)
        page4.add_field(name="$setcolor", value="Set tier color\nUsage: `$setcolor <tier> #hexcolor`", inline=False)
        page4.add_field(name="$config", value="View configuration\nUsage: `$config`", inline=False)
        page4.set_footer(text="Page 4/5 • Use arrows to navigate")
        pages.append(page4)
        
        page5 = discord.Embed(title="🛡️ Moderation", description="User management", color=Config.COLORS['primary'])
        page5.add_field(name="$blacklist", value="Blacklist user\nUsage: `$blacklist @user <reason>`", inline=False)
        page5.add_field(name="$unblacklist", value="Remove from blacklist\nUsage: `$unblacklist @user`", inline=False)
        page5.add_field(name="$blacklists", value="View blacklisted users\nUsage: `$blacklists`", inline=False)
        page5.add_field(name="$clear", value="Clear bot messages\nUsage: `$clear`", inline=False)
        page5.set_footer(text="Page 5/5 • Use arrows to navigate")
        pages.append(page5)
    else:
        page1.set_footer(text="Page 1/3 • Use arrows to navigate")
        page2.set_footer(text="Page 2/3 • Use arrows to navigate")
        page3.set_footer(text="Page 3/3 • Use arrows to navigate")
    
    class HelpView(View):
        def __init__(self, pages):
            super().__init__(timeout=60)
            self.pages = pages
            self.current_page = 0
            self.message = None
        
        @discord.ui.button(emoji='◀️', style=discord.ButtonStyle.gray)
        async def prev_button(self, interaction: discord.Interaction, button: Button):
            if interaction.user.id != ctx.author.id:
                return await interaction.response.send_message(embed=create_error_embed("This is not your help menu."), ephemeral=True)
            self.current_page = (self.current_page - 1) % len(self.pages)
            await interaction.response.edit_message(embed=self.pages[self.current_page])
        
        @discord.ui.button(emoji='▶️', style=discord.ButtonStyle.gray)
        async def next_button(self, interaction: discord.Interaction, button: Button):
            if interaction.user.id != ctx.author.id:
                return await interaction.response.send_message(embed=create_error_embed("This is not your help menu."), ephemeral=True)
            self.current_page = (self.current_page + 1) % len(self.pages)
            await interaction.response.edit_message(embed=self.pages[self.current_page])
        
        async def on_timeout(self):
            for child in self.children:
                child.disabled = True
            if self.message:
                try:
                    await self.message.edit(view=self)
                except:
                    pass
    
    view = HelpView(pages) if len(pages) > 1 else None
    message = await ctx.reply(embed=pages[0], view=view)
    if view:
        view.message = message

# ═══════════════════════════════════════════════════════════════
#                    WEB SERVER FOR RENDER
# ═══════════════════════════════════════════════════════════════

async def start_web_server():
    """Web server for Render health checks"""
    app = web.Application()
    
    async def health_check(request):
        return web.json_response({
            'status': 'healthy',
            'bot': bot.user.name if bot.user else 'Not ready',
            'guilds': len(bot.guilds),
            'latency': f"{round(bot.latency * 1000)}ms",
            'uptime': str(datetime.utcnow() - bot._start_time) if hasattr(bot, '_start_time') else 'N/A'
        })
    
    async def root(request):
        return web.Response(text=f"🤖 {bot.user.name if bot.user else 'Discord Bot'} is running!")
    
    app.router.add_get('/', root)
    app.router.add_get('/health', health_check)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', Config.WEB_PORT)
    await site.start()
    
    logger.info(f"🌐 Web server started on port {Config.WEB_PORT}")

# ═══════════════════════════════════════════════════════════════
#                        EVENT HANDLERS
# ═══════════════════════════════════════════════════════════════

@bot.event
async def on_ready():
    bot._start_time = datetime.utcnow()
    
    logger.info("━" * 60)
    logger.info(f"✅ Bot logged in as {bot.user.name} (ID: {bot.user.id})")
    logger.info(f"📊 Connected to {len(bot.guilds)} guild(s)")
    logger.info(f"👥 Serving {sum(g.member_count for g in bot.guilds)} users")
    logger.info("━" * 60)
    
    try:
        await db.connect()
    except Exception as e:
        logger.critical(f"Failed to connect to database: {e}")
        await bot.close()
        sys.exit(1)
    
    bot.add_view(TicketPanelView())
    bot.add_view(TicketControlView())
    
    bot.loop.create_task(rate_limiter.cleanup_old_entries())
    
    await bot.change_presence(
        activity=discord.Activity(type=discord.ActivityType.watching, name="tickets | $help"),
        status=discord.Status.online
    )
    
    logger.info("✅ Bot is fully ready and operational!")

@bot.event
async def on_guild_join(guild: discord.Guild):
    logger.info(f"📥 Joined new guild: {guild.name} (ID: {guild.id})")

@bot.event
async def on_guild_remove(guild: discord.Guild):
    logger.info(f"📤 Left guild: {guild.name} (ID: {guild.id})")

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        embed = create_error_embed("You don't have permission to use this command.")
    elif isinstance(error, commands.MemberNotFound):
        embed = create_error_embed("Member not found. Please mention a valid member.")
    elif isinstance(error, commands.ChannelNotFound):
        embed = create_error_embed("Channel not found. Please provide a valid channel.")
    elif isinstance(error, commands.RoleNotFound):
        embed = create_error_embed("Role not found. Please provide a valid role.")
    elif isinstance(error, commands.MissingRequiredArgument):
        embed = create_error_embed(f"Missing required argument: `{error.param.name}`")
    elif isinstance(error, commands.CommandNotFound):
        return
    elif isinstance(error, commands.CommandOnCooldown):
        embed = create_warning_embed(f"This command is on cooldown. Try again in {error.retry_after:.1f}s")
    else:
        embed = create_error_embed("An error occurred while processing your command.")
        logger.error(f"Command error in {ctx.command}: {error}")
    
    try:
        await ctx.reply(embed=embed)
    except:
        pass

# ═══════════════════════════════════════════════════════════════
#                        MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════

async def main():
    logger.info("=" * 60)
    logger.info("      🚀 Starting Discord Ticket Bot")
    logger.info("=" * 60)
    
    await start_web_server()
    
    token = os.getenv('DISCORD_TOKEN')
    if not token:
        logger.critical("❌ DISCORD_TOKEN environment variable not found!")
        sys.exit(1)
    
    try:
        await bot.start(token)
    except KeyboardInterrupt:
        logger.info("🛑 Received keyboard interrupt")
    except Exception as e:
        logger.critical(f"❌ Fatal error: {e}")
    finally:
        logger.info("🔄 Shutting down gracefully...")
        await db.close()
        await bot.close()
        logger.info("👋 Goodbye!")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
