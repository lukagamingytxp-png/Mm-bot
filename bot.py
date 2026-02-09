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

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('TicketBot')

# ===== RATE LIMIT PROTECTION =====
class RateLimiter:
    def __init__(self):
        self.cooldowns = defaultdict(float)
        self.message_counts = defaultdict(int)
        
    def check_cooldown(self, user_id: int, command: str, cooldown: int = 3) -> bool:
        """Check if user is on cooldown for a command"""
        key = f"{user_id}:{command}"
        now = datetime.utcnow().timestamp()
        
        if key in self.cooldowns:
            if now - self.cooldowns[key] < cooldown:
                return False
        
        self.cooldowns[key] = now
        return True
    
    def check_spam(self, channel_id: int, limit: int = 5) -> bool:
        """Check if channel is being spammed"""
        self.message_counts[channel_id] += 1
        
        # Reset counter after reaching limit
        if self.message_counts[channel_id] > limit:
            self.message_counts[channel_id] = 0
            return False
        
        return True
    
    async def cleanup_old_entries(self):
        """Cleanup old cooldown entries every 5 minutes"""
        while True:
            await asyncio.sleep(300)  # 5 minutes
            now = datetime.utcnow().timestamp()
            
            # Remove cooldowns older than 1 hour
            self.cooldowns = {
                k: v for k, v in self.cooldowns.items() 
                if now - v < 3600
            }
            
            # Reset message counts
            self.message_counts.clear()

rate_limiter = RateLimiter()

# ===== DATABASE =====
class Database:
    def __init__(self):
        self.pool = None
        
    async def connect(self):
        """Connect to PostgreSQL database"""
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            raise Exception("DATABASE_URL environment variable not set")
        
        if database_url.startswith('postgres://'):
            database_url = database_url.replace('postgres://', 'postgresql://', 1)
        
        self.pool = await asyncpg.create_pool(database_url, min_size=1, max_size=10)
        await self.create_tables()
        
    async def create_tables(self):
        """Create all necessary tables"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS config (
                    guild_id BIGINT PRIMARY KEY,
                    ticket_category_id BIGINT,
                    log_channel_id BIGINT,
                    panel_message_id BIGINT,
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS ticket_roles (
                    guild_id BIGINT,
                    ticket_type TEXT,
                    role_id BIGINT,
                    PRIMARY KEY (guild_id, ticket_type)
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS tickets (
                    ticket_id TEXT PRIMARY KEY,
                    guild_id BIGINT,
                    channel_id BIGINT,
                    user_id BIGINT,
                    ticket_type TEXT,
                    tier TEXT,
                    claimed_by BIGINT,
                    status TEXT DEFAULT 'open',
                    trade_details JSONB,
                    created_at TIMESTAMP DEFAULT NOW(),
                    closed_at TIMESTAMP
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS stats (
                    user_id BIGINT PRIMARY KEY,
                    tickets_claimed INT DEFAULT 0,
                    tickets_closed INT DEFAULT 0,
                    last_claim TIMESTAMP
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS mm_tiers (
                    tier_id TEXT PRIMARY KEY,
                    guild_id BIGINT,
                    name TEXT,
                    role_id BIGINT,
                    emoji TEXT,
                    position INT
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS blacklist (
                    user_id BIGINT PRIMARY KEY,
                    guild_id BIGINT,
                    reason TEXT,
                    blacklisted_by BIGINT,
                    blacklisted_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS feedback (
                    feedback_id SERIAL PRIMARY KEY,
                    ticket_id TEXT,
                    user_id BIGINT,
                    rating INT,
                    comment TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
    async def get_config(self, guild_id: int) -> Optional[Dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM config WHERE guild_id = $1', guild_id)
            return dict(row) if row else None
            
    async def set_config(self, guild_id: int, **kwargs):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO config (guild_id, ticket_category_id, log_channel_id, panel_message_id, updated_at)
                VALUES ($1, $2, $3, $4, NOW())
                ON CONFLICT (guild_id) 
                DO UPDATE SET 
                    ticket_category_id = COALESCE($2, config.ticket_category_id),
                    log_channel_id = COALESCE($3, config.log_channel_id),
                    panel_message_id = COALESCE($4, config.panel_message_id),
                    updated_at = NOW()
            ''', guild_id, kwargs.get('ticket_category_id'), kwargs.get('log_channel_id'), kwargs.get('panel_message_id'))
    
    async def set_ticket_role(self, guild_id: int, ticket_type: str, role_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO ticket_roles (guild_id, ticket_type, role_id)
                VALUES ($1, $2, $3)
                ON CONFLICT (guild_id, ticket_type)
                DO UPDATE SET role_id = $3
            ''', guild_id, ticket_type, role_id)
            
    async def get_ticket_role(self, guild_id: int, ticket_type: str) -> Optional[int]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT role_id FROM ticket_roles WHERE guild_id = $1 AND ticket_type = $2',
                guild_id, ticket_type
            )
            return row['role_id'] if row else None
            
    async def get_all_ticket_roles(self, guild_id: int) -> List[Dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('SELECT ticket_type, role_id FROM ticket_roles WHERE guild_id = $1', guild_id)
            return [dict(row) for row in rows]
    
    async def create_ticket(self, ticket_id: str, guild_id: int, channel_id: int, 
                          user_id: int, ticket_type: str, tier: str = None, 
                          trade_details: Dict = None):
        async with self.pool.acquire() as conn:
            import json
            trade_details_json = json.dumps(trade_details) if trade_details else None
            await conn.execute('''
                INSERT INTO tickets (ticket_id, guild_id, channel_id, user_id, ticket_type, tier, trade_details)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            ''', ticket_id, guild_id, channel_id, user_id, ticket_type, tier, trade_details_json)
            
    async def claim_ticket(self, ticket_id: str, user_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE tickets SET claimed_by = $2, status = 'claimed'
                WHERE ticket_id = $1
            ''', ticket_id, user_id)
            
            await conn.execute('''
                INSERT INTO stats (user_id, tickets_claimed, last_claim)
                VALUES ($1, 1, NOW())
                ON CONFLICT (user_id)
                DO UPDATE SET 
                    tickets_claimed = stats.tickets_claimed + 1,
                    last_claim = NOW()
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
                    UPDATE stats SET tickets_closed = tickets_closed + 1
                    WHERE user_id = $1
                ''', row['claimed_by'])
                
    async def get_user_stats(self, user_id: int) -> Dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM stats WHERE user_id = $1', user_id)
            if row:
                return dict(row)
            return {'tickets_claimed': 0, 'tickets_closed': 0, 'last_claim': None}
            
    async def get_mm_tiers(self, guild_id: int) -> List[Dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT * FROM mm_tiers WHERE guild_id = $1 ORDER BY position',
                guild_id
            )
            return [dict(row) for row in rows]
            
    async def set_mm_tier(self, tier_id: str, guild_id: int, name: str, 
                         role_id: int, emoji: str, position: int):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO mm_tiers (tier_id, guild_id, name, role_id, emoji, position)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (tier_id)
                DO UPDATE SET 
                    name = $3,
                    role_id = $4,
                    emoji = $5,
                    position = $6
            ''', tier_id, guild_id, name, role_id, emoji, position)
            
    async def delete_mm_tier(self, tier_id: str):
        async with self.pool.acquire() as conn:
            await conn.execute('DELETE FROM mm_tiers WHERE tier_id = $1', tier_id)
    
    async def blacklist_user(self, user_id: int, guild_id: int, reason: str, blacklisted_by: int):
        """Blacklist a user from creating tickets"""
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
        """Remove user from blacklist"""
        async with self.pool.acquire() as conn:
            await conn.execute('DELETE FROM blacklist WHERE user_id = $1', user_id)
    
    async def is_blacklisted(self, user_id: int, guild_id: int) -> Optional[Dict]:
        """Check if user is blacklisted"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT * FROM blacklist WHERE user_id = $1 AND guild_id = $2',
                user_id, guild_id
            )
            return dict(row) if row else None
    
    async def add_feedback(self, ticket_id: str, user_id: int, rating: int, comment: str):
        """Add feedback for a ticket"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO feedback (ticket_id, user_id, rating, comment)
                VALUES ($1, $2, $3, $4)
            ''', ticket_id, user_id, rating, comment)
    
    async def get_feedback_stats(self, guild_id: int) -> Dict:
        """Get average rating and total feedback count"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('''
                SELECT AVG(rating) as avg_rating, COUNT(*) as total_feedback
                FROM feedback f
                JOIN tickets t ON f.ticket_id = t.ticket_id
                WHERE t.guild_id = $1
            ''', guild_id)
            return dict(row) if row else {'avg_rating': 0, 'total_feedback': 0}
            
    async def close(self):
        if self.pool:
            await self.pool.close()

db = Database()

# ===== UTILITIES =====
async def generate_ticket_id():
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))

async def create_ticket(guild: discord.Guild, user: discord.Member, 
                       ticket_type: str, tier_id: str = None, 
                       trade_details: dict = None):
    # Check blacklist
    blacklist_data = await db.is_blacklisted(user.id, guild.id)
    if blacklist_data:
        raise Exception(f"You are blacklisted from creating tickets. Reason: {blacklist_data['reason']}")
    
    config = await db.get_config(guild.id)
    if not config or not config.get('ticket_category_id'):
        raise Exception('Ticket category not configured!')
    
    category = guild.get_channel(config['ticket_category_id'])
    if not category:
        raise Exception('Ticket category not found!')
    
    ticket_id = await generate_ticket_id()
    
    if ticket_type == 'middleman':
        channel_name = f'mm-{user.name}-{ticket_id[:4]}'
    else:
        channel_name = f'{ticket_type}-{user.name}-{ticket_id[:4]}'
    
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(read_messages=False),
        user: discord.PermissionOverwrite(
            read_messages=True,
            send_messages=True,
            attach_files=True,
            embed_links=True
        ),
        guild.me: discord.PermissionOverwrite(
            read_messages=True,
            send_messages=True,
            manage_channels=True,
            manage_messages=True
        )
    }
    
    role_id = await db.get_ticket_role(guild.id, ticket_type)
    if role_id:
        role = guild.get_role(role_id)
        if role:
            overwrites[role] = discord.PermissionOverwrite(
                read_messages=True,
                send_messages=True
            )
    
    channel = await category.create_text_channel(name=channel_name, overwrites=overwrites)
    
    await db.create_ticket(
        ticket_id=ticket_id,
        guild_id=guild.id,
        channel_id=channel.id,
        user_id=user.id,
        ticket_type=ticket_type,
        tier=tier_id,
        trade_details=trade_details
    )
    
    embed = discord.Embed(
        title=f'🎫 {ticket_type.title()} Ticket',
        description=f'Welcome {user.mention}!\nSupport will be with you shortly.',
        color=0x5865F2
    )
    
    if ticket_type == 'middleman' and trade_details:
        embed.add_field(name='Trading with', value=trade_details.get('trader', 'N/A'), inline=False)
        embed.add_field(name='Giving', value=trade_details.get('giving', 'N/A'), inline=True)
        embed.add_field(name='Receiving', value=trade_details.get('receiving', 'N/A'), inline=True)
        
        if trade_details.get('tip') and trade_details['tip'] != 'None':
            embed.add_field(name='Tip', value=trade_details['tip'], inline=False)
    
    embed.set_footer(text=f'Ticket ID: {ticket_id}')
    
    view = TicketControlView()
    
    ping_msg = ""
    if role_id:
        role = guild.get_role(role_id)
        if role:
            ping_msg = role.mention
    
    await channel.send(content=ping_msg, embed=embed, view=view)
    
    # Log ticket creation
    config = await db.get_config(guild.id)
    if config and config.get('log_channel_id'):
        log_channel = guild.get_channel(config['log_channel_id'])
        if log_channel:
            log_embed = discord.Embed(
                title='🎫 Ticket Opened',
                color=0x57F287
            )
            log_embed.add_field(name='Ticket ID', value=f"`{ticket_id}`", inline=True)
            log_embed.add_field(name='Type', value=ticket_type.title(), inline=True)
            log_embed.add_field(name='Channel', value=channel.mention, inline=True)
            log_embed.add_field(name='Opened by', value=user.mention, inline=True)
            
            if tier_id:
                tiers = await db.get_mm_tiers(guild.id)
                tier_data = next((t for t in tiers if t['tier_id'] == tier_id), None)
                if tier_data:
                    log_embed.add_field(name='Tier', value=f"{tier_data['emoji']} {tier_data['name']}", inline=True)
            
            if trade_details:
                trade_info = f"**Trading with:** {trade_details.get('trader', 'N/A')}\n"
                trade_info += f"**Giving:** {trade_details.get('giving', 'N/A')}\n"
                trade_info += f"**Receiving:** {trade_details.get('receiving', 'N/A')}"
                if trade_details.get('tip') and trade_details['tip'] != 'None':
                    trade_info += f"\n**Tip:** {trade_details['tip']}"
                log_embed.add_field(name='Trade Details', value=trade_info, inline=False)
            
            await log_channel.send(embed=log_embed)
    
    return channel

async def close_ticket(channel: discord.TextChannel, closed_by: discord.Member):
    tickets = await db.pool.fetch(
        'SELECT * FROM tickets WHERE channel_id = $1 AND status != $2',
        channel.id, 'closed'
    )
    
    if not tickets:
        raise Exception('This is not an active ticket!')
    
    ticket = dict(tickets[0])
    
    # Send feedback request to ticket opener
    opener = channel.guild.get_member(ticket['user_id'])
    if opener:
        try:
            # Create feedback button
            class FeedbackView(View):
                def __init__(self, ticket_id: str):
                    super().__init__(timeout=None)
                    self.ticket_id = ticket_id
                
                @discord.ui.button(label='Leave Feedback', style=discord.ButtonStyle.primary, emoji='⭐')
                async def feedback_button(self, interaction: discord.Interaction, button: Button):
                    modal = FeedbackModal(self.ticket_id)
                    await interaction.response.send_modal(modal)
            
            feedback_embed = discord.Embed(
                title='⭐ Rate Your Experience',
                description=(
                    f'Your ticket has been closed!\n\n'
                    f'How was your experience? Click below to rate us!'
                ),
                color=0xFEE75C
            )
            
            await opener.send(embed=feedback_embed, view=FeedbackView(ticket['ticket_id']))
        except:
            pass  # User has DMs disabled
    
    await db.close_ticket(ticket['ticket_id'])
    
    embed = discord.Embed(
        title='🔒 Ticket Closing',
        description=f'Closed by {closed_by.mention}\nChannel will be deleted in 5 seconds.',
        color=0xED4245
    )
    
    await channel.send(embed=embed)
    
    # Send detailed logs
    config = await db.get_config(channel.guild.id)
    if config and config.get('log_channel_id'):
        log_channel = channel.guild.get_channel(config['log_channel_id'])
        if log_channel:
            opener = channel.guild.get_member(ticket['user_id'])
            claimer = channel.guild.get_member(ticket['claimed_by']) if ticket.get('claimed_by') else None
            
            log_embed = discord.Embed(
                title='🔒 Ticket Closed',
                color=0xED4245
            )
            log_embed.add_field(name='Ticket ID', value=f"`{ticket['ticket_id']}`", inline=True)
            log_embed.add_field(name='Type', value=ticket['ticket_type'].title(), inline=True)
            log_embed.add_field(name='Channel', value=f"#{channel.name}", inline=True)
            log_embed.add_field(name='Opened by', value=opener.mention if opener else 'Unknown', inline=True)
            log_embed.add_field(name='Claimed by', value=claimer.mention if claimer else 'Unclaimed', inline=True)
            log_embed.add_field(name='Closed by', value=closed_by.mention, inline=True)
            
            if ticket.get('tier'):
                log_embed.add_field(name='MM Tier', value=ticket['tier'], inline=True)
            
            if ticket.get('trade_details'):
                details = ticket['trade_details']
                trade_info = f"**Trading with:** {details.get('trader', 'N/A')}\n"
                trade_info += f"**Giving:** {details.get('giving', 'N/A')}\n"
                trade_info += f"**Receiving:** {details.get('receiving', 'N/A')}"
                if details.get('tip') and details['tip'] != 'None':
                    trade_info += f"\n**Tip:** {details['tip']}"
                log_embed.add_field(name='Trade Details', value=trade_info, inline=False)
            
            duration = datetime.utcnow() - ticket['created_at']
            hours = int(duration.total_seconds() // 3600)
            minutes = int((duration.total_seconds() % 3600) // 60)
            log_embed.add_field(name='Duration', value=f"{hours}h {minutes}m", inline=True)
            
            await log_channel.send(embed=log_embed)
    
    import asyncio
    await asyncio.sleep(5)
    await channel.delete()

async def claim_ticket(channel: discord.TextChannel, claimer: discord.Member):
    tickets = await db.pool.fetch('SELECT * FROM tickets WHERE channel_id = $1', channel.id)
    
    if not tickets:
        raise Exception('This is not a ticket!')
    
    ticket = dict(tickets[0])
    
    if ticket.get('claimed_by'):
        raise Exception('Ticket already claimed!')
    
    await db.claim_ticket(ticket['ticket_id'], claimer.id)
    
    embed = discord.Embed(
        title='✅ Ticket Claimed',
        description=f'Claimed by {claimer.mention}',
        color=0x57F287
    )
    
    await channel.send(embed=embed)
    
    # Log claim
    config = await db.get_config(channel.guild.id)
    if config and config.get('log_channel_id'):
        log_channel = channel.guild.get_channel(config['log_channel_id'])
        if log_channel:
            log_embed = discord.Embed(
                title='✋ Ticket Claimed',
                description=f"{claimer.mention} claimed {channel.mention}",
                color=0x57F287
            )
            log_embed.add_field(name='Ticket ID', value=f"`{ticket['ticket_id']}`", inline=True)
            log_embed.add_field(name='Type', value=ticket['ticket_type'].title(), inline=True)
            await log_channel.send(embed=log_embed)

async def unclaim_ticket(channel: discord.TextChannel):
    tickets = await db.pool.fetch('SELECT * FROM tickets WHERE channel_id = $1', channel.id)
    
    if not tickets:
        raise Exception('This is not a ticket!')
    
    ticket = dict(tickets[0])
    
    if not ticket.get('claimed_by'):
        raise Exception('Ticket is not claimed!')
    
    claimer = channel.guild.get_member(ticket['claimed_by'])
    
    await db.unclaim_ticket(ticket['ticket_id'])
    
    embed = discord.Embed(
        title='↩️ Ticket Unclaimed',
        description='Ticket is now available to claim',
        color=0x5865F2
    )
    
    await channel.send(embed=embed)
    
    # Log unclaim
    config = await db.get_config(channel.guild.id)
    if config and config.get('log_channel_id'):
        log_channel = channel.guild.get_channel(config['log_channel_id'])
        if log_channel:
            log_embed = discord.Embed(
                title='↩️ Ticket Unclaimed',
                description=f"{claimer.mention if claimer else 'Someone'} unclaimed {channel.mention}",
                color=0x5865F2
            )
            log_embed.add_field(name='Ticket ID', value=f"`{ticket['ticket_id']}`", inline=True)
            await log_channel.send(embed=log_embed)

# ===== MODALS =====
class FeedbackModal(Modal, title='Rate Your Experience'):
    def __init__(self, ticket_id: str):
        super().__init__()
        self.ticket_id = ticket_id
        
        self.rating = TextInput(
            label='Rating (1-5 stars)',
            placeholder='Enter 1, 2, 3, 4, or 5',
            required=True,
            max_length=1
        )
        
        self.comment = TextInput(
            label='Comments (optional)',
            placeholder='How was your experience?',
            style=discord.TextStyle.paragraph,
            required=False,
            max_length=500
        )
        
        self.add_item(self.rating)
        self.add_item(self.comment)
        
    async def on_submit(self, interaction: discord.Interaction):
        try:
            rating_value = int(self.rating.value)
            
            if rating_value < 1 or rating_value > 5:
                return await interaction.response.send_message(
                    '❌ Rating must be between 1 and 5!',
                    ephemeral=True
                )
            
            await db.add_feedback(
                self.ticket_id,
                interaction.user.id,
                rating_value,
                self.comment.value or 'No comment'
            )
            
            stars = '⭐' * rating_value
            
            embed = discord.Embed(
                title='✅ Feedback Submitted!',
                description=f'Thank you for your feedback!\n\n**Rating:** {stars} ({rating_value}/5)',
                color=0x57F287
            )
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
            
            # Log feedback
            config = await db.get_config(interaction.guild.id)
            if config and config.get('log_channel_id'):
                log_channel = interaction.guild.get_channel(config['log_channel_id'])
                if log_channel:
                    log_embed = discord.Embed(
                        title='⭐ Feedback Received',
                        color=0xFEE75C
                    )
                    log_embed.add_field(name='User', value=interaction.user.mention, inline=True)
                    log_embed.add_field(name='Rating', value=f'{stars} ({rating_value}/5)', inline=True)
                    log_embed.add_field(name='Ticket ID', value=f'`{self.ticket_id}`', inline=True)
                    if self.comment.value:
                        log_embed.add_field(name='Comment', value=self.comment.value, inline=False)
                    await log_channel.send(embed=log_embed)
                    
        except ValueError:
            await interaction.response.send_message(
                '❌ Please enter a valid number (1-5)!',
                ephemeral=True
            )

class MiddlemanModal(Modal, title='Middleman Request'):
    def __init__(self, tier_id: str, tier_name: str):
        super().__init__()
        self.tier_id = tier_id
        self.tier_name = tier_name
        
        self.trader = TextInput(
            label='Trading with',
            placeholder='@username or ID',
            required=True,
            max_length=100
        )
        
        self.giving = TextInput(
            label='You are giving',
            placeholder='e.g., 1 garam',
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=500
        )
        
        self.receiving = TextInput(
            label='You are receiving',
            placeholder='e.g., 296 Robux',
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=500
        )
        
        self.tip = TextInput(
            label='Tip amount (optional)',
            placeholder='Optional',
            required=False,
            max_length=200
        )
        
        self.add_item(self.trader)
        self.add_item(self.giving)
        self.add_item(self.receiving)
        self.add_item(self.tip)
        
    async def on_submit(self, interaction: discord.Interaction):
        # Rate limit check
        if not rate_limiter.check_cooldown(interaction.user.id, 'ticket_create', 10):
            return await interaction.response.send_message(
                '⏱️ Slow down! Wait 10 seconds before creating another ticket.',
                ephemeral=True
            )
        
        await interaction.response.defer(ephemeral=True)
        
        try:
            await create_ticket(
                interaction.guild,
                interaction.user,
                'middleman',
                self.tier_id,
                {
                    'trader': self.trader.value,
                    'giving': self.giving.value,
                    'receiving': self.receiving.value,
                    'tip': self.tip.value or 'None'
                }
            )
            await interaction.followup.send('✅ Ticket created!', ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f'❌ Error: {str(e)}', ephemeral=True)

# ===== DROPDOWNS =====
class MiddlemanTierSelect(Select):
    def __init__(self):
        super().__init__(
            placeholder='Select trade value tier',
            custom_id='mm_tier_select'
        )
        
    async def callback(self, interaction: discord.Interaction):
        tiers = await db.get_mm_tiers(interaction.guild.id)
        
        selected = self.values[0]
        tier = next((t for t in tiers if t['tier_id'] == selected), None)
        
        if tier:
            modal = MiddlemanModal(tier['tier_id'], tier['name'])
            await interaction.response.send_modal(modal)

class TicketTypeSelect(Select):
    def __init__(self):
        options = [
            discord.SelectOption(
                label='Partnership',
                description='Partnership inquiries',
                emoji='🤝',
                value='partnership'
            ),
            discord.SelectOption(
                label='Middleman',
                description='Middleman services',
                emoji='⚖️',
                value='middleman'
            ),
            discord.SelectOption(
                label='Support',
                description='General support',
                emoji='🎫',
                value='support'
            )
        ]
        
        super().__init__(
            placeholder='Select ticket type',
            options=options,
            custom_id='ticket_type_select'
        )
        
    async def callback(self, interaction: discord.Interaction):
        ticket_type = self.values[0]
        
        if ticket_type == 'middleman':
            tiers = await db.get_mm_tiers(interaction.guild.id)
            
            if not tiers:
                return await interaction.response.send_message(
                    '❌ No MM tiers configured!', 
                    ephemeral=True
                )
            
            select = MiddlemanTierSelect()
            
            for tier in tiers:
                select.add_option(
                    label=tier['name'],
                    value=tier['tier_id'],
                    emoji=tier['emoji']
                )
            
            view = View(timeout=300)
            view.add_item(select)
            
            embed = discord.Embed(
                title='⚖️ Select Trade Value',
                description='Choose the tier that matches your trade value',
                color=0xFEE75C
            )
            
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        else:
            # Rate limit check
            if not rate_limiter.check_cooldown(interaction.user.id, 'ticket_create', 10):
                return await interaction.response.send_message(
                    '⏱️ Slow down! Wait 10 seconds before creating another ticket.',
                    ephemeral=True
                )
            
            await interaction.response.defer(ephemeral=True)
            
            try:
                await create_ticket(
                    interaction.guild,
                    interaction.user,
                    ticket_type
                )
                await interaction.followup.send('✅ Ticket created!', ephemeral=True)
            except Exception as e:
                await interaction.followup.send(f'❌ Error: {str(e)}', ephemeral=True)

# ===== VIEWS =====
class TicketPanelView(View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label='Partnership', style=discord.ButtonStyle.primary, emoji='🤝', custom_id='partnership_btn')
    async def partnership_button(self, interaction: discord.Interaction, button: Button):
        # Rate limit check
        if not rate_limiter.check_cooldown(interaction.user.id, 'ticket_create', 10):
            return await interaction.response.send_message(
                '⏱️ Slow down! Wait 10 seconds before creating another ticket.',
                ephemeral=True
            )
        
        await interaction.response.defer(ephemeral=True)
        
        try:
            await create_ticket(interaction.guild, interaction.user, 'partnership')
            await interaction.followup.send('✅ Ticket created!', ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f'❌ Error: {str(e)}', ephemeral=True)
    
    @discord.ui.button(label='Middleman', style=discord.ButtonStyle.success, emoji='⚖️', custom_id='middleman_btn')
    async def middleman_button(self, interaction: discord.Interaction, button: Button):
        tiers = await db.get_mm_tiers(interaction.guild.id)
        
        if not tiers:
            return await interaction.response.send_message('❌ No MM tiers configured!', ephemeral=True)
        
        select = MiddlemanTierSelect()
        
        for tier in tiers:
            select.add_option(label=tier['name'], value=tier['tier_id'], emoji=tier['emoji'])
        
        view = View(timeout=300)
        view.add_item(select)
        
        embed = discord.Embed(
            title='⚖️ Select Trade Value',
            description='Choose the tier that matches your trade value',
            color=0xFEE75C
        )
        
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    
    @discord.ui.button(label='Support', style=discord.ButtonStyle.secondary, emoji='🎫', custom_id='support_btn')
    async def support_button(self, interaction: discord.Interaction, button: Button):
        # Rate limit check
        if not rate_limiter.check_cooldown(interaction.user.id, 'ticket_create', 10):
            return await interaction.response.send_message(
                '⏱️ Slow down! Wait 10 seconds before creating another ticket.',
                ephemeral=True
            )
        
        await interaction.response.defer(ephemeral=True)
        
        try:
            await create_ticket(interaction.guild, interaction.user, 'support')
            await interaction.followup.send('✅ Ticket created!', ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f'❌ Error: {str(e)}', ephemeral=True)

class TicketControlView(View):
    def __init__(self):
        super().__init__(timeout=None)
        
    @discord.ui.button(label='Claim', style=discord.ButtonStyle.green, custom_id='claim_ticket', emoji='✋')
    async def claim_button(self, interaction: discord.Interaction, button: Button):
        if not rate_limiter.check_cooldown(interaction.user.id, 'claim', 2):
            return await interaction.response.send_message('⏱️ Slow down!', ephemeral=True)
        
        # Check if user has MM role
        mm_role_id = await db.get_ticket_role(interaction.guild.id, 'middleman')
        
        has_mm_role = False
        if mm_role_id:
            has_mm_role = any(role.id == mm_role_id for role in interaction.user.roles)
        
        if not has_mm_role and not interaction.user.guild_permissions.administrator:
            return await interaction.response.send_message('❌ Only middlemen can claim tickets!', ephemeral=True)
        
        try:
            await claim_ticket(interaction.channel, interaction.user)
            await interaction.response.send_message('✅ Claimed!', ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f'❌ {str(e)}', ephemeral=True)
            
    @discord.ui.button(label='Unclaim', style=discord.ButtonStyle.gray, custom_id='unclaim_ticket', emoji='↩️')
    async def unclaim_button(self, interaction: discord.Interaction, button: Button):
        if not rate_limiter.check_cooldown(interaction.user.id, 'unclaim', 2):
            return await interaction.response.send_message('⏱️ Slow down!', ephemeral=True)
        
        # Check if user has MM role
        mm_role_id = await db.get_ticket_role(interaction.guild.id, 'middleman')
        
        has_mm_role = False
        if mm_role_id:
            has_mm_role = any(role.id == mm_role_id for role in interaction.user.roles)
        
        if not has_mm_role and not interaction.user.guild_permissions.administrator:
            return await interaction.response.send_message('❌ Only middlemen can unclaim tickets!', ephemeral=True)
        
        try:
            await unclaim_ticket(interaction.channel)
            await interaction.response.send_message('✅ Unclaimed!', ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f'❌ {str(e)}', ephemeral=True)
            
    @discord.ui.button(label='Close', style=discord.ButtonStyle.red, custom_id='close_ticket', emoji='🔒')
    async def close_button(self, interaction: discord.Interaction, button: Button):
        if not rate_limiter.check_cooldown(interaction.user.id, 'close', 3):
            return await interaction.response.send_message('⏱️ Wait before closing!', ephemeral=True)
        
        # Check if user has MM role
        mm_role_id = await db.get_ticket_role(interaction.guild.id, 'middleman')
        
        has_mm_role = False
        if mm_role_id:
            has_mm_role = any(role.id == mm_role_id for role in interaction.user.roles)
        
        if not has_mm_role and not interaction.user.guild_permissions.administrator:
            return await interaction.response.send_message('❌ Only middlemen can close tickets!', ephemeral=True)
        
        await interaction.response.send_message('🔒 Closing ticket...', ephemeral=True)
        try:
            await close_ticket(interaction.channel, interaction.user)
        except Exception as e:
            await interaction.followup.send(f'❌ {str(e)}', ephemeral=True)

# ===== PANEL EMBEDS =====
def create_panel_embed():
    embed = discord.Embed(
        title='🎫 Support Tickets',
        description=(
            'Select a ticket type below to get started.\n\n'
            '**Available Types:**\n'
            '🤝 Partnership - Business inquiries\n'
            '⚖️ Middleman - Secure trades\n'
            '🎫 Support - General help'
        ),
        color=0x5865F2
    )
    
    embed.set_footer(text='Professional ticket system')
    
    return embed

# ===== WEB SERVER =====
async def handle_health(request):
    return web.Response(text='OK', status=200)

async def handle_root(request):
    return web.Response(
        text='<h1 style="text-align:center;margin-top:50px;font-family:sans-serif;">Ticket Bot Online</h1>',
        content_type='text/html'
    )

async def start_web_server():
    app = web.Application()
    app.router.add_get('/', handle_root)
    app.router.add_get('/health', handle_health)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    port = int(os.getenv('PORT', 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    
    logger.info(f'Web server started on port {port}')

# ===== BOT SETUP =====
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.guilds = True

bot = commands.Bot(command_prefix='$', intents=intents, help_command=None)

# ===== EVENTS =====
@bot.event
async def on_ready():
    logger.info(f'Logged in as {bot.user} (ID: {bot.user.id})')
    logger.info(f'Connected to {len(bot.guilds)} guilds')
    
    try:
        await db.connect()
        logger.info('✅ Database connected')
    except Exception as e:
        logger.error(f'❌ Database connection failed: {e}')
        return
    
    bot.add_view(TicketPanelView())
    bot.add_view(TicketControlView())
    
    logger.info('✅ Persistent views registered')
    
    # Start rate limiter cleanup
    bot.loop.create_task(rate_limiter.cleanup_old_entries())
    logger.info('✅ Rate limiter active')
    
    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.watching,
            name='tickets | $help'
        )
    )
    
    logger.info('🚀 Bot ready!')

# ===== COMMANDS =====
@bot.command(name='setup')
@commands.has_permissions(administrator=True)
async def setup_panel(ctx):
    embed = create_panel_embed()
    view = TicketPanelView()
    
    message = await ctx.send(embed=embed, view=view)
    await db.set_config(ctx.guild.id, panel_message_id=message.id)
    
    await ctx.reply('✅ Ticket panel created!', delete_after=5)

@bot.command(name='setcategory')
@commands.has_permissions(administrator=True)
async def set_category(ctx, category: discord.CategoryChannel):
    await db.set_config(ctx.guild.id, ticket_category_id=category.id)
    
    embed = discord.Embed(
        title='✅ Category Set',
        description=f'Ticket category: {category.mention}',
        color=0x57F287
    )
    await ctx.reply(embed=embed)

@bot.command(name='setlogs')
@commands.has_permissions(administrator=True)
async def set_logs(ctx, channel: discord.TextChannel):
    await db.set_config(ctx.guild.id, log_channel_id=channel.id)
    
    embed = discord.Embed(
        title='✅ Logs Channel Set',
        description=f'Log channel: {channel.mention}',
        color=0x57F287
    )
    await ctx.reply(embed=embed)

@bot.command(name='ticketrole')
@commands.has_permissions(administrator=True)
async def set_ticket_role(ctx, ticket_type: str, role: discord.Role):
    valid_types = ['partnership', 'middleman', 'support']
    if ticket_type.lower() not in valid_types:
        return await ctx.reply(f'❌ Invalid type! Use: {", ".join(valid_types)}')
        
    await db.set_ticket_role(ctx.guild.id, ticket_type.lower(), role.id)
    
    embed = discord.Embed(
        title='✅ Ticket Role Set',
        description=f'**{ticket_type}** → {role.mention}',
        color=0x57F287
    )
    await ctx.reply(embed=embed)

@bot.command(name='ticketroles')
async def view_ticket_roles(ctx):
    roles = await db.get_all_ticket_roles(ctx.guild.id)
    
    if not roles:
        return await ctx.reply('No ticket roles configured!')
        
    embed = discord.Embed(
        title='🎫 Ticket Roles',
        color=0x5865F2
    )
    
    for role_data in roles:
        role = ctx.guild.get_role(role_data['role_id'])
        if role:
            embed.add_field(
                name=role_data['ticket_type'].title(),
                value=role.mention,
                inline=True
            )
            
    await ctx.reply(embed=embed)

@bot.command(name='mmtier')
@commands.has_permissions(administrator=True)
async def set_mm_tier(ctx, tier_id: str, role: discord.Role, emoji: str, *, name: str):
    tiers = await db.get_mm_tiers(ctx.guild.id)
    position = len(tiers)
    
    await db.set_mm_tier(tier_id, ctx.guild.id, name, role.id, emoji, position)
    
    embed = discord.Embed(
        title='✅ MM Tier Configured',
        description=f'{emoji} **{name}**\nRole: {role.mention}',
        color=0x57F287
    )
    await ctx.reply(embed=embed)

@bot.command(name='mmtiers')
async def view_mm_tiers(ctx):
    tiers = await db.get_mm_tiers(ctx.guild.id)
    
    if not tiers:
        return await ctx.reply('No MM tiers configured! Use `$mmtier` to set them.')
        
    embed = discord.Embed(
        title='⚖️ Middleman Tiers',
        color=0xFEE75C
    )
    
    for tier in tiers:
        role = ctx.guild.get_role(tier['role_id'])
        embed.add_field(
            name=f"{tier['emoji']} {tier['name']}",
            value=role.mention if role else 'Role deleted',
            inline=False
        )
        
    await ctx.reply(embed=embed)

@bot.command(name='deltier')
@commands.has_permissions(administrator=True)
async def delete_tier(ctx, tier_id: str):
    await db.delete_mm_tier(tier_id)
    
    embed = discord.Embed(
        title='✅ Tier Deleted',
        description=f'Removed tier: `{tier_id}`',
        color=0x57F287
    )
    await ctx.reply(embed=embed)

@bot.command(name='config')
@commands.has_permissions(administrator=True)
async def view_config(ctx):
    config = await db.get_config(ctx.guild.id)
    
    embed = discord.Embed(
        title='⚙️ Server Configuration',
        color=0x5865F2
    )
    
    if config:
        category = ctx.guild.get_channel(config['ticket_category_id']) if config.get('ticket_category_id') else None
        logs = ctx.guild.get_channel(config['log_channel_id']) if config.get('log_channel_id') else None
        
        embed.add_field(
            name='Ticket Category',
            value=category.mention if category else 'Not set',
            inline=False
        )
        embed.add_field(
            name='Log Channel',
            value=logs.mention if logs else 'Not set',
            inline=False
        )
    else:
        embed.description = 'No configuration found. Use setup commands to configure.'
        
    await ctx.reply(embed=embed)

@bot.command(name='close')
async def close_cmd(ctx):
    if not rate_limiter.check_cooldown(ctx.author.id, 'close_cmd', 3):
        return await ctx.reply('⏱️ Wait 3 seconds between closes!')
    
    # Check if user has MM role
    config = await db.get_config(ctx.guild.id)
    mm_role_id = await db.get_ticket_role(ctx.guild.id, 'middleman')
    
    has_mm_role = False
    if mm_role_id:
        has_mm_role = any(role.id == mm_role_id for role in ctx.author.roles)
    
    if not has_mm_role and not ctx.author.guild_permissions.administrator:
        return await ctx.reply('❌ Only middlemen can close tickets!')
    
    try:
        await close_ticket(ctx.channel, ctx.author)
    except Exception as e:
        await ctx.reply(f'❌ {str(e)}')

@bot.command(name='claim')
async def claim_cmd(ctx):
    if not rate_limiter.check_cooldown(ctx.author.id, 'claim_cmd', 2):
        return await ctx.reply('⏱️ Wait 2 seconds between claims!')
    
    # Check if user has MM role
    mm_role_id = await db.get_ticket_role(ctx.guild.id, 'middleman')
    
    has_mm_role = False
    if mm_role_id:
        has_mm_role = any(role.id == mm_role_id for role in ctx.author.roles)
    
    if not has_mm_role and not ctx.author.guild_permissions.administrator:
        return await ctx.reply('❌ Only middlemen can claim tickets!')
    
    try:
        await claim_ticket(ctx.channel, ctx.author)
    except Exception as e:
        await ctx.reply(f'❌ {str(e)}')

@bot.command(name='unclaim')
async def unclaim_cmd(ctx):
    if not rate_limiter.check_cooldown(ctx.author.id, 'unclaim_cmd', 2):
        return await ctx.reply('⏱️ Wait 2 seconds!')
    
    # Check if user has MM role
    mm_role_id = await db.get_ticket_role(ctx.guild.id, 'middleman')
    
    has_mm_role = False
    if mm_role_id:
        has_mm_role = any(role.id == mm_role_id for role in ctx.author.roles)
    
    if not has_mm_role and not ctx.author.guild_permissions.administrator:
        return await ctx.reply('❌ Only middlemen can unclaim tickets!')
    
    try:
        await unclaim_ticket(ctx.channel)
    except Exception as e:
        await ctx.reply(f'❌ {str(e)}')

@bot.command(name='add')
async def add_user(ctx, member: discord.Member):
    if not ctx.channel.category or 'ticket' not in ctx.channel.name.lower():
        return await ctx.reply('❌ This is not a ticket channel!')
    
    # Check if user has MM role
    mm_role_id = await db.get_ticket_role(ctx.guild.id, 'middleman')
    
    has_mm_role = False
    if mm_role_id:
        has_mm_role = any(role.id == mm_role_id for role in ctx.author.roles)
    
    if not has_mm_role and not ctx.author.guild_permissions.administrator:
        return await ctx.reply('❌ Only middlemen can add users to tickets!')
        
    await ctx.channel.set_permissions(
        member,
        read_messages=True,
        send_messages=True
    )
    
    embed = discord.Embed(
        title='➕ User Added',
        description=f'{member.mention} added to ticket',
        color=0x57F287
    )
    await ctx.reply(embed=embed)
    
    # Log user add
    config = await db.get_config(ctx.guild.id)
    if config and config.get('log_channel_id'):
        log_channel = ctx.guild.get_channel(config['log_channel_id'])
        if log_channel:
            tickets = await db.pool.fetch('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
            if tickets:
                ticket = dict(tickets[0])
                log_embed = discord.Embed(
                    title='➕ User Added to Ticket',
                    description=f"{ctx.author.mention} added {member.mention} to {ctx.channel.mention}",
                    color=0x57F287
                )
                log_embed.add_field(name='Ticket ID', value=f"`{ticket['ticket_id']}`", inline=True)
                await log_channel.send(embed=log_embed)

@bot.command(name='remove')
async def remove_user(ctx, member: discord.Member):
    if not ctx.channel.category or 'ticket' not in ctx.channel.name.lower():
        return await ctx.reply('❌ This is not a ticket channel!')
    
    # Check if user has MM role
    mm_role_id = await db.get_ticket_role(ctx.guild.id, 'middleman')
    
    has_mm_role = False
    if mm_role_id:
        has_mm_role = any(role.id == mm_role_id for role in ctx.author.roles)
    
    if not has_mm_role and not ctx.author.guild_permissions.administrator:
        return await ctx.reply('❌ Only middlemen can remove users from tickets!')
        
    await ctx.channel.set_permissions(member, overwrite=None)
    
    embed = discord.Embed(
        title='➖ User Removed',
        description=f'{member.mention} removed from ticket',
        color=0xED4245
    )
    await ctx.reply(embed=embed)
    
    # Log user remove
    config = await db.get_config(ctx.guild.id)
    if config and config.get('log_channel_id'):
        log_channel = ctx.guild.get_channel(config['log_channel_id'])
        if log_channel:
            tickets = await db.pool.fetch('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
            if tickets:
                ticket = dict(tickets[0])
                log_embed = discord.Embed(
                    title='➖ User Removed from Ticket',
                    description=f"{ctx.author.mention} removed {member.mention} from {ctx.channel.mention}",
                    color=0xED4245
                )
                log_embed.add_field(name='Ticket ID', value=f"`{ticket['ticket_id']}`", inline=True)
                await log_channel.send(embed=log_embed)

@bot.command(name='rename')
async def rename_ticket(ctx, *, new_name: str):
    if not ctx.channel.category or 'ticket' not in ctx.channel.name.lower():
        return await ctx.reply('❌ This is not a ticket channel!')
    
    # Check if user has MM role
    mm_role_id = await db.get_ticket_role(ctx.guild.id, 'middleman')
    
    has_mm_role = False
    if mm_role_id:
        has_mm_role = any(role.id == mm_role_id for role in ctx.author.roles)
    
    if not has_mm_role and not ctx.author.guild_permissions.administrator:
        return await ctx.reply('❌ Only middlemen can rename tickets!')
    
    old_name = ctx.channel.name
    await ctx.channel.edit(name=new_name)
    
    embed = discord.Embed(
        title='✏️ Ticket Renamed',
        description=f'New name: `{new_name}`',
        color=0x5865F2
    )
    await ctx.reply(embed=embed)
    
    # Log rename
    config = await db.get_config(ctx.guild.id)
    if config and config.get('log_channel_id'):
        log_channel = ctx.guild.get_channel(config['log_channel_id'])
        if log_channel:
            tickets = await db.pool.fetch('SELECT * FROM tickets WHERE channel_id = $1', ctx.channel.id)
            if tickets:
                ticket = dict(tickets[0])
                log_embed = discord.Embed(
                    title='✏️ Ticket Renamed',
                    description=f"{ctx.author.mention} renamed ticket",
                    color=0x5865F2
                )
                log_embed.add_field(name='Ticket ID', value=f"`{ticket['ticket_id']}`", inline=True)
                log_embed.add_field(name='Old Name', value=f"`{old_name}`", inline=True)
                log_embed.add_field(name='New Name', value=f"`{new_name}`", inline=True)
                await log_channel.send(embed=log_embed)

@bot.command(name='blacklist', aliases=['bl'])
@commands.has_permissions(administrator=True)
async def blacklist_user(ctx, member: discord.Member, *, reason: str = "No reason provided"):
    """Blacklist a user from creating tickets"""
    await db.blacklist_user(member.id, ctx.guild.id, reason, ctx.author.id)
    
    embed = discord.Embed(
        title='🚫 User Blacklisted',
        description=f'{member.mention} can no longer create tickets',
        color=0xED4245
    )
    embed.add_field(name='Reason', value=reason, inline=False)
    embed.set_footer(text=f'Blacklisted by {ctx.author}')
    
    await ctx.reply(embed=embed)
    
    # Log blacklist
    config = await db.get_config(ctx.guild.id)
    if config and config.get('log_channel_id'):
        log_channel = ctx.guild.get_channel(config['log_channel_id'])
        if log_channel:
            await log_channel.send(embed=embed)

@bot.command(name='unblacklist', aliases=['ubl'])
@commands.has_permissions(administrator=True)
async def unblacklist_user(ctx, member: discord.Member):
    """Remove a user from blacklist"""
    await db.unblacklist_user(member.id)
    
    embed = discord.Embed(
        title='✅ User Unblacklisted',
        description=f'{member.mention} can now create tickets again',
        color=0x57F287
    )
    
    await ctx.reply(embed=embed)

@bot.command(name='blacklists', aliases=['bllist'])
@commands.has_permissions(administrator=True)
async def view_blacklist(ctx):
    """View all blacklisted users"""
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            'SELECT * FROM blacklist WHERE guild_id = $1',
            ctx.guild.id
        )
    
    if not rows:
        return await ctx.reply('No blacklisted users!')
    
    embed = discord.Embed(
        title='🚫 Blacklisted Users',
        color=0xED4245
    )
    
    for row in rows:
        user = ctx.guild.get_member(row['user_id'])
        username = user.mention if user else f"ID: {row['user_id']}"
        embed.add_field(
            name=username,
            value=f"**Reason:** {row['reason']}\n**Date:** {row['blacklisted_at'].strftime('%Y-%m-%d')}",
            inline=False
        )
    
    await ctx.reply(embed=embed)

@bot.command(name='stats')
async def view_stats(ctx, member: discord.Member = None):
    target = member or ctx.author
    stats = await db.get_user_stats(target.id)
    
    embed = discord.Embed(
        title=f'📊 Stats - {target.display_name}',
        color=0x5865F2
    )
    
    embed.add_field(
        name='Tickets Claimed',
        value=f"**{stats['tickets_claimed']}**",
        inline=True
    )
    embed.add_field(
        name='Tickets Closed',
        value=f"**{stats['tickets_closed']}**",
        inline=True
    )
    
    if stats.get('last_claim'):
        embed.set_footer(text=f"Last claim: {stats['last_claim'].strftime('%Y-%m-%d %H:%M')}")
        
    embed.set_thumbnail(url=target.display_avatar.url)
    
    await ctx.reply(embed=embed)

@bot.command(name='feedbackstats', aliases=['fbstats'])
@commands.has_permissions(administrator=True)
async def feedback_stats(ctx):
    """View feedback statistics"""
    stats = await db.get_feedback_stats(ctx.guild.id)
    
    avg_rating = stats.get('avg_rating', 0) or 0
    total_feedback = stats.get('total_feedback', 0) or 0
    
    if total_feedback == 0:
        return await ctx.reply('No feedback received yet!')
    
    # Calculate stars
    full_stars = int(avg_rating)
    half_star = 1 if (avg_rating - full_stars) >= 0.5 else 0
    empty_stars = 5 - full_stars - half_star
    
    star_display = '⭐' * full_stars
    if half_star:
        star_display += '✨'
    star_display += '☆' * empty_stars
    
    embed = discord.Embed(
        title='⭐ Feedback Statistics',
        color=0xFEE75C
    )
    
    embed.add_field(
        name='Average Rating',
        value=f'{star_display}\n**{avg_rating:.1f}/5.0**',
        inline=False
    )
    
    embed.add_field(
        name='Total Feedback',
        value=f'**{total_feedback}** reviews',
        inline=False
    )
    
    await ctx.reply(embed=embed)

@bot.command(name='leaderboard', aliases=['lb', 'top'])
async def leaderboard(ctx):
    """View top staff by tickets closed"""
    async with db.pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT user_id, tickets_claimed, tickets_closed 
            FROM stats 
            WHERE tickets_closed > 0
            ORDER BY tickets_closed DESC 
            LIMIT 10
        ''')
    
    if not rows:
        return await ctx.reply('No stats available yet!')
    
    embed = discord.Embed(
        title='🏆 Top Staff - Leaderboard',
        description='Top 10 by tickets closed',
        color=0xFEE75C
    )
    
    medals = ['🥇', '🥈', '🥉']
    
    leaderboard_text = ""
    for i, row in enumerate(rows, 1):
        user = ctx.guild.get_member(row['user_id'])
        if user:
            medal = medals[i-1] if i <= 3 else f"`{i}.`"
            leaderboard_text += f"{medal} **{user.display_name}** - {row['tickets_closed']} closed\n"
    
    embed.description = leaderboard_text
    
    await ctx.reply(embed=embed)

@bot.command(name='help')
async def help_command(ctx):
    embed = discord.Embed(
        title='📚 Bot Commands',
        description='Professional ticket management system',
        color=0x5865F2
    )
    
    embed.add_field(
        name='🎫 Ticket Commands',
        value=(
            '`$close` - Close current ticket\n'
            '`$claim` - Claim ticket\n'
            '`$unclaim` - Unclaim ticket\n'
            '`$add @user` - Add user to ticket\n'
            '`$remove @user` - Remove user\n'
            '`$rename <n>` - Rename ticket\n'
            '`$stats [@user]` - View stats\n'
            '`$leaderboard` - Top staff rankings'
        ),
        inline=False
    )
    
    if ctx.author.guild_permissions.administrator:
        embed.add_field(
            name='⚙️ Setup Commands',
            value=(
                '`$setup` - Create ticket panel\n'
                '`$setcategory #cat` - Set category\n'
                '`$setlogs #channel` - Set logs\n'
                '`$ticketrole <type> @role` - Set role\n'
                '`$mmtier <id> @role <emoji> <n>` - MM tier\n'
                '`$config` - View configuration\n'
                '`$feedbackstats` - View feedback stats'
            ),
            inline=False
        )
        
        embed.add_field(
            name='🚫 Moderation',
            value=(
                '`$blacklist @user <reason>` - Block tickets\n'
                '`$unblacklist @user` - Unblock user\n'
                '`$blacklists` - View blacklist'
            ),
            inline=False
        )
    
    embed.set_footer(text='Feedback requested after ticket closes')
    
    await ctx.reply(embed=embed)

@bot.command(name='ping')
async def ping(ctx):
    latency = round(bot.latency * 1000)
    
    embed = discord.Embed(
        title='🏓 Pong!',
        description=f'Latency: **{latency}ms**',
        color=0x57F287 if latency < 200 else 0xFEE75C if latency < 500 else 0xED4245
    )
    
    await ctx.reply(embed=embed)

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.reply('❌ You lack permissions for this command!')
    elif isinstance(error, commands.MemberNotFound):
        await ctx.reply('❌ User not found!')
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.reply(f'❌ Missing argument! Use `$help` for usage.')
    elif isinstance(error, commands.CommandNotFound):
        pass
    else:
        logger.error(f'Command error: {error}')

# ===== MAIN =====
async def main():
    await start_web_server()
    
    token = os.getenv('DISCORD_TOKEN')
    if not token:
        logger.error('❌ DISCORD_TOKEN not found!')
        return
    
    try:
        await bot.start(token)
    except KeyboardInterrupt:
        logger.info('Shutting down...')
    finally:
        await db.close()
        await bot.close()

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
