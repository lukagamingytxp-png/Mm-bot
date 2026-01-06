import discord
from discord.ext import commands
from discord.ui import Button, View, Select, Modal, TextInput
import os
import json
from datetime import datetime
import asyncio
import random
from flask import Flask
from threading import Thread
from datetime import timedelta

# for keeping bot alive on Render
app = Flask('')

@app.route('/')
def home():
    return "<h1 style='text-align:center; margin-top:50px; font-family:Arial;'>Bot is Active</h1>"

def run():
    app.run(host='0.0.0.0', port=5000)

def keep_alive():
    t = Thread(target=run)
    t.start()

# Bot Configuration
PREFIX = '$'
TICKET_CATEGORY = 'Tickets'
LOG_CHANNEL = 'ticket-logs'

# Bot Setup
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix=PREFIX, intents=intents, help_command=None)

# ===== CONSTANTS =====
TICKET_CATEGORY_NAME = "Base Tickets"
BASE_PROVIDER_ROLE_ID = 1457797160564298031
# =====================

# Storage
active_tickets = {}
claimed_tickets = {}
ticket_roles = {}

# Color Scheme
COLORS = {
    'partnership': 0x5865F2,
    'middleman': 0xFEE75C,
    'support': 0x57F287,
    'error': 0xED4245,
    'success': 0x57F287,
    'info': 0x5865F2
}

# Ticket Types
TICKET_TYPES = {
    'partnership': {
        'name': 'Partnership',
        'emoji': '🤝',
        'color': COLORS['partnership'],
        'description': 'Discuss partnership opportunities'
    },
    'middleman': {
        'name': 'Middleman',
        'emoji': '⚖️',
        'color': COLORS['middleman'],
        'description': 'Request middleman services'
    },
    'support': {
        'name': 'Support',
        'emoji': '🎫',
        'color': COLORS['support'],
        'description': 'Get help and support'
    }
}

# MM Tier definitions
MM_TIERS = {
    'trial': {'name': 'Trial Middleman', 'range': 'Up to 100m/s'},
    'middleman': {'name': 'Middleman', 'range': '100m/s - 250m/s'},
    'pro': {'name': 'Pro Middleman', 'range': '250m/s - 500m/s'},
    'head': {'name': 'Head Middleman', 'range': '500m/s+'},
    'owner': {'name': 'Owner', 'range': '500m/s+ (fee required)'}
}

MM_TICKET_ROLE_MAP = {
    "trial middleman": "trial",
    "middleman": "middleman",
    "pro middleman": "pro",
    "head middleman": "head",
    "owner middleman": "owner"
}

MM_ROLE_IDS = {
    "trial": 1453757017218093239,      # Trial MM role ID
    "middleman": 1434610759140118640,  # Middleman role ID
    "pro": 1453757157144137911,        # Pro MM role ID
    "head": 1453757225267892276,       # Head MM role ID
    "owner": 1448796685672386712     # Owner role ID
}

# Save and Load Data Functions
def save_data():
    data = {
        'ticket_roles': ticket_roles,
        'active_tickets': active_tickets,
        'claimed_tickets': claimed_tickets
    }
    with open('bot_data.json', 'w') as f:
        json.dump(data, f, indent=4)

def load_data():
    global ticket_roles, warnings, active_tickets, claimed_tickets
    try:
        with open('bot_data.json', 'r') as f:
            data = json.load(f)
            ticket_roles.update(data.get('ticket_roles', {}))
            warnings.update(data.get('warnings', {}))
            active_tickets.update(data.get('active_tickets', {}))
            claimed_tickets.update(data.get('claimed_tickets', {}))
        print('✅ Data loaded successfully!')
    except FileNotFoundError:
        print('⚠️ No saved data found, starting fresh.')
    except Exception as e:
        print(f'❌ Error loading data: {e}')

# MM Trade Details Modal
class MMTradeModal(Modal, title='Middleman Trade Details'):
    def __init__(self, tier):
        super().__init__()
        self.tier = tier

        self.trader = TextInput(
            label='Who are you trading with?',
            placeholder='@user or ID',
            required=True,
            max_length=100
        )

        self.giving = TextInput(
            label='What are you giving?',
            placeholder='Example: 1 garam',
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=500
        )

        self.receiving = TextInput(
            label='What is the other trader giving?',
            placeholder='Example: 296 Robux',
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=500
        )

        self.both_join = TextInput(
            label='Can both users join links?',
            placeholder='YES or NO',
            required=True,
            max_length=10
        )

        self.tip = TextInput(
            label='Will you tip the MM?',
            placeholder='Optional',
            required=False,
            max_length=200
        )

        self.add_item(self.trader)
        self.add_item(self.giving)
        self.add_item(self.receiving)
        self.add_item(self.both_join)
        self.add_item(self.tip)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        try:
            await interaction.followup.send(
                '✅ Middleman ticket created successfully!',
                ephemeral=True
            )

        except Exception as e:
            await interaction.followup.send(
                f'❌ Failed to create ticket:\n```{e}```',
                ephemeral=True
            )
        
        # Create the ticket NOW with all the info
        try:
            # Create ticket channel
            await create_ticket_with_details(
                interaction.guild, 
                interaction.user, 
                'middleman',
                self.tier,
                self.trader.value,
                self.giving.value,
                self.receiving.value,
                self.both_join.value,
                self.tip.value if self.tip.value else 'None'
            )
            await interaction.followup.send('✅ Middleman ticket created! Check the ticket channel.', ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f'❌ Error creating ticket: {str(e)}', ephemeral=True)

# Tier Selection Dropdown
class TierSelect(Select):
    def __init__(self):
        options = [
            discord.SelectOption(
                label='Trial Middleman',
                description='Up to 100m/s',
                value='trial',
                emoji='🆕'
            ),
            discord.SelectOption(
                label='Middleman',
                description='100m/s - 250m/s',
                value='middleman',
                emoji='⚖️'
            ),
            discord.SelectOption(
                label='Pro Middleman',
                description='250m/s - 500m/s',
                value='pro',
                emoji='⭐'
            ),
            discord.SelectOption(
                label='Head Middleman',
                description='500m/s+',
                value='head',
                emoji='👑'
            ),
            discord.SelectOption(
                label='Owner',
                description='500m/s+ (fee required)',
                value='owner',
                emoji='💎'
            )
        ]
        
        super().__init__(
            placeholder='Select tier based on your trade value',
            options=options,
            custom_id='tier_select'
        )
    
    async def callback(self, interaction: discord.Interaction):
        selected_tier = self.values[0]
        
        # Open the modal with trade details
        modal = MMTradeModal(selected_tier)
        await interaction.response.send_modal(modal)
        
        # Delete the tier selection message after they select
        try:
            await interaction.message.delete()
        except:
            pass

class TierSelectView(View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(TierSelect())

# Button Views
class TicketButtons(View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Partnership', emoji='🤝', style=discord.ButtonStyle.primary, custom_id='ticket_partnership')
    async def partnership_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer(ephemeral=True)
        try:
            await create_ticket(interaction.guild, interaction.user, 'partnership')
            await interaction.followup.send('✅ Ticket created! Check the ticket channel.', ephemeral=True)
        except discord.Forbidden:
    
            await interaction.followup.send('❌ I don\'t have permission to create channels!', ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f'❌ Error: {str(e)}', ephemeral=True)
    


    @discord.ui.button(label='Middleman', emoji='⚖️', style=discord.ButtonStyle.secondary, custom_id='ticket_middleman')
    async def middleman_button(self, interaction: discord.Interaction, button: Button):
        # Show tier selection dropdown instead of creating ticket immediately
        tier_embed = discord.Embed(
            title='Select your middleman tier:',
            color=COLORS['middleman']
        )
        await interaction.response.send_message(embed=tier_embed, view=TierSelectView(), ephemeral=True)

    @discord.ui.button(label='Support', emoji='🎫', style=discord.ButtonStyle.success, custom_id='ticket_support')
    async def support_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer(ephemeral=True)
        try:
            await create_ticket(interaction.guild, interaction.user, 'support')
            await interaction.followup.send('✅ Ticket created! Check the ticket channel.', ephemeral=True)
        except discord.Forbidden:
            await interaction.followup.send('❌ I don\'t have permission to create channels!', ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f'❌ Error: {str(e)}', ephemeral=True)

class CloseTicketView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Close Ticket', emoji='🔒', style=discord.ButtonStyle.danger, custom_id='confirm_close')
    async def close_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        await close_ticket(interaction.channel, interaction.user)

# ===================== DROPDOWN =====================
class BaseServiceSelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(
                label="Halloween Base",
                description="🎃 Open a Halloween base service ticket",
                emoji="🎃",
                value="halloween"
            ),
            discord.SelectOption(
                label="Aqua Base",
                description="🌊 Open an Aqua base service ticket",
                emoji="🌊",
                value="aqua"
            )
        ]

        super().__init__(
            placeholder="Choose a base service…",
            options=options,
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):
        # TEMP CONFIRMATION (no ticket logic yet)
        await interaction.response.send_message(
            "✅ Dropdown works (ticket logic comes next)",
            ephemeral=True
        )


class BasePanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(BaseServiceSelect())
# ====================================================


# ===================== COMMAND ======================
@bot.command(name="basepanel")
@commands.has_permissions(administrator=True)
async def basepanel(ctx):
    embed = discord.Embed(
        title="🛠️ Base Services Panel",
        description=(
            "Select the base service you need:\n\n"
            "🎃 **Halloween Base Service**\n"
            "🌊 **Aqua Base Service**"
        ),
        color=discord.Color.purple()
    )

    await ctx.send(embed=embed, view=BasePanelView())
# ====================================================

# Events
@bot.event
async def on_ready():
    print(f'✅ Bot is online as {bot.user}')
    print(f'📊 Serving {len(bot.guilds)} servers')
    await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name='$help | Ticket System'))

    # Add persistent views
    bot.add_view(TicketButtons())
    bot.add_view(CloseTicketView())
    bot.add_view(TierSelectView())
    bot.add_view(MMTicketView())
    load_giveaways()
    bot.add_view(GiveawayView(0))
    
# Load saved data
    load_data()

# Help Command
@bot.command(name='help')
async def help_command(ctx):
    embed = discord.Embed(
        title='🎫 Ticket System - Help',
        description='Professional ticket management system for your server',
        color=COLORS['info']
    )
    embed.add_field(
        name='📋 Ticket Commands',
        value='```\n$new <type> - Create a new ticket\n$close - Close current ticket\n$claim - Claim a ticket\n$unclaim - Unclaim a ticket\n$add <user> - Add user to ticket\n$remove <user> - Remove user from ticket\n$rename <n> - Rename ticket channel```',
        inline=False
    )
    embed.add_field(
        name='🏷️ Ticket Types',
        value='```\npartnership - Partnership inquiries\nmiddleman - Middleman services\nsupport - General support```',
        inline=False
    )
    embed.add_field(
        name='⚙️ Setup Commands',
        value='```\n.setup - Create ticket panel\n.stats - View ticket statistics```',
        inline=False
    )
    embed.set_footer(text=f'Requested by {ctx.author}', icon_url=ctx.author.display_avatar.url)
    embed.timestamp = datetime.utcnow()

    await ctx.reply(embed=embed)



# ===== COINFLIP COMMANDS - ADD THESE TO YOUR EXISTING BOT =====

# Prefix command: $coinflip
@bot.command(name='gambleflip')
async def coinflip_prefix(ctx, user1: discord.Member, user2: discord.Member, flips: int):
    """Coinflip between two users with a specified number of flips"""
    
    if flips < 1:
        await ctx.send("❌ Number of flips must be at least 1!")
        return
    
    if flips > 100:
        await ctx.send("❌ Maximum 100 flips allowed!")
        return
    
    # Initialize scores
    scores = {user1.display_name: 0, user2.display_name: 0}
    
    # Create initial embed
    embed = discord.Embed(
        title="🪙 Coinflip Battle!",
        description=f"{user1.mention} vs {user2.mention}",
        color=discord.Color.gold()
    )
    embed.add_field(name=user1.display_name, value="0", inline=True)
    embed.add_field(name=user2.display_name, value="0", inline=True)
    embed.add_field(name="Flips Remaining", value=str(flips), inline=False)
    
    message = await ctx.send(embed=embed)
    
    # Perform flips
    for i in range(flips):
        await asyncio.sleep(1)  # Wait 1 second between flips
        
        # Random coinflip
        winner = random.choice([user1, user2])
        scores[winner.display_name] += 1
        
        # Update embed
        embed = discord.Embed(
            title="🪙 Coinflip Battle!",
            description=f"{user1.mention} vs {user2.mention}\n\n**Flip #{i+1}**: {winner.mention} wins!",
            color=discord.Color.gold()
        )
        embed.add_field(name=user1.display_name, value=str(scores[user1.display_name]), inline=True)
        embed.add_field(name=user2.display_name, value=str(scores[user2.display_name]), inline=True)
        embed.add_field(name="Flips Remaining", value=str(flips - i - 1), inline=False)
        
        await message.edit(embed=embed)
    
    # Final results
    await asyncio.sleep(1)
    
    if scores[user1.display_name] > scores[user2.display_name]:
        final_winner = user1
        color = discord.Color.green()
    elif scores[user2.display_name] > scores[user1.display_name]:
        final_winner = user2
        color = discord.Color.green()
    else:
        final_winner = None
        color = discord.Color.blue()
    
    final_embed = discord.Embed(
        title="🏆 Coinflip Results!",
        description=f"{user1.mention} vs {user2.mention}",
        color=color
    )
    final_embed.add_field(name=user1.display_name, value=f"**{scores[user1.display_name]}** wins", inline=True)
    final_embed.add_field(name=user2.display_name, value=f"**{scores[user2.display_name]}** wins", inline=True)
    
    if final_winner:
        final_embed.add_field(name="Winner", value=f"🎉 {final_winner.mention}", inline=False)
    else:
        final_embed.add_field(name="Result", value="🤝 It's a tie!", inline=False)
    
    await message.edit(embed=final_embed)

# Setup Command
@bot.command(name='setup')
@commands.has_permissions(administrator=True)
async def setup(ctx):
    embed = discord.Embed(
        title='🎫 Create a Ticket',
        description='Click the button below to create a ticket based on your needs.\n\n**Available Ticket Types:**',
        color=COLORS['info']
    )
    embed.add_field(name='🤝 Partnership', value='For partnership opportunities and collaborations', inline=True)
    embed.add_field(name='⚖️ Middleman', value='For secure middleman services', inline=True)
    embed.add_field(name='🎫 Support', value='For general help and support', inline=True)
    embed.set_footer(text='Select a ticket type to get started')
    embed.timestamp = datetime.utcnow()

    await ctx.send(embed=embed, view=TicketButtons())
    await ctx.reply('✅ Ticket panel created successfully!')

# remind me cmd
@bot.command(name='remindme')
async def remind_me(ctx, time: int, unit: str, *, reminder: str):
    """Set a reminder: $remindme 10 minutes Buy milk"""
    
    units = {'seconds': 1, 'minutes': 60, 'hours': 3600, 'days': 86400}
    
    if unit not in units:
        return await ctx.reply('❌ Use: seconds, minutes, hours, or days')
    
    wait_time = time * units[unit]
    
    await ctx.reply(f'⏰ I\'ll remind you in {time} {unit}!')
    
    await asyncio.sleep(wait_time)
    
    await ctx.author.send(f'🔔 **Reminder:** {reminder}')


#snipe command
deleted_messages = {}

@bot.event
async def on_message_delete(message):
    if not message.author.bot:
        deleted_messages[message.channel.id] = {
            'author': message.author,
            'content': message.content,
            'time': datetime.utcnow()
        }

@bot.command(name='snipe')
async def snipe(ctx):
    """See the last deleted message"""
    if ctx.channel.id not in deleted_messages:
        return await ctx.reply('❌ No deleted messages!')
    
    data = deleted_messages[ctx.channel.id]
    embed = discord.Embed(
        description=data['content'],
        color=0xED4245
    )
    embed.set_author(name=data['author'].name, icon_url=data['author'].display_avatar.url)
    embed.set_footer(text='Deleted')
    embed.timestamp = data['time']
    
    await ctx.reply(embed=embed)
 
# ============================================
# PROFESSIONAL GIVEAWAY SYSTEM - COMPLETE
# ============================================

# Storage for active giveaways
active_giveaways = {}

def load_giveaways():
    global active_giveaways
    try:
        with open('giveaways.json', 'r') as f:
            active_giveaways = json.load(f)
        print('✅ Giveaway data loaded')
    except FileNotFoundError:
        print('⚠️ No giveaway data found')
    except Exception as e:
        print(f'❌ Error loading giveaways: {e}')

def save_giveaways():
    with open('giveaways.json', 'w') as f:
        json.dump(active_giveaways, f, indent=4)

# ============================================
# GIVEAWAY ENTRY BUTTON
# ============================================

class GiveawayView(View):
    def __init__(self, giveaway_id):
        super().__init__(timeout=None)
        self.giveaway_id = giveaway_id
    
    @discord.ui.button(label='🎉 Enter Giveaway', style=discord.ButtonStyle.success, custom_id='enter_giveaway')
    async def enter_button(self, interaction: discord.Interaction, button: Button):
        giveaway = active_giveaways.get(str(self.giveaway_id))
        
        if not giveaway:
            return await interaction.response.send_message('❌ This giveaway no longer exists!', ephemeral=True)
        
        user_id = str(interaction.user.id)
        
        # Check if already entered
        if user_id in giveaway['entries']:
            return await interaction.response.send_message('❌ You have already entered this giveaway!', ephemeral=True)
        
        # Add entry
        giveaway['entries'].append(user_id)
        save_giveaways()
        
        # Update embed with new entry count
        try:
            message = await interaction.channel.fetch_message(self.giveaway_id)
            embed = message.embeds[0]
            
            # Update entries field
            for i, field in enumerate(embed.fields):
                if 'Entries' in field.name:
                    embed.set_field_at(i, name='📊 Entries', value=f'**{len(giveaway["entries"])}** participants', inline=True)
                    break
            
            await message.edit(embed=embed)
        except:
            pass
        
        await interaction.response.send_message(
            f'✅ You have successfully entered the giveaway for **{giveaway["prize"]}**!\n'
            f'Good luck! 🍀',
            ephemeral=True
        )

# ============================================
# CREATE GIVEAWAY FUNCTION
# ============================================

async def create_giveaway(channel, host, prize, duration_minutes, winner_count, description=None, requirements=None, image_url=None):
    """Create a professional giveaway"""
    
    end_time = datetime.utcnow() + timedelta(minutes=duration_minutes)
    
    # Create beautiful embed
    embed = discord.Embed(
        title='🎉 GIVEAWAY 🎉',
        description=f'Click the button below to enter!\n\n**Prize:** {prize}',
        color=0x5865F2
    )
    
    if description:
        embed.add_field(name='📝 Description', value=description, inline=False)
    
    embed.add_field(name='⏰ Ends', value=f'<t:{int(end_time.timestamp())}:R>', inline=True)
    embed.add_field(name='🏆 Winners', value=f'**{winner_count}** winner(s)', inline=True)
    embed.add_field(name='📊 Entries', value='**0** participants', inline=True)
    
    if requirements:
        embed.add_field(name='✅ Requirements', value=requirements, inline=False)
    
    embed.add_field(
        name='📌 How to Enter',
        value='Click the **🎉 Enter Giveaway** button below!',
        inline=False
    )
    
    # Add image if provided
    if image_url:
        embed.set_image(url=image_url)
    
    embed.set_footer(text=f'Hosted by {host.name}', icon_url=host.display_avatar.url)
    embed.timestamp = end_time
    
    # Send message with button
    view = GiveawayView(0)  # Temporary ID
    msg = await channel.send('🎊 **NEW GIVEAWAY** 🎊', embed=embed, view=view)
    
    # Store giveaway data
    giveaway_id = str(msg.id)
    active_giveaways[giveaway_id] = {
        'message_id': msg.id,
        'channel_id': channel.id,
        'guild_id': channel.guild.id,
        'host_id': host.id,
        'prize': prize,
        'winners': winner_count,
        'entries': [],
        'end_time': end_time.isoformat(),
        'description': description,
        'requirements': requirements,
        'image_url': image_url,
        'ended': False
    }
    save_giveaways()
    
    # Update view with correct ID
    view = GiveawayView(msg.id)
    await msg.edit(view=view)
    
    # Schedule end
    asyncio.create_task(end_giveaway_timer(msg.id, duration_minutes * 60))

# ============================================
# END GIVEAWAY
# ============================================

async def end_giveaway_timer(giveaway_id, wait_seconds):
    """Wait and then end the giveaway"""
    await asyncio.sleep(wait_seconds)
    await end_giveaway(giveaway_id)

async def end_giveaway(giveaway_id):
    """End a giveaway and pick winners"""
    giveaway_id = str(giveaway_id)
    giveaway = active_giveaways.get(giveaway_id)
    
    if not giveaway or giveaway.get('ended'):
        return
    
    giveaway['ended'] = True
    save_giveaways()
    
    try:
        # Get message
        guild = bot.get_guild(giveaway['guild_id'])
        channel = guild.get_channel(giveaway['channel_id'])
        message = await channel.fetch_message(giveaway['message_id'])
        
        entries = giveaway['entries']
        winner_count = giveaway['winners']
        
        # Pick winners
        if len(entries) < winner_count:
            winners = entries
        else:
            winners = random.sample(entries, winner_count)
        
        # Create results embed
        if winners:
            winner_mentions = ', '.join([f'<@{uid}>' for uid in winners])
            
            embed = discord.Embed(
                title='🎊 GIVEAWAY ENDED 🎊',
                description=f'**Prize:** {giveaway["prize"]}\n\n**Winner(s):** {winner_mentions}',
                color=0x57F287
            )
            
            embed.add_field(name='🏆 Total Entries', value=f'{len(entries)} participants', inline=True)
            embed.add_field(name='🎉 Winners', value=f'{len(winners)} winner(s)', inline=True)
            
            host = guild.get_member(giveaway['host_id'])
            if host:
                embed.set_footer(text=f'Hosted by {host.name}', icon_url=host.display_avatar.url)
            
            embed.timestamp = datetime.utcnow()
            
            await message.edit(embed=embed, view=None)
            
            # Announce winners
            winner_msg = f'🎉 Congratulations {winner_mentions}! You won **{giveaway["prize"]}**!'
            await channel.send(winner_msg)
            
            # DM winners
            for winner_id in winners:
                try:
                    winner = guild.get_member(int(winner_id))
                    if winner:
                        dm_embed = discord.Embed(
                            title='🎉 YOU WON A GIVEAWAY!',
                            description=f'Congratulations! You won **{giveaway["prize"]}**!',
                            color=0x57F287
                        )
                        dm_embed.add_field(name='Server', value=guild.name, inline=True)
                        dm_embed.add_field(name='Prize', value=giveaway["prize"], inline=True)
                        dm_embed.add_field(name='What to do?', value=f'Contact {host.mention if host else "the host"} to claim your prize!', inline=False)
                        await winner.send(embed=dm_embed)
                except:
                    pass
        else:
            # No entries
            embed = discord.Embed(
                title='🎊 GIVEAWAY ENDED 🎊',
                description=f'**Prize:** {giveaway["prize"]}\n\n❌ **No one entered the giveaway!**',
                color=0xED4245
            )
            
            host = guild.get_member(giveaway['host_id'])
            if host:
                embed.set_footer(text=f'Hosted by {host.name}', icon_url=host.display_avatar.url)
            
            embed.timestamp = datetime.utcnow()
            
            await message.edit(embed=embed, view=None)
            await channel.send('😢 The giveaway ended with no entries!')
        
        # Remove from active giveaways
        del active_giveaways[giveaway_id]
        save_giveaways()
        
    except Exception as e:
        print(f'Error ending giveaway: {e}')

# ============================================
# COMMANDS - FIXED TO POST IN CHANNEL
# ============================================

@bot.command(name='gcreate', aliases=['gstart', 'giveaway'])
@commands.has_permissions(manage_guild=True)
async def create_giveaway_command(ctx, duration: int, winners: int, image_url: str = None, *, prize: str):
    """
    Create a giveaway in THIS channel
    Usage: $gcreate <minutes> <winners> [image_url] <prize>
    Examples: 
    - $gcreate 60 1 Discord Nitro
    - $gcreate 60 1 https://i.imgur.com/example.png Discord Nitro
    """
    
    if duration < 1:
        return await ctx.reply('❌ Duration must be at least 1 minute!')
    
    if winners < 1:
        return await ctx.reply('❌ Must have at least 1 winner!')
    
    # Check if image_url is actually part of the prize (if it doesn't look like a URL)
    if image_url and not (image_url.startswith('http://') or image_url.startswith('https://')):
        # It's part of the prize, not an image
        prize = f"{image_url} {prize}"
        image_url = None
    
    # Create giveaway in THIS channel
    await create_giveaway(
        ctx.channel,
        ctx.author,
        prize,
        duration,
        winners,
        None,  # description
        None,  # requirements
        image_url  # image
    )
    
    try:
        await ctx.message.delete()  # Delete command message
    except:
        pass

@bot.command(name='gend')
@commands.has_permissions(manage_guild=True)
async def end_giveaway_command(ctx, message_id: int):
    """
    End a giveaway early
    Usage: $gend <message_id>
    Example: $gend 1234567890
    """
    
    giveaway_id = str(message_id)
    
    if giveaway_id not in active_giveaways:
        return await ctx.reply('❌ No active giveaway with that ID!')
    
    await end_giveaway(giveaway_id)
    await ctx.reply('✅ Giveaway ended successfully!')

@bot.command(name='greroll')
@commands.has_permissions(manage_guild=True)
async def reroll_giveaway(ctx, message_id: int):
    """
    Reroll a giveaway winner
    Usage: $greroll <message_id>
    Example: $greroll 1234567890
    """
    
    giveaway_id = str(message_id)
    
    # Check if giveaway exists
    if giveaway_id not in active_giveaways:
        return await ctx.reply('❌ Could not find that giveaway!')
    
    giveaway = active_giveaways[giveaway_id]
    entries = giveaway['entries']
    
    if len(entries) == 0:
        return await ctx.reply('❌ No one entered that giveaway!')
    
    # Pick new winner
    new_winner_id = random.choice(entries)
    new_winner = ctx.guild.get_member(int(new_winner_id))
    
    embed = discord.Embed(
        title='🔄 GIVEAWAY REROLLED!',
        description=f'**New Winner:** {new_winner.mention}\n**Prize:** {giveaway["prize"]}',
        color=0x5865F2
    )
    
    await ctx.send(f'🎉 Congratulations {new_winner.mention}! You won **{giveaway["prize"]}**!', embed=embed)

@bot.command(name='glist')
async def list_giveaways(ctx):
    """
    List all active giveaways in this server
    Usage: $glist
    """
    
    guild_giveaways = {k: v for k, v in active_giveaways.items() 
                       if v['guild_id'] == ctx.guild.id and not v.get('ended')}
    
    if not guild_giveaways:
        return await ctx.reply('❌ No active giveaways in this server!')
    
    embed = discord.Embed(
        title='🎉 Active Giveaways',
        description=f'There are **{len(guild_giveaways)}** active giveaway(s) in this server',
        color=0x5865F2
    )
    
    for gid, giveaway in guild_giveaways.items():
        end_time = datetime.fromisoformat(giveaway['end_time'])
        channel = ctx.guild.get_channel(giveaway['channel_id'])
        
        embed.add_field(
            name=f'🎁 {giveaway["prize"]}',
            value=f'**Channel:** {channel.mention if channel else "Unknown"}\n**Entries:** {len(giveaway["entries"])}\n**Winners:** {giveaway["winners"]}\n**Ends:** <t:{int(end_time.timestamp())}:R>\n**ID:** `{giveaway["message_id"]}`',
            inline=False
        )
    
    await ctx.reply(embed=embed)

@bot.command(name='gdelete', aliases=['gcancel'])
@commands.has_permissions(manage_guild=True)
async def delete_giveaway(ctx, message_id: int):
    """
    Delete/cancel a giveaway without picking winners
    Usage: $gdelete <message_id>
    Example: $gdelete 1234567890
    """
    
    giveaway_id = str(message_id)
    
    if giveaway_id not in active_giveaways:
        return await ctx.reply('❌ No active giveaway with that ID!')
    
    giveaway = active_giveaways[giveaway_id]
    
    try:
        # Delete the giveaway message
        channel = ctx.guild.get_channel(giveaway['channel_id'])
        message = await channel.fetch_message(giveaway['message_id'])
        await message.delete()
        
        # Remove from active giveaways
        del active_giveaways[giveaway_id]
        save_giveaways()
        
        await ctx.reply('✅ Giveaway cancelled and deleted!')
    except:
        await ctx.reply('❌ Could not delete the giveaway message!')

# ============================================
# FUN OWNER-ONLY COMMANDS (Selected)
# ============================================

# PUT YOUR DISCORD USER ID HERE
OWNER_ID = 1302769760655769671  # REPLACE WITH YOUR ID

# ============================================
# 1. NUKE COMMAND (Fake)
# ============================================

@bot.command(name='nuke')
async def nuke_command(ctx):
    """Fake nuke the server (Owner only)"""
    if ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ You do not have permission to use this command!')
    
    # Fake loading
    msg = await ctx.send('☢️ **INITIALIZING NUKE SEQUENCE...**')
    await asyncio.sleep(2)
    await msg.edit(content='🚨 **WARNING: NUCLEAR LAUNCH DETECTED**')
    await asyncio.sleep(2)
    await msg.edit(content='💥 **5...**')
    await asyncio.sleep(1)
    await msg.edit(content='💥 **4...**')
    await asyncio.sleep(1)
    await msg.edit(content='💥 **3...**')
    await asyncio.sleep(1)
    await msg.edit(content='💥 **2...**')
    await asyncio.sleep(1)
    await msg.edit(content='💥 **1...**')
    await asyncio.sleep(1)
    await msg.edit(content='💥💥💥 **BOOM!** 💥💥💥\n\nyea nobody nuking shit lil bro ')

# ============================================
# 2. SAY COMMAND
# ============================================

@bot.command(name='say')
async def say_command(ctx, *, message: str):
    """Make the bot say something (Owner only)"""
    if ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ You do not have permission to use this command!')
    
    await ctx.message.delete()
    await ctx.send(message)

# ============================================
# 3. DM COMMAND
# ============================================

@bot.command(name='dm')
async def dm_user(ctx, member: discord.Member, *, message: str):
    """DM someone anonymously (Owner only)"""
    if ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ You do not have permission to use this command!')
    
    try:
        await member.send(message)
        await ctx.reply(f'✅ Sent DM to {member.name}!', delete_after=5)
        await ctx.message.delete()
    except:
        await ctx.reply('❌ Could not DM that user! They might have DMs disabled.', delete_after=5)

# ============================================
# 4. RAINBOW ROLE (Changes role color)
# ============================================

@bot.command(name='rainbow')
async def rainbow_role(ctx, role: discord.Role, duration: int = 30):
    """Make a role change colors (Owner only)
    Usage: $rainbow @role [duration_in_seconds]
    Example: $rainbow @VIP 60
    """
    if ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ You do not have permission to use this command!')
    
    if duration > 300:
        return await ctx.reply('❌ Maximum duration is 300 seconds (5 minutes)!')
    
    colors = [
        discord.Color.red(),
        discord.Color.orange(),
        discord.Color.gold(),
        discord.Color.green(),
        discord.Color.blue(),
        discord.Color.purple(),
        discord.Color.magenta(),
    ]
    
    original_color = role.color
    
    await ctx.reply(f'🌈 Rainbow mode activated for **{role.name}** for {duration} seconds!')
    
    # Calculate how many cycles
    cycles = duration // len(colors)
    
    for _ in range(cycles):
        for color in colors:
            await role.edit(color=color)
            await asyncio.sleep(1)
    
    # Restore original color
    await role.edit(color=original_color)
    await ctx.send(f'🌈 Rainbow mode finished! **{role.name}** restored to original color.')
 
# ============================================
# MORE FUN OWNER-ONLY COMMANDS
# ============================================

# PUT YOUR DISCORD USER ID HERE
OWNER_ID = 1302769760655769671  # REPLACE WITH YOUR ID

# ============================================
# 5. FAKE BAN
# ============================================

@bot.command(name='ban')
async def fake_ban(ctx, member: discord.Member, *, reason: str = "Being too awesome"):
    """Fake ban someone (Owner only)"""
    if ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ You do not have permission to use this command!')
    
    embed = discord.Embed(
        title='🔨 Member Banned',
        description=f'**{member.name}** has been permanently banned from the server!',
        color=0xED4245
    )
    embed.add_field(name='Reason', value=reason, inline=False)
    embed.add_field(name='Banned by', value=ctx.author.name, inline=True)
    embed.set_thumbnail(url=member.display_avatar.url)
    
    await ctx.send(embed=embed)
    await asyncio.sleep(3)
    await ctx.send(f'😂 im jk my nigga')

# ============================================
# 6. HACK COMMAND
# ============================================

@bot.command(name='hack')
async def fake_hack(ctx, member: discord.Member):
    """Fake hack someone (Owner only)"""
    if ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ You do not have permission to use this command!')
    
    msg = await ctx.send(f'💻 Hacking {member.name}...')
    await asyncio.sleep(1)
    await msg.edit(content=f'🔍 [▓▓▓░░░░░░░] 30% - Finding IP address...')
    await asyncio.sleep(1.5)
    await msg.edit(content=f'📧 [▓▓▓▓▓▓░░░░] 60% - Accessing email: {member.name.lower()}@gmail.com')
    await asyncio.sleep(1.5)
    await msg.edit(content=f'🔑 [▓▓▓▓▓▓▓▓░░] 80% - Password: ●●●●●●●●')
    await asyncio.sleep(1.5)
    await msg.edit(content=f'💳 [▓▓▓▓▓▓▓▓▓▓] 100% - Credit Card: 4532 ●●●● ●●●● {random.randint(1000, 9999)}')
    await asyncio.sleep(2)
    await msg.edit(content=f'✅ **HACK COMPLETE!**\n\n😂 chill im jk bruz')

# ============================================
# 7. ANNOY COMMAND
# ============================================

@bot.command(name='annoy')
async def annoy_user(ctx, member: discord.Member, times: int = 5):
    """Ping someone multiple times (Owner only)"""
    if ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ You do not have permission to use this command!')
    
    if times > 20:
        return await ctx.reply('❌ Maximum 20 times!')
    
    await ctx.message.delete()
    
    for i in range(times):
        await ctx.send(f'{member.mention} 😈 ({i+1}/{times})')
        await asyncio.sleep(1)
    
    await ctx.send(f'😂 Sorry mb nga {member.mention}')

# ============================================
# 8. EMBED SAY
# ============================================

@bot.command(name='embedsay')
async def embed_say(ctx, color: str = "blue", *, message: str):
    """Make the bot say something in an embed (Owner only)
    Colors: red, blue, green, yellow, purple, orange
    Usage: $embedsay red This is a message
    """
    if ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ You do not have permission to use this command!')
    
    colors = {
        'red': 0xED4245,
        'blue': 0x5865F2,
        'green': 0x57F287,
        'yellow': 0xFEE75C,
        'purple': 0x9B59B6,
        'orange': 0xE67E22,
        'pink': 0xFF69B4,
        'black': 0x000000
    }
    
    embed_color = colors.get(color.lower(), 0x5865F2)
    
    embed = discord.Embed(
        description=message,
        color=embed_color
    )
    
    await ctx.message.delete()
    await ctx.send(embed=embed)

# ============================================
# 9. IMPERSONATE (with webhook)
# ============================================

@bot.command(name='impersonate', aliases=['imitate', 'fake'])
async def impersonate(ctx, member: discord.Member, *, message: str):
    """Impersonate someone with webhook (Owner only)
    Usage: $impersonate @user I love pizza
    """
    if ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ You do not have permission to use this command!')
    
    try:
        webhook = await ctx.channel.create_webhook(name=member.name)
        await webhook.send(
            content=message, 
            username=member.display_name, 
            avatar_url=member.display_avatar.url
        )
        await webhook.delete()
        await ctx.message.delete()
    except:
        await ctx.reply('❌ Could not create webhook! I need Manage Webhooks permission.')

# ============================================
# 10. SPAM COMMAND
# ============================================

@bot.command(name='spam')
async def spam_bypass(ctx, times: int, *, message: str):
    """Bypass slowmode and spam (Owner only)
    Usage: $spam 10 SPAM MESSAGE
    """
    if ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ You do not have permission to use this command!')
    
    if times > 50:
        return await ctx.reply('❌ Maximum 50 messages!')
    
    await ctx.message.delete()
    
    for i in range(times):
        await ctx.send(f'{message}')
        await asyncio.sleep(0.5)

# ============================================
# 11. SLOWMODE
# ============================================

@bot.command(name='slowmode')
async def slowmode_command(ctx, seconds: int = 0):
    """Set channel slowmode (Owner only)
    Usage: $slowmode 10
    """
    if ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ You do not have permission to use this command!')
    
    if seconds < 0 or seconds > 21600:
        return await ctx.reply('❌ Slowmode must be between 0 and 21600 seconds (6 hours)!')
    
    await ctx.channel.edit(slowmode_delay=seconds)
    
    if seconds == 0:
        await ctx.reply('✅ Slowmode disabled!')
    else:
        await ctx.reply(f'✅ Slowmode set to {seconds} seconds!')

# ============================================
# 12. LOCKDOWN CHANNEL
# ============================================

@bot.command(name='lock')
async def lock_channel(ctx):
    """Lock the channel (Owner only)"""
    if ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ You do not have permission to use this command!')
    
    await ctx.channel.set_permissions(ctx.guild.default_role, send_messages=False)
    
    embed = discord.Embed(
        title='🔒 Channel Locked',
        description=f'This channel has been locked by {ctx.author.mention}',
        color=0xED4245
    )
    await ctx.send(embed=embed)

@bot.command(name='unlock')
async def unlock_channel(ctx):
    """Unlock the channel (Owner only)"""
    if ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ You do not have permission to use this command!')
    
    await ctx.channel.set_permissions(ctx.guild.default_role, send_messages=None)
    
    embed = discord.Embed(
        title='🔓 Channel Unlocked',
        description=f'This channel has been unlocked by {ctx.author.mention}',
        color=0x57F287
    )
    await ctx.send(embed=embed)

# ============================================
# 13. NICK EVERYONE
# ============================================

@bot.command(name='nickall')
async def nick_everyone(ctx, *, nickname: str):
    """Change everyone's nickname (Owner only)
    Usage: $nickall Clown
    """
    if ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ You do not have permission to use this command!')
    
    count = 0
    msg = await ctx.send('🔄 Changing nicknames...')
    
    for member in ctx.guild.members:
        if not member.bot and member.top_role < ctx.guild.me.top_role:
            try:
                await member.edit(nick=nickname)
                count += 1
            except:
                pass
    
    await msg.edit(content=f'✅ Changed {count} nicknames to **{nickname}**!')

@bot.command(name='resetnicks')
async def reset_nicks(ctx):
    """Reset everyone's nickname (Owner only)"""
    if ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ You do not have permission to use this command!')
    
    count = 0
    msg = await ctx.send('🔄 Resetting nicknames...')
    
    for member in ctx.guild.members:
        if not member.bot and member.nick:
            try:
                await member.edit(nick=None)
                count += 1
            except:
                pass
    
    await msg.edit(content=f'✅ Reset {count} nicknames!')

# ============================================
# 14. GHOST PING
# ============================================

@bot.command(name='ghostping')
async def ghost_ping(ctx, member: discord.Member):
    """Ghost ping someone (Owner only)"""
    if ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ You do not have permission to use this command!')
    
    await ctx.message.delete()
    msg = await ctx.send(member.mention)
    await msg.delete()

# ============================================
# 15. TYPING
# ============================================

@bot.command(name='typing')
async def fake_typing(ctx, seconds: int = 10):
    """Make the bot type for X seconds (Owner only)"""
    if ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ You do not have permission to use this command!')
    
    if seconds > 60:
        return await ctx.reply('❌ Maximum 60 seconds!')
    
    await ctx.message.delete()
    
    async with ctx.typing():
        await asyncio.sleep(seconds)
    
    await ctx.send('whatcu waiting for nga?')

 

# ============================================
# 17. RAID MODE (Fake)
# ============================================

@bot.command(name='raidmode')
async def raid_mode(ctx):
    """Activate fake raid mode (Owner only)"""
    if ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ You do not have permission to use this command!')
    
    embed = discord.Embed(
        title='🚨 RAID MODE ACTIVATED',
        description='**WARNING:** Server is under attack!\n\n🛡️ Anti-raid protection enabled\n🔒 New members: Auto-ban\n⏰ Duration: Until manually disabled',
        color=0xED4245
    )
    
    await ctx.send(embed=embed)
    await asyncio.sleep(5)
    await ctx.send('😂 Relax! No raid detected. Just having fun!')

# ============================================
# 18. CLEAR COMMAND
# ============================================

@bot.command(name='clear', aliases=['purge'])
async def clear_messages(ctx, amount: int = 10):
    """Clear messages in channel (Owner only)
    Usage: $clear 50
    """
    if ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ You do not have permission to use this command!')
    
    if amount > 100:
        return await ctx.reply('❌ Maximum 100 messages!')
    
    deleted = await ctx.channel.purge(limit=amount + 1)
    
    msg = await ctx.send(f'✅ Deleted {len(deleted) - 1} messages!')
    await asyncio.sleep(3)
    await msg.delete()

# ============================================
# 19. STATUS CHANGE
# ============================================

@bot.command(name='status')
async def change_status(ctx, status_type: str, *, status_text: str):
    """Change bot status (Owner only)
    Types: playing, watching, listening, streaming
    Usage: $status playing Minecraft
    """
    if ctx.author.id != OWNER_ID:
        return await ctx.reply('❌ You do not have permission to use this command!')
    
    status_types = {
        'playing': discord.ActivityType.playing,
        'watching': discord.ActivityType.watching,
        'listening': discord.ActivityType.listening,
        'streaming': discord.ActivityType.streaming
    }
    
    activity_type = status_types.get(status_type.lower())
    
    if not activity_type:
        return await ctx.reply('❌ Invalid status type! Use: playing, watching, listening, or streaming')
    
    await bot.change_presence(activity=discord.Activity(type=activity_type, name=status_text))
    await ctx.reply(f'✅ Status changed to **{status_type} {status_text}**!')


# New Ticket Command
@bot.command(name='new')
async def new_ticket(ctx, ticket_type: str = None):
    if not ticket_type or ticket_type.lower() not in TICKET_TYPES:
        await ctx.reply('❌ Invalid ticket type! Use: `partnership`, `middleman`, or `support`')
        return

    try:
        await create_ticket(ctx.guild, ctx.author, ticket_type.lower())
        await ctx.reply('✅ Ticket created! Check your DMs or the ticket channel.')
    except discord.Forbidden:
        await ctx.reply('❌ I don\'t have permission to create channels! Give me **Manage Channels** permission.')
    except Exception as e:
        await ctx.reply(f'❌ Error creating ticket: {str(e)}')
        print(f'Ticket creation error:  {e}')
    
    

# Close Command
@bot.command(name='close')
async def close_command(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        await ctx.reply('❌ This command can only be used in ticket channels!')
        return

    embed = discord.Embed(
        title='⚠️ Close Ticket',
        description='Are you sure you want to close this ticket?',
        color=COLORS['error']
    )
    embed.set_footer(text='This action cannot be undone')

    view = CloseTicketView()
    cancel_button = Button(label='Cancel', style=discord.ButtonStyle.secondary)

    async def cancel_callback(interaction):
        await interaction.response.edit_message(content='❌ Ticket closure cancelled.', embed=None, view=None)

    cancel_button.callback = cancel_callback
    view.add_item(cancel_button)

    await ctx.reply(embed=embed, view=view)

class MMTicketView(View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label='✅ Claim Ticket', style=discord.ButtonStyle.success, custom_id='claim_mm_ticket')
    async def claim_button(self, interaction: discord.Interaction, button: Button):
        # Check if user is MM or admin
        mm_role_ids = [
            1453757017218093239,  # Trial MM
            1434610759140118640,  # Middleman
            1453757157144137911,  # Pro MM
            1453757225267892276,  # Head MM
            1448796685672386712   # Owner
        ]
        
        user_role_ids = [role.id for role in interaction.user.roles]
        is_mm = any(role_id in user_role_ids for role_id in mm_role_ids)
        is_admin = interaction.user.guild_permissions.administrator
        
        if not is_mm and not is_admin:
            return await interaction.response.send_message('❌ Only middlemen or administrators can claim tickets!', ephemeral=True)
        
        # Check if already claimed
        if interaction.channel.id in claimed_tickets:
            claimer_id = claimed_tickets[interaction.channel.id]
            claimer = interaction.guild.get_member(claimer_id)
            return await interaction.response.send_message(f'❌ This ticket is already claimed by {claimer.mention if claimer else "someone"}!', ephemeral=True)
        
        # Claim the ticket
        claimed_tickets[interaction.channel.id] = interaction.user.id
        
        # Get ticket creator
        ticket_data = active_tickets.get(interaction.channel.id)
        ticket_creator_id = ticket_data.get('user_id') if ticket_data else None
        ticket_creator = interaction.guild.get_member(ticket_creator_id) if ticket_creator_id else None
        
        # Update permissions - only claimer and ticket creator can talk
        await interaction.channel.set_permissions(
            interaction.user,
            view_channel=True,
            send_messages=True,
            read_message_history=True
        )
        
        # Keep ticket creator's permissions
        if ticket_creator:
            await interaction.channel.set_permissions(
                ticket_creator,
                view_channel=True,
                send_messages=True,
                read_message_history=True
            )
        
        embed = discord.Embed(
            description=f'✅ Ticket claimed by {interaction.user.mention}\n\n🔒 **Only the claimer and ticket creator can now send messages.**',
            color=COLORS['success']
        )
        embed.timestamp = datetime.utcnow()
        
        await interaction.response.send_message(embed=embed)
        await interaction.channel.edit(name=f"{interaction.channel.name}-claimed")
    
    @discord.ui.button(label='🔒 Close Ticket', style=discord.ButtonStyle.danger, custom_id='close_mm_ticket')
    async def close_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        await close_ticket(interaction.channel, interaction.user)

# Claim Command
@bot.command(name='claim')
async def claim(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        await ctx.reply('❌ This command can only be used in ticket channels!')
        return

    if ctx.channel.id in claimed_tickets:
        await ctx.reply('❌ This ticket is already claimed!')
        return

    claimed_tickets[ctx.channel.id] = ctx.author.id
    
    # Get ticket creator
    ticket_data = active_tickets.get(ctx.channel.id)
    ticket_creator_id = ticket_data.get('user_id') if ticket_data else None
    ticket_creator = ctx.guild.get_member(ticket_creator_id) if ticket_creator_id else None
    
    # Update permissions - only claimer and ticket creator can talk
    await ctx.channel.set_permissions(
        ctx.author,
        view_channel=True,
        send_messages=True,
        read_message_history=True
    )
    
    # Keep ticket creator's permissions
    if ticket_creator:
        await ctx.channel.set_permissions(
            ticket_creator,
            view_channel=True,
            send_messages=True,
            read_message_history=True
        )
    
    # Remove @everyone's ability to send messages (they already can't see it)
    # This prevents anyone else from talking even if added later
    
    embed = discord.Embed(
        description=f'✅ Ticket claimed by {ctx.author.mention}\n\n🔒 **Only the claimer and ticket creator can now send messages.**',
        color=COLORS['success']
    )
    embed.timestamp = datetime.utcnow()

    await ctx.send(embed=embed)
    await ctx.channel.edit(name=f"{ctx.channel.name}-claimed")

# Unclaim Command
@bot.command(name='unclaim')
async def unclaim(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        await ctx.reply('❌ This command can only be used in ticket channels!')
        return

    if ctx.channel.id not in claimed_tickets:
        await ctx.reply('❌ This ticket is not claimed!')
        return

    claimer = claimed_tickets[ctx.channel.id]
    if claimer != ctx.author.id and not ctx.author.guild_permissions.administrator:
        await ctx.reply('❌ Only the claimer or an administrator can unclaim this ticket!')
        return

    del claimed_tickets[ctx.channel.id]
    
    # Restore normal permissions (remove the claim restriction)
    claimer_member = ctx.guild.get_member(claimer)
    if claimer_member:
        await ctx.channel.set_permissions(claimer_member, overwrite=None)

    embed = discord.Embed(
        description=f'✅ Ticket unclaimed by {ctx.author.mention}\n\n🔓 **All staff can now send messages again.**',
        color=COLORS['info']
    )
    embed.timestamp = datetime.utcnow()

    await ctx.send(embed=embed)
    new_name = ctx.channel.name.replace('-claimed', '')
    await ctx.channel.edit(name=new_name)

# Add User Command
@bot.command(name='add')
async def add_user(ctx, member: discord.Member = None):
    if not ctx.channel.name.startswith('ticket-'):
        await ctx.reply('❌ This command can only be used in ticket channels!')
        return

    if not member:
        await ctx.reply('❌ Please mention a valid user!')
        return

    await ctx.channel.set_permissions(member, view_channel=True, send_messages=True, read_message_history=True)

    embed = discord.Embed(
        description=f'✅ {member.mention} has been added to the ticket',
        color=COLORS['success']
    )
    embed.timestamp = datetime.utcnow()

    await ctx.reply(embed=embed)

# Remove User Command
@bot.command(name='remove')
async def remove_user(ctx, member: discord.Member = None):
    if not ctx.channel.name.startswith('ticket-'):
        await ctx.reply('❌ This command can only be used in ticket channels!')
        return

    if not member:
        await ctx.reply('❌ Please mention a valid user!')
        return

    await ctx.channel.set_permissions(member, overwrite=None)

    embed = discord.Embed(
        description=f'✅ {member.mention} has been removed from the ticket',
        color=COLORS['success']
    )
    embed.timestamp = datetime.utcnow()

    await ctx.reply(embed=embed)

# Rename Command
@bot.command(name='rename')
async def rename(ctx, *, new_name: str = None):
    if not ctx.channel.name.startswith('ticket-'):
        await ctx.reply('❌ This command can only be used in ticket channels!')
        return

    if not new_name:
        await ctx.reply('❌ Please provide a new name for the ticket!')
        return

    new_name = new_name.lower().replace(' ', '-')
    await ctx.channel.edit(name=f'ticket-{new_name}')

    embed = discord.Embed(
        description=f'✅ Ticket renamed to **ticket-{new_name}**',
        color=COLORS['success']
    )
    embed.timestamp = datetime.utcnow()

    await ctx.reply(embed=embed)
    save_data()

# Stats Command
@bot.command(name='stats')
async def stats(ctx):
    tickets = [c for c in ctx.guild.channels if c.name.startswith('ticket-')]
    claimed = [c for c in tickets if c.id in claimed_tickets]

    embed = discord.Embed(
        title='📊 Ticket Statistics',
        color=COLORS['info']
    )
    embed.add_field(name='🎫 Active Tickets', value=f'`{len(tickets)}`', inline=True)
    embed.add_field(name='✅ Claimed Tickets', value=f'`{len(claimed)}`', inline=True)
    embed.add_field(name='⏳ Unclaimed Tickets', value=f'`{len(tickets) - len(claimed)}`', inline=True)
    embed.set_footer(text=f'Requested by {ctx.author}', icon_url=ctx.author.display_avatar.url)
    embed.timestamp = datetime.utcnow()

    await ctx.reply(embed=embed)

# MM Proof Command
@bot.command(name='proof')
async def proof_command(ctx):
    if not ctx.channel.name.startswith('ticket-'):
        return await ctx.reply('❌ This command can only be used in a ticket.')

    ticket = active_tickets.get(ctx.channel.id)
    if not ticket:
        return await ctx.reply('❌ No ticket data found.')

    requester = ctx.guild.get_member(ticket['user_id'])
    trader = ticket.get('trader', 'Unknown')
    giving = ticket.get('giving', 'Unknown')
    receiving = ticket.get('receiving', 'Unknown')
    tier = ticket.get('tier', 'Unknown')

    # CHANGE THIS TO YOUR REAL CHANNEL ID
    PROOF_CHANNEL_ID = 1441905610340962486
    proof_channel = ctx.guild.get_channel(PROOF_CHANNEL_ID)

    if not proof_channel:
        return await ctx.reply('❌ Proof channel not found.')

    embed = discord.Embed(
        title='✅ Trade Completed',
        color=0x57F287
    )

    embed.add_field(name='Middleman', value=ctx.author.mention, inline=False)
    embed.add_field(name='Type', value='MM', inline=False)
    embed.add_field(name='Tier', value=MM_TIERS[tier]['name'], inline=False)
    embed.add_field(name='Requester', value=requester.mention if requester else 'Unknown', inline=False)
    embed.add_field(name='Trader', value=trader, inline=False)
    embed.add_field(name='Gave', value=giving, inline=False)
    embed.add_field(name='Received', value=receiving, inline=False)

    ticket_number = ctx.channel.name.replace('ticket-', '')
    embed.set_footer(text=f"Ticket #{ticket_number}")

    await proof_channel.send(embed=embed)
    await ctx.reply('✅ Proof sent successfully!')
    
    
    
    
    # Add tier info if available
    if 'tier' in ticket_data:
        tier = ticket_data['tier']
        embed.add_field(name='Tier', value=f"{MM_TIERS[tier]['name']} ({MM_TIERS[tier]['range']})", inline=False)
    
    embed.add_field(name='Middleman', value=middleman.mention, inline=False)
    embed.add_field(name='Type', value='MM', inline=False)
    embed.add_field(name='Requester', value=requester.mention, inline=False)
    embed.add_field(name='Trader', value=trader.display_name, inline=False)
    embed.add_field(name='Gave', value=gave, inline=False)
    embed.add_field(name='Received', value=received, inline=False)
    
    # Add stored trade details if available
    if 'both_join' in ticket_data:
        embed.add_field(name='Both Could Join Links', value=ticket_data['both_join'], inline=False)
    if 'tip' in ticket_data and ticket_data['tip'] != 'None':
        embed.add_field(name='Tip', value=ticket_data['tip'], inline=False)
    
    # Add ticket info
    ticket_number = ctx.channel.name.split('-')[1] if len(ctx.channel.name.split('-')) > 1 else 'Unknown'
    embed.set_footer(text=f'Ticket #{ticket_number} | {datetime.utcnow().strftime("%B %d, %Y at %I:%M %p")}')
    embed.timestamp = datetime.utcnow()
    
    # Send to mm-proofs channel
    await proof_channel.send(embed=embed)
    await ctx.reply('✅ Proof sent to MM proofs channel!')

# Helper function to create ticket
async def create_ticket(guild, user, ticket_type):
    """Create a simple ticket (for partnership and support)"""
    try:
        print(f'[DEBUG] Starting ticket creation for {user.name}')
        ticket_info = TICKET_TYPES[ticket_type]
        
        # Find or create category
        category = discord.utils.get(guild.categories, name=TICKET_CATEGORY)
        if not category:
            category = await guild.create_category(TICKET_CATEGORY)
        
        # Create ticket channel with permissions
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            user: discord.PermissionOverwrite(
                view_channel=True, 
                send_messages=True, 
                read_message_history=True,
                mention_everyone=True
            ),
            guild.me: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                manage_channels=True,
                manage_messages=True
            )
        }
        
        ticket_channel = await guild.create_text_channel(
            name=f'ticket-{user.name}-{ticket_type}',
            category=category,
            overwrites=overwrites
        )
        
        # Store ticket data
        active_tickets[ticket_channel.id] = {
            'user_id': user.id,
            'type': ticket_type,
            'created_at': datetime.utcnow().isoformat()
        }
        
        # Get the role to ping based on ticket type
        guild_id = str(guild.id)
        role_to_ping = None
        
        if guild_id in ticket_roles and ticket_type in ticket_roles[guild_id]:
            role_id = ticket_roles[guild_id][ticket_type]
            role_to_ping = guild.get_role(role_id)
        
        # Send initial message with role ping
        if role_to_ping:
            ping_message = f"{role_to_ping.mention} - New {ticket_info['name']} ticket opened!"
            await ticket_channel.send(ping_message, allowed_mentions=discord.AllowedMentions(roles=True))
        
        embed = discord.Embed(
            title=f"{ticket_info['emoji']} {ticket_info['name']} Ticket",
            description=f"Welcome {user.mention}!\n\n**Ticket Type:** {ticket_info['description']}\n\nOur team will be with you shortly. Please describe your inquiry in detail.",
            color=ticket_info['color']
        )
        embed.add_field(
            name='📌 Commands',
            value='`$close` - Close this ticket\n`$claim` - Claim this ticket\n`$add <user>` - Add a user\n`$remove <user>` - Remove a user',
            inline=False
        )
        embed.set_footer(text=f'Ticket created by {user}', icon_url=user.display_avatar.url)
        embed.timestamp = datetime.utcnow()
        
        await ticket_channel.send(content=user.mention, embed=embed, view=CloseTicketView())
        
        print(f'[DEBUG] Ticket creation completed successfully!')
        
    except Exception as e:
        print(f'[ERROR] Ticket creation failed: {e}')
        raise
        
# Helper function to create middleman tickets with details
# This should be at the LEFT margin (no indentation)
async def create_ticket_with_details(guild, user, ticket_type, tier, trader, giving, receiving, both_join, tip):   
    """Create a middleman ticket with trade details"""
    try:
        print(f'[DEBUG] Starting MM ticket creation for {user.name}')
        ticket_info = TICKET_TYPES[ticket_type]
        
        # Find or create category
        category = discord.utils.get(guild.categories, name=TICKET_CATEGORY)
        if not category:
            category = await guild.create_category(TICKET_CATEGORY)
        
        # Get all MM roles
        mm_role_ids = [
            1453757017218093239,  # Trial MM
            1434610759140118640,  # Middleman
            1453757157144137911,  # Pro MM
            1453757225267892276,  # Head MM
            1448796685672386712   # Owner
        ]
        
        # Create base overwrites
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            user: discord.PermissionOverwrite(
                view_channel=True, 
                send_messages=True, 
                read_message_history=True,
                mention_everyone=True
            ),
            guild.me: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                manage_channels=True,
                manage_messages=True
            )
        }
        
        # Add ALL MM roles to be able to see the ticket
        for role_id in mm_role_ids:
            role = guild.get_role(role_id)
            if role:
                overwrites[role] = discord.PermissionOverwrite(
                    view_channel=True,
                    send_messages=True,
                    read_message_history=True,
                    manage_messages=True
                )
        
        # Create ticket channel
        ticket_channel = await guild.create_text_channel(
            name=f'ticket-{user.name}-{ticket_type}',
            category=category,
            overwrites=overwrites
        )
        
        # Store ticket data with MM details
        active_tickets[ticket_channel.id] = {
            'user_id': user.id,
            'type': ticket_type,
            'created_at': datetime.utcnow().isoformat(),
            'tier': tier,
            'trader': trader,
            'giving': giving,
            'receiving': receiving,
            'both_join': both_join,
            'tip': tip
        }
        
        # Get the specific tier role for ping
        tier_role_id = MM_ROLE_IDS.get(tier)
        tier_role = guild.get_role(tier_role_id) if tier_role_id else None
        
        # Ping the specific tier role
        if tier_role:
            ping_message = f"{tier_role.mention} - New {MM_TIERS[tier]['name']} ticket opened!"
            await ticket_channel.send(ping_message, allowed_mentions=discord.AllowedMentions(roles=True))
        
        # Send welcome embed
        embed = discord.Embed(
            title=f"{ticket_info['emoji']} {ticket_info['name']} Ticket",
            description=f"Welcome {user.mention}!\n\nOur team will be with you shortly.",
            color=ticket_info['color']
        )
        embed.set_footer(text=f'Ticket created by {user}', icon_url=user.display_avatar.url)
        embed.timestamp = datetime.utcnow()
        
        await ticket_channel.send(content=user.mention, embed=embed)
        
        # Send trade details with claim button
        details_embed = discord.Embed(
            title='📋 Middleman Trade Request',
            description=f'**Selected Tier:** {MM_TIERS[tier]["name"]}\n**Range:** {MM_TIERS[tier]["range"]}',
            color=COLORS['middleman']
        )
        
        details_embed.add_field(name='Trading With', value=trader, inline=False)
        details_embed.add_field(name='Requester Giving', value=giving, inline=False)
        details_embed.add_field(name='Requester Receiving', value=receiving, inline=False)
        details_embed.add_field(name='Both Can Join Links?', value=both_join, inline=False)
        details_embed.add_field(name='Tip', value=tip, inline=False)
        
        details_embed.set_footer(text=f'Requested by {user}', icon_url=user.display_avatar.url)
        details_embed.timestamp = datetime.utcnow()
        
        # Send with claim and close buttons
        await ticket_channel.send(embed=details_embed, view=MMTicketView())
        
        print(f'[DEBUG] MM Ticket creation completed successfully!')
        
    except Exception as e:
        print(f'[ERROR] MM Ticket creation failed: {e}')
        raise
    
        # Find or create category
        category = discord.utils.get(guild.categories, name=TICKET_CATEGORY)
        if not category:
            print(f'[DEBUG] Creating category: {TICKET_CATEGORY}')
            category = await guild.create_category(TICKET_CATEGORY)
            print(f'[DEBUG] Category created: {category.id}')
        else:
            print(f'[DEBUG] Found existing category: {category.id}')
        
        # Create ticket channel with permissions that allow pinging everyone/roles
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            user: discord.PermissionOverwrite(
                view_channel=True, 
                send_messages=True, 
                read_message_history=True,
                mention_everyone=True
            ),
            guild.me: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                manage_channels=True,
                manage_messages=True
            )
        }
        
        print(f'[DEBUG] Creating ticket channel...')
        ticket_channel = await guild.create_text_channel(
            name=f'ticket-{user.name}-{ticket_type}',
            category=category,
            overwrites=overwrites
        )
        print(f'[DEBUG] Ticket channel created: {ticket_channel.id}')
        
        active_tickets[ticket_channel.id] = {
    'user_id': user.id,
    'type': ticket_type,
    'created_at':
            datetime.utcnow().isoformat()
}
 
    
        
        # Get the role to ping based on ticket type
        guild_id = str(guild.id)
        role_to_ping = None
        
        if guild_id in ticket_roles and ticket_type in ticket_roles[guild_id]:
            role_id = ticket_roles[guild_id][ticket_type]
            role_to_ping = guild.get_role(role_id)
        
        # Send initial message with role ping
        if role_to_ping:
            ping_message = f"{role_to_ping.mention} - New {ticket_info['name']} ticket opened!"
            await ticket_channel.send(ping_message, allowed_mentions=discord.AllowedMentions(roles=True))
        
        embed = discord.Embed(
            title=f"{ticket_info['emoji']} {ticket_info['name']} Ticket",
            description=f"Welcome {user.mention}!\n\n**Ticket Type:** {ticket_info['description']}\n\nOur team will be with you shortly. Please describe your inquiry in detail.",
            color=ticket_info['color']
        )
        embed.add_field(
            name='📌 Commands',
            value='`$close` - Close this ticket\n`$claim` - Claim this ticket\n`$add <user>` - Add a user\n`$remove <user>` - Remove a user',
            inline=False
        )
        embed.set_footer(text=f'Ticket created by {user}', icon_url=user.display_avatar.url)
        embed.timestamp = datetime.utcnow()
        
        await ticket_channel.send(content=user.mention, embed=embed, view=CloseTicketView())
            
        print(f'[DEBUG] Ticket creation completed successfully!')
        
    except discord.Forbidden as e:
        print(f'[ERROR] Permission denied: {e}')
        raise
    except Exception as e:
        print(f'[ERROR] Ticket creation failed: {e}')
        raise
    
    # Find or create category
    category = discord.utils.get(guild.categories, name=TICKET_CATEGORY)
    if not category:
        category = await guild.create_category(TICKET_CATEGORY)

    # Ticket role setup command
@bot.command(name='ticketrole')
@commands.has_permissions(administrator=True)
async def ticket_role(ctx, ticket_type: str, role: discord.Role):
    """Set which role gets pinged for each ticket type"""
    ticket_type = ticket_type.lower()
    
    if ticket_type not in ['partnership', 'middleman', 'support']:
        embed = discord.Embed(
            title='❌ Invalid Ticket Type',
            description='Valid types: `partnership`, `middleman`, `support`',
            color=COLORS['error']
        )
        return await ctx.send(embed=embed)
    
    guild_id = str(ctx.guild.id)
    if guild_id not in ticket_roles:
        ticket_roles[guild_id] = {}
    
    ticket_roles[guild_id][ticket_type] = role.id
    save_data()
    
    embed = discord.Embed(
        title='✅ Ticket Role Set',
        description=f'**{ticket_type.title()}** tickets will now ping {role.mention}',
        color=COLORS['success']
    )
    await ctx.send(embed=embed)

    # Set tier-specific MM roles
@bot.command(name='mmrole')
@commands.has_permissions(administrator=True)
async def mm_role(ctx, tier: str, role: discord.Role):
    """Set which role gets pinged for each MM tier"""
    tier = tier.lower()
    
    valid_tiers = ['trial', 'middleman', 'pro', 'head', 'owner']
    if tier not in valid_tiers:
        embed = discord.Embed(
            title='❌ Invalid Tier',
            description=f'Valid tiers: `{", ".join(valid_tiers)}`',
            color=COLORS['error']
        )
        return await ctx.send(embed=embed)
    
    guild_id = str(ctx.guild.id)
    if guild_id not in ticket_roles:
        ticket_roles[guild_id] = {}
    
    # Store as middleman_tier (e.g., middleman_trial, middleman_pro)
    ticket_roles[guild_id][f'middleman_{tier}'] = role.id
    save_data()
    
    embed = discord.Embed(
        title='✅ MM Tier Role Set',
        description=f'**{MM_TIERS[tier]["name"]}** tickets will now ping {role.mention}',
        color=COLORS['success']
    )
    await ctx.send(embed=embed)

    # View MM tier role settings
@bot.command(name='mmroles')
@commands.has_permissions(administrator=True)
async def mm_roles_list(ctx):
    """View current MM tier role settings"""
    guild_id = str(ctx.guild.id)
    
    embed = discord.Embed(
        title='⚖️ MM Tier Role Settings',
        color=COLORS['middleman']
    )
    
    if guild_id not in ticket_roles:
        embed.description = 'No MM tier roles configured yet.\n\nUse `$mmrole <tier> <role>` to set them.'
    else:
        found_any = False
        for tier in ['trial', 'middleman', 'pro', 'head', 'owner']:
            tier_key = f'middleman_{tier}'
            if tier_key in ticket_roles[guild_id]:
                role_id = ticket_roles[guild_id][tier_key]
                role = ctx.guild.get_role(role_id)
                if role:
                    embed.add_field(
                        name=f'{MM_TIERS[tier]["name"]} ({MM_TIERS[tier]["range"]})',
                        value=f'Pings: {role.mention}',
                        inline=False
                    )
                    found_any = True
        
        if not found_any:
            embed.description = 'No MM tier roles configured yet.\n\nUse `$mmrole <tier> <role>` to set them.'
    
    await ctx.send(embed=embed)

# View ticket role settings
@bot.command(name='ticketroles')
@commands.has_permissions(administrator=True)
async def ticket_roles_list(ctx):
    """View current ticket role settings"""
    guild_id = str(ctx.guild.id)
    
    embed = discord.Embed(
        title='🎫 Ticket Role Settings',
        color=COLORS['info']
    )
    
    if guild_id not in ticket_roles or not ticket_roles[guild_id]:
        embed.description = 'No ticket roles configured yet.\n\nUse `.ticketrole <type> <role>` to set them.'
    else:
        for ticket_type, role_id in ticket_roles[guild_id].items():
            role = ctx.guild.get_role(role_id)
            if role:
                embed.add_field(
                    name=f'{ticket_type.title()} Tickets',
                    value=f'Pings: {role.mention}',
                    inline=False
                )
    
    await ctx.send(embed=embed)
    
    # Get the role to ping based on ticket type
    guild_id = str(guild.id)
    role_to_ping = None
    
    if guild_id in ticket_roles and ticket_type in ticket_roles[guild_id]:
        role_id = ticket_roles[guild_id][ticket_type]
        role_to_ping = guild.get_role(role_id)
    
    # Send initial message with role ping
    if role_to_ping:
        ping_message = f"{role_to_ping.mention} - New {ticket_info['name']} ticket opened!"
        await ticket_channel.send(ping_message, allowed_mentions=discord.AllowedMentions(roles=True))
    
    embed = discord.Embed(
        title=f"{ticket_info['emoji']} {ticket_info['name']} Ticket",
        description=f"Welcome {user.mention}!\n\n**Ticket Type:** {ticket_info['description']}\n\nOur team will be with you shortly. Please describe your inquiry in detail.",
        color=ticket_info['color']
    )
    embed.add_field(
        name='📌 Commands',
        value='`.close` - Close this ticket\n`.claim` - Claim this ticket\n`.add <user>` - Add a user\n`.remove <user>` - Remove a user',
        inline=False
    )
    embed.set_footer(text=f'Ticket created by {user}', icon_url=user.display_avatar.url)
    embed.timestamp = datetime.utcnow()

    # Rest of ticket creation code...

    active_tickets[ticket_channel.id] = {
        'user_id': user.id,
        'type': ticket_type,
        'created_at': datetime.utcnow()
    }

    embed = discord.Embed(
        title=f"{ticket_info['emoji']} {ticket_info['name']} Ticket",
        description=f"Welcome {user.mention}!\n\n**Ticket Type:** {ticket_info['description']}\n\nOur team will be with you shortly. Please describe your inquiry in detail.",
        color=ticket_info['color']
    )
    embed.add_field(
        name='📌 Commands',
        value='`$close` - Close this ticket\n`$claim` - Claim this ticket\n`$add <user>` - Add a user\n`$remove <user>` - Remove a user',
        inline=False
    )
    embed.set_footer(text=f'Ticket created by {user}', icon_url=user.display_avatar.url)
    embed.timestamp = datetime.utcnow()

    await ticket_channel.send(content=user.mention, embed=embed, view=CloseTicketView())

async def close_ticket(channel, user):
    ticket_data = active_tickets.get(channel.id)

    embed = discord.Embed(
        title='🔒 Ticket Closed',
        description=f'Ticket closed by {user.mention}',
        color=COLORS['error']
    )
    embed.timestamp = datetime.utcnow()

    await channel.send(embed=embed)

    # Log to ticket-logs if exists
    log_channel = discord.utils.get(channel.guild.channels, name=LOG_CHANNEL)
    if log_channel:
        log_embed = discord.Embed(
            title='🎫 Ticket Closed',
            color=COLORS['error']
        )
        log_embed.add_field(name='Channel', value=channel.name, inline=True)
        log_embed.add_field(name='Closed By', value=user.name, inline=True)
        log_embed.add_field(name='Type', value=ticket_data.get('type', 'Unknown') if ticket_data else 'Unknown', inline=True)
        log_embed.timestamp = datetime.utcnow()

        await log_channel.send(embed=log_embed)

    if channel.id in active_tickets:
        del active_tickets[channel.id]
    if channel.id in claimed_tickets:
        del claimed_tickets[channel.id]

    await asyncio.sleep(5)
    await channel.delete()

# Error Handling
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        try:
            await ctx.reply('❌ You do not have permission to use this command!')
        except:
            pass
    elif isinstance(error, commands.MemberNotFound):
        try:
            await ctx.reply('❌ User not found!')
        except:
            pass
    elif isinstance(error, commands.CommandInvokeError):
        if 'Forbidden' in str(error):
            print(f'Permission Error: Bot lacks permissions in {ctx.guild.name} - {ctx.channel.name}')
            try:
                await ctx.author.send(f'❌ I don\'t have permission to send messages in #{ctx.channel.name}. Please give me "Send Messages" and "Embed Links" permissions!')
            except:
                pass
        else:
            print(f'Error: {error}')
    else:
        print(f'Error: {error}')

# Run Bot
if __name__ == '__main__':
    keep_alive()
    TOKEN = os.getenv('TOKEN')
    if not TOKEN:
        print('❌ ERROR: No TOKEN found in environment variables!')
        print('Please set your Discord bot token as TOKEN environment variable')
    else:
        print('🚀 Starting Discord Ticket Bot...')
        bot.run(TOKEN)
    
