from redbot.core import checks, Config, utils
from redbot.core.i18n import Translator, cog_i18n
import discord
from redbot.core import commands
from redbot.core.utils import mod
import asyncio
import datetime

class Account(commands.Cog):
    """The Account Cog"""

    def __init__(self, bot):
        self.bot = bot
        default_member = {
            "Name": None,
            "Program": None,
            "Level": None,
            "Age": None,
            "Pronoun": None,
            "About": None,
            "Interests": None,
            "Email": None,
            "Site": None,
            "Characterpic": None
        }
        default_guild = {
            "db": []
        }
        self.config = Config.get_conf(self, identifier=42)
        self.config.register_guild(**default_guild)
        self.config.register_member(**default_member)

    async def _sendMsg(self, ctx, user, title, msg, silent = False):
        data = discord.Embed(colour=user.colour)
        data.add_field(name = title, value=msg)
        if not silent:
            await ctx.send(embed=data)
        else:
            await ctx.author.send(embed=data)
            await asyncio.sleep(1)
            await utils.mod.slow_deletion([ctx.message])
        #await asyncio.sleep(5)
        #await utils.mod.slow_deletion([msg_id, ctx.message])

    #@commands.command(name="signup")
    #@commands.guild_only()
    async def _reg(self, ctx, user):
        """Sign up to get your own account today!"""

        server = ctx.guild
        #user = ctx.author
        db = await self.config.guild(server).db()
        if user.id not in db:
            db.append(user.id)
            await self.config.guild(server).db.set(db)
            name = user.display_name #user.display_name if user.display_name else str(user)        # register and set up name field
            await self.config.member(user).Name.set(name)


        #     data = discord.Embed(colour=user.colour)
        #     data.add_field(name="Congrats!:sparkles:", value="You have officially created your account for **{}**, {}.".format(server.name, user.mention))
        #     await ctx.send(embed=data)
        # else: 
        #     data = discord.Embed(colour=user.colour)
        #     data.add_field(name="Error:warning:",value="Opps, it seems like you already have an account, {}.".format(user.mention))
        #     await ctx.send(embed=data)
        
    
    @commands.command(name="account")
    @commands.guild_only()
    async def _acc(self, ctx, user = None, *args): # : discord.Member=None
        """Your/Others Account"""
                    
        server = ctx.guild
        db = await self.config.guild(server).db()
        if not user:
            users = [ctx.author]
        elif user[:3] == "<@!":
            converter = discord.ext.commands.MemberConverter()
            user = await converter.convert(ctx, user)
            if user.id not in db:
                await self._reg(ctx, user)

            if user == ctx.author and args and args[0].lower() == "reset":
                db.remove(user.id)
                await self.config.guild(server).db.set(db)
                await self.config.member(user).clear()
                await self._reg(ctx, user)
                await self._sendMsg(ctx, user, "Success", "Your profile has been reset!")
                return
            users = [user]
        else:
            user = user.lower()
            users = []
            for id in db:
                member = server.get_member(id)
                name = await self.config.member(member).get_raw("Name")
                if user in name.lower():
                    users.append(member)
                    
        if args and args[-1] == "-s":
            args = args[:-1]
            silent = True
        else:
            silent = False

        if len(users) > 4:  # only show results if fewer than 4 to prevent spam

            await self._sendMsg(ctx, ctx.author, "Too many results", "Refine your search", silent)
            return

        for user in users:
            userdata = await self.config.member(user).all()
            pic = userdata["Characterpic"]
            data = discord.Embed(colour=user.colour)   #description="{}".format(server) 
            hiddenfields = {"Characterpic", "Name"}  ## fields to hide on bio cards
            newlinefields = {"About", "Interests", "Email", "Site"}
            if not args:
                fields = [data.add_field(name=k, value=v, inline=k not in newlinefields) for k,v in userdata.items() if v and k not in hiddenfields]
            else:   # filter for fields
                fieldfilter = set([arg.lower() for arg in args])
                fields = [data.add_field(name=k, value=v, inline=k not in newlinefields) for k,v in userdata.items() if k.lower() in fieldfilter and v and k not in hiddenfields]

            name = userdata["Name"]
            if user.avatar_url and not pic:
                # name = str(user)
                # name = " ~ ".join((name, user.nick)) if user.nick else name
                data.set_author(name=name, url=user.avatar_url)
                data.set_thumbnail(url=user.avatar_url)
            elif pic:
                data.set_author(name=name, url=user.avatar_url)
                data.set_thumbnail(url=pic)
            else:
                data.set_author(name=name)
            
            # if len(fields) != 0:
            if not silent:
                await ctx.send(embed=data)
            else:
                await ctx.author.send(embed=data)
            # else:
                # data = discord.Embed(colour=user.colour)
                # data.add_field(name="Error:warning:",value="{} doesn't have an account at the moment, sorry.".format(user.mention))
                # await ctx.send(embed=data)
        # wait asyncio.sleep(1)
        await utils.mod.slow_deletion([ctx.message])

    @commands.group(name="update")
    @commands.guild_only()
    async def update(self, ctx):
        """Update your TPC"""
        pass

    @update.command(pass_context=True)
    @commands.guild_only()
    async def name(self, ctx, *, name):
        """Your name"""
        
        server = ctx.guild
        user = ctx.author
        prefix = ctx.prefix
        db = await self.config.guild(server).db()
        
        if user.id not in db:
            await self._reg(ctx, user)

        await self.config.member(user).Name.set(name)
        await self._sendMsg(ctx, user, "Congrats!:sparkles:", "You have updated your name to {}".format(name))


    @update.command(pass_context=True)
    @commands.guild_only()
    async def about(self, ctx, *, about):
        """Tell us about yourself"""
        
        server = ctx.guild
        user = ctx.author
        prefix = ctx.prefix
        db = await self.config.guild(server).db()
        
        if user.id not in db:
            await self._reg(ctx, user)

        if about.lower() == "reset":
            about = ""

        await self.config.member(user).About.set(about)
        await self._sendMsg(ctx, user, "Congrats!:sparkles:", "You have updated your About Me to {}".format(about))

    @update.command(pass_context=True)
    @commands.guild_only()
    async def website(self, ctx, *, site):
        """Do you have a website?"""
        
        server = ctx.guild
        user = ctx.message.author
        prefix = ctx.prefix
        db = await self.config.guild(server).db()

        if user.id not in db:
            await self._reg(ctx, user)

        if site.lower() == "reset":
            site = ""

        await self.config.member(user).Site.set(site)
        await self._sendMsg(ctx, user, "Congrats!:sparkles:", "You have updated your Website to {}".format(site))

    @update.command(pass_context=True)
    @commands.guild_only()
    async def age(self, ctx, *, age):
        """How old are you?"""
        
        server = ctx.guild
        user = ctx.author
        prefix = ctx.prefix
        db = await self.config.guild(server).db()

        if user.id not in db:
            await self._reg(ctx, user)

        if age.lower() == "reset":
            age = ""

        await self.config.member(user).Age.set(age)
        await self._sendMsg(ctx, user, "Congrats!:sparkles:", "You have updated your age to {}".format(age))

    @update.command(pass_context=True)
    @commands.guild_only()
    async def interests(self, ctx, *, interests):
        """What are your interests?"""
        
        server = ctx.guild
        user = ctx.author
        prefix = ctx.prefix
        db = await self.config.guild(server).db()

        if user.id not in db:
            await self._reg(ctx, user)

        if interests.lower() == "reset":
            interests = ""

        await self.config.member(user).Interests.set(interests)
        await self._sendMsg(ctx, user, "Congrats!:sparkles:", "You have updated your interests to {}".format(interests))
    
    @update.command(pass_context=True)
    @commands.guild_only()
    async def pronoun(self, ctx, *, pronoun):
        """What are your preferred pronouns?"""

        server = ctx.guild
        user = ctx.author
        prefix = ctx.prefix
        db = await self.config.guild(server).db()

        if user.id not in db:
            await self._reg(ctx, user)

        if pronoun.lower() == "reset":
            pronoun = ""

        await self.config.member(user).Pronoun.set(pronoun)
        await self._sendMsg(ctx, user, "Congrats!:sparkles:", "You have updated your pronoun to {}".format(pronoun))
 
    @update.command(pass_context=True)
    @commands.guild_only()
    async def email(self, ctx, *, email):
        """What's your email address?"""

        
        server = ctx.guild
        user = ctx.author
        prefix = ctx.prefix
        db = await self.config.guild(server).db()

        if user.id not in db:
            await self._reg(ctx, user)

        if email.lower() == "reset":
            email = ""

        await self.config.member(user).Email.set(email)
        await self._sendMsg(ctx, user, "Congrats!:sparkles:", "You have updated your email to {}".format(email))

    @update.command(pass_context=True)
    @commands.guild_only()
    async def program(self, ctx, *, program):
        """Which academic program are you in?"""
        
        server = ctx.guild
        user = ctx.author
        prefix = ctx.prefix
        db = await self.config.guild(server).db()

        if user.id not in db:
            await self._reg(ctx, user)

        if program.lower() == "reset":
            program = ""

        await self.config.member(user).Program.set(program)
        await self._sendMsg(ctx, user, "Congrats!:sparkles:", "You have updated your program to {}".format(program))

    @update.command(pass_context=True)
    @commands.guild_only()
    async def level(self, ctx, *, level):
        """Which level/year are you currently enrolled in?"""
        
        server = ctx.guild
        user = ctx.author
        prefix = ctx.prefix
        db = await self.config.guild(server).db()

        if user.id not in db:
            await self._reg(ctx, user)

        if level.lower() == "reset":
            level = ""

        await self.config.member(user).Level.set(level)
        await self._sendMsg(ctx, user, "Congrats!:sparkles:", "You have updated your level to {}".format(level))

    @update.command(pass_context=True)
    @commands.guild_only()
    async def characterpic(self, ctx, *, characterpic):
        """What does your character look like?"""
        
        server = ctx.guild
        user = ctx.author
        prefix = ctx.prefix
        db = await self.config.guild(server).db()

        if user.id not in db:
            await self._reg(ctx, user)

        if characterpic.lower() == "reset":
            characterpic = ""

        await self.config.member(user).Characterpic.set(characterpic)
        data = discord.Embed(colour=user.colour)
        await self._sendMsg(ctx, user, "Congrats!:sparkles:", "You have updated your profile picture to {}".format(characterpic))
