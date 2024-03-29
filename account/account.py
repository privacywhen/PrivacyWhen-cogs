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
            "Pronouns": None,
            "About": None,
            "Interests": None,
            "Email": None,
            "Site": None,
            "Picture": None
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
                if not member: continue             # ignore if member no longer in guild
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
        elif not users:
            await self._sendMsg(ctx, ctx.author, "No results", "Double check your search term", silent)
        else:
            for user in users:
                userdata = await self.config.member(user).all()
                pic = userdata["Picture"]
                data = discord.Embed(colour=user.colour)   #description="{}".format(server) 
                hiddenfields = {"Picture", "Name"}  ## fields to hide on bio cards
                newlinefields = {"About", "Interests", "Email", "Site"}
                if not args:
                    fields = [data.add_field(name=k, value=v, inline=k not in newlinefields) for k,v in userdata.items() if v and k not in hiddenfields]
                else:   # filter for fields
                    fieldfilter = set([arg.lower() for arg in args])
                    fields = [data.add_field(name=k, value=v, inline=k not in newlinefields) for k,v in userdata.items() if k.lower() in fieldfilter and v and k not in hiddenfields]

                name = userdata["Name"]
                if user.avatar.url and not pic:
                    # name = str(user)
                    # name = " ~ ".join((name, user.nick)) if user.nick else name
                    data.set_author(name=name, url=user.avatar.url)
                    data.set_thumbnail(url=user.avatar.url)
                elif pic:
                    data.set_author(name=name, url=user.avatar.url)
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
        if silent: await utils.mod.slow_deletion([ctx.message])

    @commands.group(name="update")
    @commands.guild_only()
    async def update(self, ctx):
        """Update your TPC"""
        pass

    @update.command(pass_context=True)
    @commands.guild_only()
    async def name(self, ctx, *, name):
        """Your name"""
        if len(name) > 3 and name[-3:] == " -s":
            silent = True
            name = name[:-3]
        else:
            silent = False
        server = ctx.guild
        user = ctx.author
        prefix = ctx.prefix
        db = await self.config.guild(server).db()
        
        if user.id not in db:
            await self._reg(ctx, user)

        await self.config.member(user).Name.set(name)
        await self._sendMsg(ctx, user, "Congrats!:sparkles:", "You have updated your name to {}".format(name), silent)


    @update.command(pass_context=True)
    @commands.guild_only()
    async def about(self, ctx, *, about):
        """Tell us about yourself"""
        if len(about) > 3 and about[-3:] == " -s":
            silent = True
            about = about[:-3]
        else:
            silent = False
        server = ctx.guild
        user = ctx.author
        prefix = ctx.prefix
        db = await self.config.guild(server).db()
        
        if user.id not in db:
            await self._reg(ctx, user)

        if about.lower() == "reset":
            about = ""

        await self.config.member(user).About.set(about)
        await self._sendMsg(ctx, user, "Congrats!:sparkles:", "You have updated your About Me to {}".format(about), silent)

    @update.command(pass_context=True)
    @commands.guild_only()
    async def website(self, ctx, *, site):
        """Do you have a website?"""
        if len(site) > 3 and site[-3:] == " -s":
            silent = True
            site = site[:-3]
        else:
            silent = False
        server = ctx.guild
        user = ctx.message.author
        prefix = ctx.prefix
        db = await self.config.guild(server).db()

        if user.id not in db:
            await self._reg(ctx, user)

        if site.lower() == "reset":
            site = ""

        await self.config.member(user).Site.set(site)
        await self._sendMsg(ctx, user, "Congrats!:sparkles:", "You have updated your Website to {}".format(site), silent)

    @update.command(pass_context=True)
    @commands.guild_only()
    async def age(self, ctx, *, age):
        """How old are you?"""
        if len(age) > 3 and age[-3:] == " -s":
            silent = True
            age = age[:-3]
        else:
            silent = False    
        server = ctx.guild
        user = ctx.author
        prefix = ctx.prefix
        db = await self.config.guild(server).db()

        if user.id not in db:
            await self._reg(ctx, user)

        if age.lower() == "reset":
            age = ""

        await self.config.member(user).Age.set(age)
        await self._sendMsg(ctx, user, "Congrats!:sparkles:", "You have updated your age to {}".format(age), silent)

    @update.command(pass_context=True)
    @commands.guild_only()
    async def interests(self, ctx, *, interests):
        """What are your interests?"""
        if len(interests) > 3 and interests[-3:] == " -s":
            silent = True
            interests = interests[:-3]
        else:
            silent = False            
        server = ctx.guild
        user = ctx.author
        prefix = ctx.prefix
        db = await self.config.guild(server).db()

        if user.id not in db:
            await self._reg(ctx, user)

        if interests.lower() == "reset":
            interests = ""

        await self.config.member(user).Interests.set(interests)
        await self._sendMsg(ctx, user, "Congrats!:sparkles:", "You have updated your interests to {}".format(interests), silent)
    
    @update.command(pass_context=True)
    @commands.guild_only()
    async def pronouns(self, ctx, *, pronouns):
        """What are your preferred pronounss?"""
        if len(pronouns) > 3 and pronouns[-3:] == " -s":
            silent = True
            pronouns = pronouns[:-3]
        else:
            silent = False      
        server = ctx.guild
        user = ctx.author
        prefix = ctx.prefix
        db = await self.config.guild(server).db()

        if user.id not in db:
            await self._reg(ctx, user)

        if pronouns.lower() == "reset":
            pronouns = ""

        await self.config.member(user).Pronouns.set(pronouns)
        await self._sendMsg(ctx, user, "Congrats!:sparkles:", "You have updated your pronouns to {}".format(pronouns), silent)
 
    @update.command(pass_context=True)
    @commands.guild_only()
    async def email(self, ctx, *, email):
        """What's your email address?"""
        if len(email) > 3 and email[-3:] == " -s":
            silent = True
            email = email[:-3]
        else:
            silent = False      
        
        server = ctx.guild
        user = ctx.author
        prefix = ctx.prefix
        db = await self.config.guild(server).db()

        if user.id not in db:
            await self._reg(ctx, user)

        if email.lower() == "reset":
            email = ""

        await self.config.member(user).Email.set(email)
        await self._sendMsg(ctx, user, "Congrats!:sparkles:", "You have updated your email to {}".format(email), silent)

    @update.command(pass_context=True)
    @commands.guild_only()
    async def program(self, ctx, *, program):
        """Which academic program are you in?"""
        if len(program) > 3 and program[-3:] == " -s":
            silent = True
            program = program[:-3]
        else:
            silent = False

        server = ctx.guild
        user = ctx.author
        prefix = ctx.prefix
        db = await self.config.guild(server).db()

        if user.id not in db:
            await self._reg(ctx, user)

        if program.lower() == "reset":
            program = ""

        await self.config.member(user).Program.set(program)
        await self._sendMsg(ctx, user, "Congrats!:sparkles:", "You have updated your program to {}".format(program), silent)

    @update.command(pass_context=True)
    @commands.guild_only()
    async def level(self, ctx, *, level):
        """Which level/year are you currently enrolled in?"""
        if len(level) > 3 and level[-3:] == " -s":
            silent = True
            level = level[:-3]
        else:
            silent = False

        server = ctx.guild
        user = ctx.author
        prefix = ctx.prefix
        db = await self.config.guild(server).db()

        if user.id not in db:
            await self._reg(ctx, user)

        if level.lower() == "reset":
            level = ""

        await self.config.member(user).Level.set(level)
        await self._sendMsg(ctx, user, "Congrats!:sparkles:", "You have updated your level to {}".format(level), silent)

    @update.command(pass_context=True)
    @commands.guild_only()
    async def picture(self, ctx, *, picture):
        """What picture would you like to display on your account?"""
        if len(picture) > 3 and picture[-3:] == " -s":
            silent = True
            picture = picture[:-3]
        else:
            silent = False   

        server = ctx.guild
        user = ctx.author
        prefix = ctx.prefix
        db = await self.config.guild(server).db()

        if user.id not in db:
            await self._reg(ctx, user)

        if picture.lower() == "reset":
            picture = ""

        await self.config.member(user).Picture.set(picture)
        data = discord.Embed(colour=user.colour)
        await self._sendMsg(ctx, user, "Congrats!:sparkles:", "You have updated your profile picture to {}".format(picture), silent)
