import discord
from redbot.core import checks, commands
from .lcd_cache import CacheHandler
from .faculty_dictionary import FACULTIES

class CourseManager(commands.Cog):
    """A cog for managing course-related channels."""
    def __init__(self, bot):
        """Initialize the CourseManager with the bot instance."""
        self.bot = bot
        self.category_name = "COURSES"
        self.channel_permissions = discord.Permissions(view_channel=True, send_messages=True, read_message_history=True)
        self.max_courses = 15
        self.logging_channel = None
        self.cache_handler = CacheHandler(bot)

    @commands.group(invoke_without_command=True)
    async def course(self, ctx):
        """Main command group."""
        await ctx.invoke(self.bot.get_command('course help'))

    @course.command()
    async def help(self, ctx):
        """Displays help menu."""
        embed = discord.Embed(title="Course Help Menu", color=discord.Color.blue())
        embed.add_field(name="Syntax", value="=course [subcommand] [arguments]", inline=False)
        embed.add_field(name="Subcommands", value="join [course_code]\nleave [course_code]\ndelete [channel]\nhelp", inline=False)
        await ctx.send(embed=embed)

    @course.command()
    @commands.cooldown(1, 10, commands.BucketType.user)
    async def join(self, ctx, *args):
        """Allows a user to join a course."""
        if len(args) < 2:
            await ctx.send("Error: Please enter a valid course code with both department and course number (e.g. PSYCH 1X03).")
            return

        course_code = " ".join([args[0].upper(), args[1]])

        if not await self.course_exists(course_code):
            await ctx.send(f"Error: The course code {course_code} is not valid. Please enter a valid course code.")
            return

        if len(self.get_user_courses(ctx.author)) >= self.max_courses:
            await ctx.send(f"Error: You have reached the maximum limit of {self.max_courses} courses. Please leave a course before joining another.")
            return

        category = self.get_category(ctx.guild)
        channel = self.get_course_channel(ctx.guild, course_code)
        if not channel:
            channel = await self.create_course_channel(ctx.guild, category, course_code)
            overwrite = discord.PermissionOverwrite.from_pair(discord.Permissions.none(), discord.Permissions.none())
            try:
                await channel.set_permissions(ctx.guild.default_role, overwrite=overwrite)
            except discord.Forbidden:
                await ctx.send("Error: I don't have permission to manage channel permissions.")
                return

        overwrite = discord.PermissionOverwrite.from_pair(self.channel_permissions, discord.Permissions.none())
        try:
            await channel.set_permissions(ctx.author, overwrite=overwrite)
        except discord.Forbidden:
            await ctx.send("Error: I don't have permission to manage channel permissions.")
            return

        await ctx.send(f"You have successfully joined {course_code}.", delete_after=120)
        if self.logging_channel:
            await self.logging_channel.send(f"{ctx.author} has joined {course_code}.")

    @course.command()
    @commands.cooldown(1, 10, commands.BucketType.user)
    async def leave(self, ctx, course_code: str):
        """Allows a user to leave a course."""
        course_code = " ".join([course_code.split(" ")[0].upper(), course_code.split(" ")[1]])
        channel = self.get_course_channel(ctx.guild, course_code)
        if not channel:
            await ctx.send(f"Error: You are not a member of {course_code}.")
            return

        overwrite = discord.PermissionOverwrite.from_pair(discord.Permissions.none(), discord.Permissions.none())
        try:
            await channel.set_permissions(ctx.author, overwrite=overwrite)
        except discord.Forbidden:
            await ctx.send("Error: I don't have permission to manage channel permissions.")
            return

        await ctx.send(f"You have successfully left {course_code}.", delete_after=120)
        if self.logging_channel:
            await self.logging_channel.send(f"{ctx.author} has left {course_code}.")

    @checks.admin()
    @course.command()
    @commands.cooldown(1, 60, commands.BucketType.user)
    async def delete(self, ctx, channel: discord.TextChannel):
        """Deletes a course channel."""
        if not channel.category or channel.category.name != self.category_name:
            await ctx.send(f"Error: {channel} is not a course channel.")
            return
        await channel.delete()
        await ctx.send(f"{channel.name} has been successfully deleted.")
        if self.logging_channel:
            await self.logging_channel.send(f"{channel} has been deleted.")

    @checks.admin()
    @commands.command()
    async def setcourse(self, ctx, option: str, channel: discord.TextChannel):
        """Sets logging channel for the cog."""
        if option.lower() == "logging":
            self.logging_channel = channel
            await ctx.send(f"Logging channel set to {channel}.")
            return

        await ctx.send("Invalid option. Use '=setcourse logging' followed by the channel.")

    @checks.admin()
    @course.command()
    async def clearcache(self, ctx):
        """Clears the course cache."""
        await self.cache_handler.config.courses.set({})
        await ctx.send("Course cache cleared.")

    def get_category(self, guild):
        """Returns the COURSES category."""
        for category in guild.categories:
            if category.name.lower() == self.category_name.lower():
                return category
        return None

    def get_course_channel(self, guild, course_code):
        """Returns a course channel if it exists."""
        category = self.get_category(guild)
        if not category:
            return None

        for channel in category.channels:
            if channel.name == course_code.lower():
                return channel
        return None

    
    async def create_course_channel(self, guild, category, course_code):
        """Creates a new course channel."""
        # Find the appropriate category for the course
        department_code = course_code.split(" ")[0]
        course_category_name = None
        for faculty, departments in FACULTIES.items():
            if department_code in departments:
                course_category_name = faculty.upper()
                break

        # If the course_category_name is still None, set it to a default category
        if course_category_name is None:
            course_category_name = "OTHER"

        # Check if the category exists, if not create it
        course_category = discord.utils.get(guild.categories, name=course_category_name)
        if course_category is None:
            course_category = await guild.create_category(course_category_name)

        overwrites = {
            guild.default_role: discord.PermissionOverwrite.from_pair(discord.Permissions.none(), discord.Permissions.none()),
            guild.me: discord.PermissionOverwrite.from_pair(discord.Permissions.all(), discord.Permissions.none())
        }
        # Use the course_category for creating the new course channel
        channel_name = course_code.replace(" ", "-").upper()
        return await guild.create_text_channel(channel_name, overwrites=overwrites, category=course_category)

    def get_user_courses(self, user):
        """Returns a list of courses a user has joined."""
        courses = []
        for guild in self.bot.guilds:
            category = self.get_category(guild)
            if not category:
                continue
            for channel in category.channels:
                if isinstance(channel, discord.TextChannel) and channel.permissions_for(user).view_channel:  # Changed from read_messages to view_channel
                    courses.append(channel.name.upper())
        return courses

    async def course_exists(self, course_code):
        """Checks if the course exists in the cache or online."""
        return await self.cache_handler.course_code_exists(course_code)