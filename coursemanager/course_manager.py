import discord
from redbot.core import checks, commands
import sqlite3
from sqlite3 import Error
import os

db_file = f"{os.path.dirname(os.path.realpath(__file__))}/courses.db"

class CourseManager(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.category_name = "COURSES"
        self.channel_permissions = discord.Permissions(view_channel=True, send_messages=True, read_message_history=True)
        self.max_courses = 10
        self.logging_channel = None

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
    async def join(self, ctx, course_code: str):
        """Allows a user to join a course."""
        course_code = course_code.upper()
        if not self.course_exists(course_code):
            await ctx.send(f"Error: The course code {course_code} is not valid. Please enter a valid course code.")
            return

        if len(self.get_user_courses(ctx.author)) >= self.max_courses:
            await ctx.send(f"Error: You have reached the maximum limit of {self.max_courses} courses. Please leave a course before joining another.")
            return

        category = self.get_category(ctx.guild)
        channel = self.get_course_channel(ctx.guild, course_code)
        if not channel:
            channel = await self.create_course_channel(ctx.guild, category, course_code)

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
    async def leave(self, ctx, course_code: str):
        """Allows a user to leave a course."""
        course_code = course_code.upper()
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

    def get_category(self, guild):
        """Returns the COURSES category."""
        for category in guild.categories:
            if category.name == self.category_name:
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
        overwrites = {
            guild.default_role: discord.PermissionOverwrite.from_pair(discord.Permissions.none(), discord.Permissions.none()),
            guild.me: discord.PermissionOverwrite.from_pair(discord.Permissions.all(), discord.Permissions.none())
        }
        return await guild.create_text_channel(course_code.lower(), overwrites=overwrites, category=category)

    def get_user_courses(self, user):
        """Returns a list of courses a user has joined."""
        courses = []
        for guild in self.bot.guilds:
            category = self.get_category(guild)
            if not category:
                continue
            for channel in category.channels:
                if isinstance(channel, discord.TextChannel) and channel.permissions_for(user).read_messages:
                    courses.append(channel.name.upper())
        return courses

    def course_exists(self, course_code):
    """Checks if the course exists in the database."""
    with sqlite3.connect(db_file) as conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM courses WHERE code = ?", (course_code,))
            return cur.fetchone() is not None
        except Error as e:
            print(e)
    return False


    def setup(bot):
        bot.add_cog(CourseManager(bot))