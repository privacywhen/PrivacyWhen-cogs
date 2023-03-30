import discord
import re
from redbot.core import checks, commands
from typing import Optional
from .lcd_cache import CacheHandler
from .faculty_dictionary import FACULTIES

class CourseManager(commands.Cog):
    """A cog for managing course-related channels."""

    def __init__(self, bot):
        """Initialize the CourseManager with the bot instance."""
        self.bot = bot
        self.main_category_name = "COURSES"
        self.channel_permissions = discord.Permissions(view_channel=True, send_messages=True, read_message_history=True)
        self.max_courses = 15
        self.logging_channel = None
        self.cache_handler = CacheHandler(bot)

    @commands.group(invoke_without_command=True)
    async def course(self, ctx):
        """Main command group."""
        await ctx.send_help(self.course)

    @course.command()
    async def help(self, ctx):
        """Displays help menu."""
        embed = discord.Embed(title="Course Help Menu", color=discord.Color.blue())
        embed.add_field(name="Syntax", value="=course [subcommand] [arguments]", inline=False)
        embed.add_field(name="Subcommands", value="join [course_code]\nleave [course_code]\ndelete [channel]\nhelp", inline=False)
        await ctx.send(embed=embed)

    @course.command()
    @commands.cooldown(1, 10, commands.BucketType.user)
    async def join(self, ctx, *, course_code: str):
        print(f"Debug: join() - course_code: {course_code}")
        formatted_course_code = await self.format_course_code(course_code)  # Format the course code

        if not formatted_course_code or not await self.course_exists(formatted_course_code):
            await ctx.send(f"Error: The course code {course_code} is not valid. Please enter a valid course code.")
            return

        if len(self.get_user_courses(ctx.guild, ctx.author)) >= self.max_courses:
            await ctx.send(f"Error: You have reached the maximum limit of {self.max_courses} courses. Please leave a course before joining another.")
            return

        category = self.get_category(ctx.guild)
        channel_name = formatted_course_code
        existing_channel = discord.utils.get(ctx.guild.channels, name=channel_name)

        if existing_channel is None:
            existing_channel = await self.create_course_channel(ctx.guild, formatted_course_code, category, ctx.author)

        user_permissions = existing_channel.overwrites_for(ctx.author)
        user_permissions.update(view_channel=True, send_messages=True)  # use view_channel and send_messages permissions
        await existing_channel.set_permissions(ctx.author, overwrite=user_permissions)

        await ctx.send(f"You have successfully joined {formatted_course_code}.")
        if self.logging_channel:
            await self.logging_channel.send(f"{ctx.author.mention} has joined {formatted_course_code}.")  # use mention to ping user

    @course.command()
    @commands.cooldown(1, 10, commands.BucketType.user)
    async def leave(self, ctx, course_code: str):
        print("Debug: leave()")
        formatted_course_code = await self.format_course_code(course_code)

        if not formatted_course_code:
            await ctx.send("Error: Invalid course code provided.")
            return

        channel_name = formatted_course_code
        existing_channel = discord.utils.get(ctx.guild.channels, name=channel_name)

        if existing_channel is None:
            await ctx.send(f"Error: You are not a member of {formatted_course_code}.")
            return

        await existing_channel.set_permissions(ctx.author, read_messages=None)
        await ctx.send(f"You have successfully left {formatted_course_code}.")
        if self.logging_channel:
            await self.logging_channel.send(f"{ctx.author.mention} has left {formatted_course_code}.")

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
            if category.name.lower() == self.main_category_name.lower():
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

    async def create_course_channel(self, guild, course_code, user):
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

        default_role_overwrites = discord.PermissionOverwrite(read_messages=False)
        bot_overwrites = discord.PermissionOverwrite(read_messages=True, send_messages=True)
        user_overwrites = discord.PermissionOverwrite(read_messages=True, send_messages=True, view_channel=True)

        overwrites = {
            guild.default_role: default_role_overwrites,
            guild.me: bot_overwrites,
            user: user_overwrites,
        }

        channel_name = course_code.replace(" ", "-").upper()
        new_channel = await guild.create_text_channel(channel_name, overwrites=overwrites, category=course_category)
        return new_channel

    def get_user_courses(self, ctx, guild):
        """Returns a list of courses a user has joined."""
        courses = []
        category = self.get_category(guild)
        if not category:
            return courses
        for channel in category.channels:
            if isinstance(channel, discord.TextChannel) and channel.permissions_for(ctx.author).view_channel:
                if category.name in FACULTIES.keys():
                    courses.append(channel.name.upper())
        return courses

    async def course_exists(self, course_code):
        """Checks if the course exists in the cache or online."""
        print("Debug: course_exists()")
        return await self.cache_handler.course_code_exists(course_code)

    async def format_course_code(self, course_code: str) -> Optional[str]:
        print(f"Debug: format_course_code() - course_code: {course_code}")
        course_code = course_code.upper().replace("-", " ").replace("_", " ")  # Convert to uppercase and replace hyphens and underscores with spaces
        print(f"Debug: course_code after replacing hyphens and underscores: {course_code}")
        course_parts = re.split(r'\s+', course_code.strip())  # Split by whitespace characters

        if len(course_parts) < 2:
            return None
        elif len(course_parts) > 2:
            course_number = " ".join(course_parts[1:])
        else:
            course_number = course_parts[1]

        department = course_parts[0]
        print(f"Debug: department: {department}, course_number: {course_number}")

        # Validate the department and course number for valid characters
        department_pattern = re.compile(r'^[A-Z]+$')
        course_number_pattern = re.compile(r'^(\d[0-9A-Za-z]{1,3}).*')

        if not department_pattern.match(department) or not course_number_pattern.match(course_number):
            return None

        # Remove any unwanted characters after the course_number
        course_number = course_number_pattern.match(course_number).group(1)
        print(f"Debug: course_number after removing unwanted characters: {course_number}")

        formatted_code = f"{department} {course_number}"
        print(f"Debug: formatted_code: {formatted_code}")

        if await self.course_exists(formatted_code):
            return formatted_code
        else:
            return None

    @course.command()
    async def mine(self, ctx):
        """Displays the courses the user belongs to."""
        courses = self.get_user_courses(ctx.guild, ctx.author)
        if courses:
            await ctx.send(f"{ctx.author.mention}, you are a member of the following courses:\n{', '.join(courses)}")
        else:
            await ctx.send(f"{ctx.author.mention}, you are not a member of any course.")