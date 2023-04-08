import discord
import re
from redbot.core import checks, commands
from typing import Optional
from .get_course_data import CourseCacheHandler as CacheHandler

from .faculty_dictionary import FACULTIES


class CourseManager(commands.Cog):
    """A cog for managing course-related channels."""

    def __init__(self, bot):
        """Initialize the CourseManager with the bot instance."""
        self.bot = bot
        self.channel_permissions = discord.Permissions(
            view_channel=True, send_messages=True, read_message_history=True)
        self.max_courses = 5
        self.logging_channel = None
        self.cache_handler = CacheHandler(bot)

    @commands.group(invoke_without_command=True)
    async def course(self, ctx):
        """Main command group."""
        await ctx.send_help(self.course)

    @course.command()
    async def help(self, ctx):
        """Displays help menu."""
        embed = discord.Embed(title="Course Help Menu",
                              color=discord.Color.blue())
        embed.add_field(
            name="Syntax", value="=course [subcommand] [arguments]", inline=False)
        embed.add_field(
            name="Subcommands", value="join [course_code]\nleave [course_code]\ndelete [channel]\nhelp", inline=False)
        await ctx.send(embed=embed)

    @course.command()
    @commands.cooldown(1, 10, commands.BucketType.user)
    async def join(self, ctx, *, course_code: str):
        """Join a course channel."""
        print(f"Debug: join() - course_code: {course_code}")
        # Format the course code
        result = await self.format_course_code(course_code)
        if not result:
            await ctx.send(f"Error: The course code {course_code} is not valid. Please enter a valid course code.")
            return

        dept, code = result

        if len(self.get_user_courses(ctx, ctx.guild)) >= self.max_courses:
            await ctx.send(f"Error: You have reached the maximum limit of {self.max_courses} courses. Please leave a course before joining another.")
            return

        channel_name = f"{dept}-{code}"
        existing_channel = discord.utils.get(
            ctx.guild.channels, name=channel_name.lower())

        if existing_channel is None:
            existing_channel = await self.create_course_channel(ctx.guild, dept, code, ctx.author)

        user_permissions = existing_channel.overwrites_for(ctx.author)
        # use view_channel and send_messages permissions
        user_permissions.update(view_channel=True, send_messages=True)
        await existing_channel.set_permissions(ctx.author, overwrite=user_permissions)

        await ctx.send(f"You have successfully joined {dept} {code}.")
        if self.logging_channel:
            # use mention to ping user
            await self.logging_channel.send(f"{ctx.author.mention} has joined {dept} {code}.")

    @course.command()
    @commands.cooldown(1, 10, commands.BucketType.user)
    async def leave(self, ctx, *, course_code: str):
        """Leave a course channel."""
        print("Debug: leave()")
        result = await self.format_course_code(course_code)

        if not result:
            await ctx.send("Error: Invalid course code provided.")
            return

        dept, code = result

        channel_name = f"{dept}-{code}"
        existing_channel = discord.utils.get(
            ctx.guild.channels, name=channel_name.lower())

        if existing_channel is None:
            await ctx.send(f"Error: You are not a member of {dept}-{code}.")
            return

        await existing_channel.set_permissions(ctx.author, read_messages=None)
        await ctx.send(f"You have successfully left {channel_name}.")
        if self.logging_channel:
            await self.logging_channel.send(f"{ctx.author.mention} has left {channel_name}.")

    @checks.admin()
    @course.command()
    @commands.cooldown(1, 60, commands.BucketType.user)
    async def delete(self, ctx, *, channel: discord.TextChannel):
        """Deletes a course channel."""
        if not channel.category or channel.category.name != self.category_name:
            await ctx.send(f"Error: {channel} is not a course channel.")
            return
        await channel.delete()
        await ctx.send(f"{channel.name} has been successfully deleted.")
        if self.logging_channel:
            await self.logging_channel.send(f"{channel} has been deleted.")


## HELPER FUNCTIONS ##

    async def course_exists(self, course_code):
        """Checks if a course exists."""
        print ("Debug: course_exists()")
        return await self.cache_handler.fetch_course_cache(course_code)
    
    def get_category(self, guild, faculty):
        """Returns a dept category if exists."""
        for category in guild.categories:
            if category.name.upper() == faculty:
                return category
        return None

    def get_all_categories(self, guild):
        """Returns ALL dept categories."""
        categories = []
        for category in guild.categories:
            if category.name.upper() in FACULTIES.keys():
                categories.append(category)
        return categories

    async def create_course_channel(self, guild, dept, code, user):
        """Creates a new course channel."""
        # Find the appropriate category for the course
        course_category_name = None
        for faculty, departments in FACULTIES.items():
            if dept in departments:
                course_category_name = faculty.upper()
                break

        if course_category_name is None:
            return None, "The department was not found in the predefined FACULTIES dictionary. Please inform an admin if you believe this is an error."

        # Check if the category exists, if not create it
        course_category = self.get_category(guild, course_category_name)
        if course_category is None:
            course_category = await guild.create_category(course_category_name)

        default_role_overwrites = discord.PermissionOverwrite(
            read_messages=False)
        bot_overwrites = discord.PermissionOverwrite(
            read_messages=True, send_messages=True)
        user_overwrites = discord.PermissionOverwrite(
            read_messages=True, send_messages=True, view_channel=True)

        overwrites = {
            guild.default_role: default_role_overwrites,
            guild.me: bot_overwrites,
            user: user_overwrites,
        }

        channel_name = f"{dept}-{code}".upper()
        new_channel = await guild.create_text_channel(channel_name, overwrites=overwrites, category=course_category)
        return new_channel, None

    def get_user_courses(self, ctx, guild):
        """Returns a list of courses a user has joined."""
        courses = []
        categories = self.get_all_categories(guild)
        for category in categories:
            for channel in category.channels:
                if isinstance(channel, discord.TextChannel) and channel.permissions_for(ctx.author).view_channel:
                    courses.append(channel.name.lower())
        return courses

    async def format_course_code(self, course_code: str) -> Optional[str]:
        print(f"Debug: format_course_code() - course_code: {course_code}")
        # Convert to uppercase and replace hyphens and underscores with spaces
        course_code = course_code.upper().replace("-", " ").replace("_", " ")
        print(
            f"Debug: course_code after replacing hyphens and underscores: {course_code}")
        # Split by whitespace characters
        course_parts = re.split(r'\s+', course_code.strip())

        if len(course_parts) < 2:
            return None
        elif len(course_parts) > 2:
            course_number = " ".join(course_parts[1:])
        else:
            course_number = course_parts[1]

        department = course_parts[0]
        print(
            f"Debug: department: {department}, course_number: {course_number}")

        # Validate the department and course number for valid characters
        department_pattern = re.compile(r'^[A-Z]+$')
        course_number_pattern = re.compile(r'^(\d[0-9A-Za-z]{1,3}).*')

        if not department_pattern.match(department) or not course_number_pattern.match(course_number):
            return None

        # Remove any unwanted characters after the course_number
        course_number = course_number_pattern.match(course_number).group(1)
        print(
            f"Debug: course_number after removing unwanted characters: {course_number}")

        formatted_code = f"{department} {course_number}"
        print(f"Debug: formatted_code: {formatted_code}")

        if await self.course_exists(formatted_code):
            return (department, course_number)
        else:
            return None

## DEV COMMANDS ## (These commands are only available to the bot owner)

    @checks.is_owner()
    @course.command()
    async def online(self, ctx, *, course_code: str):
        """Gets course data from the McMaster API."""
        print(f"Debug: join() - course_code: {course_code}")
        # Format the course code
        result = await self.format_course_code(course_code)
        if not result:
            await ctx.send(f"Error: The course code {course_code} is not valid. Please enter a valid course code.")
            return

        dept, code = result
        formatted_course_code = f"{dept} {code}"
        
        course_data = await self.cache_handler.fetch_course_online(formatted_course_code)
        await ctx.send(course_data)

    @checks.is_owner()
    @course.command()
    async def setfall(self, ctx, fall: int):
        """Sets the Fall term code."""
        await self.cache_handler.term_codes(ctx, "fall", fall)
        await ctx.send("Fall term code set.")

    @checks.is_owner()
    @course.command()
    async def setwinter(self, ctx, winter: int):
        """Sets the Winter term code."""
        await self.cache_handler.term_codes(ctx, "winter", winter)
        await ctx.send("Winter term code set.")

    @checks.is_owner()
    @course.command()
    async def setspring(self, ctx, spring: int):
        """Sets the Spring/Summer term code."""
        await self.cache_handler.term_codes(ctx, "spring", spring)
        await ctx.send("Spring/Summer term code set.")

    @checks.is_owner()
    @course.command()
    async def mine(self, ctx):
        """Displays the courses the user belongs to."""
        courses = self.get_user_courses(ctx, ctx.guild, ctx.author)
        if courses:
            await ctx.send(f"{ctx.author.mention}, you are a member of the following courses:\n{', '.join(courses)}")
        else:
            await ctx.send(f"{ctx.author.mention}, you are not a member of any course.")

    @checks.is_owner()
    @course.command()
    async def clearcache(self, ctx):
        """Clears the course cache."""
        await self.cache_handler.config.courses.set({})
        await ctx.send("Course cache cleared.")

    @checks.is_owner()
    @course.command()
    async def setlog(self, ctx, option: str, channel: discord.TextChannel):
        """Sets logging channel for the cog."""
        if option.lower() == "logging":
            self.logging_channel = channel
            await ctx.send(f"Logging channel set to {channel}.")
            return

        await ctx.send("Invalid option. Use '=course setlog logging' followed by the channel.")


## Removed Code ##

    # def get_course_channel(self, guild, course_code):
    #     """Returns a course channel if it exists."""
    #     category = self.get_category(guild)
    #     if not category:
    #         return None

    #     for channel in category.channels:
    #         if channel.name == course_code.lower():
    #             return channel
    #     return None