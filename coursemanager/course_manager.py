import discord
import re
from redbot.core import checks, commands
from typing import Tuple, Optional
from .get_course_data import CourseCacheHandler

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
        self.cache_handler = CourseCacheHandler(bot)

    @commands.group(invoke_without_command=True)
    async def course(self, ctx):
        """Main command group."""
        await ctx.send_help(self.course)

#    @course.command()
#    async def help(self, ctx):
#        """Displays help menu."""
#        embed = discord.Embed(title="Course Help Menu",
#                              color=discord.Color.blue())
#        embed.add_field(
#            name="Syntax", value="=course [subcommand] [arguments]", inline=False)
#        embed.add_field(
#            name="Subcommands", value="join [course_code]\nleave [course_code]\ndelete [channel]\nhelp", inline=False)
#        await ctx.send(embed=embed)

#    @course.command()
#    @commands.cooldown(1, 10, commands.BucketType.user)
#    async def join(self, ctx, *, course_code: str):
#        """Join a course channel."""
#        print(f"Debug: join() - course_code: {course_code}")
        # Format the course code
#        result = await self.format_course_code(course_code)
#        if not result:
#            await ctx.send(f"Error: The course code {course_code} is not valid. Please enter a valid course code.")
#            return

#        dept, code = result

#        if len(self.get_user_courses(ctx, ctx.guild)) >= self.max_courses:
#            await ctx.send(f"Error: You have reached the maximum limit of {self.max_courses} courses. Please leave a course before joining another.")
#            return

#        channel_name = f"{dept}-{code}"
#        existing_channel = discord.utils.get(
#            ctx.guild.channels, name=channel_name.lower())

#        if existing_channel is None:
#            existing_channel = await self.create_course_channel(ctx.guild, dept, code, ctx.author)

#        user_permissions = existing_channel.overwrites_for(ctx.author)
        # use view_channel and send_messages permissions
#        user_permissions.update(view_channel=True, send_messages=True)
#        await existing_channel.set_permissions(ctx.author, overwrite=user_permissions)

#        await ctx.send(f"You have successfully joined {dept} {code}.")
#        if self.logging_channel:
            # use mention to ping user
#            await self.logging_channel.send(f"{ctx.author.mention} has joined {dept} {code}.")

#    @course.command()
#    @commands.cooldown(1, 10, commands.BucketType.user)
#    async def leave(self, ctx, *, course_code: str):
#        """Leave a course channel."""
#        print("Debug: leave()")
#        result = await self.format_course_code(course_code)

#        if not result:
#            await ctx.send("Error: Invalid course code provided.")
#            return

#        dept, code = result

#        channel_name = f"{dept}-{code}"
#        existing_channel = discord.utils.get(
#            ctx.guild.channels, name=channel_name.lower())

#        if existing_channel is None:
#            await ctx.send(f"Error: You are not a member of {dept}-{code}.")
#            return

#        await existing_channel.set_permissions(ctx.author, read_messages=None)
#        await ctx.send(f"You have successfully left {channel_name}.")
#        if self.logging_channel:
#            await self.logging_channel.send(f"{ctx.author.mention} has left {channel_name}.")

#    @checks.admin()
#    @course.command()
#    @commands.cooldown(1, 60, commands.BucketType.user)
#    async def delete(self, ctx, *, channel: discord.TextChannel):
#        """Deletes a course channel."""
#        if not channel.category or channel.category.name != self.category_name:
#            await ctx.send(f"Error: {channel} is not a course channel.")
#            return
#        await channel.delete()
#        await ctx.send(f"{channel.name} has been successfully deleted.")
#        if self.logging_channel:
#            await self.logging_channel.send(f"{channel} has been deleted.")


## HELPER FUNCTIONS ##

#    async def course_exists(self, course_code):
#        """Checks if the course exists in the cache or online."""
#        print ("Debug: course_exists()")
#        return await self.cache_handler.course_code_exists(course_code)
    
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

    def format_course_code(self, course_code: str) -> Optional[Tuple[str, str]]:
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

        department_match = department_pattern.match(department)
        course_number_match = course_number_pattern.match(course_number)

        if not department_match or not course_number_match:
            return None

        # Remove any unwanted characters after the course_number
        course_number = course_number_match.group(1)
        print(
            f"Debug: course_number after removing unwanted characters: {course_number}")

        formatted_code = f"{department} {course_number}"
        print(f"Debug: formatted_code: {formatted_code}")

        return (department, course_number)

    async def send_long_message(self, ctx, content, max_length=2000):
        while content:
            message_chunk = content[:max_length]
            await ctx.send(message_chunk)
            content = content[max_length:]

    def create_course_embed(self, course_data, formatted_course_code):
        embed = discord.Embed(title=f"{formatted_course_code}", color=0x00FF00)

        for course_info in course_data:
            course_name = f"{course_info['course']} {course_info['section']}"

            course_details = [
                f"**Teacher**: {course_info['teacher']}\n" if course_info['teacher'] else "",
                f"**Term**: {course_info['term_found']}\n" if course_info['term_found'] else "",
                f"**Description**: {course_info['description']}\n" if course_info['description'] else "",
                f"**Notes**: {course_info['notes']}\n" if course_info['notes'] else "",
                f"**Prerequisites**: {course_info['prerequisites']}\n" if course_info['prerequisites'] else "",
                f"**Antirequisites**: {course_info['antirequisites']}" if course_info['antirequisites'] else ""
            ]

            if course_info['title']:
                embed.set_author(name=formatted_course_code)
                embed.title = course_info['title']

            if course_info['location']:
                footer_text = f"{course_info['location']} ({course_info['campus']})" if course_info['campus'] else f"{course_info['location']}"
                embed.set_footer(text=footer_text)

            embed.add_field(name=course_name, value="".join(course_details), inline=False)

        return embed


## DEV COMMANDS ## (These commands are only available to the bot owner)

    @checks.is_owner()
    @course.command()
    async def online(self, ctx, *, raw_course_code: str):
        """Gets course data from the McMaster API."""
        print(f"Debug: online start() - course_code: {raw_course_code}")
        # Format the course code
        result = self.format_course_code(raw_course_code)
        if not result:
            await ctx.send(f"Error: The course code {raw_course_code} is not valid. Please enter a valid course code.")
            return

        dept, code = result
        formatted_course_code = f"{dept}-{code}"
        
        course_data = await self.cache_handler.fetch_course_online(formatted_course_code)
        print(f"Debug: course_data: {course_data}") # Debug
        
        if course_data is None: # Course not found
            await ctx.send(f"Error: The course {formatted_course_code} was not found. Please enter a valid course code.")
            return
        
        # Format the course data
        soup, error_message = course_data

        if soup is not None:
            processed_course_data = self.cache_handler.process_soup_content(soup)  # Process the soup content
        else:
            await ctx.send(f"Error: {error_message}")
            return

        # Create the Discord embed and add fields with course data
        embed = self.create_course_embed(processed_course_data, formatted_course_code)
        await ctx.send(embed=embed)


    @checks.is_owner()
    @course.command()
    async def cache(self, ctx, *, raw_course_code: str):
        """Gets course data from the cache."""
        print(f"Debug: cache start() - course_code: {raw_course_code}")
        # Format the course code
        result = self.format_course_code(raw_course_code)
        if not result:
            await ctx.send(f"Error: The course code {raw_course_code} is not valid. Please enter a valid course code.")
            return

        dept, code = result
        formatted_course_code = f"{dept}-{code}"

        course_data = await self.cache_handler.fetch_course_cache(formatted_course_code)
        print(f"Debug: course_data: {course_data}") # Debug

        if course_data is None: # Course not found
            await ctx.send(f"Error: The course {formatted_course_code} was not found. Please enter a valid course code.")
            return
        
        # Format the course data
        soup, error_message = course_data

        if soup is not None:
            processed_course_data = self.cache_handler.process_soup_content(soup)
        else:
            await ctx.send(f"Error: {error_message}")
            return
        
        # Create the Discord embed and add fields with course data
        embed = self.create_course_embed(processed_course_data, formatted_course_code)
        await ctx.send(embed=embed)

    @checks.is_owner()
    @course.command()
    async def setterm(self, ctx, term_name: str, term_id: int):
        """
        Set the term code for the specified term.

        :param ctx: The command context.
        :param term_name: The term name (winter, spring, or fall).
        :param term_id: The term code.
        """
        term_name = term_name.lower()
        if term_name in ["winter", "spring", "fall"]:
            await self.cache_handler.term_codes(ctx, term_name, term_id)
        else:
            await ctx.send("Invalid term name. Use 'fall', 'winter', or 'spring' followed by the correct term id.")

#    @checks.is_owner()
#    @course.command()
#    async def mine(self, ctx):
#        """Displays the courses the user belongs to."""
#        courses = self.get_user_courses(ctx, ctx.guild, ctx.author)
#        if courses:
#            await ctx.send(f"{ctx.author.mention}, you are a member of the following courses:\n{', '.join(courses)}")
#        else:
#            await ctx.send(f"{ctx.author.mention}, you are not a member of any course.")

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