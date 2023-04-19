import re
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from collections import namedtuple
from datetime import datetime, time, timedelta, timezone
from math import floor
from typing import Dict, List, Optional, Tuple

from redbot.core import Config, commands, group, checks


class CourseManager(commands.Cog):
    """Cog for managing course data."""

    CACHE_STALE_DAYS = 120
    CACHE_EXPIRY_DAYS = 240
    TERM_NAMES = ["winter", "spring", "fall"]
    URL_BASE = "https://mytimetable.mcmaster.ca/getclassdata.jsp?term={term}&course_0_0={course_str}&t={t}&e={e}"
    CacheCheckResult = namedtuple("CacheCheckResult", ["course_data", "is_stale"])

    def __init__(self, bot):
        """Initialize the CourseManager class."""
        self.bot = bot
        self.config = Config.get_conf(
            self.bot, identifier=3720194665, force_registration=True
        )
        self.config.register_global(courses={}, term_codes={})
        self.session = ClientSession()

    async def cog_unload(self):
        """Close the aiohttp session when the cog is unloaded."""
        self.bot.loop.create_task(self.close_session())

    async def close_session(self):
        """Close the aiohttp session."""
        await self.session.close()

    async def get_course_data(
        self, course_str: str, ctx=None
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """Fetch course data from the cache, if available. Otherwise, fetch from the online source."""
        course_data, is_stale = await self.check_course_cache(course_str)

        if course_data is None or is_stale:
            fetched_course_data = await self.fetch_and_process_course_data(
                course_str, ctx
            )

            if fetched_course_data:
                await self.update_cache(course_str, fetched_course_data)
                course_data = fetched_course_data

        if is_stale:
            course_data_note = {
                "note": "The returned data may be out of date as it is older than 4 months and could not be updated from the online source."
            }
            if isinstance(course_data, list):
                course_data.append(course_data_note)
            else:
                course_data = [course_data, course_data_note["note"]]

        if not isinstance(course_data, list):
            # If course_data is not a list, return it as the soup object and None as the error message
            return course_data, None
        # Return the first item in the list as the soup object
        soup = course_data[0] if course_data else None
        # Return the second item in the list as the error message string
        error_message = course_data[1] if len(course_data) > 1 else None
        return soup, error_message

    async def fetch_and_process_course_data(self, course_str: str, ctx) -> list:
        """Fetch course data from the online source and process it."""
        soup, error_message = await self.fetch_course_online(course_str)

        if soup is not None:
            return self.process_soup_content(soup)
        if error_message is not None and ctx is not None:
            await ctx.send(f"Error: {error_message}")
        return []

    def current_term(self) -> str:
        """Determine the current term based on the current month."""
        now = datetime.now(timezone.utc)
        if 1 <= now.month <= 4:
            return self.TERM_NAMES[0]
        elif 5 <= now.month <= 8:
            return self.TERM_NAMES[1]
        else:
            return self.TERM_NAMES[2]

    def generate_time_code(self) -> Tuple[int, int]:
        """Generate a time code for use in the query."""
        t = floor(time() / 60) % 1000
        e = t % 3 + t % 39 + t % 42
        return t, e

    async def get_term_id(self, term_name: str) -> int:
        term_codes = await self.config.term_codes()
        return term_codes.get(term_name, None)


class CommandHandler:
    @commands.group(invoke_without_command=True)
    async def course(self, ctx):
        await ctx.send_help(self.course)

    @checks.is_owner()
    @course.command()
    async def online(self, ctx, *, raw_course_code: str):
        """Gets course data from the McMaster API."""
        print(f"Debug: online start() - course_code: {raw_course_code}")
        # Format the course code
        result = self.format_course_code(raw_course_code)
        if not result:
            await ctx.send(
                f"Error: The course code {raw_course_code} is not valid. Please enter a valid course code."
            )
            return

        dept, code = result
        formatted_course_code = f"{dept}-{code}"

        course_data = await self.cache_handler.fetch_course_online(
            formatted_course_code
        )
        print(f"Debug: course_data: {course_data}")  # Debug

        if course_data is None:  # Course not found
            await ctx.send(
                f"Error: The course {formatted_course_code} was not found. Please enter a valid course code."
            )
            return

        # Format the course data
        soup, error_message = course_data

        if soup is not None:
            processed_course_data = self.cache_handler.process_soup_content(
                soup
            )  # Process the soup content
        else:
            await ctx.send(f"Error: {error_message}")
            return

        # Create the Discord embed and add fields with course data
        embed = self.create_course_embed(processed_course_data, formatted_course_code)
        await ctx.send(embed=embed)


class CacheHandler:
    async def update_cache(self, course_str: str, course_data: list) -> None:
        """Update the course cache with the new course data."""
        course_key = course_str
        now = datetime.now(timezone.utc)
        expiry = (now + timedelta(days=self.CACHE_EXPIRY_DAYS)).isoformat()
        async with self.config.courses() as courses:
            courses[course_key] = {"expiry": expiry, "data": course_data}

    async def check_course_cache(
        self, course_str: str
    ) -> Tuple[Optional[BeautifulSoup], bool]:
        """Check if the course data is in the cache and if it is still valid."""
        courses = await self.config.courses()
        course_key = course_str

        if course_key in courses:
            expiry = datetime.fromisoformat(courses[course_key]["expiry"])
            stale_time = expiry - timedelta(days=self.CACHE_STALE_DAYS)
            now = datetime.now(timezone.utc)
            if now < expiry:
                return courses[course_key]["data"], now >= stale_time
            del courses[course_key]
            await self.config.courses.set(courses)

        return None, False


class DataScraper:
    async def fetch_course_online(
        self, course_str: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """
        Fetch course data from the online source.

        :param course_str: The formatted course string.
        :return: A tuple with a BeautifulSoup object containing the course data,
        or None if there was an error, and an error message string, or None if there was no error.
        """
        term_name = self.current_term()
        term_id = await self.get_term_id(term_name)

        if term_id is None:
            return (
                None,
                f"Error: Term code for {term_name.capitalize()} has not been set.",
            )

        t, e = self.generate_time_code()
        url = self.URL_BASE.format(term=term_id, course_str=course_str, t=t, e=e)

        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    return (
                        None,
                        f"Error: Unable to fetch course data (HTTP {response.status})",
                    )
                content = await response.text()
                soup = BeautifulSoup(content, "html.parser")
                return soup, None
        except Exception as e:
            return None, f"Error: Exception occurred while fetching course data: {e}"

    def create_course_info(self) -> Dict[str, str]:
        """
        Create an empty course info dictionary.

        :return: An empty course info dictionary.
        """
        return {
            "teacher": "",
            "location": "",
            "campus": "",
            "courseKey": "",
            "prerequisites": "",
            "antirequisites": "",
            "notes": "",
            "term_found": "",
            "description": "",
            "title": "",
            "type": "",
        }

    def process_soup_content(self, soup: BeautifulSoup) -> List[Dict]:
        """
        Process the BeautifulSoup content to extract course data.

        :param soup: BeautifulSoup object containing the course data.
        :return: A list of dictionaries containing the processed course data.
        """
        course_data = []

        for course in soup.find_all("course"):
            course_info = self.create_course_info()
            if offering := course.find("offering"):
                course_info["title"] = offering["title"]
                course_info["courseKey"] = offering["key"]
                desc = offering.get("desc", "")

                prereq_info = re.findall(
                    r"Prerequisite"
                    + re.escape("(s):")
                    + r"(.+?)(Antirequisite"
                    + re.escape("(s):")
                    + r"|Not open to|$)",
                    desc,
                )

                course_info["prerequisites"] = (
                    prereq_info[0][0].strip() if prereq_info else ""
                )

                antireq_info = re.findall(
                    r"Antirequisite" + re.escape("(s):") + r"(.+?)(Not open to|$)", desc
                )
                course_info["antirequisites"] = (
                    antireq_info[0][0].strip() if antireq_info else ""
                )

                course_info["description"] = re.sub(
                    r"Prerequisite"
                    + re.escape("(s):")
                    + r"(.+?)(Antirequisite"
                    + re.escape("(s):")
                    + r"|Not open to|$)",
                    "",
                    desc,
                ).strip()

            term_elem = course.find("term")
            term_found = term_elem.get("v") if term_elem else ""
            course_info["term_found"] = term_found

            block = course.find("block")
            course_info["type"] = block.get("type", "") if block else ""
            course_info["teacher"] = block.get("teacher", "") if block else ""
            course_info["location"] = block.get("location", "") if block else ""
            course_info["campus"] = block.get("campus", "") if block else ""
            course_info["notes"] = block.get("n", "") if block else ""

            course_data.append(course_info)

        for course in course_data:
            course.update(
                {
                    key: value.replace("<br/>", "\n").replace("_", " ")
                    for key, value in course.items()
                }
            )

        return course_data


class ErrorHandler(commands.CommandError):
    pass


def setup(bot):
    bot.add_cog(CourseManager(bot))


class DevCommands(commands.ownercommand):
    @checks.is_owner()
    @commands.group(invoke_without_command=True)
    async def dev_course(self, ctx):
        await ctx.send_help(self.course)

    @dev_course.command()
    async def set_term_codes(self, ctx, term_name: str, term_id: int):
        """Set the term code for the specified term."""
        async with self.config.term_codes() as term_codes:
            term_codes[term_name] = term_id
        await ctx.send(
            f"Term code for {term_name.capitalize()} has been set to: {term_id}"
        )

    @dev_course.command()
    async def set_log(self, ctx, option: str, channel: commands.TextChannelConverter):
        """Sets logging channel for the cog."""
        if option.lower() == "logging":
            await self.config.logging_channel.set(channel.id)
            await ctx.send(f"Logging channel set to {channel}.")
            return

        await ctx.send(
            "Invalid option. Use '=course setlog logging' followed by the channel."
        )

    @checks.is_owner()
    @dev_course.command()
    async def mine(self, ctx):
        """Displays the courses the user belongs to."""
        if courses := self.get_user_courses(ctx, ctx.guild, ctx.author):
            await ctx.send(
                f"{ctx.author.mention}, you are a member of the following courses:\n{', '.join(courses)}"
            )
        else:
            await ctx.send(f"{ctx.author.mention}, you are not a member of any course.")

    @checks.admin()
    @dev_course.command()
    @commands.cooldown(1, 60, commands.BucketType.user)
    async def delete(self, ctx, *, channel: commands.TextChannel):
        """Deletes a course channel."""
        if not channel.category or channel.category.name != self.category_name:
            await ctx.send(f"Error: {channel} is not a course channel.")
            return
        await channel.delete()
        await ctx.send(f"{channel.name} has been successfully deleted.")
        if self.logging_channel:
            await self.logging_channel.send(f"{channel} has been deleted.")


class AdultFriendFinder:
    """Find people you share a class with"""

    pass


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


## HELPER FUNCTIONS ##

#    async def course_exists(self, course_code):
#        """Checks if the course exists in the cache or online."""
#        print ("Debug: course_exists()")
#        return await self.cache_handler.course_code_exists(course_code)
