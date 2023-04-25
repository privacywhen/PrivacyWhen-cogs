import re
import discord
from aiohttp import ClientSession
from bs4 import BeautifulSoup, Tag
from datetime import datetime, timedelta, timezone
from math import floor
from typing import Dict, List, Optional, Tuple
import time

from redbot.core import Config, commands, checks
from redbot.core.utils import AsyncIter

import functools
import asyncio


class CourseDataProxy:
    _CACHE_STALE_DAYS = 120
    _CACHE_EXPIRY_DAYS = 240
    _TERM_NAMES = ["winter", "spring", "fall"]
    _URL_BASE = "https://mytimetable.mcmaster.ca/getclassdata.jsp?term={term}&course_0_0={course_str}&t={t}&e={e}"

    def __init__(self, session: ClientSession):
        self.session = session

    ## CACHE MANAGEMENT

    async def _maintain_freshness(self):
        """
        Maintain the freshness of the data in the proxy by checking the date_added
        attribute of each course. If the data_age_days is greater than the stale_days,
        set is_fresh to False and call _web_updater. If data_age_days is greater than
        the expiry_days, remove the course from the proxy and call _web_updater.
        """
        async for course_str, course_data in AsyncIter(
            self._proxy.items(), delay=2, steps=1
        ):
            data_age_days = (datetime.now() - course_data["date_added"]).days
            if data_age_days > self._CACHE_STALE_DAYS:
                course_data["is_fresh"] = False
                await self._web_updater(course_str)
            if data_age_days > self._CACHE_EXPIRY_DAYS:
                self._proxy.pop(course_str)
                await self._web_updater(course_str)

    def find_course(self, course_str):
        """
        Find the course data in the proxy or update it if needed.

        Args:
            course_str (str): The course identifier.

        Returns:
            dict: The course data and its freshness status or 'Not Found' if the course is not found.

        Notes:
        Do not call _maintain freshness here because it meant to run on interval.
        """
        course_data = self._proxy.get(course_str, None)
        if course_data is None:
            self._web_updater(course_str)
            course_data = self._proxy.get(course_str, "Not Found")
        return course_data

    ## Section - WEB UPDATE: Fetches course data from the online sourse. Requires term_id, course_str, t, and e.

    async def _web_updater(self, course_str):
        """
        Fetch course data from the online source and process it into a dictionary.

        :param course_str: The formatted course string.
        :return: A dictionary containing the course data.
        """
        soup, error = await self._fetch_course_online(course_str)
        if soup is not None:
            course_data_processed = self._process_soup_content(soup)
            self._proxy[course_str] = {
                "course_data": course_data_processed,
                "date_added": datetime.now(),
                "is_fresh": True,
            }
        elif error is not None:
            print(f"Error fetching course data for {course_str}: {error}")

    def _current_term(self) -> str:
        """Determine the current term based on the current month."""
        now = datetime.now(timezone.utc)
        if 1 <= now.month <= 4:
            return self._TERM_NAMES[0]
        elif 5 <= now.month <= 8:
            return self._TERM_NAMES[1]
        else:
            return self._TERM_NAMES[2]

    async def _get_term_id(self, term_name: str) -> int:
        term_codes = await self.config.term_codes()
        return term_codes.get(term_name, None)

    def _generate_time_code(self) -> Tuple[int, int]:
        """Generate a time code for use in the query."""
        t = floor(time() / 60) % 1000
        e = t % 3 + t % 39 + t % 42
        return t, e

    async def _fetch_course_online(
        self, course_str: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """
        Fetch course data from the online source.

        :param course_str: The formatted course string.
        :return: A tuple with a BeautifulSoup object containing the course data,
        or None if there was an error, and an error message string, or None if there was no error.
        """
        term_name = self._current_term()
        term_id = await self._get_term_id(term_name)

        if term_id is None:
            return (
                None,
                f"Error: Term code for {term_name.capitalize()} has not been set.",
            )

        t, e = self._generate_time_code()
        url = self._URL_BASE.format(term=term_id, course_str=course_str, t=t, e=e)

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

    ## COURSE DATA PROCESSING: Processes the course data from the online source into a dictionary.

    def _extract_prereq_antireq(self, description: str) -> Tuple[str, str]:
        """
        Extract prerequisites and antirequisites from the description.

        :param description: The course description containing prerequisites and antirequisites.
        :return: A tuple with prerequisites and antirequisites.
        """
        prereq_info = re.findall(
            r"Prerequisite"
            + re.escape("(s):")
            + r"(.+?)(Antirequisite"
            + re.escape("(s):")
            + r"|Not open to|$)",
            description,
        )

        antireq_info = re.findall(
            r"Antirequisite" + re.escape("(s):") + r"(.+?)(Not open to|$)",
            description,
        )

        return (
            prereq_info[0][0].strip() if prereq_info else "",
            antireq_info[0][0].strip() if antireq_info else "",
        )

    def _extract_course_details(self, course: Tag, offering: Tag) -> Dict[str, str]:
        """
        Extract course details from the course and offering tags.

        :param course: BeautifulSoup Tag object containing course information.
        :param offering: BeautifulSoup Tag object containing offering information.
        :return: A dictionary with the extracted course details.
        """
        term_elem = course.find("term")
        block = course.find("block")

        prerequisites, antirequisites = self._extract_prereq_antireq(
            offering.get("desc", "")
        )

        return {
            "title": offering["title"],
            "courseKey": offering["key"],
            "description": re.sub(
                r"Prerequisite"
                + re.escape("(s):")
                + r"(.+?)(Antirequisite"
                + re.escape("(s):")
                + r"|Not open to|$)",
                "",
                offering.get("desc", ""),
            ).strip(),
            "prerequisites": prerequisites,
            "antirequisites": antirequisites,
            "term_found": term_elem.get("v") if term_elem else "",
            "type": block.get("type", "") if block else "",
            "teacher": block.get("teacher", "") if block else "",
            "location": block.get("location", "") if block else "",
            "campus": block.get("campus", "") if block else "",
            "notes": block.get("n", "") if block else "",
        }

    def _process_soup_content(self, soup: BeautifulSoup) -> List[Dict]:
        """
        Process the BeautifulSoup content to extract course data.

        :param soup: BeautifulSoup object containing the course data.
        :return: A list of dictionaries containing the processed course data.
        """
        course_data = []

        for course in soup.find_all("course"):
            offering = course.find("offering")
            course_details = self._extract_course_details(course, offering)
            course_data.append(course_details)

        for course in course_data:
            course.update(
                {
                    key: value.replace("<br/>", "\n").replace("_", " ")
                    for key, value in course.items()
                }
            )

        return course_data


class CourseManager(commands.Cog):
    """Cog for managing course data."""

    def __init__(self, bot):
        """Initialize the CourseManager class."""
        self.bot = bot
        self.config = Config.get_conf(
            self.bot, identifier=3720194665, force_registration=True
        )
        self.config.register_global(courses={}, term_codes={})
        self.session = ClientSession()
        self.course_data_proxy = CourseDataProxy()
        self.bot.loop.create_task(self.maintain_freshness())

    async def maintain_freshness(self):
        while True:
            await self.course_data_proxy._maintain_freshness()
            await asyncio.sleep(24 * 60 * 60)  # sleep for 24 hours

    async def _log(self, message: str):
        """Log a message to the logging channel if it is set."""
        logging_channel_id = await self.config.logging_channel()
        if logging_channel_id:
            if logging_channel := self.bot.get_channel(logging_channel_id):
                await logging_channel.send(message)

    def log(func):
        """A decorator to log function calls with their arguments."""

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            cls_instance = args[0]
            await cls_instance._log(
                f"Calling {func.__name__} with args: {args[1:]}, kwargs: {kwargs}"
            )
            return await func(*args, **kwargs)

        return wrapper

    ### Helper Functions
    def format_course_code(self, course_code: str) -> Optional[Tuple[str, str]]:
        print(f"Debug: format_course_code() - course_code: {course_code}")
        # Convert to uppercase and replace hyphens and underscores with spaces
        course_code = course_code.upper().replace("-", " ").replace("_", " ")
        print(
            f"Debug: course_code after replacing hyphens and underscores: {course_code}"
        )
        # Split by whitespace characters
        course_parts = re.split(r"\s+", course_code.strip())

        if len(course_parts) < 2:
            return None
        elif len(course_parts) > 2:
            course_number = " ".join(course_parts[1:])
        else:
            course_number = course_parts[1]

        department = course_parts[0]
        print(f"Debug: department: {department}, course_number: {course_number}")

        # Validate the department and course number for valid characters
        department_pattern = re.compile(r"^[A-Z]+$")
        course_number_pattern = re.compile(r"^(\d[0-9A-Za-z]{1,3}).*")

        department_match = department_pattern.match(department)
        course_number_match = course_number_pattern.match(course_number)

        if not department_match or not course_number_match:
            return None

        # Remove any unwanted characters after the course_number
        course_number = course_number_match[1]
        print(
            f"Debug: course_number after removing unwanted characters: {course_number}"
        )

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

        field_info = [
            ("teacher", "Teacher"),
            ("term_found", "Term"),
            ("description", "Description"),
            ("notes", "Notes"),
            ("prerequisites", "Prerequisites"),
            ("antirequisites", "Antirequisites"),
        ]

        for course_info in course_data:
            course_name = f"{course_info['course']} {course_info['section']}"

            course_details = [
                f"**{label}**: {course_info[field]}\n" if course_info[field] else ""
                for field, label in field_info
            ]

            if course_info["title"]:
                embed.set_author(name=formatted_course_code)
                embed.title = course_info["title"]

            if course_info["location"]:
                footer_text = (
                    f"{course_info['location']} ({course_info['campus']})"
                    if course_info["campus"]
                    else f"{course_info['location']}"
                )
                embed.set_footer(text=footer_text)

            embed.add_field(
                name=course_name, value="".join(course_details), inline=False
            )

        return embed

    ### User Command Section

    @commands.group(invoke_without_command=True)
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def course(self, ctx):
        await ctx.send_help(self.course)

    ### Dev Command Section

    @checks.is_owner()
    @commands.group(invoke_without_command=True)
    async def dev_course(self, ctx):
        """Developer commands for the course cog."""
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
    async def set_log(self, ctx, option: str, channel: discord.TextChannel):
        """Sets logging channel for the cog."""
        if option.lower() == "logging":
            await self.config.logging_channel.set(channel.id)
            await ctx.send(f"Logging channel set to {channel}.")
            return

        await ctx.send(
            "Invalid option. Use '=course setlog logging' followed by the channel."
        )

    @dev_course.command()
    async def mine(self, ctx):
        """Displays the courses the user belongs to."""
        if courses := self.get_user_courses(ctx, ctx.guild, ctx.author):
            await ctx.send(
                f"{ctx.author.mention}, you are a member of the following courses:\n{', '.join(courses)}"
            )
        else:
            await ctx.send(f"{ctx.author.mention}, you are not a member of any course.")

    @dev_course.command()
    async def delete(self, ctx, *, channel: discord.TextChannel):
        """Deletes a course channel."""
        if not channel.category or channel.category.name != self.category_name:
            await ctx.send(f"Error: {channel} is not a course channel.")
            return
        await channel.delete()
        await ctx.send(f"{channel.name} has been successfully deleted.")
        if self.logging_channel:
            await self.logging_channel.send(f"{channel} has been deleted.")

    @dev_course.command()
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
