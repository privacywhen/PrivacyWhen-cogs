import re
import aiohttp
import discord
from aiohttp import ClientSession
from bs4 import BeautifulSoup, Tag
from datetime import datetime
from math import floor
from typing import Dict, List, Optional, Tuple
from time import time

from redbot.core import Config, commands, checks
from redbot.core.utils import AsyncIter

import asyncio


class CourseDataProxy:
    _CACHE_STALE_DAYS = 120
    _CACHE_EXPIRY_DAYS = 240
    _TERM_NAMES = ["winter", "spring", "fall"]
    _URL_BASE = "https://mytimetable.mcmaster.ca/getclassdata.jsp?term={term}&course_0_0={course_str}&t={t}&e={e}"

    def __init__(self, session: ClientSession, config: Config):
        self.session = session
        self.config = config

    ## CACHE MANAGEMENT: Maintains the freshness of the data in the proxy.
    async def _maintain_freshness(self):
        async for course_str, course_data in AsyncIter(
            await self.config.courses.all(), delay=2, steps=1
        ):
            data_age_days = (datetime.now - course_data["date_added"]).days
            if data_age_days > self._CACHE_STALE_DAYS:
                course_data["is_fresh"] = False
                await self._web_updater(course_str)
            if data_age_days > self._CACHE_EXPIRY_DAYS:
                await self.config.courses().pop(course_str)
        print(
            f"DEBUG: Maintaining freshness for {course_str}, data_age_days: {data_age_days}"
        )

    async def find_course(self, course_str: str) -> Optional[Dict[str, str]]:
        """
        Find the course data in the proxy or update it if needed.

        Args:
            course_str (str): The course identifier.

        Returns:
            dict: The course data and its freshness status or 'Not Found' if the course is not found.

        Note: This function should not call _maintain_freshness() as it is intended to run on a schedule.
        """
        course_data = (await self.config.courses()).get(course_str, None)

        if course_data is None:
            await self._web_updater(course_str)
            course_data = self.config.courses().get(course_str, "Not Found")
            if course_data != "Not Found":
                print(f"Course data found in config.courses: {course_data}")
            else:
                print("Course not found in config.courses after calling web updater.")
        else:
            print(f"Course data found in config.courses: {course_data}")

        return course_data["course_data"] if course_data != "Not Found" else course_data

    ## Section - WEB UPDATE: Fetches course data from the online sourse. Requires term_id, course_str, t, and e.

    async def _web_updater(self, course_str: str):
        """
        Fetch course data from the online source and process it into a dictionary.

        :param course_str: The formatted course string.
        :return: A dictionary containing the course data.
        """
        soup, error = await self._fetch_course_online(course_str)
        if soup is not None:
            course_data_processed = self._process_soup_content(soup)
            await self.config.courses().set(
                course_str,
                {
                    "course_data": course_data_processed,
                    "date_added": datetime.now().strftime("%Y %b %d"),
                    "is_fresh": True,
                },
            )
            return course_data_processed

        elif error is not None:
            print(f"Error fetching course data for {course_str}: {error}")
            return None

    def _current_term(self) -> str:
        """Determine the current term based on the current month."""
        now = datetime.now()
        return self._TERM_NAMES[(now.month - 1) // 4]

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
        :return: A tuple containing the BeautifulSoup object and an error message, if any.
        """
        current_term = self._current_term()
        term_order = (
            self._TERM_NAMES[self._TERM_NAMES.index(current_term) :]
            + self._TERM_NAMES[: self._TERM_NAMES.index(current_term)]
        )

        soup = None
        error_message = None

        for term_name in term_order:
            term_id = await self._get_term_id(term_name)
            if term_id is None:
                continue

            t, e = self._generate_time_code()
            url = self._URL_BASE.format(term=term_id, course_str=course_str, t=t, e=e)

            try:
                async with self.session.get(url) as response:
                    print(f"DEBUG: Fetching course data from {url}")
                    if response.status != 200:
                        continue
                    content = await response.text()
                    soup = BeautifulSoup(content, "xml")
                    error_tag = soup.find("error")

                    if error_tag is not None:
                        error_message = error_tag.text
                        continue  # Continue to the next term_id if there is an error

                    break  # No error, break the loop
            except aiohttp.ClientResponseError as error:
                print(f"Error fetching course data: {error}")
                error_message = (
                    "Error: An issue occurred while fetching the course data."
                )
                break
            except aiohttp.ClientConnectionError as error:
                print(f"Error connecting to server: {error}")
                error_message = (
                    "Error: An issue occurred while connecting to the server."
                )
                break

        return soup, error_message

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

        course_code = course["code"]
        course_number = course["number"]

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
            "course_code": course_code,
            "course_number": course_number,
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
        print(f"DEBUG: _process_soup_content: {course_data}")
        return course_data


class CourseManager(commands.Cog):
    """Cog for managing course data."""

    def __init__(self, bot):
        """Initialize the CourseManager class."""
        self.bot = bot
        self.session = ClientSession()
        self.config = Config.get_conf(
            self.bot, identifier=3720194665, force_registration=True
        )
        self.config.register_global(courses={}, term_codes={})
        self.course_data_proxy = CourseDataProxy(self.session, self.config)
        self.bot.loop.create_task(self.maintain_freshness())

    async def maintain_freshness(self):
        """Maintain the freshness of the course data."""
        while True:
            print("DEBUG: Starting maintain_freshness loop")
            await self.course_data_proxy._maintain_freshness()
            await asyncio.sleep(24 * 60 * 60)  # sleep for 24 hours

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
        print(f"DEBUG: Course parts: {course_parts}")

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

        return department, course_number

    async def send_long_message(self, ctx, content, max_length=2000):
        while content:
            message_chunk = content[:max_length]
            await ctx.send(message_chunk)
            content = content[max_length:]

    def create_course_embed(self, course_data):
        if course_data == "Not Found":
            return discord.Embed(
                title="Course not found",
                description="No data available for this course.",
                color=0xFF0000,
            )
        print(f"DEBUG: course_data: {course_data}")
        if len(course_data["course_data"]) > 0:
            course_code = course_data["course_data"][0]["course_code"]
            course_number = course_data["course_data"][0]["course_number"]
        embed = discord.Embed(title=f"{course_code} {course_number}", color=0x00FF00)

        field_info = [
            ("teacher", "Teacher"),
            ("term_found", "Term"),
            ("description", "Description"),
            ("notes", "Notes"),
            ("prerequisites", "Prerequisites"),
            ("antirequisites", "Antirequisites"),
        ]

        for course_info in course_data["course_data"]:
            course_name = f"{course_info['course_code']} {course_info['course_number']}"

            print(f"DEBUG: Creating embed for course_name: {course_name}")

            course_details = [
                f"**{label}**: {course_info[field]}\n" if course_info[field] else ""
                for field, label in field_info
            ]

            if course_info["title"]:
                embed.set_author(name=course_name)
                embed.title = course_info["title"]

            freshness_icon = "🟢" if course_data.get("is_fresh") else "🔴"

            date_added = course_data.get("date_added")
            date_added_str = date_added or "Unknown"

            footer_text = f"{freshness_icon} Last Updated: {date_added_str}"
            embed.set_footer(text=footer_text)

            embed.add_field(
                name=course_name, value="".join(course_details), inline=False
            )

        return embed

    ### create a revised version of create_course_embed() that uses the new course_data format and freshness data

    ### User Command Section

    @commands.group(invoke_without_command=True)
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def course(self, ctx):
        await ctx.send_help(self.course)

    @course.command(name="details")
    async def course_details(self, ctx, *, course_code: str):
        """Get the details of a course."""
        formatted_course_code = self.format_course_code(course_code)
        if not formatted_course_code:
            await ctx.send(
                f"Invalid course code: {course_code}. Please use the format: `department course_number`"
            )
            return

        course_data = await self.course_data_proxy.find_course(formatted_course_code)
        print(f"DEBUG: Course data for {formatted_course_code}: {course_data}")

        if not course_data:
            await ctx.send(f"Course not found: {formatted_course_code}")
            return

        embed = self.create_course_embed(course_data)
        await ctx.send(embed=embed)

    ### Dev Command Section

    @checks.is_owner()
    @commands.group(name="dc", invoke_without_command=True)
    async def dev_course(self, ctx):
        """Developer commands for the course cog."""
        await ctx.send_help(self.course)

    @dev_course.command(name="term")
    async def set_term_codes(self, ctx, term_name: str, term_id: int):
        """Set the term code for the specified term."""
        async with self.config.term_codes() as term_codes:
            term_codes[term_name] = term_id
        await ctx.send(
            f"Term code for {term_name.capitalize()} has been set to: {term_id}"
        )

    @dev_course.command(name="log")
    async def set_log(self, ctx, option: str, channel: discord.TextChannel):
        """Sets logging channel for the cog."""
        if option.lower() == "logging":
            await self.config.logging_channel.set(channel.id)
            await ctx.send(f"Logging channel set to {channel}.")
            return

        await ctx.send(
            "Invalid option. Use '=course setlog logging' followed by the channel."
        )

    @dev_course.command(name="clearall")
    async def clear_all(self, ctx):
        """Clear all config data."""
        await self.config.clear_all()
        await ctx.send("All config data cleared.")

    ### create a command that prints the entire config to the console.
    @dev_course.command(name="printconfig")
    async def print_config(self, ctx):
        config = await self.config.all()
        print(config)

    ### create a command that prints the entire guild config to the console.
    @dev_course.command(name="printguildconfig")
    async def print_guild_config(self, ctx):
        guild_config = await self.config.guild(ctx.guild).all()
        print(guild_config)

    ### create a command that tests bypassing the cache and getting the latest data from the API and returning it as an embed. It should use _fetch_course_online(), _process_course_data(), and create_course_embed() to do this. Ignore private method indicators for now.

    @dev_course.command(name="onlineembed")
    async def test_online_embed(self, ctx, *, course_code: str):
        """Find a course by its course code."""
        formatted_course_code = self.format_course_code(course_code)
        if not formatted_course_code:
            await ctx.send("Invalid course code.")
            return

        department, course_number = formatted_course_code
        course_data = await self._fetch_course_online(department, course_number)
        course_data = await self._process_course_data(course_data)
        embed = self.create_course_embed(course_data, formatted_course_code)
        await ctx.send(embed=embed)

    ### create a command that tests _current_term() and term_codes() and returns the current term code and the term codes dict
    @dev_course.command(name="testterm")
    async def test_term(self, ctx):
        """Test _current_term() and term_codes()"""
        current_term = await self._current_term()
        term_codes = await self.config.term_codes()
        await ctx.send(f"Current term: {current_term}\nTerm codes: {term_codes}")

    ### create a command that tests the proxy's _process_course_data() method and returns the result
    @dev_course.command(name="testprocess")
    async def test_process(self, ctx, *, course_code: str):
        """Test the proxy's _process_course_data() method"""
        formatted_course_code = self.format_course_code(course_code)
        if not formatted_course_code:
            await ctx.send("Invalid course code.")
            return

        department, course_number = formatted_course_code
        course_data = await self.course_data_proxy._fetch_course_online(
            department, course_number
        )
        course_data = await self.course_data_proxy._process_course_data(course_data)
        await ctx.send(course_data)

    ### create a command that tests the proxy's _fetch_course_online() method and returns the result
    @dev_course.command(name="testfetch")
    async def test_fetch(self, ctx, *, course_code: str):
        """Test the proxy's _fetch_course_online() method"""
        formatted_course_code = self.format_course_code(course_code)
        if not formatted_course_code:
            await ctx.send("Invalid course code.")
            return

        department, course_number = formatted_course_code
        course_data = await self.course_data_proxy._fetch_course_online(
            department, course_number
        )
        await ctx.send(course_data)

    ### create a command that tests the proxy's freshness functionality and returns the result
    @dev_course.command(name="testfresh")
    async def test_fresh(self, ctx, *, course_code: str):
        """Test the proxy's freshness functionality"""
        formatted_course_code = self.format_course_code(course_code)
        if not formatted_course_code:
            await ctx.send("Invalid course code.")
            return

        department, course_number = formatted_course_code
        course_data = await self.course_data_proxy.find_course(
            department, course_number
        )
        await ctx.send(course_data)

    @dev_course.command(name="testsuite")
    async def testsuite(self, ctx, *, course_code: str):
        """Run a test suite for the CourseManager Cog."""

        # Test format_course_code()
        print("Testing format_course_code()")
        department, course_number = self.format_course_code(course_code)
        print(f"Formatted course code: {department} {course_number}")

        # Test _current_term()
        print("Testing _current_term()")
        current_term = self.course_data_proxy._current_term()
        print(f"Current term: {current_term}")

        # Test _get_term_id()
        print("Testing _get_term_id()")
        term_id = await self.course_data_proxy._get_term_id(current_term)
        print(f"Term ID for {current_term}: {term_id}")

        # Test _fetch_course_online()
        print("Testing _fetch_course_online()")
        soup, error = await self.course_data_proxy._fetch_course_online(course_code)
        if soup:
            print("Course data fetched successfully.")
        else:
            print(f"Error: {error}")

        # Test _process_soup_content()
        print("Testing _process_soup_content()")
        course_data = self.course_data_proxy._process_soup_content(soup)
        print(f"Course data: {course_data}")

        # Test find_course()
        print("Testing find_course()")
        course_data_found = await self.course_data_proxy.find_course(
            course_code
        )  # Updated with 'await'
        print(f"Found course data: {course_data_found}")

        # Test create_course_embed()
        print("Testing create_course_embed()")
        embed = self.create_course_embed(
            course_data_found, f"{department} {course_number}"
        )
        await ctx.send(embed=embed)

        # Test maintain_freshness() indirectly
        print(
            "Testing maintain_freshness() indirectly by checking the freshness status of the found course data"
        )
        is_fresh = course_data_found.get("is_fresh", None)
        freshness_status = "Fresh" if is_fresh else "Stale"
        print(f"Course data freshness status: {freshness_status}")
