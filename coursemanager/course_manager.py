import re
import aiohttp
import discord
from aiohttp import ClientSession
from bs4 import BeautifulSoup, Tag
from datetime import date
from math import floor
from typing import Dict, List, Optional, Tuple, Any
from time import time

from redbot.core import Config, commands, checks

# from redbot.core.utils import AsyncIter

import asyncio


class CourseDataProxy:
    _CACHE_STALE_DAYS = 120
    _CACHE_EXPIRY_DAYS = 240
    _TERM_NAMES = ["winter", "spring", "fall"]
    _URL_BASE = "https://mytimetable.mcmaster.ca/getclassdata.jsp?term={term}&course_0_0={course_key_formatted}&t={t}&e={e}"

    def __init__(self, session: ClientSession, config: Config):
        self.session = session
        self.config = config

    ## CACHE MANAGEMENT: Maintains the freshness of the data in the proxy.
    async def _maintain_freshness(self):
        courses = await self.config.courses()
        for course_key_formatted, course_data in courses.items():
            data_age_days = (
                date.today() - date.fromisoformat(course_data["date_added"])
            ).days

            # Check if the data is expired and remove it
            if data_age_days > self._CACHE_EXPIRY_DAYS:
                await self.config.courses.pop(course_key_formatted)
            elif data_age_days > self._CACHE_STALE_DAYS and not course_data["is_fresh"]:
                # Fetch fresh data using the get_course_data method
                await self.get_course_data(course_key_formatted)

        print(
            f"DEBUG: Maintaining freshness for {course_key_formatted}, data_age_days: {data_age_days}"
        )

    async def get_course_data(self, course_key_formatted: str) -> Dict[str, Any]:
        """
        Get the course data from the cache or update it if needed.

        Args:
            course_key_formatted (str): The course identifier.

        Returns:
            dict: The course data or an empty dictionary if the course is not found.

        Note: This function should not call _maintain_freshness() as it is intended to run on a schedule.
        """
        courses = await self.config.courses()
        course_data = courses.get(course_key_formatted, None)

        if course_data is None or not course_data.get("is_fresh", False):
            soup, error = await self._fetch_course_online(course_key_formatted)
            if soup is not None:
                course_data_processed = self._process_soup_content(soup)

                await self.config.courses.set_raw(
                    course_key_formatted,
                    value={
                        "course_data": course_data_processed,
                        "date_added": date.today().isoformat(),
                        "is_fresh": True,
                    },
                )

                courses = await self.config.courses()

            elif error is not None:
                print(f"Error fetching course data for {course_key_formatted}: {error}")
                return {}

        return (
            courses[course_key_formatted]["course_data"]
            if course_key_formatted in courses
            else {}
        )

    ## Section - WEB UPDATE: Fetches course data from the online sourse. Requires term_id, course_key_formatted, t, and e.

    def _current_term(self) -> str:
        """Determine the current term based on the current month."""
        now = date.today()
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
        self, course_key_formatted: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """
        Fetch course data from the online source.
        :param course_key_formatted: The formatted course string.
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
            url = self._URL_BASE.format(
                term=term_id, course_key_formatted=course_key_formatted, t=t, e=e
            )

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
        course_key_extracted = course["key"]
        course_code = course["code"]
        course_number = course["number"]

        return {
            "title": offering["title"],
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
            "course_key_extracted": course_key_extracted,
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
    def _split_course_key_raw(self, course_key_raw) -> Tuple[str, str]:
        course_key_raw = re.sub(r"[-_]", " ", course_key_raw.upper())
        course_parts = re.split(r"\s+", course_key_raw.strip())
        course_code, course_number = course_parts[0], " ".join(course_parts[1:])
        return course_code, course_number

    def _validate_course_key(
        self, course_code: str, course_number: str
    ) -> Optional[Tuple[str, str]]:
        if not (
            re.match(r"^[A-Z]+$", course_code)
            and re.match(r"^(\d[\w]{1,3})", course_number)
        ):
            return None

        course_number = re.match(r"^(\d[\w]{1,3})", course_number)[1]
        return course_code, course_number

    def _format_course_key(self, course_key_raw) -> Optional[str]:
        course_code, course_number = self._split_course_key_raw(course_key_raw)
        validated_course_key = self._validate_course_key(course_code, course_number)

        if validated_course_key is None:
            return None

        return f"{validated_course_key[0]} {validated_course_key[1]}"

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

        if not course_data or not course_data.get("course_data"):
            return None

        course_key = course_data["course_data"][0]["course_key_extracted"]
        embed = discord.Embed(title=course_key, color=0x00FF00)

        field_info = [
            ("teacher", "Teacher"),
            ("term_found", "Term"),
            ("description", "Description"),
            ("notes", "Notes"),
            ("prerequisites", "Prerequisites"),
            ("antirequisites", "Antirequisites"),
        ]

        for course_info in course_data["course_data"]:
            course_details = [
                f"**{label}**: {course_info[field]}\n" if course_info[field] else ""
                for field, label in field_info
            ]

            if course_info["title"]:
                embed.set_author(name=course_key)
                embed.title = course_info["title"]

            freshness_icon = "🟢" if course_data.get("is_fresh") else "🔴"
            date_added = course_data.get("date_added")
            date_added_str = date_added or "Unknown"
            footer_text = f"{freshness_icon} Last Updated: {date_added_str}"
            embed.set_footer(text=footer_text)

            embed.add_field(
                name=course_key, value="".join(course_details), inline=False
            )

        return embed

    ### create a revised version of create_course_embed() that uses the new course_data format and freshness data

    ### User Command Section

    @commands.group(invoke_without_command=True)
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def course(self, ctx):
        await ctx.send_help(self.course)

    @course.command(name="details")
    async def course_details(self, ctx, *, course_key_raw: str):
        """Get the details of a course."""
        course_key_formatted = self._format_course_key(course_key_raw)
        if not course_key_formatted:
            await ctx.send(
                f"Invalid course code: {course_key_raw}. Please use the format: `course_code course_number`"
            )
            return

        course_data = await self.course_data_proxy.get_course_data(course_key_formatted)
        print(f"DEBUG: Course data for {course_key_formatted}: {course_data}")

        if not course_data:
            await ctx.send(f"Course not found: {course_key_formatted}")
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

    ## Create a command that will print the global config data to console
    @dev_course.command(name="printconfig")
    async def print_config(self, ctx):
        """Prints the global config data to console"""
        print(await self.config.all())
