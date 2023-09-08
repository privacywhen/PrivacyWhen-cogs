import asyncio
import re
from datetime import date
from math import floor
from typing import Dict, List, Optional, Tuple, Any

import discord
import logging
from aiohttp import (
    ClientSession,
    ClientTimeout,
    ClientConnectionError,
    ClientResponseError,
)
from bs4 import BeautifulSoup, Tag
from time import time
from redbot.core import Config, commands, checks
from discord.ext import commands as discord_commands
from redbot.core.utils import bounded_gather, AsyncIter
from redbot.core.utils.menus import DEFAULT_CONTROLS, menu
from redbot.core.utils.chat_formatting import humanize_list

# from .error_handler import ErrorHandler
from .faculty_dictionary import FACULTIES

log = logging.getLogger("red.course_manager")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())


class CourseDataProxy:
    _CACHE_STALE_DAYS = 120
    _CACHE_EXPIRY_DAYS = 240
    _TERM_NAMES = ["winter", "spring", "fall"]
    _URL_BASE = "https://mytimetable.mcmaster.ca/getclassdata.jsp?term={term}&course_0_0={course_key_formatted}&t={t}&e={e}"

    def __init__(self, config: Config):
        self.config = config

    ## CACHE MANAGEMENT: Maintains the freshness of the data in the proxy.
    async def _maintain_freshness(self):
        """Maintain the freshness of the data in the proxy."""
        course_key_formatted = None
        data_age_days = None
        courses = await self.config.courses()
        log.debug("Before API call: courses = await self.config.courses()")
        for course_key_formatted, course_data in courses.items():
            data_age_days = (
                date.today() - date.fromisoformat(course_data["date_added"])
            ).days

            if data_age_days > self._CACHE_EXPIRY_DAYS:
                await self.config.courses.pop(course_key_formatted)
                log.debug(
                    "Before API call: await self.config.courses.pop(course_key_formatted)"
                )
            elif data_age_days > self._CACHE_STALE_DAYS and not course_data["is_fresh"]:
                await self.get_course_data(course_key_formatted)
                log.debug(
                    "Before API call: await self.get_course_data(course_key_formatted)"
                )
        log.debug(
            f"DEBUG: Maintaining freshness for {course_key_formatted}, data_age_days: {data_age_days}"
        )

    async def get_course_data(self, course_key_formatted: str) -> Dict[str, Any]:
        """
        Get the course data from the cache or update it if needed.

        Args:
            course_key_formatted (str): The course identifier.

        Returns:
            dict: The course data or an empty dictionary if the course is not found.
        """
        log.debug("Entered get_course_data")

        courses = await self.config.courses()
        log.debug(
            f"Before API call: courses = await self.config.courses(), Fetched courses: {courses}"
        )

        course_data = courses.get(course_key_formatted)
        log.debug(f"Fetched course_data from courses: {course_data}")

        # Initialize variables
        soup = None
        error = None
        log.debug("Initialized soup and error to None")

        if not course_data or not course_data.get("is_fresh", False):
            log.debug(
                "Entering _fetch_course_online because course_data is either missing or not fresh."
            )
            soup, error = await self._fetch_course_online(course_key_formatted)
            log.debug(
                f"After API call: soup, error = await self._fetch_course_online(course_key_formatted), soup: {soup}, error: {error}"
            )

        if soup:
            log.debug("Soup exists, proceeding to process and update course data.")
            course_data_processed = self._process_soup_content(soup)
            log.debug(f"Processed course_data: {course_data_processed}")

            await self.config.courses.set_raw(
                course_key_formatted,
                value={
                    "course_data": course_data_processed,
                    "date_added": date.today().isoformat(),
                    "is_fresh": True,
                },
            )
            log.debug("After API call: await self.config.courses.set_raw()")

            courses = await self.config.courses()
            log.debug(
                f"After API call: courses = await self.config.courses(), Updated courses: {courses}"
            )
        elif error:
            log.error(f"Error fetching course data for {course_key_formatted}: {error}")
            return {}

        log.debug(f"Returning: {course_data}")

        return course_data if course_data else {}

    async def _get_term_id(self, term_name: str) -> int:
        """Get the term id from the config."""
        log.debug("Before API call: term_codes = await self.config.term_codes()")
        term_codes = await self.config.term_codes()
        log.debug("Returning: term_codes.get(term_name, None)")
        return term_codes.get(term_name, None)

    def _generate_time_code(self) -> Tuple[int, int]:
        """Generate the time code for the request."""
        t = floor(time() / 60) % 1000
        e = t % 3 + t % 39 + t % 42
        log.debug("Returning: t, e")
        return t, e

    async def _fetch_single_attempt(
        self, url: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """Fetch the data with a single attempt."""
        timeout = ClientTimeout(total=15)
        try:
            async with ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    log.debug(f"Fetching course data from {url}")
                    if response.status != 200:
                        log.debug("Returning: None, None")
                        return None, None
                    log.debug("Before API call: content = await response.text()")
                    content = await response.text()
                    soup = BeautifulSoup(content, "xml")
                    if not (error_tag := soup.find("error")):
                        log.debug("Returning: soup, None")
                        return soup, None
                    error_message = error_tag.text.strip()
                    log.debug("Returning: None, error_message or None")
                    return None, error_message or None
        except Exception as e:
            log.debug(f"Exception caught: {str(e)}")
            log.error(f"An error occurred while fetching data from {url}: {e}")
            log.debug("Returning: None, str(e)")
            return None, str(e)

    def _check_error_message_for_matches(self, error_message: str) -> Tuple[str, str]:
        log.debug("Entered _check_error_message_for_matches")
        """Check the error message for matches with term names or other provided strings."""
        original_error_message = error_message
        error_message = error_message.lower()

        if matched_term := next(
            (term for term in self._TERM_NAMES if term in error_message), None
        ):
            log.debug(f"Returning: f'term_match:{matched_term}', ''")
            return f"term_match:{matched_term}", ""

        error_dict = {
            "could not be found in any enabled term": "no_term_match",
            "check your pc time and timezone": "time_error",
            "not authorized": "auth_error",
        }
        log.debug("Returning: next(")
        return next(
            (
                (value, original_error_message)
                for key, value in error_dict.items()
                if key in error_message
            ),
            ("unmatched_error", original_error_message),
        )

    def _determine_term_order(self) -> List[str]:
        log.debug("Entered _determine_term_order")
        """Determine the order of the terms to check."""
        now = date.today()
        current_term_index = (now.month - 1) // 4
        log.debug("Returning: (")
        return (
            self._TERM_NAMES[current_term_index:]
            + self._TERM_NAMES[:current_term_index]
        )

    def _build_url(self, term_id: int, course_key_formatted: str) -> str:
        log.debug("Entered _build_url")
        """Build the URL for the request."""
        t, e = self._generate_time_code()
        log.debug("Returning: self._URL_BASE.format(")
        return self._URL_BASE.format(
            term=term_id, course_key_formatted=course_key_formatted, t=t, e=e
        )

    async def _fetch_data_with_retries(
        self, term_order: List[str], course_key_formatted: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """Fetch the data with retries."""
        max_retries = 1
        retry_delay = 5
        url = None

        for term_name in term_order:
            term_id = await self._get_term_id(term_name)
            log.debug("Before API call: term_id = await self._get_term_id(term_name)")
            if not term_id:
                continue

            url = self._build_url(term_id, course_key_formatted)

            for retry_count in range(max_retries):
                try:
                    soup, error_message = await self._fetch_single_attempt(url)
                    log.debug(
                        "Before API call: soup, error_message = await self._fetch_single_attempt(url)"
                    )
                    if soup:
                        log.debug("Returning: soup, None")
                        return soup, None
                    elif error_message:
                        (
                            match_result,
                            original_error_message,
                        ) = self._check_error_message_for_matches(error_message)
                        if match_result.startswith("term_match:"):
                            log.error(
                                f"{original_error_message} matches {match_result[10:]}"
                            )
                            break  # Break the retry loop to try the next term
                        if (
                            match_result != "unmatched_error"
                            or retry_count == max_retries - 1
                        ):
                            log.error(original_error_message)
                            log.debug("Returning: None, original_error_message")
                            return None, original_error_message
                        await asyncio.sleep(retry_delay)
                        log.debug("Before API call: await asyncio.sleep(retry_delay)")
                except (
                    ClientResponseError,
                    ClientConnectionError,
                    asyncio.TimeoutError,
                ) as error:
                    log.error(f"Error fetching course data: {error}")
                    if retry_count == max_retries - 1:
                        error_message = (
                            "Error: An issue occurred while fetching the course data."
                        )
                        log.debug("Returning: None, error_message")
                        return None, error_message
                    await asyncio.sleep(retry_delay)
                    log.debug("Before API call: await asyncio.sleep(retry_delay)")

        log.error(
            f"Reached max retries ({max_retries}) while fetching course data from {url}"
        )
        error_message = "Error: Max retries reached while fetching the course data."
        log.debug("Returning: None, error_message")
        return None, error_message

    async def _fetch_course_online(
        self, course_key_formatted: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """Fetch the course data from the online source."""
        term_order = self._determine_term_order()

        log.debug(
            "Before API call: soup, error_message = await self._fetch_data_with_retries("
        )
        soup, error_message = await self._fetch_data_with_retries(
            term_order, course_key_formatted
        )
        log.debug("Returning: (soup, None) if soup else (None, error_message)")
        return (soup, None) if soup else (None, error_message)

    ## COURSE DATA PROCESSING: Processes the course data from the online source into a dictionary.

    @staticmethod
    def _find_and_remove_pattern(pattern, course_description):
        log.debug("Entered _find_and_remove_pattern")
        """Find and remove a pattern from the course description."""
        if match := re.search(pattern, course_description):
            result = match[1].strip()
            course_description = re.sub(pattern, "", course_description)
        else:
            result = ""
        log.debug("Returning: result, course_description")
        return result, course_description

    def _preprocess_course_description(self, course_description):
        log.debug("Entered _preprocess_course_description")
        """Preprocess the course description to remove unnecessary content."""
        course_desc = {
            "course_information": "",
            "course_format_and_duration": "",
            "prerequisites": "",
            "corequisites": "",
            "antirequisites": "",
            "restrictions_and_priority": "",
            "additional_notes_and_schedule": "",
            "cross_listings": "",
        }

        patterns = {
            "prerequisites": r"(?i)Prerequisite\(s\):(.+?)(\n|<br/>|$)",
            "corequisites": r"(?i)Co-requisite\(s\):(.+?)(\n|<br/>|$)",
            "antirequisites": r"(?i)Antirequisite\(s\):(.+?)(\n|<br/>|$)",
            "restrictions_and_priority": r"(?i)(Not open to.+?|Priority.+?)(\n|<br/>|$)",
            "cross-listings": r"(?i)Cross-list\(s\):(.+?)(\n|<br/>|$)",
            "additional_notes_and_schedule": r"(?i)(Formerly.+?|Students are strongly encouraged.+?|Offered on an irregular basis.)(\n|<br/>|$)",
        }

        for key, pattern in patterns.items():
            course_desc[key], course_description = self._find_and_remove_pattern(
                pattern, course_description
            )

        course_description = re.sub(r"<br/>", "", course_description).strip()
        course_parts = re.split(
            r"(?i)(Three lectures|Lectures \(three hours\)|Two lectures|Three hours|Three lectures, two hour seminar/lab every other week)",
            course_description,
        )

        course_desc["course_information"] = course_parts[0].strip()
        course_desc["course_format_and_duration"] = "".join(course_parts[1:]).strip()

        log.debug("Returning: course_desc")
        return course_desc

    def _extract_course_details(self, course: Tag, offering: Tag) -> Dict[str, str]:
        log.debug("Entered _extract_course_details")
        """Extract course details from the course data."""
        term_elem = course.find("term")
        block = course.find("block")

        course_description = offering.get("desc", "")
        preprocessed_description = self._preprocess_course_description(
            course_description
        )

        extracted_details = {
            "title": offering["title"],
            "term_found": term_elem.get("v") if term_elem else "",
            "type": block.get("type", "") if block else "",
            "teacher": block.get("teacher", "") if block else "",
            "location": block.get("location", "") if block else "",
            "campus": block.get("campus", "") if block else "",
            "notes": block.get("n", "") if block else "",
            "course_code": course["code"],
            "course_number": course["number"],
            "course_key_extracted": course["key"],
        }

        merged_details = {**preprocessed_description, **extracted_details}
        log.debug(f"Returning: {merged_details}")

        return {**preprocessed_description, **extracted_details}

    def _process_soup_content(self, soup: BeautifulSoup) -> List[Dict]:
        log.debug("Entered _process_soup_content")
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
        log.debug(f"Returning: {course_data}")
        return course_data


class CourseManager(commands.Cog):
    """Cog for managing course data."""

    def __init__(self, bot):
        log.debug(f"Entered __init__ for CourseManager with ID: {id(self)}")
        """Initialize the CourseManager class."""
        self.bot = bot
        self.config = Config.get_conf(
            self.bot, identifier=3720194665, force_registration=True
        )
        self.config.register_global(term_codes={}, courses={})
        self.config.register_guild(channels={}, course_info={})
        self.course_data_proxy = CourseDataProxy(self.config)
        self.course_channel = CourseChannel(
            self.bot, self.config, self, self.course_data_proxy
        )
        self.bot.loop.create_task(self.maintain_freshness_task())

    async def maintain_freshness_task(self):
        await self.bot.wait_until_ready()
        log.debug(
            f"Entered maintain_freshness_task for CourseManager with ID: {id(self)}"
        )
        """A coroutine to wrap maintain_freshness function."""
        while True:
            log.debug("DEBUG: Starting maintain_freshness loop")
            await self.course_data_proxy._maintain_freshness()
            log.debug(
                "Before API call: await self.course_data_proxy._maintain_freshness()"
            )
            await asyncio.sleep(24 * 60 * 60)
            log.debug("Before API call: await asyncio.sleep(24 * 60 * 60)")

    ### Helper Functions
    def _split_course_key_raw(self, course_key_raw) -> Tuple[str, str]:
        log.debug("Entered _split_course_key_raw")
        course_parts = re.sub(r"[-_]", " ", course_key_raw).upper().split()
        course_code, course_number = course_parts[0], " ".join(course_parts[1:])
        log.debug(f"Returning: {course_code}, {course_number}")
        return course_code, course_number

    def _validate_course_key(
        self, course_code: str, course_number: str
    ) -> Optional[Tuple[str, str]]:
        log.debug("Entered _validate_course_key")
        if not (
            re.match(r"^[A-Z]+$", course_code)
            and re.match(r"^(\d[\w]{1,3})", course_number)
        ):
            log.debug("Returning: None")
            return None

        course_number = re.match(r"^(\d[\w]{1,3})", course_number)[1]
        log.debug(f"Returning: {course_code, course_number}")
        return course_code, course_number

    def _format_course_key(self, course_key_raw) -> Optional[str]:
        log.debug("Entered _format_course_key")
        course_code, course_number = self._split_course_key_raw(course_key_raw)
        validated_course_key = self._validate_course_key(course_code, course_number)

        if validated_course_key is None:
            log.debug("Returning: None")
            return None

        log.debug(f"Returning: {validated_course_key[0]}-{validated_course_key[1]}")
        return f"{validated_course_key[0]}-{validated_course_key[1]}"

    async def send_long_message(self, ctx, content, max_length=2000):
        log.debug("Entered send_long_message")
        """The Menu class paginates long messages for easier reading and the example shows how to create a paginated message with a timeout and navigation controls."""
        if len(content) <= max_length:
            log.debug("Before API call: await ctx.send(content)")
            await ctx.send(content)
        else:
            pages = []
            for i in range(0, len(content), max_length):
                page = discord.Embed(description=content[i : i + max_length])
                pages.append(page)

            log.debug(
                "Before API call: await menu(ctx, pages, DEFAULT_CONTROLS, timeout=60)"
            )
            await menu(ctx, pages, DEFAULT_CONTROLS, timeout=60)

    def create_course_embed(self, course_data):
        log.debug("Entered create_course_embed")
        if course_data == "Not Found":
            log.debug("Returning: discord.Embed with title 'Course not found'")
            return discord.Embed(
                title="Course not found",
                description="No data available for this course.",
                color=0xFF0000,
            )

        if not course_data or not course_data.get("course_data"):
            log.debug("Returning: None")
            return None

        course_key = course_data["course_data"][0]["course_key_extracted"]
        embed = discord.Embed(title=course_key, color=0x00FF00)

        field_info = [
            ("teacher", "Teacher"),
            ("term_found", "Term"),
            ("course_information", "Description"),
            ("prerequisites", "Prerequisites"),
            ("corequisites", "Corequisites"),
            ("antirequisites", "Antirequisites"),
            ("restrictions_and_priority", "Access"),
            ("course_format_and_duration", "Format"),
            ("notes", "Notes"),
            ("additional_notes_and_schedule", "Other"),
            ("cross_listings", "Alt Names"),
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

            embed.add_field(name="", value="".join(course_details), inline=False)

        log.debug("Returning: embed")
        return embed

    ### User Command Section

    @commands.group(invoke_without_command=True, help="Cog for managing course data.")
    @commands.cooldown(1, 5, commands.BucketType.user)
    @commands.max_concurrency(1, commands.BucketType.user)
    @commands.guild_only()
    async def course(self, ctx):
        log.debug("Before API call: await ctx.send_help(self.course)")
        await ctx.send_help(self.course)

    @course.command(name="details")
    async def course_details(self, ctx, *, course_key_raw: str):
        """Get the details of a course."""
        course_key_formatted = self._format_course_key(course_key_raw)
        if not course_key_formatted:
            log.debug(
                "Before API call: await ctx.send with Invalid course code message"
            )
            await ctx.send(
                f"Invalid course code: {course_key_raw}. Please use the format: `course_code course_number`"
            )
            return

        log.debug(
            "Before API call: course_data = await self.course_data_proxy.get_course_data(course_key_formatted)"
        )
        course_data = await self.course_data_proxy.get_course_data(course_key_formatted)

        # Debug log to print course_data
        log.debug(f"Fetched course_data: {course_data}")

        if not course_data:
            log.debug("Before API call: await ctx.send with Course not found message")
            await ctx.send(f"Course not found: {course_key_formatted}")
            return

        # Debug log to print details before creating embed
        log.debug(
            "Before creating embed: Course data exists, proceeding to create embed."
        )

        embed = self.create_course_embed(course_data)

        # Debug log to print the embed object
        log.debug(f"Created embed object: {embed}")

        log.debug("Before API call: await ctx.send(embed=embed)")
        await ctx.send(embed=embed)

    ### Dev Command Section

    @checks.is_owner()
    @commands.group(name="dc", invoke_without_command=True)
    async def dev_course(self, ctx):
        """Developer commands for the course cog."""
        log.debug("Before API call: await ctx.send_help(self.course)")
        await ctx.send_help(self.course)

    @dev_course.command(name="term")
    async def set_term_codes(self, ctx, term_name: str, term_id: int):
        """Set the term code for the specified term."""
        log.debug("Before API call: await ctx.send for setting term code")
        async with self.config.term_codes() as term_codes:
            term_codes[term_name] = term_id
        await ctx.send(
            f"Term code for {term_name.capitalize()} has been set to: {term_id}"
        )

    @dev_course.command(name="log")
    async def set_log(self, ctx, option: str, channel: discord.TextChannel):
        """Sets logging channel for the cog."""
        log.debug("Before API call: set log channel and option")
        if option.lower() == "logging":
            await self.config.logging_channel.set(channel.id)
            await ctx.send(f"Logging channel set to {channel}.")
            return
        await ctx.send(
            "Invalid option. Use '=course setlog logging' followed by the channel."
        )

    @dev_course.command(name="printconfig")
    async def print_config(self, ctx):
        """Prints the global config data to console"""
        log.debug("Before API call: print(await self.config.all())")
        print(await self.config.all())

    @dev_course.command(name="clearcourses")
    async def clear_courses(self, ctx):
        """Clears courses from the global config"""
        log.debug("Before API call: await self.config.courses.set({})")
        await self.config.courses.set({})
        print(await self.config.courses())

    @course.command(name="managecoursechannels", aliases=["mc"])
    async def manage_course_channels(
        self, ctx, subcommand: str, *, course_keys_raw: str
    ):
        """
        Takes user input and passes it to the decision tree
        """
        log.debug("Before API call: check valid subcommand and pass to decision tree")
        valid_subcommands = {"join", "leave", "list"}
        if subcommand.lower() not in valid_subcommands:
            await ctx.send("Invalid subcommand. Use 'join', 'leave', or 'list'")
            return
        await self.decision_tree(ctx, subcommand, course_keys_raw)


class CourseChannel:
    def __init__(self, bot, config, course_manager, course_data_proxy):
        log.debug("Entered __init__")
        self.bot = bot
        self.config = config
        self.course_data_proxy = course_data_proxy
        self.course_manager = course_manager

    async def decision_tree(self, ctx, subcommand, course_keys_raw):
        log.debug(
            f"Entered decision_tree with subcommand: {subcommand}, course_keys_raw: {course_keys_raw}"
        )
        author = ctx.message.author
        tasks, allowed_to_join_list = self._create_tasks(ctx, course_keys_raw)
        log.debug(f"Tasks created in decision_tree: {tasks}")

        log.debug("Executing bounded_gather")
        results = await bounded_gather(*tasks, limit=5)
        log.debug(f"Results after bounded_gather: {results}")

        subcommand = subcommand.lower()

        if subcommand == "join":
            log.debug("Before API call: await self._process_results(")
            await self._process_results(
                ctx, course_keys_raw, results, allowed_to_join_list
            )
            if results == allowed_to_join_list:
                log.debug(
                    "Before API call: await self._update_user_channel_permissions(ctx, course_keys_raw, True)"
                )
                await self._update_user_channel_permissions(ctx, course_keys_raw, True)
        elif subcommand == "leave":
            log.debug(
                "Before API call: if await self._get_course_channels(author) == course_keys_raw:"
            )
            if await self._get_course_channels(author) == course_keys_raw:
                log.debug(
                    "Before API call: await self._update_user_channel_permissions(ctx, course_keys_raw, False)"
                )
                await self._update_user_channel_permissions(ctx, course_keys_raw, False)
        elif subcommand == "list":
            log.debug(
                "Before API call: await ctx.send(humanize_list(self._get_course_channels(author)))"
            )
            await ctx.send(humanize_list(self._get_course_channels(author)))

    async def _create_tasks(self, ctx, course_keys_raw):
        log.debug(f"Entered _create_tasks with course_keys_raw: {course_keys_raw}")

        async def channel_and_course_data_task(course_key_formatted):
            log.debug(
                f"Entered channel_and_course_data_task with course_key_formatted: {course_key_formatted}"
            )
            channel_exists = await self._is_channel_found(ctx, course_key_formatted)
            log.debug(f"Channel exists: {channel_exists}")
            course_data = (
                None
                if channel_exists
                else await self.course_data_proxy.get_course_data(course_key_formatted)
            )
            log.debug(f"Course data fetched: {course_data}")
            return channel_exists, course_data

        course_channels = self._get_allowed_channels(ctx.message.author)
        log.debug(f"Allowed channels for user: {course_channels}")

        async def process_course_key(course_key_raw):
            log.debug(f"Processing course key: {course_key_raw}")
            course_key_formatted = self.course_manager._format_course_key(
                course_key_raw
            )
            log.debug(f"Formatted course key: {course_key_formatted}")

            if len(course_channels) >= 10:
                allowed_to_join, join_error_message = (
                    False,
                    "User attempting to add more than allowed courses.",
                )
            elif any(
                channel.name.upper() == course_key_formatted
                for channel in course_channels
            ):
                allowed_to_join, join_error_message = (
                    False,
                    "User has already added this course.",
                )
            else:
                allowed_to_join, join_error_message = True, None

            channel_and_course_data = await channel_and_course_data_task(
                course_key_formatted
            )
            log.debug(f"Channel and course data: {channel_and_course_data}")
            return channel_and_course_data, (allowed_to_join, join_error_message)

        tasks = await AsyncIter(course_keys_raw, process_course_key).flatten()
        log.debug(f"Total number of tasks created: {len(tasks)}")
        log.debug(f"Tasks details: {tasks}")
        log.debug("Returning: tasks, [")
        return tasks, [
            (allowed_to_join, join_error_message)
            for _, (allowed_to_join, join_error_message) in tasks
        ]

    async def _process_results(
        self, ctx, course_keys_raw, results, allowed_to_join_list
    ):
        for i, course_key_raw in enumerate(course_keys_raw):
            (channel_exists, course_data) = results[i]
            allowed_to_join, join_error_message = allowed_to_join_list[i]
            course_key_formatted = self.course_manager._format_course_key(
                course_key_raw
            )

            valid_course = course_data is not None
            if not valid_course:
                log.debug("Before API call: await ctx.send(")
                await ctx.send(
                    f"Course {course_key_formatted} does not exist. Please check your spelling and try again."
                )
                continue

            if not channel_exists:
                log.debug("Before API call: await self._create_course_channel(")
                await self._create_course_channel(
                    ctx, course_key_formatted, course_data
                ).send(f"Course {course_key_formatted} has been added to the server.")

            if not allowed_to_join:
                log.debug("Before API call: await ctx.send(join_error_message)")
                await ctx.send(join_error_message)

    async def _is_channel_found(self, ctx, course_key_formatted) -> bool:
        log.debug(
            "Before API call: course_info = await self.config.guild(ctx.guild).course_info()"
        )
        course_info = await self.config.guild(ctx.guild).course_info()
        log.debug(
            "Returning: course_key_formatted in course_info or discord.utils.get("
        )
        return course_key_formatted in course_info or discord.utils.get(
            ctx.guild.text_channels, name=course_key_formatted
        )

    def _get_allowed_channels(self, author):
        log.debug("Entered _get_allowed_channels")
        valid_courses = {course for courses in FACULTIES.values() for course in courses}
        log.debug("Returning: [")
        return [
            channel
            for channel in author.guild.channels
            if (perms := channel.overwrites_for(author)).view_channel
            and perms.send_messages
            and channel.name.upper() in valid_courses
        ]

    def _get_course_faculty(self, course_key_formatted):
        log.debug("Entered _get_course_faculty")
        course_code = course_key_formatted.split("-")[0]
        log.debug("Returning: next(")
        return next(
            (
                faculty
                for faculty, courses in FACULTIES.items()
                if course_code in courses
            ),
            None,
        )

    async def _get_course_channels(self, ctx, user=None):
        if ctx.guild.categories is None:
            log.debug("No categories found in guild.")
            return []
        log.debug("Returning: []")
        course_channels = [
            channel
            for category_name in FACULTIES.keys()
            if (category := discord.utils.get(ctx.guild.categories, name=category_name))
            for channel in category.channels
            if self._channel_accessible_by_user(channel, user)
        ]
        log.debug(f"List of course channels: {course_channels}")
        return course_channels

    log.debug("Returning: course_channels")

    def _channel_accessible_by_user(self, channel, user):
        log.debug("Entered _channel_accessible_by_user")
        if user is None:
            return True
        log.debug("Returning: True")
        return (
            log.debug("Returning: ("),
            channel.overwrites_for(user).view_channel
            and channel.overwrites_for(user).send_messages,
        )

    async def _create_course_channel(self, ctx, course_key_formatted):
        user = ctx.message.author
        if faculty := self._get_course_faculty(course_key_formatted):
            overwrites = {
                ctx.guild.default_role: discord.PermissionOverwrite(
                    read_messages=False, send_messages=False
                ),
                user: discord.PermissionOverwrite(
                    read_messages=True, send_messages=True
                ),
            }
            category = discord.utils.get(ctx.guild.categories, name=faculty)
            log.info(
                f"Creating channel {course_key_formatted} with category {category}"
            )
            log.debug("Before API call: await ctx.guild.create_text_channel(")
            await ctx.guild.create_text_channel(
                course_key_formatted, category=category, overwrites=overwrites
            )
            log.debug(
                "Before API call: await self._update_course_info(ctx, course_key_formatted)"
            )
            await self._update_course_info(ctx, course_key_formatted)
        else:
            log.info(f"Faculty not found for course {course_key_formatted}")

    async def _update_course_info(self, ctx, course_key_formatted):
        if course_channel := discord.utils.get(
            ctx.guild.text_channels, name=course_key_formatted
        ):
            channel_info = {
                "faculty_name": course_channel.category.name,
                "creation_date": course_channel.created_at.isoformat(),
                "last_message_date": course_channel.last_message.created_at.isoformat(),
                "member_list": [
                    member.id
                    for member in course_channel.members
                    if not member.bot and member.id != ctx.guild.owner.id
                ],
                "channel_id": course_channel.id,
            }
            log.debug(f"Channel info: {channel_info}")

            async with self.config.guild(ctx.guild).channels() as channels:
                channels[course_key_formatted] = channel_info
        log.debug(f"Channel info stored in config: {channel_info}")

    async def _update_user_channel_permissions(
        self, ctx, course_channel, user, add=True
    ):
        if add:
            perms = discord.PermissionOverwrite(read_messages=True, send_messages=True)
        else:
            perms = discord.PermissionOverwrite(
                read_messages=False, send_messages=False
            )
        if course_channel is not None:
            current_perms = course_channel.permissions_for(user)

        if (
            current_perms.read_messages != perms.read_messages
            or current_perms.send_messages != perms.send_messages
        ):
            log.debug(
                "Before API call: await course_channel.set_permissions(user, overwrite=perms)"
            )
            await course_channel.set_permissions(user, overwrite=perms)

            log.info(f"{user} has been granted access to {course_channel}")
            log.debug("Before API call: await self._update_course_info(ctx)")
            await self._update_course_info(ctx)

        else:
            log.error(f"Course channel {course_channel} not found")


### EXAMPLE OF REDBOT MENU ###
"""
import discord
from redbot.core.utils.menus import menu, close_menu, next_page, prev_page, start_adding_reactions

async def my_handler(ctx, page_num, emoji):
    await ctx.send(f"You selected page {page_num} with {emoji}")

async def my_menu(ctx):
    pages = ["Page 1", "Page 2", "Page 3"]
    controls = {"⬅️": prev_page, "❌": close_menu, "➡️": next_page, "🔢": lambda ctx, pages, controls, message, page, timeout, emoji: my_handler(ctx, int(emoji), emoji)}
    await menu(ctx, pages, controls=controls)

@commands.command()
async def show_menu(self, ctx):
    await my_menu(ctx)
"""
"""
Reasons to incorporate redbot functions:

The @commands.group(invoke_without_command=True, help="Cog for managing course data.") decorator simplifies the course command by removing the need to manually send the help message. By setting invoke_without_command=True, the help message will automatically be sent when no subcommands are provided by the user. This makes the code cleaner and easier to manage.

The @commands.max_concurrency(1, commands.BucketType.user) decorator is useful for controlling the maximum number of concurrent invocations of a command. By limiting the number of concurrent invocations, you can prevent potential issues in your bot caused by users invoking the command simultaneously. This is especially important for tasks that involve fetching data, as it could lead to rate limiting or other issues.

The send_long_message function can be improved by using Redbot's built-in Menu class. The Menu class allows you to paginate long messages, making them easier for users to read. The example provided demonstrates how to use the Menu class to create a paginated message with a timeout and default controls for navigation.
"""
