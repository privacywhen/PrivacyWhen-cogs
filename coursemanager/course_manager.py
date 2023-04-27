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
from .error_handler import ErrorHandler
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
        courses = await self.config.courses()
        for course_key_formatted, course_data in courses.items():
            data_age_days = (
                date.today() - date.fromisoformat(course_data["date_added"])
            ).days

            if data_age_days > self._CACHE_EXPIRY_DAYS:
                await self.config.courses.pop(course_key_formatted)
            elif data_age_days > self._CACHE_STALE_DAYS and not course_data["is_fresh"]:
                await self.get_course_data(course_key_formatted)

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
        courses = await self.config.courses()
        course_data = courses.get(course_key_formatted)

        if not course_data or not course_data.get("is_fresh", False):
            soup, error = await self._fetch_course_online(course_key_formatted)
            if soup:
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
            elif error:
                print(f"Error fetching course data for {course_key_formatted}: {error}")
                return {}

        return courses.get(course_key_formatted, {})

    async def _get_term_id(self, term_name: str) -> int:
        """Get the term id from the config."""
        term_codes = await self.config.term_codes()
        return term_codes.get(term_name, None)

    def _generate_time_code(self) -> Tuple[int, int]:
        """Generate the time code for the request."""
        t = floor(time() / 60) % 1000
        e = t % 3 + t % 39 + t % 42
        return t, e

    async def _fetch_single_attempt(
        self, url: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """Fetch the data with a single attempt."""
        timeout = ClientTimeout(total=15)
        async with ClientSession(timeout=timeout) as session:
            async with session.get(url) as response:
                log.debug(f"Fetching course data from {url}")
                if response.status != 200:
                    return None, None
                content = await response.text()
                soup = BeautifulSoup(content, "xml")
                if not (error_tag := soup.find("error")):
                    return soup, None
                error_message = error_tag.text.strip()
                return None, error_message or None

    def _check_error_message_for_matches(self, error_message: str) -> Tuple[str, str]:
        """Check the error message for matches with term names or other provided strings."""
        original_error_message = error_message
        error_message = error_message.lower()

        if matched_term := next(
            (term for term in self._TERM_NAMES if term in error_message), None
        ):
            return f"term_match:{matched_term}", ""

        error_dict = {
            "could not be found in any enabled term": "no_term_match",
            "check your pc time and timezone": "time_error",
            "not authorized": "auth_error",
        }
        return next(
            (
                (value, original_error_message)
                for key, value in error_dict.items()
                if key in error_message
            ),
            ("unmatched_error", original_error_message),
        )

    def _determine_term_order(self) -> List[str]:
        """Determine the order of the terms to check."""
        now = date.today()
        current_term_index = (now.month - 1) // 4
        return (
            self._TERM_NAMES[current_term_index:]
            + self._TERM_NAMES[:current_term_index]
        )

    def _build_url(self, term_id: int, course_key_formatted: str) -> str:
        """Build the URL for the request."""
        t, e = self._generate_time_code()
        return self._URL_BASE.format(
            term=term_id, course_key_formatted=course_key_formatted, t=t, e=e
        )

    async def _fetch_data_with_retries(
        self, term_order: List[str], course_key_formatted: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """Fetch the data with retries."""
        max_retries = 3
        retry_delay = 5

        for term_name in term_order:
            term_id = await self._get_term_id(term_name)
            if not term_id:
                continue

            url = self._build_url(term_id, course_key_formatted)

            for retry_count in range(max_retries):
                try:
                    soup, error_message = await self._fetch_single_attempt(url)
                    if soup:
                        return soup, None
                    elif error_message:
                        (
                            match_result,
                            original_error_message,
                        ) = self._check_error_message_for_matches(error_message)
                        if match_result.startswith("term_match:"):
                            print(
                                f"{original_error_message} matches {match_result[10:]}"
                            )
                            break  # Break the retry loop to try the next term
                        elif match_result == "no_term_match":
                            return None, original_error_message
                        elif match_result == "time_error":
                            log.error(
                                "Time and timezone error. Please check your PC time and timezone."
                            )
                            return None, original_error_message
                        elif match_result == "auth_error":
                            log.error(
                                "Error 7133: Not Authorized. Check encryption key and time key."
                            )
                            return None, original_error_message
                        elif retry_count != max_retries - 1:
                            await asyncio.sleep(retry_delay)
                except (ClientResponseError, ClientConnectionError) as error:
                    log.error(f"Error fetching course data: {error}")
                    error_message = (
                        "Error: An issue occurred while fetching the course data."
                    )
                except asyncio.TimeoutError:
                    log.error(f"Timeout error while fetching course data from {url}")
                    error_message = "Error: Timeout while fetching the course data."

        log.error(
            f"Reached max retries ({max_retries}) while fetching course data from {url}"
        )
        error_message = "Error: Max retries reached while fetching the course data."

        return None, error_message

    async def _fetch_course_online(
        self, course_key_formatted: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """Fetch the course data from the online source."""
        term_order = self._determine_term_order()

        soup, error_message = await self._fetch_data_with_retries(
            term_order, course_key_formatted
        )
        return (soup, None) if soup else (None, error_message)

    ## COURSE DATA PROCESSING: Processes the course data from the online source into a dictionary.

    @staticmethod
    def _find_and_remove_pattern(pattern, course_description):
        """Find and remove a pattern from the course description."""
        if match := re.search(pattern, course_description):
            result = match[1].strip()
            course_description = re.sub(pattern, "", course_description)
        else:
            result = ""
        return result, course_description

    def _preprocess_course_description(self, course_description):
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

        return course_desc

    def _extract_course_details(self, course: Tag, offering: Tag) -> Dict[str, str]:
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

        return {**preprocessed_description, **extracted_details}

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
        self.course_data_proxy = CourseDataProxy(self.config)
        self.bot.loop.create_task(self.maintain_freshness_task())

    async def maintain_freshness_task(self):
        """A coroutine to wrap maintain_freshness function."""
        while True:
            log.debug("DEBUG: Starting maintain_freshness loop")
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
        if len(content) <= max_length:
            await ctx.send(content)
        else:
            while content:
                message_chunk = content[:max_length]
                try:
                    await ctx.send(message_chunk)
                except Exception as e:
                    print(f"Error sending message: {e}")
                    break
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

        return embed

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

    @dev_course.command(name="printconfig")
    async def print_config(self, ctx):
        """Prints the global config data to console"""
        print(await self.config.all())

    @dev_course.command(name="clearcourses")
    async def clear_courses(self, ctx):
        """Clears courses from the global config"""
        await self.config.courses.set({})
        print(await self.config.courses())

    ### ACTIVE DEVELOPMENT ###
    @course.command(name="addcourse", aliases=["add"])
    async def add_course(self, ctx, *, course_list: str):
        course_keys_raw = [course.strip() for course in course_list.split(";")]
        if len(course_keys_raw) > 5:
            await ctx.send("You can only add up to 5 courses at a time.")
            return

        for course_key_raw in course_keys_raw:
            course_key_formatted = self._format_course_key(course_key_raw)
            channel_exists = await self._check_channel_exists(ctx, course_key_formatted)
            (
                allowed_to_join,
                join_error_message,
            ) = await self._check_user_allowed_to_join(ctx, course_key_formatted)
            course_data = await self.course_data_proxy.get_course_data(
                course_key_formatted
            )
            log.info(f"channel_exists: {channel_exists}")
            log.info(f"allowed_to_join: {allowed_to_join}")
            log.error(f"join_error_message: {join_error_message}")
            log.info(f"course_data: {course_data}")

            if channel_exists:
                await self._add_user_to_channel(ctx, course_key_formatted)
                log.info("Adding user to channel")

            elif not allowed_to_join:
                await ctx.send(f"{course_key_formatted}: {join_error_message}")
                log.error("User cannot join course.")

            elif course_key_formatted in course_data:
                await self._create_course_channel(ctx, course_key_formatted)
                log.info("Creating course channel.")

            else:
                await ctx.send(f"{course_key_formatted} is an invalid course code.")
                log.error("Invalid course code.")

            await asyncio.sleep(5)

    async def _check_channel_exists(self, ctx, course_key_formatted):
        """
        Checks if a channel with the given course_key_formatted exists.
        """
        temp_dict = {
            course: faculty
            for faculty, courses in FACULTIES.items()
            for course in courses
        }
        log.info(f"Checking if course {course_key_formatted} exists in {temp_dict}")
        return course_key_formatted in temp_dict

    async def _list_of_course_channels(self, ctx):
        """
        Returns a list of all course channels.
        """
        course_channels = []
        for category_name in FACULTIES.keys():
            if category := discord.utils.get(ctx.guild.categories, name=category_name):
                course_channels.extend(category.channels)
        log.debug(f"List of course channels: {course_channels}")
        return course_channels

    async def _check_user_allowed_to_join(self, ctx, course_key_formatted):
        """
        Checks if the user is allowed to join the course.
        """
        user = ctx.message.author
        user_allowed_channels = [
            channel
            for channel in user.guild.channels
            if channel.overwrites_for(user).view_channel
            and channel.overwrites_for(user).send_messages
        ]

        course_channels = [
            channel
            for channel in user_allowed_channels
            if channel.name.upper()
            in {course for courses in FACULTIES.values() for course in courses}
        ]
        if len(course_channels) >= 10:
            log.info("User attempting to add more than allowed courses.")
            return False, "User attempting to add more than allowed courses."
        elif any(
            channel.name.upper() == course_key_formatted for channel in course_channels
        ):
            log.info("User has already added this course.")
            return False, "User has already added this course."
        else:
            return True, None

    async def _get_course_faculty(self, course_key_formatted):
        """
        Returns the faculty of the course.
        """
        course_code = course_key_formatted.split()[0]
        faculty = next(
            (
                faculty
                for faculty, courses in FACULTIES.items()
                if course_code in courses
            ),
            None,
        )
        if faculty is None:
            log.info(f"Faculty not found for course {course_code}")
        return faculty

    async def _create_course_channel(self, ctx, course_key_formatted):
        """
        Creates a course channel and sets permissions for the user.
        """
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
            print(f"Creating channel {course_key_formatted} with category {category}")
            await ctx.guild.create_text_channel(
                course_key_formatted, category=category, overwrites=overwrites
            )
        else:
            print(f"Faculty not found for course {course_key_formatted}")

    async def _update_user_channel_perms(self, ctx, course_channel, user, add=True):
        """
        Updates the user's permissions for the course channel.
        """
        if add:
            perms = discord.PermissionOverwrite(read_messages=True, send_messages=True)
        else:
            perms = discord.PermissionOverwrite(
                read_messages=False, send_messages=False
            )
        await course_channel.set_permissions(user, overwrite=perms)
        log.info(f"{user} has been granted access to {course_channel}")


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
