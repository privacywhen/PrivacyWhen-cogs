import asyncio
import re
import logging
from math import floor
from time import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

import discord
# from discord.ext import commands
from redbot.core import commands, Config
from redbot.core.utils import bounded_gather
from redbot.core.utils.chat_formatting import error, info, success, warning, box, pagify
from redbot.core.utils.menus import menu, DEFAULT_CONTROLS
from aiohttp import ClientSession, ClientTimeout, ClientConnectionError, ClientResponseError
from bs4 import BeautifulSoup

# Configure logging
log = logging.getLogger("red.course_helper")
log.setLevel(logging.DEBUG)
if not log.handlers:
    log.addHandler(logging.StreamHandler())


###############################################################################
# CourseDataProxy
###############################################################################
class CourseDataProxy:
    """
    Handles fetching and caching of course data from an external endpoint.
    Course data is stored in config under the global key 'courses'.
    """

    _CACHE_STALE_DAYS: int = 120
    _CACHE_EXPIRY_DAYS: int = 240
    _TERM_NAMES: List[str] = ["winter", "spring", "fall"]
    _URL_BASE: str = (
        "https://mytimetable.mcmaster.ca/api/class-data?"
        "term={term}&course_0_0={course_key_formatted}&t={t}&e={e}"
    )

    def __init__(self, config: Config) -> None:
        """Initialize the proxy with the bot's Config instance."""
        self.config: Config = config
        log.debug("CourseDataProxy initialized with config: %s", config)

    async def get_course_data(self, course_key_formatted: str) -> Dict[str, Any]:
        """
        Retrieve course data from config if available and fresh.
        Otherwise, fetch from the remote API, cache it, and return the new data.
        """
        log.debug("Attempting to retrieve course data for %s", course_key_formatted)
        course_data = await self.config.courses.get_raw(course_key_formatted, default=None)
        if not course_data or not course_data.get("is_fresh", False):
            log.debug(
                "Course data missing or stale for %s. Fetching online.",
                course_key_formatted,
            )
            soup, error_msg = await self._fetch_course_online(course_key_formatted)
            if soup:
                processed_data = self._process_soup_content(soup)
                new_data = {
                    "course_data": processed_data,
                    "date_added": date.today().isoformat(),
                    "is_fresh": True,
                }
                await self.config.courses.set_raw(course_key_formatted, value=new_data)
                log.debug(
                    "Fetched and processed data for %s: %s",
                    course_key_formatted,
                    new_data,
                )
                course_data = await self.config.courses.get_raw(course_key_formatted, default=None)
            elif error_msg:
                log.error(
                    "Error fetching course data for %s: %s",
                    course_key_formatted,
                    error_msg,
                )
                return {}
        else:
            log.debug("Using cached course data for %s", course_key_formatted)
        return course_data if course_data else {}

    async def _fetch_course_online(self, course_key_formatted: str) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """
        Attempt to fetch course data from the external endpoint.
        """
        log.debug("Fetching course data online for %s", course_key_formatted)
        term_order = self._determine_term_order()
        log.debug("Determined term order: %s", term_order)
        soup, error_message = await self._fetch_data_with_retries(term_order, course_key_formatted)
        return (soup, None) if soup else (None, error_message)

    def _determine_term_order(self) -> List[str]:
        """
        Determine a prioritized list of term names based on the current month.
        """
        now = date.today()
        current_term_index = (now.month - 1) // 4  # Roughly dividing months into 3 blocks.
        term_order = self._TERM_NAMES[current_term_index:] + self._TERM_NAMES[:current_term_index]
        log.debug("Current date: %s, term order: %s", now, term_order)
        return term_order

    async def _fetch_data_with_retries(
        self, term_order: List[str], course_key_formatted: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """
        Try to fetch data across multiple terms with a retry mechanism.
        """
        max_retries = 1
        retry_delay = 5
        url: Optional[str] = None

        for term_name in term_order:
            term_id = await self._get_term_id(term_name)
            if not term_id:
                log.debug("Term ID not found for term: %s", term_name)
                continue

            log.debug("Using term %s with ID %s", term_name, term_id)
            url = self._build_url(term_id, course_key_formatted)
            log.debug("Built URL: %s", url)
            for retry_count in range(max_retries):
                log.debug("Attempt %s for URL: %s", retry_count + 1, url)
                try:
                    soup, error_message = await self._fetch_single_attempt(url)
                    if soup:
                        log.debug("Successfully fetched data from URL: %s", url)
                        return soup, None
                    elif error_message:
                        log.debug("Error message received: %s", error_message)
                        if "not found" in error_message.lower():
                            log.error("Course not found: %s", course_key_formatted)
                            return None, error_message
                        if retry_count == max_retries - 1:
                            return None, error_message
                        log.debug("Retrying after error, sleeping for %s seconds", retry_delay)
                        await asyncio.sleep(retry_delay)
                except (ClientResponseError, ClientConnectionError, asyncio.TimeoutError) as error:
                    log.error("Error fetching course data: %s", error)
                    if retry_count == max_retries - 1:
                        return None, "Error: Issue occurred while fetching course data."
                    log.debug("Retrying after exception, sleeping for %s seconds", retry_delay)
                    await asyncio.sleep(retry_delay)
        if url:
            log.error("Max retries reached while fetching data from %s", url)
        return None, "Error: Max retries reached while fetching course data."

    async def _get_term_id(self, term_name: str) -> Optional[int]:
        """
        Retrieve the term code from config.
        """
        log.debug("Retrieving term ID for term: %s", term_name)
        term_id = await self.config.term_codes.get_raw(term_name, default=None)
        log.debug("Term ID for %s: %s", term_name, term_id)
        return term_id

    def _build_url(self, term_id: int, course_key_formatted: str) -> str:
        """
        Construct the URL for querying the course data.
        """
        t, e = self._generate_time_code()
        url = self._URL_BASE.format(term=term_id, course_key_formatted=course_key_formatted, t=t, e=e)
        log.debug("Generated time codes t: %s, e: %s, URL: %s", t, e, url)
        return url

    def _generate_time_code(self) -> Tuple[int, int]:
        """
        Generate a time-based code for the remote API.
        """
        t = floor(time() / 60) % 1000
        e = t % 3 + t % 39 + t % 42
        log.debug("Time code generated: t=%s, e=%s", t, e)
        return t, e

    async def _fetch_single_attempt(self, url: str) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """
        Perform a single HTTP request to fetch course data.
        """
        log.debug("Making HTTP GET request to %s", url)
        timeout = ClientTimeout(total=15)
        try:
            async with ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    log.debug("Received HTTP response with status %s for URL: %s", response.status, url)
                    if response.status != 200:
                        return None, f"Error: HTTP {response.status}"
                    content = await response.text()
                    soup = BeautifulSoup(content, "xml")
                    if not (error_tag := soup.find("error")):
                        log.debug("No error tag found in response for URL: %s", url)
                        return soup, None
                    error_message = error_tag.text.strip()
                    log.debug("Error tag found: %s", error_message)
                    return None, error_message or None
        except Exception as e:
            log.error("An error occurred while fetching data from %s: %s", url, e)
            return None, str(e)

    def _process_soup_content(self, soup: BeautifulSoup) -> List[Dict]:
        """
        Parse the BeautifulSoup object to extract relevant course data,
        including title, term, credits, description, prerequisites, and antirequisites.
        """
        courses = soup.find_all("course")
        log.debug("Processing soup content. Found %s courses.", len(courses))
        processed_courses = []
        for course in courses:
            offering = course.find("offering")
            title = offering.get("title", "") if offering else ""
            desc_attr = offering.get("desc", "") if offering else ""
            # Initialize description fields.
            description = ""
            prerequisites = ""
            antirequisites = ""
            if desc_attr:
                # Split on <br/> tags (handling <br> or <br />)
                desc_parts = re.split(r"<br\s*/?>", desc_attr)
                # Remove any extra whitespace and empty parts.
                desc_parts = [part.strip() for part in desc_parts if part.strip()]
                if desc_parts:
                    # Assume the first line is the main description.
                    description = desc_parts[0]
                # Search through parts for prerequisites and antirequisites.
                for part in desc_parts:
                    lower = part.lower()
                    if lower.startswith("prerequisite"):
                        # Split on the colon and strip out the label.
                        prerequisites = part.split(":", 1)[1].strip() if ":" in part else ""
                    elif lower.startswith("antirequisite"):
                        antirequisites = part.split(":", 1)[1].strip() if ":" in part else ""
            # Get credits from the first selection element if available.
            selection = course.find("selection")
            credits = selection.get("credits", "") if selection else ""
            term_found = course.find("term").get("v", "") if course.find("term") else ""
            teacher = ""
            block = course.find("block")
            if block:
                teacher = block.get("teacher", "")
            processed_courses.append(
                {
                    "title": title,
                    "term_found": term_found,
                    "teacher": teacher,
                    "course_code": course.get("code", ""),
                    "course_number": course.get("number", ""),
                    "credits": credits,
                    "description": description,
                    "prerequisites": prerequisites,
                    "antirequisites": antirequisites,
                }
            )
        return processed_courses


###############################################################################
# CourseManager Cog
###############################################################################
class CourseManager(commands.Cog):
    """
    Cog for managing course channels and course details.

    Channel Management:
      • Users can join or leave course channels under the "COURSES" category.
      • Admins can delete channels and set a logging channel for notifications.
      • Channels inactive for more than 120 days are auto-pruned.
      • Users can list their enrolled courses (duplicate joins are prevented).
      • Channels are only created if valid course details are found.

    Course Details:
      • Course data is retrieved and cached via an external API.
      • Details for one or multiple courses can be displayed.
      • Commands exist to refresh course data.

    Developer Commands (Owner-only):
      • Manage term codes, clear stale config entries, and trigger manual pruning.
    """

    def __init__(self, bot: commands.Bot) -> None:
        self.bot: commands.Bot = bot

        # Channel management settings.
        self.category_name: str = "COURSES"
        self.channel_permissions: discord.PermissionOverwrite = discord.PermissionOverwrite.from_pair(
            discord.Permissions(view_channel=True, send_messages=True, read_message_history=True),
            discord.Permissions.none(),
        )
        self.max_courses: int = 10
        self.logging_channel: Optional[discord.TextChannel] = None

        # Global defaults for config.
        default_global: Dict[str, Any] = {"term_codes": {}, "courses": {}}
        self.config: Config = Config.get_conf(self, identifier=3720194665, force_registration=True)
        self.config.register_global(**default_global)
        self.course_data_proxy: CourseDataProxy = CourseDataProxy(self.config)

        # Start the background auto-prune task.
        self._prune_task: asyncio.Task = self.bot.loop.create_task(self._auto_prune_task())
        log.debug("CourseManager initialized with max_courses: %s", self.max_courses)

    def cog_unload(self) -> None:
        """Cancel the auto-prune background task when the cog is unloaded."""
        log.debug("Unloading CourseManager cog. Cancelling auto-prune task.")
        self._prune_task.cancel()

    async def _auto_prune_task(self) -> None:
        """
        Background task that auto-prunes inactive course channels.
        A channel is pruned if its most recent non-bot message is older than 120 days.
        Only channels within the "COURSES" category are considered.
        """
        PRUNE_INTERVAL = 3600  # Check every hour.
        PRUNE_THRESHOLD = timedelta(days=120)
        await self.bot.wait_until_ready()
        log.debug("Auto-prune task started.")
        while not self.bot.is_closed():
            for guild in self.bot.guilds:
                category = self.get_category(guild)
                if not category:
                    log.debug("No '%s' category found in guild %s.", self.category_name, guild.name)
                    continue
                for channel in category.channels:
                    if not isinstance(channel, discord.TextChannel):
                        continue
                    try:
                        last_user_message = None
                        async for msg in channel.history(limit=10):
                            if not msg.author.bot:
                                last_user_message = msg
                                break
                        if not last_user_message:
                            log.debug("No user message found in channel %s", channel.name)
                        elif datetime.utcnow() - last_user_message.created_at > PRUNE_THRESHOLD:
                            log.info("Auto-pruning channel '%s' in '%s'.", channel.name, guild.name)
                            await channel.delete(reason="Auto-pruned due to inactivity.")
                    except Exception as e:
                        log.error("Error while pruning channel '%s' in '%s': %s", channel.name, guild.name, e)
            await asyncio.sleep(PRUNE_INTERVAL)

    #####################################################
    # Course Code & Channel Name Processing Helpers
    #####################################################
    def _format_course_key(self, course_key_raw: str) -> Optional[str]:
        """
        Normalize an input course string into a standardized format.
        Accepts various separators (hyphen, underscore, space) and an optional suffix.

        Examples:
          - "socwork-2a06" or "SOCWORK2A06" become "SOCWORK-2A06"
          - "SocWork-2A06A" becomes "SOCWORK-2A06A"
          - Leading/trailing whitespace is removed.
        """
        log.debug("Formatting course key: %s", course_key_raw)
        # The regex accepts:
        #   • A subject (letters)
        #   • Optional separators (space, hyphen, underscore)
        #   • A course number: digits plus an optional inner letter group (e.g., A06)
        #   • An optional trailing suffix (A or B)
        pattern = r"^\s*([A-Za-z]+)[\s\-_]*" r"(\d+(?:[A-Za-z]+\d+)?)([ABab])?\s*$"
        match = re.match(pattern, course_key_raw)
        if not match:
            log.debug("Course key %s did not match expected pattern.", course_key_raw)
            return None
        subject, number, suffix = match.groups()
        subject = subject.upper()
        number = number.upper()
        if suffix:
            suffix = suffix.upper()
        # Build the standardized course code.
        formatted = f"{subject}-{number}" + (suffix if suffix else "")
        log.debug("Formatted course key: %s", formatted)
        return formatted

    def _get_channel_name(self, course_key: str) -> str:
        """
        Generate a Discord channel name from a standardized course code
        by stripping any trailing suffix (A or B) and converting to lowercase.

        Examples:
          - "SOCWORK-2A06A" or "SOCWORK-2A06B" become "socwork-2a06"
          - "SOCWORK-2A06" remains "socwork-2a06"
        """
        # If the code ends with A or B, remove it.
        if course_key and course_key[-1] in ("A", "B"):
            course_key = course_key[:-1]
        channel_name = course_key.lower()
        log.debug("Channel name derived from %s: %s", course_key, channel_name)
        return channel_name

    def _get_course_variants(self, formatted: str) -> List[str]:
        """
        Given a standardized course key (e.g., "SOCWORK-2A06" or "SOCWORK-2A06A"),
        return a list of variants to try for lookup.

        - If no suffix is present:
            Returns [base, base+"A", base+"B"].
          e.g., "SOCWORK-2A06" ->
            ["SOCWORK-2A06", "SOCWORK-2A06A", "SOCWORK-2A06B"]

        - If a suffix is present:
            Returns [original, fallback] where fallback is the opposite suffix.
          e.g., "SOCWORK-2A06A" -> ["SOCWORK-2A06A", "SOCWORK-2A06B"]
                "SOCWORK-2A06B" -> ["SOCWORK-2A06B", "SOCWORK-2A06A"]
        """
        if formatted[-1] in ("A", "B"):
            base = formatted[:-1]
            suffix = formatted[-1]
            fallback = "B" if suffix == "A" else "A"
            variants = [formatted, base + fallback]
        else:
            variants = [formatted, formatted + "A", formatted + "B"]
        log.debug("Course variants for lookup: %s", variants)
        return variants

    async def _lookup_course_data(
        self, formatted: str
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """
        Attempt to fetch course data using fallback suffix logic.

        Process:
          - If the input does not include a suffix, try base, then "A", then "B".
          - If the input includes a suffix, try it first, then the opposite suffix.

        Returns:
          A tuple of (course_code_variant, course_data) if found; otherwise (None, None).
        """
        variants = self._get_course_variants(formatted)
        for variant in variants:
            log.debug("Trying course lookup for variant: %s", variant)
            data = await self.course_data_proxy.get_course_data(variant)
            if data and data.get("course_data"):
                log.debug("Found course data for variant: %s", variant)
                return variant, data
        log.debug("No course data found for any variants of %s", formatted)
        return None, None

    #####################################################
    # Main Command Group: course
    #####################################################
    @commands.group(invoke_without_command=True)
    async def course(self, ctx: commands.Context) -> None:
        """
        Main command group for course functionalities.
        Use help to see available subcommands.
        """
        log.debug("course command group invoked by %s", ctx.author)
        await ctx.send_help(self.course)

    #####################################################
    # Listing Enrollments
    #####################################################
    @course.command(name="list")
    async def list_enrollments(self, ctx: commands.Context) -> None:
        """List all course channels you are currently enrolled in."""
        log.debug("Listing enrollments for user %s in guild %s", ctx.author, ctx.guild.name)
        courses = self.get_user_courses(ctx.author, ctx.guild)
        if courses:
            await ctx.send("You are enrolled in the following courses:\n" + "\n".join(courses))
        else:
            await ctx.send("You are not enrolled in any courses.")

    #####################################################
    # Refreshing Course Data
    #####################################################
    @course.command()
    async def refresh(self, ctx: commands.Context, course_code: str) -> None:
        """
        Force refresh the course data for a specified course.
        Example: `!course refresh MATH 1A03`
        """
        formatted = self._format_course_key(course_code)
        log.debug("Refresh command invoked for course code %s (formatted: %s)", course_code, formatted)
        if not formatted:
            await ctx.send(error(f"Invalid course code: {course_code}."))
            return
        # Use fallback lookup to try all variants.
        variant, data = await self._lookup_course_data(formatted)
        if not variant or not (data and data.get("course_data")):
            await ctx.send(error(f"Failed to refresh course data for {formatted}."))
            return
        # Mark the data as stale using the variant that returned data.
        await self.config.courses.set_raw(variant, value={"is_fresh": False})
        async with ctx.typing():
            data = await self.course_data_proxy.get_course_data(variant)
        if data and data.get("course_data"):
            await ctx.send(success(f"Course data for {variant} refreshed successfully."))
        else:
            await ctx.send(error(f"Failed to refresh course data for {variant}."))

    #####################################################
    # Channel Management Commands
    #####################################################
    @course.command()
    async def join(self, ctx: commands.Context, course_code: str) -> None:
        """
        Join a course channel.
        Validates the course code, checks enrollment limits and duplicate joins, and sets per-user permissions.
        """
        formatted: Optional[str] = self._format_course_key(course_code)
        log.debug("%s attempting to join course: %s", ctx.author, formatted)
        if not formatted:
            await ctx.send(error(f"Invalid course code: {course_code}."))
            return

        async with ctx.typing():
            variant, data = await self._lookup_course_data(formatted)
        if not variant or not (data and data.get("course_data")):
            await ctx.send(error(f"Error: No valid course data found for {formatted}."))
            return

        # Duplicate join prevention.
        if variant.upper() in self.get_user_courses(ctx.author, ctx.guild):
            await ctx.send(info(f"You are already enrolled in {variant}."))
            return

        if len(self.get_user_courses(ctx.author, ctx.guild)) >= self.max_courses:
            await ctx.send(error(f"You have reached the maximum limit of {self.max_courses} courses. Leave one before joining another."))
            return

        category = self.get_category(ctx.guild)
        if category is None:
            try:
                category = await ctx.guild.create_category(self.category_name)
                log.debug("Created new category '%s' in guild %s", self.category_name, ctx.guild.name)
            except discord.Forbidden:
                await ctx.send(error("I don't have permission to create the courses category."))
                return

        # Use the base channel name (without any suffix) for channel operations.
        channel = self.get_course_channel(ctx.guild, variant)
        if not channel:
            log.debug("Course channel for %s does not exist. Creating new channel.", variant)
            channel = await self.create_course_channel(ctx.guild, category, variant)

        try:
            await channel.set_permissions(ctx.author, overwrite=self.channel_permissions)
            log.debug("Set permissions for user %s on channel %s", ctx.author, channel.name)
        except discord.Forbidden:
            await ctx.send(error("I don't have permission to manage channel permissions."))
            return

        await ctx.send(success(f"You have successfully joined {variant}."), delete_after=120)
        if self.logging_channel:
            await self.logging_channel.send(f"{ctx.author} has joined {variant}.")

    @course.command()
    async def leave(self, ctx: commands.Context, course_code: str) -> None:
        """
        Leave a course channel by removing your permission override.
        """
        formatted = self._format_course_key(course_code)
        log.debug("%s attempting to leave course: %s", ctx.author, formatted)
        if not formatted:
            await ctx.send(error(f"Invalid course code: {course_code}."))
            return

        channel = self.get_course_channel(ctx.guild, formatted)
        if not channel:
            await ctx.send(error(f"You are not a member of {formatted}."))
            return

        try:
            await channel.set_permissions(ctx.author, overwrite=None)
            log.debug("Removed permissions for user %s on channel %s", ctx.author, channel.name)
        except discord.Forbidden:
            await ctx.send(error("I don't have permission to manage channel permissions."))
            return

        await ctx.send(success(f"You have successfully left {formatted}."), delete_after=120)
        if self.logging_channel:
            await self.logging_channel.send(f"{ctx.author} has left {formatted}.")

    #####################################################
    # Admin Commands: Delete and Set Logging
    #####################################################
    @commands.admin()
    @course.command()
    async def delete(self, ctx: commands.Context, channel: discord.TextChannel) -> None:
        """Delete a course channel (admin-only)."""
        log.debug("Admin %s attempting to delete channel %s", ctx.author, channel.name)
        if not channel.category or channel.category.name != self.category_name:
            await ctx.send(error(f"{channel.mention} is not a course channel."))
            return
        try:
            await channel.delete()
            log.debug("Channel %s deleted by admin %s", channel.name, ctx.author)
        except discord.Forbidden:
            await ctx.send(error("I don't have permission to delete that channel."))
            return
        await ctx.send(success(f"{channel.name} has been successfully deleted."))
        if self.logging_channel:
            await self.logging_channel.send(f"{channel.name} has been deleted.")

    @commands.admin()
    @course.command(name="setlogging")
    async def set_logging(self, ctx: commands.Context, channel: discord.TextChannel) -> None:
        """Set the logging channel for join/leave notifications (admin-only)."""
        self.logging_channel = channel
        log.debug("Logging channel set to %s by admin %s", channel.name, ctx.author)
        await ctx.send(success(f"Logging channel set to {channel.mention}."))

    #####################################################
    # Course Details Commands
    #####################################################
    @course.command(name="details")
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def course_details(self, ctx: commands.Context, *, course_key_raw: str) -> None:
        """
        Display details for a specified course.
        Example: `!course details MATH 1A03`
        """
        formatted = self._format_course_key(course_key_raw)
        log.debug("Fetching course details for %s (formatted: %s)", course_key_raw, formatted)
        if not formatted:
            await ctx.send(error(f"Invalid course code: {course_key_raw}. Use format like 'MATH 1A03'."))
            return

        variant, data = await self._lookup_course_data(formatted)
        if not variant or not (data and data.get("course_data")):
            await ctx.send(error(f"Course not found: {formatted}"))
            return

        embed = self._create_course_embed(variant, data)
        await ctx.send(embed=embed)

    @course.command(name="multidetails")
    @commands.cooldown(1, 10, commands.BucketType.user)
    async def multi_course_details(self, ctx: commands.Context, *course_codes: str) -> None:
        """
        Fetch details for multiple courses concurrently.
        Example: `!course multidetails MATH 1A03 PHYSICS 1B03`

        For each code, fallback lookup is performed:
          - "SOCWORK-2A06" -> try base, then "SOCWORK-2A06A", then "SOCWORK-2A06B"
          - "SOCWORK-2A06A" -> try "SOCWORK-2A06A", then "SOCWORK-2A06B", etc.
        """
        log.debug("multi_course_details invoked by %s with codes: %s", ctx.author, course_codes)
        if not course_codes:
            await ctx.send(info("You must specify at least one course code."))
            return

        valid_courses: List[str] = []
        for code in course_codes:
            formatted = self._format_course_key(code)
            if formatted:
                valid_courses.append(formatted)
            else:
                await ctx.send(warning(f"Skipping invalid code: {code}"))

        log.debug("Valid courses for multidetails: %s", valid_courses)
        # Launch fallback lookup for each course.
        tasks = [self._lookup_course_data(vc) for vc in valid_courses]
        results = await bounded_gather(*tasks, limit=3)

        output_lines: List[str] = []
        for idx, (resolved_code, res) in enumerate(results):
            if not resolved_code or not (res and res.get("course_data")):
                output_lines.append(error(f"No data found for {valid_courses[idx]}."))
                continue

            data_item = res["course_data"][0]
            updated = res.get("date_added", "Unknown")
            freshness_icon = "🟢" if res.get("is_fresh", False) else "🔴"
            text_block = (
                f"{freshness_icon} **{resolved_code}**\n"
                f"  Title: {data_item.get('title', '')}\n"
                f"  Instructor: {data_item.get('teacher', '')}\n"
                f"  Term: {data_item.get('term_found', '')}\n"
                f"  Last Updated: {updated}\n"
            )
            output_lines.append(box(text_block, lang="md"))

        final_text = "\n".join(output_lines)
        pages = list(pagify(final_text, page_length=2000))
        if len(pages) == 1:
            await ctx.send(pages[0])
        else:
            await menu(ctx, pages, DEFAULT_CONTROLS)

    #####################################################
    # Utility Functions
    #####################################################
    def _create_course_embed(self, course_key: str, course_data: Dict[str, Any]) -> discord.Embed:
        """
        Build a Discord embed to display comprehensive course details.
        """
        log.debug("Creating course embed for %s", course_key)
        embed = discord.Embed(title=f"Course Details: {course_key}", color=discord.Color.green())
        data_item = course_data.get("course_data", [{}])[0]
        is_fresh = course_data.get("is_fresh", False)
        date_added = course_data.get("date_added", "Unknown")
        footer_icon = "🟢" if is_fresh else "🔴"
        embed.set_footer(text=f"{footer_icon} Last updated: {date_added}")

        # Basic fields
        basic_fields = [
            ("Title", data_item.get("title", "")),
            ("Term", data_item.get("term_found", "")),
            ("Instructor", data_item.get("teacher", "")),
            ("Code", data_item.get("course_code", "")),
            ("Number", data_item.get("course_number", "")),
            ("Credits", data_item.get("credits", "")),
        ]
        for name, value in basic_fields:
            if value:
                embed.add_field(name=name, value=value, inline=True)

        # Longer text fields: description, prerequisites, antirequisites
        if data_item.get("description"):
            embed.add_field(name="Description", value=data_item.get("description"), inline=False)
        if data_item.get("prerequisites"):
            embed.add_field(name="Prerequisite(s)", value=data_item.get("prerequisites"), inline=True)
        if data_item.get("antirequisites"):
            embed.add_field(name="Antirequisite(s)", value=data_item.get("antirequisites"), inline=True)

        return embed

    def get_category(self, guild: discord.Guild) -> Optional[discord.CategoryChannel]:
        """
        Find the category named self.category_name in the given guild.
        """
        log.debug("Searching for category '%s' in guild %s", self.category_name, guild.name)
        for category in guild.categories:
            if category.name == self.category_name:
                log.debug("Found category '%s' in guild %s", self.category_name, guild.name)
                return category
        log.debug("Category '%s' not found in guild %s", self.category_name, guild.name)
        return None

    def get_course_channel(self, guild: discord.Guild, course_key: str) -> Optional[discord.TextChannel]:
        """
        Retrieve a course channel by its standardized course key.
        Uses _get_channel_name to ensure the channel name is derived from the base course code.
        """
        category = self.get_category(guild)
        if not category:
            log.debug("No category found in guild %s when searching for course channel %s", guild.name, course_key)
            return None
        target_name = self._get_channel_name(course_key)
        for channel in category.channels:
            if channel.name == target_name:
                log.debug("Found course channel %s in guild %s", channel.name, guild.name)
                return channel
        log.debug("Course channel %s not found in guild %s", target_name, guild.name)
        return None

    async def create_course_channel(
        self, guild: discord.Guild, category: discord.CategoryChannel, course_key: str
    ) -> discord.TextChannel:
        """
        Create a new course channel under the courses category.
        The channel name is derived from the base course code (without suffix) and is lowercase.
        """
        target_name = self._get_channel_name(course_key)
        log.debug("Creating course channel for %s in guild %s", target_name, guild.name)
        overwrites = {
            guild.default_role: discord.PermissionOverwrite.none(),
            guild.me: discord.PermissionOverwrite.all(),
        }
        channel = await guild.create_text_channel(target_name, overwrites=overwrites, category=category)
        log.debug("Created course channel %s in guild %s", channel.name, guild.name)
        return channel

    def get_user_courses(self, user: discord.Member, guild: discord.Guild) -> List[str]:
        """
        List the course channels the user currently has access to in the guild.
        """
        category = self.get_category(guild)
        if not category:
            log.debug("No category found in guild %s when listing courses for user %s", guild.name, user)
            return []
        courses = [
            channel.name.upper()
            for channel in category.channels
            if isinstance(channel, discord.TextChannel) and channel.permissions_for(user).read_messages
        ]
        log.debug("User %s has access to courses: %s", user, courses)
        return courses

    #####################################################
    # Developer Commands (Owner-only)
    #####################################################
    @commands.is_owner()
    @commands.group(name="dc", invoke_without_command=True)
    async def dev_course(self, ctx: commands.Context) -> None:
        """Developer commands for managing config data for the course cog."""
        log.debug("Developer command group 'dev_course' invoked by %s", ctx.author)
        await ctx.send_help(self.dev_course)

    @dev_course.command(name="term")
    async def set_term_codes(self, ctx: commands.Context, term_name: str, term_id: int) -> None:
        """
        Set the term code for a specified term.
        Example: `!dc term winter 2241`
        """
        async with self.config.term_codes() as term_codes:
            term_codes[term_name.lower()] = term_id
        log.debug("Set term code for %s to %s", term_name, term_id)
        await ctx.send(success(f"Term code for {term_name.capitalize()} set to: {term_id}"))

    @dev_course.command(name="clearstale")
    async def clear_stale_config(self, ctx: commands.Context) -> None:
        """
        Clear stale course config entries that no longer have a corresponding channel.
        """
        log.debug("Clearing stale config entries.")
        stale = []
        courses = await self.config.courses.all()
        for course_key in courses.keys():
            found = False
            for guild in self.bot.guilds:
                if self.get_course_channel(guild, course_key):
                    found = True
                    break
            if not found:
                stale.append(course_key)
        for course_key in stale:
            await self.config.courses.clear_raw(course_key)
            log.debug("Cleared stale config entry for course %s", course_key)
        if stale:
            await ctx.send(success(f"Cleared stale config entries for courses: {', '.join(stale)}"))
        else:
            await ctx.send(info("No stale course config entries found."))

    @dev_course.command(name="prune")
    async def manual_prune(self, ctx: commands.Context) -> None:
        """
        Manually trigger the auto-prune process for inactive course channels.
        """
        log.debug("Manual prune triggered by %s", ctx.author)
        pruned_channels = []
        PRUNE_THRESHOLD = timedelta(days=120)
        for guild in self.bot.guilds:
            category = self.get_category(guild)
            if not category:
                continue
            for channel in category.channels:
                if not isinstance(channel, discord.TextChannel):
                    continue
                try:
                    last_user_message = None
                    async for msg in channel.history(limit=10):
                        if not msg.author.bot:
                            last_user_message = msg
                            break
                    if not last_user_message or (datetime.utcnow() - last_user_message.created_at > PRUNE_THRESHOLD):
                        pruned_channels.append(f"{guild.name} - {channel.name}")
                        log.debug("Manually pruning channel %s in guild %s", channel.name, guild.name)
                        await channel.delete(reason="Manually pruned due to inactivity.")
                except Exception as e:
                    log.error("Error pruning channel '%s' in '%s': %s", channel.name, guild.name, e)
        if pruned_channels:
            await ctx.send(success("Manually pruned channels:\n" + "\n".join(pruned_channels)))
        else:
            await ctx.send(info("No inactive channels to prune."))

    @dev_course.command(name="printconfig")
    async def print_config(self, ctx: commands.Context) -> None:
        """
        Print the entire global config to the console.
        """
        cfg = await self.config.all()
        log.debug("Current config: %s", cfg)
        await ctx.send(info("Config has been printed to the console log."))

    @dev_course.command(name="clearcourses")
    async def clear_courses(self, ctx: commands.Context) -> None:
        """
        Clear all cached course data from the config.
        """
        await self.config.courses.set({})
        log.debug("All course data cleared from config by %s", ctx.author)
        await ctx.send(warning("All courses have been cleared from the config."))
