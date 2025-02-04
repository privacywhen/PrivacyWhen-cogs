import asyncio
import re
import logging
from math import floor
from time import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import discord
from redbot.core import commands, Config
from redbot.core.utils import bounded_gather
from redbot.core.utils.chat_formatting import box, error, info, success, warning, pagify
from redbot.core.utils.menus import menu, DEFAULT_CONTROLS
from aiohttp import ClientConnectionError, ClientResponseError, ClientSession, ClientTimeout
from bs4 import BeautifulSoup

# Configure logging
log = logging.getLogger("red.course_helper")
log.setLevel(logging.DEBUG)
if not log.handlers:
    log.addHandler(logging.StreamHandler())

# Constant for course key pattern.
COURSE_KEY_PATTERN = re.compile(
    r"^\s*([A-Za-z]+)[\s\-_]*(\d+(?:[A-Za-z]+\d+)?)([ABab])?\s*$"
)


###############################################################################
# CourseDataProxy
###############################################################################
class CourseDataProxy:
    """
    Handles fetching and caching of course data from an external endpoint.
    Cached data is stored in config under the global key 'courses'.
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
        Otherwise, fetch it from the remote API, cache it, and return the new data.
        """
        log.debug("Retrieving course data for %s", course_key_formatted)
        course_data = await self.config.courses.get_raw(course_key_formatted, default=None)
        if not course_data or not course_data.get("is_fresh", False):
            log.debug("Course data missing/stale for %s; fetching online.", course_key_formatted)
            soup, error_msg = await self._fetch_course_online(course_key_formatted)
            if soup:
                processed_data = self._process_soup_content(soup)
                new_data = {
                    "course_data": processed_data,
                    "date_added": date.today().isoformat(),
                    "is_fresh": True,
                }
                await self.config.courses.set_raw(course_key_formatted, value=new_data)
                log.debug("Fetched and cached data for %s: %s", course_key_formatted, new_data)
                course_data = await self.config.courses.get_raw(course_key_formatted, default=None)
            elif error_msg:
                log.error("Error fetching data for %s: %s", course_key_formatted, error_msg)
                return {}
        else:
            log.debug("Using cached data for %s", course_key_formatted)
        return course_data or {}

    async def _fetch_course_online(
        self, course_key_formatted: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """Fetch course data from the external API."""
        log.debug("Fetching online data for %s", course_key_formatted)
        term_order = self._determine_term_order()
        log.debug("Term order: %s", term_order)
        soup, error_message = await self._fetch_data_with_retries(term_order, course_key_formatted)
        return (soup, None) if soup else (None, error_message)

    def _determine_term_order(self) -> List[str]:
        """
        Determine a prioritized list of term names based on the current month.
        Roughly divides the year into three terms.
        """
        now = date.today()
        current_term_index = (now.month - 1) // 4
        term_order = self._TERM_NAMES[current_term_index:] + self._TERM_NAMES[:current_term_index]
        log.debug("Date: %s, term order: %s", now, term_order)
        return term_order

    async def _fetch_data_with_retries(
        self, term_order: List[str], course_key_formatted: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """
        Try to fetch data using multiple term IDs with a retry mechanism.
        """
        max_retries = 1
        retry_delay = 5
        url: Optional[str] = None

        for term_name in term_order:
            term_id = await self._get_term_id(term_name)
            if not term_id:
                log.debug("Term ID not found for term: %s", term_name)
                continue

            log.debug("Using term '%s' with ID %s", term_name, term_id)
            url = self._build_url(term_id, course_key_formatted)
            log.debug("Built URL: %s", url)
            for retry_count in range(max_retries):
                log.debug("Attempt %s for URL: %s", retry_count + 1, url)
                try:
                    soup, error_message = await self._fetch_single_attempt(url)
                    if soup:
                        log.debug("Successfully fetched data from %s", url)
                        return soup, None
                    elif error_message:
                        log.debug("Received error: %s", error_message)
                        if "not found" in error_message.lower():
                            log.error("Course not found: %s", course_key_formatted)
                            return None, error_message
                        if retry_count == max_retries - 1:
                            return None, error_message
                        log.debug("Retrying in %s seconds...", retry_delay)
                        await asyncio.sleep(retry_delay)
                except (ClientResponseError, ClientConnectionError, asyncio.TimeoutError) as error:
                    log.error("Exception during fetch from %s: %s", url, error)
                    if retry_count == max_retries - 1:
                        return None, "Error: Issue occurred while fetching course data."
                    log.debug("Retrying in %s seconds...", retry_delay)
                    await asyncio.sleep(retry_delay)
        if url:
            log.error("Max retries reached for %s", url)
        return None, "Error: Max retries reached while fetching course data."

    async def _get_term_id(self, term_name: str) -> Optional[int]:
        """Retrieve the term code from the config."""
        log.debug("Retrieving term ID for: %s", term_name)
        term_id = await self.config.term_codes.get_raw(term_name, default=None)
        log.debug("Term ID for %s: %s", term_name, term_id)
        return term_id

    def _build_url(self, term_id: int, course_key_formatted: str) -> str:
        """Construct the URL for the course data query."""
        t, e = self._generate_time_code()
        url = self._URL_BASE.format(term=term_id, course_key_formatted=course_key_formatted, t=t, e=e)
        log.debug("Generated URL with t=%s, e=%s: %s", t, e, url)
        return url

    def _generate_time_code(self) -> Tuple[int, int]:
        """Generate a time-based code for the remote API."""
        t = floor(time() / 60) % 1000
        e = t % 3 + t % 39 + t % 42
        log.debug("Generated time codes: t=%s, e=%s", t, e)
        return t, e

    async def _fetch_single_attempt(self, url: str) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """Perform a single HTTP GET request to fetch course data."""
        log.debug("HTTP GET: %s", url)
        timeout = ClientTimeout(total=15)
        try:
            async with ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    log.debug("Response %s from URL: %s", response.status, url)
                    if response.status != 200:
                        return None, f"Error: HTTP {response.status}"
                    content = await response.text()
                    soup = BeautifulSoup(content, "xml")
                    if not (error_tag := soup.find("error")):
                        log.debug("No error tag in response for %s", url)
                        return soup, None
                    error_message = error_tag.text.strip()
                    log.debug("Error tag found: %s", error_message)
                    return None, error_message or None
        except Exception as e:
            log.error("Exception during HTTP GET from %s: %s", url, e)
            return None, str(e)

    def _process_soup_content(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Parse the BeautifulSoup object to extract course data including title, term,
        credits, description, prerequisites, and antirequisites.
        """
        courses = soup.find_all("course")
        log.debug("Processing soup: found %s course entries.", len(courses))
        processed_courses = []
        for course in courses:
            offering = course.find("offering")
            title = offering.get("title", "") if offering else ""
            desc_attr = offering.get("desc", "") if offering else ""
            description = ""
            prerequisites = ""
            antirequisites = ""
            if desc_attr:
                # Split description using <br> tags (handles various formats)
                desc_parts = [part.strip() for part in re.split(r"<br\s*/?>", desc_attr) if part.strip()]
                if desc_parts:
                    description = desc_parts[0]
                for part in desc_parts:
                    lower = part.lower()
                    if lower.startswith("prerequisite"):
                        prerequisites = part.split(":", 1)[1].strip() if ":" in part else ""
                    elif lower.startswith("antirequisite"):
                        antirequisites = part.split(":", 1)[1].strip() if ":" in part else ""
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
    Manages course channels and details.

    Features:
      • Users can join/leave course channels under the "COURSES" category.
      • Auto-prunes inactive channels.
      • Retrieves and caches course data via an external API.
      • Provides commands to refresh course data and view details.
      • Owner-only commands to manage term codes and stale config.
    """

    def __init__(self, bot: commands.Bot) -> None:
        self.bot: commands.Bot = bot

        # Channel management settings.
        # Allowed permissions for users are defined by the bitmask (446676945984) and no denied permissions.
        self.channel_permissions: discord.PermissionOverwrite = discord.PermissionOverwrite.from_pair(
            discord.Permissions(446676945984),
            discord.Permissions(0),
        )
        self.category_name: str = "COURSES"
        self.max_courses: int = 10
        self.logging_channel: Optional[discord.TextChannel] = None

        # Global defaults for config.
        default_global: Dict[str, Any] = {"term_codes": {}, "courses": {}}
        self.config: Config = Config.get_conf(self, identifier=3720194665, force_registration=True)
        self.config.register_global(**default_global)
        self.course_data_proxy: CourseDataProxy = CourseDataProxy(self.config)

        # Start the background auto-prune task.
        self._prune_task: asyncio.Task = self.bot.loop.create_task(self._auto_prune_task())
        log.debug("CourseManager initialized with max_courses=%s", self.max_courses)

    def cog_unload(self) -> None:
        """Cancel the auto-prune task when the cog unloads."""
        log.debug("Unloading CourseManager cog; cancelling auto-prune task.")
        self._prune_task.cancel()

    async def _auto_prune_task(self) -> None:
        """
        Background task to auto-prune inactive course channels (no non-bot messages in 120 days).
        """
        PRUNE_INTERVAL = 3600  # every hour
        PRUNE_THRESHOLD = timedelta(days=120)
        await self.bot.wait_until_ready()
        log.debug("Auto-prune task started.")
        while not self.bot.is_closed():
            for guild in self.bot.guilds:
                category = self.get_category(guild)
                if not category:
                    log.debug("Category '%s' not found in guild %s", self.category_name, guild.name)
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
                            log.debug("No user messages in channel %s", channel.name)
                        elif datetime.utcnow() - last_user_message.created_at > PRUNE_THRESHOLD:
                            log.info("Auto-pruning channel '%s' in '%s'", channel.name, guild.name)
                            await channel.delete(reason="Auto-pruned due to inactivity.")
                    except Exception as e:
                        log.error("Error pruning channel '%s' in '%s': %s", channel.name, guild.name, e)
            await asyncio.sleep(PRUNE_INTERVAL)

    #####################################################
    # Course Code & Channel Name Helpers
    #####################################################
    def _format_course_key(self, course_key_raw: str) -> Optional[str]:
        """
        Normalize an input course string into a standardized format.
        Accepts various separators (hyphen, underscore, space) and an optional suffix.
        
        Examples:
          - "socwork-2a06" or "SOCWORK2A06"  -> "SOCWORK-2A06"
          - "SocWork-2A06A"                  -> "SOCWORK-2A06A"
        """
        log.debug("Formatting course key: %s", course_key_raw)
        match = COURSE_KEY_PATTERN.match(course_key_raw)
        if not match:
            log.debug("Input '%s' does not match expected course pattern.", course_key_raw)
            return None
        subject, number, suffix = match.groups()
        subject = subject.upper()
        number = number.upper()
        formatted = f"{subject}-{number}" + (suffix.upper() if suffix else "")
        log.debug("Formatted course key: %s", formatted)
        return formatted

    def _get_channel_name(self, course_key: str) -> str:
        """
        Return a Discord channel name from a standardized course code by removing any trailing suffix
        and converting it to lowercase.
        
        Examples:
          - "SOCWORK-2A06A" or "SOCWORK-2A06B"  -> "socwork-2a06"
          - "SOCWORK-2A06"                       -> "socwork-2a06"
        """
        if course_key and course_key[-1] in ("A", "B"):
            course_key = course_key[:-1]
        channel_name = course_key.lower()
        log.debug("Derived channel name: %s", channel_name)
        return channel_name

    def _get_course_variants(self, formatted: str) -> List[str]:
        """
        Given a standardized course key (with or without suffix), return variants for lookup.
        
        If no suffix is present:
          Returns [base, base+"A", base+"B"].
        If a suffix is present:
          Returns [original, fallback] (e.g. "SOCWORK-2A06A" → ["SOCWORK-2A06A", "SOCWORK-2A06B"]).
        """
        if formatted[-1] in ("A", "B"):
            base = formatted[:-1]
            suffix = formatted[-1]
            fallback = "B" if suffix == "A" else "A"
            variants = [formatted, base + fallback]
        else:
            variants = [formatted, formatted + "A", formatted + "B"]
        log.debug("Lookup variants for '%s': %s", formatted, variants)
        return variants

    async def _lookup_course_data(
        self, formatted: str
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """
        Attempt to fetch course data using fallback suffix logic.
        If the input has no suffix, try base, then "A", then "B".
        If the input has a suffix, try it first then the opposite.
        Returns a tuple (course_variant, data) or (None, None) if not found.
        """
        for variant in self._get_course_variants(formatted):
            log.debug("Trying lookup for variant: %s", variant)
            data = await self.course_data_proxy.get_course_data(variant)
            if data and data.get("course_data"):
                log.debug("Found course data for %s", variant)
                return variant, data
        log.debug("No course data found for variants of %s", formatted)
        return None, None

    #####################################################
    # Command Group: course
    #####################################################
    @commands.group(invoke_without_command=True)
    async def course(self, ctx: commands.Context) -> None:
        """Main command group for course functionalities."""
        log.debug("Course command group invoked by %s", ctx.author)
        await ctx.send_help(self.course)

    @course.command(name="list")
    async def list_enrollments(self, ctx: commands.Context) -> None:
        """List all course channels you are currently enrolled in."""
        log.debug("Listing courses for user %s in guild %s", ctx.author, ctx.guild.name)
        courses = self.get_user_courses(ctx.author, ctx.guild)
        if courses:
            await ctx.send("You are enrolled in the following courses:\n" + "\n".join(courses))
        else:
            await ctx.send("You are not enrolled in any courses.")

    @course.command()
    async def refresh(self, ctx: commands.Context, course_code: str) -> None:
        """
        Force refresh the course data for a specified course.
        Example: `!course refresh MATH 1A03`
        """
        formatted = self._format_course_key(course_code)
        log.debug("Refresh invoked for '%s' (formatted: %s)", course_code, formatted)
        if not formatted:
            await ctx.send(error(f"Invalid course code: {course_code}."))
            return
        variant, data = await self._lookup_course_data(formatted)
        if not variant or not (data and data.get("course_data")):
            await ctx.send(error(f"Failed to refresh data for {formatted}."))
            return
        await self.config.courses.set_raw(variant, value={"is_fresh": False})
        async with ctx.typing():
            data = await self.course_data_proxy.get_course_data(variant)
        if data and data.get("course_data"):
            await ctx.send(success(f"Course data for {variant} refreshed successfully."))
        else:
            await ctx.send(error(f"Failed to refresh course data for {variant}."))

    @course.command()
    async def join(self, ctx: commands.Context, *, course_code: str) -> None:
        """
        Join a course channel.
        Validates the course code, checks enrollment limits, and sets permissions.
        """
        formatted = self._format_course_key(course_code)
        log.debug("%s attempting to join course: %s", ctx.author, formatted)
        if not formatted:
            await ctx.send(error(f"Invalid course code: {course_code}."))
            return

        async with ctx.typing():
            variant, data = await self._lookup_course_data(formatted)
        if not variant or not (data and data.get("course_data")):
            await ctx.send(error(f"No valid course data found for {formatted}."))
            return

        if variant.upper() in self.get_user_courses(ctx.author, ctx.guild):
            await ctx.send(info(f"You are already enrolled in {variant}."))
            return

        if len(self.get_user_courses(ctx.author, ctx.guild)) >= self.max_courses:
            await ctx.send(
                error(f"You have reached the maximum limit of {self.max_courses} courses. Leave one to join another.")
            )
            return

        category = self.get_category(ctx.guild)
        if category is None:
            try:
                category = await ctx.guild.create_category(self.category_name)
                log.debug("Created category '%s' in guild %s", self.category_name, ctx.guild.name)
            except discord.Forbidden:
                await ctx.send(error("I don't have permission to create the courses category."))
                return

        channel = self.get_course_channel(ctx.guild, variant)
        if not channel:
            log.debug("Course channel for %s not found; creating new channel.", variant)
            channel = await self.create_course_channel(ctx.guild, category, variant)

        try:
            await channel.set_permissions(ctx.author, overwrite=self.channel_permissions)
            log.debug("Permissions set for %s on channel %s", ctx.author, channel.name)
        except discord.Forbidden:
            await ctx.send(error("I don't have permission to manage channel permissions."))
            return

        await ctx.send(success(f"You have successfully joined {variant}."), delete_after=120)
        if self.logging_channel:
            await self.logging_channel.send(f"{ctx.author} has joined {variant}.")

    @course.command()
    async def leave(self, ctx: commands.Context, course_code: str) -> None:
        """Leave a course channel by removing your permission override."""
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
            log.debug("Removed permissions for %s on channel %s", ctx.author, channel.name)
        except discord.Forbidden:
            await ctx.send(error("I don't have permission to manage channel permissions."))
            return

        await ctx.send(success(f"You have successfully left {formatted}."), delete_after=120)
        if self.logging_channel:
            await self.logging_channel.send(f"{ctx.author} has left {formatted}.")

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

    @course.command(name="details")
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def course_details(self, ctx: commands.Context, *, course_key_raw: str) -> None:
        """
        Display details for a specified course.
        Example: `!course details MATH 1A03`
        """
        formatted = self._format_course_key(course_key_raw)
        log.debug("Fetching details for '%s' (formatted: %s)", course_key_raw, formatted)
        if not formatted:
            await ctx.send(error(f"Invalid course code: {course_key_raw}. Use a format like 'MATH 1A03'."))
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
        Fallback lookup is performed for each code.
        """
        log.debug("Multi-details invoked by %s with codes: %s", ctx.author, course_codes)
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
        """Build a Discord embed with comprehensive course details."""
        log.debug("Creating embed for course: %s", course_key)
        embed = discord.Embed(title=f"Course Details: {course_key}", color=discord.Color.green())
        data_item = course_data.get("course_data", [{}])[0]
        is_fresh = course_data.get("is_fresh", False)
        date_added = course_data.get("date_added", "Unknown")
        footer_icon = "🟢" if is_fresh else "🔴"
        embed.set_footer(text=f"{footer_icon} Last updated: {date_added}")

        for name, value in [
            ("Title", data_item.get("title", "")),
            ("Term", data_item.get("term_found", "")),
            ("Instructor", data_item.get("teacher", "")),
            ("Code", data_item.get("course_code", "")),
            ("Number", data_item.get("course_number", "")),
            ("Credits", data_item.get("credits", "")),
        ]:
            if value:
                embed.add_field(name=name, value=value, inline=True)

        if data_item.get("description"):
            embed.add_field(name="Description", value=data_item.get("description"), inline=False)
        if data_item.get("prerequisites"):
            embed.add_field(name="Prerequisite(s)", value=data_item.get("prerequisites"), inline=True)
        if data_item.get("antirequisites"):
            embed.add_field(name="Antirequisite(s)", value=data_item.get("antirequisites"), inline=True)

        return embed

    def get_category(self, guild: discord.Guild) -> Optional[discord.CategoryChannel]:
        """
        Find and return the category matching self.category_name in the guild.
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
        Retrieve the course channel by its base (formatted) course key.
        """
        category = self.get_category(guild)
        if not category:
            log.debug("No category in guild %s for course key %s", guild.name, course_key)
            return None
        target_name = self._get_channel_name(course_key)
        for channel in category.channels:
            if channel.name == target_name:
                log.debug("Found course channel '%s' in guild %s", channel.name, guild.name)
                return channel
        log.debug("Course channel '%s' not found in guild %s", target_name, guild.name)
        return None

    async def create_course_channel(
        self, guild: discord.Guild, category: discord.CategoryChannel, course_key: str
    ) -> discord.TextChannel:
        """
        Create a new course channel under the designated category.
        The channel is created with no permissions for @everyone (hidden by default).
        Only the bot is given full permissions until a user joins via the join command.
        """
        target_name = self._get_channel_name(course_key)
        log.debug("Creating channel '%s' in guild %s", target_name, guild.name)
        # Deny view_channel for @everyone.
        # For the bot, we grant full permissions by setting administrator=True.
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            guild.me: discord.PermissionOverwrite(administrator=True),
        }
        channel = await guild.create_text_channel(target_name, overwrites=overwrites, category=category)
        log.debug("Created channel '%s' in guild %s", channel.name, guild.name)
        return channel

    def get_user_courses(self, user: discord.Member, guild: discord.Guild) -> List[str]:
        """
        Return a list of course channels (by name) that the user can access.
        """
        category = self.get_category(guild)
        if not category:
            log.debug("No category in guild %s for user %s", guild.name, user)
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
        """Developer commands for managing course config data."""
        log.debug("Dev command group 'dev_course' invoked by %s", ctx.author)
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
        """Clear stale course config entries that no longer correspond to a channel."""
        log.debug("Clearing stale config entries.")
        stale = []
        courses = await self.config.courses.all()
        for course_key in courses.keys():
            if not any(self.get_course_channel(guild, course_key) for guild in self.bot.guilds):
                stale.append(course_key)
        for course_key in stale:
            await self.config.courses.clear_raw(course_key)
            log.debug("Cleared stale entry for course %s", course_key)
        if stale:
            await ctx.send(success(f"Cleared stale config entries: {', '.join(stale)}"))
        else:
            await ctx.send(info("No stale course config entries found."))

    @dev_course.command(name="prune")
    async def manual_prune(self, ctx: commands.Context) -> None:
        """Manually trigger the auto-prune process for inactive course channels."""
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
                        log.debug("Pruning channel %s in guild %s", channel.name, guild.name)
                        await channel.delete(reason="Manually pruned due to inactivity.")
                except Exception as e:
                    log.error("Error pruning channel '%s' in '%s': %s", channel.name, guild.name, e)
        if pruned_channels:
            await ctx.send(success("Pruned channels:\n" + "\n".join(pruned_channels)))
        else:
            await ctx.send(info("No inactive channels to prune."))

    @dev_course.command(name="printconfig")
    async def print_config(self, ctx: commands.Context) -> None:
        """Print the entire global config to the console."""
        cfg = await self.config.all()
        log.debug("Current config: %s", cfg)
        await ctx.send(info("Config has been printed to the console log."))

    @dev_course.command(name="clearcourses")
    async def clear_courses(self, ctx: commands.Context) -> None:
        """Clear all cached course data from the config."""
        await self.config.courses.set({})
        log.debug("All course data cleared by %s", ctx.author)
        await ctx.send(warning("All courses have been cleared from the config."))
