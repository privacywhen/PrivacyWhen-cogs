import asyncio
import re
import logging
from math import floor
from time import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

import discord
from discord.ext import commands
from redbot.core import checks, Config
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
        "https://mytimetable.mcmaster.ca/getclassdata.jsp?"
        "term={term}&course_0_0={course_key_formatted}&t={t}&e={e}"
    )

    def __init__(self, config: Config) -> None:
        """
        Initialize the proxy with the bot's Config instance.
        """
        self.config: Config = config

    async def get_course_data(self, course_key_formatted: str) -> Dict[str, Any]:
        """
        Retrieve course data from config if available and fresh.
        Otherwise, fetch from the remote API, cache it, and return the new data.
        
        :param course_key_formatted: Standardized course key (e.g., "MATH-1A03")
        :return: A dictionary containing course details.
        """
        course_data = await self.config.courses.get_raw(course_key_formatted, default=None)
        if not course_data or not course_data.get("is_fresh", False):
            soup, error_msg = await self._fetch_course_online(course_key_formatted)
            if soup:
                processed_data = self._process_soup_content(soup)
                new_data = {
                    "course_data": processed_data,
                    "date_added": date.today().isoformat(),
                    "is_fresh": True,
                }
                await self.config.courses.set_raw(course_key_formatted, value=new_data)
                course_data = await self.config.courses.get_raw(course_key_formatted, default=None)
            elif error_msg:
                log.error(f"Error fetching course data for {course_key_formatted}: {error_msg}")
                return {}
        return course_data if course_data else {}

    async def _fetch_course_online(self, course_key_formatted: str) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """
        Attempt to fetch course data from the external endpoint.
        
        :param course_key_formatted: Standardized course key.
        :return: Tuple of (BeautifulSoup object or None, error message if any)
        """
        term_order = self._determine_term_order()
        soup, error_message = await self._fetch_data_with_retries(term_order, course_key_formatted)
        return (soup, None) if soup else (None, error_message)

    def _determine_term_order(self) -> List[str]:
        """
        Determine a prioritized list of term names based on the current month.
        
        :return: Ordered list of term names.
        """
        now = date.today()
        current_term_index = (now.month - 1) // 4  # Roughly dividing months into 3 blocks.
        return self._TERM_NAMES[current_term_index:] + self._TERM_NAMES[:current_term_index]

    async def _fetch_data_with_retries(
        self, term_order: List[str], course_key_formatted: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """
        Try to fetch data across multiple terms with a retry mechanism.
        
        :param term_order: List of term names to try.
        :param course_key_formatted: Standardized course key.
        :return: Tuple (soup, None) on success or (None, error message) on failure.
        """
        max_retries = 1
        retry_delay = 5
        url: Optional[str] = None

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
                        if "not found" in error_message.lower():
                            log.error(f"Course not found: {course_key_formatted}")
                            return None, error_message
                        if retry_count == max_retries - 1:
                            return None, error_message
                        await asyncio.sleep(retry_delay)
                except (ClientResponseError, ClientConnectionError, asyncio.TimeoutError) as error:
                    log.error(f"Error fetching course data: {error}")
                    if retry_count == max_retries - 1:
                        return None, "Error: Issue occurred while fetching course data."
                    await asyncio.sleep(retry_delay)
        if url:
            log.error(f"Max retries reached while fetching data from {url}")
        return None, "Error: Max retries reached while fetching course data."

    async def _get_term_id(self, term_name: str) -> Optional[int]:
        """
        Retrieve the term code from config.
        
        :param term_name: Name of the term.
        :return: Term code as int or None.
        """
        return await self.config.term_codes.get_raw(term_name, default=None)

    def _build_url(self, term_id: int, course_key_formatted: str) -> str:
        """
        Construct the URL for querying the course data.
        
        :param term_id: Term identifier.
        :param course_key_formatted: Standardized course key.
        :return: Formatted URL string.
        """
        t, e = self._generate_time_code()
        return self._URL_BASE.format(term=term_id, course_key_formatted=course_key_formatted, t=t, e=e)

    def _generate_time_code(self) -> Tuple[int, int]:
        """
        Generate a time-based code for the remote API.
        
        :return: Tuple (t, e) as integers.
        """
        t = floor(time() / 60) % 1000
        e = t % 3 + t % 39 + t % 42
        return t, e

    async def _fetch_single_attempt(self, url: str) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """
        Perform a single HTTP request to fetch course data.
        
        :param url: URL to fetch.
        :return: Tuple of (BeautifulSoup object or None, error message if any)
        """
        timeout = ClientTimeout(total=15)
        try:
            async with ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        return None, f"Error: HTTP {response.status}"
                    content = await response.text()
                    soup = BeautifulSoup(content, "xml")
                    if not (error_tag := soup.find("error")):
                        return soup, None
                    error_message = error_tag.text.strip()
                    return None, error_message or None
        except Exception as e:
            log.error(f"An error occurred while fetching data from {url}: {e}")
            return None, str(e)

    def _process_soup_content(self, soup: BeautifulSoup) -> List[Dict]:
        """
        Parse the BeautifulSoup object to extract relevant course data.
        
        :param soup: BeautifulSoup instance containing the XML data.
        :return: List of dictionaries with course details.
        """
        course_data = []
        for course in soup.find_all("course"):
            offering = course.find("offering")
            term_elem = course.find("term")
            block = course.find("block")
            offering_title = offering["title"] if (offering and "title" in offering.attrs) else ""
            term_found = term_elem.get("v") if term_elem else ""
            teacher = block.get("teacher", "") if block else ""
            extracted_details = {
                "title": offering_title,
                "term_found": term_found,
                "teacher": teacher,
                "course_code": course.get("code", ""),
                "course_number": course.get("number", ""),
            }
            course_data.append(extracted_details)
        return course_data


###############################################################################
# CourseManager Cog
###############################################################################
class CourseManager(commands.Cog):
    """
    Cog for managing course channels and course details.

    Channel Management:
      • Allows users to join or leave course channels under the "COURSES" category.
      • Admins can delete channels and set a logging channel for notifications.
      • Auto-prunes channels within the "COURSES" category if inactive for more than 120 days.
      • Users can list their enrolled courses.
      • Prevents duplicate joins.
      • Only creates channels if course details are found.

    Course Details:
      • Retrieves and caches course details via an external API.
      • Displays details for one or multiple courses.
      • Includes commands for refreshing course data.

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
        default_global: Dict[str, Any] = {
            "term_codes": {},
            "courses": {},
        }
        self.config: Config = Config.get_conf(self, identifier=3720194665, force_registration=True)
        self.config.register_global(**default_global)
        self.course_data_proxy: CourseDataProxy = CourseDataProxy(self.config)

        # Start the background auto-prune task.
        self._prune_task: asyncio.Task = self.bot.loop.create_task(self._auto_prune_task())

    def cog_unload(self) -> None:
        """Cancel the auto-prune background task when the cog is unloaded."""
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
        while not self.bot.is_closed():
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
                            log.info(f"Auto-pruning channel '{channel.name}' in '{guild.name}'.")
                            await channel.delete(reason="Auto-pruned due to inactivity.")
                    except Exception as e:
                        log.error(f"Error while pruning channel '{channel.name}' in '{guild.name}': {e}")
            await asyncio.sleep(PRUNE_INTERVAL)

    #####################################################
    # Main Command Group: course
    #####################################################
    @commands.group(invoke_without_command=True)
    async def course(self, ctx: commands.Context) -> None:
        """
        Main command group for course functionalities.
        
        Subcommands:
          • join [course_code]       - Join a course channel.
          • leave [course_code]      - Leave a course channel.
          • list                     - List your enrolled courses.
          • refresh [course_code]    - Refresh course data from the API.
          • delete [channel]         - Delete a course channel (admin-only).
          • setlogging [#channel]    - Set the logging channel (admin-only).
          • details [course_code]    - Show course details.
          • multidetails [codes...]  - Show details for multiple courses.
        """
        await ctx.send_help(self.course)

    #####################################################
    # Listing Enrollments
    #####################################################
    @course.command(name="list")
    async def list_enrollments(self, ctx: commands.Context) -> None:
        """
        List all course channels you are currently enrolled in.
        """
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
        if not formatted:
            await ctx.send(error(f"Invalid course code: {course_code}."))
            return
        # Mark the data as stale.
        await self.config.courses.set_raw(formatted, value={"is_fresh": False})
        data = await self.course_data_proxy.get_course_data(formatted)
        if data and data.get("course_data"):
            await ctx.send(success(f"Course data for {formatted} refreshed successfully."))
        else:
            await ctx.send(error(f"Failed to refresh course data for {formatted}."))

    #####################################################
    # Channel Management Commands
    #####################################################
    @course.command()
    async def join(self, ctx: commands.Context, course_code: str) -> None:
        """
        Join a course channel.
        
        Validates the course code using the external API, checks if the user has not exceeded their course limit,
        and grants access by setting per–user permission overwrites.
        Prevents duplicate enrollment.
        """
        formatted: Optional[str] = self._format_course_key(course_code)
        if not formatted:
            await ctx.send(error(f"Invalid course code: {course_code}."))
            return

        # Check if course details can be found.
        data: Dict[str, Any] = await self.course_data_proxy.get_course_data(formatted)
        if not data or not data.get("course_data"):
            await ctx.send(error(f"Error: No valid course data found for {formatted}."))
            return

        # Duplicate join prevention.
        if formatted.upper() in self.get_user_courses(ctx.author, ctx.guild):
            await ctx.send(info(f"You are already enrolled in {formatted}."))
            return

        if len(self.get_user_courses(ctx.author, ctx.guild)) >= self.max_courses:
            await ctx.send(error(f"You have reached the maximum limit of {self.max_courses} courses. Leave one before joining another."))
            return

        category = self.get_category(ctx.guild)
        if category is None:
            try:
                category = await ctx.guild.create_category(self.category_name)
            except discord.Forbidden:
                await ctx.send(error("I don't have permission to create the courses category."))
                return

        channel = self.get_course_channel(ctx.guild, formatted)
        if not channel:
            channel = await self.create_course_channel(ctx.guild, category, formatted)

        try:
            await channel.set_permissions(ctx.author, overwrite=self.channel_permissions)
        except discord.Forbidden:
            await ctx.send(error("I don't have permission to manage channel permissions."))
            return

        await ctx.send(success(f"You have successfully joined {formatted}."), delete_after=120)
        if self.logging_channel:
            await self.logging_channel.send(f"{ctx.author} has joined {formatted}.")

    @course.command()
    async def leave(self, ctx: commands.Context, course_code: str) -> None:
        """
        Leave a course channel by removing your permission override.
        """
        formatted = self._format_course_key(course_code)
        if not formatted:
            await ctx.send(error(f"Invalid course code: {course_code}."))
            return

        channel = self.get_course_channel(ctx.guild, formatted)
        if not channel:
            await ctx.send(error(f"You are not a member of {formatted}."))
            return

        try:
            await channel.set_permissions(ctx.author, overwrite=None)
        except discord.Forbidden:
            await ctx.send(error("I don't have permission to manage channel permissions."))
            return

        await ctx.send(success(f"You have successfully left {formatted}."), delete_after=120)
        if self.logging_channel:
            await self.logging_channel.send(f"{ctx.author} has left {formatted}.")

    @checks.admin()
    @course.command()
    async def delete(self, ctx: commands.Context, channel: discord.TextChannel) -> None:
        """
        Delete a course channel (admin-only).
        """
        if not channel.category or channel.category.name != self.category_name:
            await ctx.send(error(f"{channel.mention} is not a course channel."))
            return
        try:
            await channel.delete()
        except discord.Forbidden:
            await ctx.send(error("I don't have permission to delete that channel."))
            return
        await ctx.send(success(f"{channel.name} has been successfully deleted."))
        if self.logging_channel:
            await self.logging_channel.send(f"{channel.name} has been deleted.")

    @checks.admin()
    @course.command(name="setlogging")
    async def set_logging(self, ctx: commands.Context, channel: discord.TextChannel) -> None:
        """
        Set the logging channel for join/leave notifications (admin-only).
        """
        self.logging_channel = channel
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
        if not formatted:
            await ctx.send(error(f"Invalid course code: {course_key_raw}. Use format like 'MATH 1A03'."))
            return

        data = await self.course_data_proxy.get_course_data(formatted)
        if not data or not data.get("course_data"):
            await ctx.send(error(f"Course not found: {formatted}"))
            return

        embed = self._create_course_embed(formatted, data)
        await ctx.send(embed=embed)

    @course.command(name="multidetails")
    @commands.cooldown(1, 10, commands.BucketType.user)
    async def multi_course_details(self, ctx: commands.Context, *course_codes: str) -> None:
        """
        Fetch details for multiple courses concurrently.
        Example: `!course multidetails MATH 1A03 PHYSICS 1B03`
        """
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

        tasks = [self.course_data_proxy.get_course_data(vc) for vc in valid_courses]
        results = await bounded_gather(*tasks, limit=3)

        output_lines: List[str] = []
        for idx, res in enumerate(results):
            if not res or not res.get("course_data"):
                output_lines.append(error(f"No data found for {valid_courses[idx]}."))
                continue

            data_item = res["course_data"][0]
            updated = res.get("date_added", "Unknown")
            freshness_icon = "🟢" if res.get("is_fresh", False) else "🔴"
            text_block = (
                f"{freshness_icon} **{valid_courses[idx]}**\n"
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
    def _format_course_key(self, course_key_raw: str) -> Optional[str]:
        """
        Convert an input course string into a standardized format.
        E.g., "MATH 1A03" becomes "MATH-1A03".
        """
        cleaned = re.sub(r"[-_]+", " ", course_key_raw).upper().strip()
        parts = cleaned.split()
        if len(parts) < 2:
            return None
        code, number = parts[0], parts[1]
        if not re.match(r"^[A-Z]+$", code):
            return None
        if not re.match(r"^\d[\w]{1,3}$", number):
            return None
        return f"{code}-{number}"

    def _create_course_embed(self, course_key: str, course_data: Dict[str, Any]) -> discord.Embed:
        """
        Build a Discord embed to display course details.
        
        :param course_key: Standardized course key.
        :param course_data: Dictionary with course data.
        :return: Configured discord.Embed object.
        """
        embed = discord.Embed(title=f"Course Details: {course_key}", color=discord.Color.green())
        data_item = course_data.get("course_data", [{}])[0]
        is_fresh = course_data.get("is_fresh", False)
        date_added = course_data.get("date_added", "Unknown")
        footer_icon = "🟢" if is_fresh else "🔴"
        embed.set_footer(text=f"{footer_icon} Last updated: {date_added}")

        for key, label in (("title", "Title"), ("term_found", "Term"),
                           ("teacher", "Instructor"), ("course_code", "Code"),
                           ("course_number", "Number")):
            value = data_item.get(key)
            if value:
                embed.add_field(name=label, value=value, inline=True)
        return embed

    def get_category(self, guild: discord.Guild) -> Optional[discord.CategoryChannel]:
        """
        Find the category named self.category_name in the given guild.
        
        :param guild: The Discord guild.
        :return: The category if found, else None.
        """
        for category in guild.categories:
            if category.name == self.category_name:
                return category
        return None

    def get_course_channel(self, guild: discord.Guild, course_key: str) -> Optional[discord.TextChannel]:
        """
        Retrieve a course channel by its standardized course key.
        
        :param guild: The Discord guild.
        :param course_key: Standardized course key.
        :return: The corresponding TextChannel if it exists.
        """
        category = self.get_category(guild)
        if not category:
            return None
        for channel in category.channels:
            if channel.name == course_key.lower():
                return channel
        return None

    async def create_course_channel(self, guild: discord.Guild, category: discord.CategoryChannel, course_key: str) -> discord.TextChannel:
        """
        Create a new course channel under the courses category.
        
        :param guild: The Discord guild.
        :param category: The courses category.
        :param course_key: Standardized course key.
        :return: The newly created TextChannel.
        """
        overwrites = {
            guild.default_role: discord.PermissionOverwrite.none(),
            guild.me: discord.PermissionOverwrite.all(),
        }
        return await guild.create_text_channel(course_key.lower(), overwrites=overwrites, category=category)

    def get_user_courses(self, user: discord.Member, guild: discord.Guild) -> List[str]:
        """
        List the course channels the user currently has access to in the guild.
        
        :param user: The Discord member.
        :param guild: The Discord guild.
        :return: List of course channel names (uppercased).
        """
        courses: List[str] = []
        category = self.get_category(guild)
        if not category:
            return courses
        for channel in category.channels:
            if isinstance(channel, discord.TextChannel) and channel.permissions_for(user).read_messages:
                courses.append(channel.name.upper())
        return courses

    #####################################################
    # Developer Commands (Owner-only)
    #####################################################
    @checks.is_owner()
    @commands.group(name="dc", invoke_without_command=True)
    async def dev_course(self, ctx: commands.Context) -> None:
        """Developer commands for managing config data for the course cog."""
        await ctx.send_help(self.dev_course)

    @dev_course.command(name="term")
    async def set_term_codes(self, ctx: commands.Context, term_name: str, term_id: int) -> None:
        """
        Set the term code for a specified term.
        Example: `!dc term winter 2241`
        """
        async with self.config.term_codes() as term_codes:
            term_codes[term_name.lower()] = term_id
        await ctx.send(success(f"Term code for {term_name.capitalize()} set to: {term_id}"))

    @dev_course.command(name="clearstale")
    async def clear_stale_config(self, ctx: commands.Context) -> None:
        """
        Clear stale course config entries that no longer have a corresponding channel.
        """
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
        if stale:
            await ctx.send(success(f"Cleared stale config entries for courses: {', '.join(stale)}"))
        else:
            await ctx.send(info("No stale course config entries found."))

    @dev_course.command(name="prune")
    async def manual_prune(self, ctx: commands.Context) -> None:
        """
        Manually trigger the auto-prune process for inactive course channels.
        """
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
                        await channel.delete(reason="Manually pruned due to inactivity.")
                except Exception as e:
                    log.error(f"Error pruning channel '{channel.name}' in '{guild.name}': {e}")
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
        log.debug(cfg)
        await ctx.send(info("Config has been printed to the console log."))

    @dev_course.command(name="clearcourses")
    async def clear_courses(self, ctx: commands.Context) -> None:
        """
        Clear all cached course data from the config.
        """
        await self.config.courses.set({})
        await ctx.send(warning("All courses have been cleared from the config."))