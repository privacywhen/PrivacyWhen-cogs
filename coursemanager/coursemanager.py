import asyncio
import re
import logging
from datetime import timezone, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import discord
from redbot.core import commands, Config
from redbot.core.utils.chat_formatting import box, error, info, success, warning, pagify
from redbot.core.utils.menus import menu, DEFAULT_CONTROLS
from .coursedata import CourseDataProxy
from rapidfuzz import process

# New import for grouping functionality.
from .coursegrouping import CourseGrouping

# Configure logging
log = logging.getLogger("red.course_helper")
log.setLevel(logging.DEBUG)
if not log.handlers:
    log.addHandler(logging.StreamHandler())

COURSE_KEY_PATTERN = re.compile(
    r"^\s*([A-Za-z]+)[\s\-_]*(\d+(?:[A-Za-z]+\d+)?)([ABab])?\s*$"
)


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
      • **Dynamic Course Grouping:** Computes course clusters on a schedule or on demand.
      • Owner-only commands to manage term codes and stale config.
    """

    def __init__(self, bot: commands.Bot) -> None:
        self.bot: commands.Bot = bot

        # Channel management settings.
        self.channel_permissions: discord.PermissionOverwrite = (
            discord.PermissionOverwrite.from_pair(
                discord.Permissions(446676945984),
                discord.Permissions(0),
            )
        )
        self.category_name: str = "COURSES"
        self.max_courses: int = 10
        self.logging_channel: Optional[discord.TextChannel] = None

        # Global defaults for config.
        default_global: Dict[str, Any] = {
            "term_codes": {},
            "courses": {},
            "course_listings": {},
            "enrollments": {},  # Maps user_id (as str) → list of course codes
            "grouping_threshold": 2,  # Minimum co-occurrence count to add an edge
        }
        self.config: Config = Config.get_conf(
            self, identifier=3720194665, force_registration=True
        )
        self.config.register_global(**default_global)
        self.course_data_proxy: CourseDataProxy = CourseDataProxy(self.config, log)

        # Enrollment helper methods remain in this file.
        # (See _add_enrollment and _remove_enrollment below.)

        # Initialize the course grouping helper.
        self.course_grouping: CourseGrouping = CourseGrouping(self.config, log)
        # Schedule the grouping update to run on a weekly interval (604800 seconds by default).
        self._group_update_task: asyncio.Task = self.bot.loop.create_task(
            self.course_grouping.schedule_group_update(interval=604800)
        )

        # Start the background auto-prune task.
        self._prune_task: asyncio.Task = self.bot.loop.create_task(
            self._auto_prune_task()
        )
        log.debug("CourseManager initialized with max_courses=%s", self.max_courses)

    def cog_unload(self) -> None:
        """Cancel background tasks when the cog unloads."""
        log.debug("Unloading CourseManager cog; cancelling background tasks.")
        self._prune_task.cancel()
        self._group_update_task.cancel()

    async def _auto_prune_task(self) -> None:
        PRUNE_INTERVAL = 3600  # every hour
        PRUNE_THRESHOLD = timedelta(days=120)
        await self.bot.wait_until_ready()
        log.debug("Auto-prune task started.")
        while not self.bot.is_closed():
            for guild in self.bot.guilds:
                category = self.get_category(guild)
                if not category:
                    log.debug(
                        "Category '%s' not found in guild %s",
                        self.category_name,
                        guild.name,
                    )
                    continue
                for channel in category.channels:
                    if isinstance(channel, discord.TextChannel):
                        await self._prune_channel(
                            channel, PRUNE_THRESHOLD, "Auto-pruned due to inactivity."
                        )
            await asyncio.sleep(PRUNE_INTERVAL)

    #####################################################
    # Enrollment Helpers (for Dynamic Grouping)
    #####################################################
    async def _add_enrollment(self, user: discord.Member, course: str) -> None:
        """
        Add a course (standardized channel name) to the user's enrollment list.
        """
        user_id = str(user.id)
        enrollments = await self.config.enrollments.get_raw(user_id, default=[])
        if course not in enrollments:
            enrollments.append(course)
            await self.config.enrollments.set_raw(user_id, value=enrollments)
            log.debug("Added enrollment: %s -> %s", user, course)

    async def _remove_enrollment(self, user: discord.Member, course: str) -> None:
        """
        Remove a course from the user's enrollment list.
        """
        user_id = str(user.id)
        enrollments = await self.config.enrollments.get_raw(user_id, default=[])
        if course in enrollments:
            enrollments.remove(course)
            await self.config.enrollments.set_raw(user_id, value=enrollments)
            log.debug("Removed enrollment: %s -> %s", user, course)

    #####################################################
    # (Existing) Course Code & Channel Name Helpers
    #####################################################
    def _format_course_key(self, course_key_raw: str) -> Optional[str]:
        log.debug("Formatting course key: %s", course_key_raw)
        match = COURSE_KEY_PATTERN.match(course_key_raw)
        if not match:
            log.debug(
                "Input '%s' does not match expected course pattern.", course_key_raw
            )
            return None
        subject, number, suffix = match.groups()
        subject = subject.upper()
        number = number.upper()
        formatted = f"{subject}-{number}" + (suffix.upper() if suffix else "")
        log.debug("Formatted course key: %s", formatted)
        return formatted

    def _get_channel_name(self, course_key: str) -> str:
        if course_key and course_key[-1] in ("A", "B"):
            course_key = course_key[:-1]
        channel_name = course_key.lower()
        log.debug("Derived channel name: %s", channel_name)
        return channel_name

    def _get_course_variants(self, formatted: str) -> List[str]:
        if formatted[-1] in ("A", "B"):
            base = formatted[:-1]
            suffix = formatted[-1]
            fallback = "B" if suffix == "A" else "A"
            variants = [formatted, base + fallback]
        else:
            variants = [formatted, f"{formatted}A", f"{formatted}B"]
        log.debug("Lookup variants for '%s': %s", formatted, variants)
        return variants

    async def _lookup_course_data(
        self, formatted: str
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        for variant in self._get_course_variants(formatted):
            log.debug("Trying lookup for variant: %s", variant)
            data = await self.course_data_proxy.get_course_data(variant)
            if data and data.get("course_data"):
                log.debug("Found course data for %s", variant)
                return variant, data
        log.debug("No course data found for variants of %s", formatted)
        return None, None

    async def _prune_channel(
        self, channel: discord.TextChannel, threshold: timedelta, reason: str
    ) -> bool:
        try:
            last_user_message: Optional[discord.Message] = None
            async for msg in channel.history(limit=10):
                if not msg.author.bot:
                    last_user_message = msg
                    break
            last_activity = (
                last_user_message.created_at
                if last_user_message
                else channel.created_at
            )
            if datetime.now(timezone.utc) - last_activity > threshold:
                log.info(
                    "Pruning channel '%s' in guild '%s' (last activity at %s)",
                    channel.name,
                    channel.guild.name,
                    last_activity,
                )
                await channel.delete(reason=reason)
                return True
        except Exception as e:
            log.error(
                "Error pruning channel '%s' in guild '%s': %s",
                channel.name,
                channel.guild.name,
                e,
            )
        return False

    #####################################################
    # Command Group: course
    #####################################################
    @commands.group(invoke_without_command=True)
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def course(self, ctx: commands.Context) -> None:
        log.debug("Course command group invoked by %s", ctx.author)
        await ctx.send_help(self.course)

    @course.command(name="list")
    @commands.cooldown(1, 600, commands.BucketType.user)
    async def list_enrollments(self, ctx: commands.Context) -> None:
        """List all course channels you are currently enrolled in."""
        log.debug("Listing courses for user %s in guild %s", ctx.author, ctx.guild.name)
        if courses := self.get_user_courses(ctx.author, ctx.guild):
            await ctx.send(
                "You are enrolled in the following courses:\n" + "\n".join(courses)
            )
        else:
            await ctx.send("You are not enrolled in any courses.")

    @course.command()
    @commands.cooldown(1, 86400, commands.BucketType.user)
    async def refresh(self, ctx: commands.Context, *, course_code: str) -> None:
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
            await ctx.send(
                success(f"Course data for {variant} refreshed successfully.")
            )
        else:
            await ctx.send(error(f"Failed to refresh course data for {variant}."))

    @course.command()
    @commands.cooldown(5, 28800, commands.BucketType.user)
    async def join(self, ctx: commands.Context, *, course_code: str) -> None:
        """
        Join a course channel.
        Validates the course code, checks enrollment limits, sets permissions,
        and records the enrollment.
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

        # Check if the user is already enrolled (based on channel name).
        enrolled_channels = self.get_user_courses(ctx.author, ctx.guild)
        target_channel_name = self._get_channel_name(variant).upper()
        if target_channel_name in enrolled_channels:
            await ctx.send(info(f"You are already enrolled in {variant}."))
            return

        if len(enrolled_channels) >= self.max_courses:
            await ctx.send(
                error(
                    f"You have reached the maximum limit of {self.max_courses} courses."
                )
            )
            return

        category = self.get_category(ctx.guild)
        if category is None:
            try:
                category = await ctx.guild.create_category(self.category_name)
                log.debug(
                    "Created category '%s' in guild %s",
                    self.category_name,
                    ctx.guild.name,
                )
            except discord.Forbidden:
                await ctx.send(
                    error("I don't have permission to create the courses category.")
                )
                return

        channel = self.get_course_channel(ctx.guild, variant)
        if not channel:
            log.debug("Course channel for %s not found; creating new channel.", variant)
            channel = await self.create_course_channel(ctx.guild, category, variant)

        try:
            await channel.set_permissions(
                ctx.author, overwrite=self.channel_permissions
            )
            log.debug("Permissions set for %s on channel %s", ctx.author, channel.name)
        except discord.Forbidden:
            await ctx.send(
                error("I don't have permission to manage channel permissions.")
            )
            return

        await ctx.send(
            success(f"You have successfully joined {variant}."), delete_after=120
        )
        if self.logging_channel:
            await self.logging_channel.send(f"{ctx.author} has joined {variant}.")

        # Record the enrollment (store the channel name in uppercase for consistency)
        await self._add_enrollment(ctx.author, self._get_channel_name(variant).upper())

    @course.command()
    @commands.cooldown(5, 28800, commands.BucketType.user)
    async def leave(self, ctx: commands.Context, *, course_code: str) -> None:
        """
        Leave a course channel by removing your permission override and updating your enrollment.
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
            log.debug(
                "Removed permissions for %s on channel %s", ctx.author, channel.name
            )
        except discord.Forbidden:
            await ctx.send(
                error("I don't have permission to manage channel permissions.")
            )
            return

        await ctx.send(
            success(f"You have successfully left {formatted}."), delete_after=120
        )
        if self.logging_channel:
            await self.logging_channel.send(f"{ctx.author} has left {formatted}.")

        # Remove the enrollment record.
        await self._remove_enrollment(
            ctx.author, self._get_channel_name(formatted).upper()
        )

    @commands.admin()
    @course.command()
    async def delete(
        self, ctx: commands.Context, *, channel: discord.TextChannel
    ) -> None:
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
    async def set_logging(
        self, ctx: commands.Context, channel: discord.TextChannel
    ) -> None:
        """Set the logging channel for join/leave notifications (admin-only)."""
        self.logging_channel = channel
        log.debug("Logging channel set to %s by admin %s", channel.name, ctx.author)
        await ctx.send(success(f"Logging channel set to {channel.mention}."))

    @course.command(name="details")
    @commands.cooldown(10, 600, commands.BucketType.guild)
    async def course_details(
        self, ctx: commands.Context, *, course_key_raw: str
    ) -> None:
        """
        Display details for a specified course.
        Example: !course details MATH 1A03
        """
        formatted = self._format_course_key(course_key_raw)
        log.debug(
            "Fetching details for '%s' (formatted: %s)", course_key_raw, formatted
        )
        if not formatted:
            await ctx.send(
                error(
                    f"Invalid course code: {course_key_raw}. Use a format like 'MATH 1A03'."
                )
            )
            return

        embed = await self._get_course_details(formatted)
        if embed is None:
            await ctx.send(error(f"Course not found: {formatted}"))
        else:
            await ctx.send(embed=embed)

    async def _get_course_details(self, course_code):
        variant, data = await self._lookup_course_data(course_code)
        if not variant or not (data and data.get("course_data")):
            return None
        embed = self._create_course_embed(variant, data)
        return embed

    def _create_course_embed(
        self, course_key: str, course_data: Dict[str, Any]
    ) -> discord.Embed:
        log.debug("Creating embed for course: %s", course_key)
        embed = discord.Embed(
            title=f"Course Details: {course_key}", color=discord.Color.green()
        )
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
            embed.add_field(
                name="Description", value=data_item.get("description"), inline=False
            )
        if data_item.get("prerequisites"):
            embed.add_field(
                name="Prerequisite(s)",
                value=data_item.get("prerequisites"),
                inline=True,
            )
        if data_item.get("antirequisites"):
            embed.add_field(
                name="Antirequisite(s)",
                value=data_item.get("antirequisites"),
                inline=True,
            )

        return embed

    def get_category(self, guild: discord.Guild) -> Optional[discord.CategoryChannel]:
        log.debug(
            "Searching for category '%s' in guild %s", self.category_name, guild.name
        )
        for category in guild.categories:
            if category.name == self.category_name:
                log.debug(
                    "Found category '%s' in guild %s", self.category_name, guild.name
                )
                return category
        log.debug("Category '%s' not found in guild %s", self.category_name, guild.name)
        return None

    def get_course_channel(
        self, guild: discord.Guild, course_key: str
    ) -> Optional[discord.TextChannel]:
        category = self.get_category(guild)
        if not category:
            log.debug(
                "No category in guild %s for course key %s", guild.name, course_key
            )
            return None
        target_name = self._get_channel_name(course_key)
        for channel in category.channels:
            if channel.name == target_name:
                log.debug(
                    "Found course channel '%s' in guild %s", channel.name, guild.name
                )
                return channel
        log.debug("Course channel '%s' not found in guild %s", target_name, guild.name)
        return None

    async def create_course_channel(
        self, guild: discord.Guild, category: discord.CategoryChannel, course_key: str
    ) -> discord.TextChannel:
        target_name = self._get_channel_name(course_key)
        log.debug("Creating channel '%s' in guild %s", target_name, guild.name)
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            guild.me: discord.PermissionOverwrite(administrator=True),
        }
        channel = await guild.create_text_channel(
            target_name, overwrites=overwrites, category=category
        )
        log.debug("Created channel '%s' in guild %s", channel.name, guild.name)
        return channel

    def get_user_courses(self, user: discord.Member, guild: discord.Guild) -> List[str]:
        """
        Return a list of course channels (by name, uppercase) that the user can access.
        """
        category = self.get_category(guild)
        if not category:
            log.debug("No category in guild %s for user %s", guild.name, user)
            return []
        courses = [
            channel.name.upper()
            for channel in category.channels
            if isinstance(channel, discord.TextChannel)
            and channel.permissions_for(user).read_messages
        ]
        log.debug("User %s has access to courses: %s", user, courses)
        return courses

    #####################################################
    # New Commands: Dynamic Grouping
    #####################################################
    @course.command(name="showclusters")
    async def show_clusters(self, ctx: commands.Context) -> None:
        """
        Display the current course clusters based on user enrollments.
        """
        clusters = self.course_grouping.course_clusters
        if not clusters:
            await ctx.send("No clusters available at this time.")
            return

        msg = "Current Course Clusters:\n"
        for cluster_id, courses in clusters.items():
            msg += f"Cluster {cluster_id}: " + ", ".join(sorted(courses)) + "\n"
        await ctx.send(box(msg))

    @course.command(name="updategroups")
    async def update_groups(self, ctx: commands.Context) -> None:
        """
        Force an update of course groupings.
        """
        try:
            await self.course_grouping.update_groups()
            await ctx.send(success("Course groupings updated successfully."))
        except Exception as e:
            log.error("Error updating course groupings: %s", e)
            await ctx.send(error("Error updating course groupings."))

    #####################################################
    # Developer Commands (Owner-only)
    #####################################################
    @commands.is_owner()
    @commands.group(name="dc", invoke_without_command=True)
    async def dev_course(self, ctx: commands.Context) -> None:
        log.debug("Dev command group 'dev_course' invoked by %s", ctx.author)
        await ctx.send_help(self.dev_course)

    @dev_course.command(name="term")
    async def set_term_codes(
        self, ctx: commands.Context, term_name: str, term_id: int
    ) -> None:
        async with self.config.term_codes() as term_codes:
            term_codes[term_name.lower()] = term_id
        log.debug("Set term code for %s to %s", term_name, term_id)
        await ctx.send(
            success(f"Term code for {term_name.capitalize()} set to: {term_id}")
        )

    @dev_course.command(name="clearstale")
    async def clear_stale_config(self, ctx: commands.Context) -> None:
        log.debug("Clearing stale config entries.")
        stale = []
        courses = await self.config.courses.all()
        stale.extend(
            course_key
            for course_key in courses.keys()
            if not any(
                self.get_course_channel(guild, course_key) for guild in self.bot.guilds
            )
        )
        for course_key in stale:
            await self.config.courses.clear_raw(course_key)
            log.debug("Cleared stale entry for course %s", course_key)
        if stale:
            await ctx.send(success(f"Cleared stale config entries: {', '.join(stale)}"))
        else:
            await ctx.send(info("No stale course config entries found."))

    @dev_course.command(name="prune")
    async def manual_prune(self, ctx: commands.Context) -> None:
        log.debug("Manual prune triggered by %s", ctx.author)
        pruned_channels = []
        PRUNE_THRESHOLD = timedelta(days=120)
        for guild in self.bot.guilds:
            category = self.get_category(guild)
            if not category:
                continue
            for channel in category.channels:
                if isinstance(channel, discord.TextChannel):
                    pruned = await self._prune_channel(
                        channel, PRUNE_THRESHOLD, "Manually pruned due to inactivity."
                    )
                    if pruned:
                        pruned_channels.append(f"{guild.name} - {channel.name}")
        if pruned_channels:
            await ctx.send(success("Pruned channels:\n" + "\n".join(pruned_channels)))
        else:
            await ctx.send(info("No inactive channels to prune."))

    @dev_course.command(name="printconfig")
    async def print_config(self, ctx: commands.Context) -> None:
        cfg = await self.config.all()
        log.debug("Current config: %s", cfg)
        await ctx.send(info("Config has been printed to the console log."))

    @dev_course.command(name="clearcourses")
    async def clear_courses(self, ctx: commands.Context) -> None:
        await self.config.courses.set({})
        log.debug("All course data cleared by %s", ctx.author)
        await ctx.send(warning("All courses have been cleared from the config."))

    @dev_course.command(name="list")
    async def list_courses(self, ctx: commands.Context) -> None:
        cfg = await self.config.courses.all()
        serialized = "\n".join([k for k in cfg])
        await ctx.send(serialized)

    @dev_course.command(name="listall")
    async def list_all_courses(self, ctx: commands.Context) -> None:
        cfg = await self.config.course_listings.all()
        if "courses" in cfg:
            courses = cfg["courses"]
            dtm = cfg["date_updated"]
            serialized_courses = "\n".join(list(courses.keys()))
            await ctx.send(
                f"{len(cfg['courses'])} courses cached on {dtm}\n{serialized_courses[:1500] + '...' if len(serialized_courses) > 1500 else ''}"
            )
        else:
            await ctx.send("Course list not found. Run populate command first.")

    @dev_course.command(name="populate")
    async def fetch_prefixes(self, ctx: commands.Context) -> None:
        course_count = await self.course_data_proxy.update_course_listing()
        if course_count > 0:
            await ctx.send(info(f"Fetched and cached {course_count} courses"))
        else:
            await ctx.send(warning("0 courses fetched. Check console log"))

    @dev_course.command(name="search")
    async def fuzzy_search(self, ctx: commands.Context, *, search_code: str) -> None:
        search_code = search_code.upper()
        cfg = await self.config.course_listings.all()
        if "courses" not in cfg:
            await ctx.send("No course listings available.")
            return

        courses = cfg["courses"]

        if search_code in courses:
            embed = await self._get_course_details(search_code)
            await ctx.send(embed=embed)
            return

        closest_matches = [
            match[0]
            for match in process.extract(
                search_code, courses.keys(), limit=5, score_cutoff=70
            )
        ]
        if not closest_matches:
            await ctx.send(
                f"❌ {search_code} not found and no similar matches available."
            )
            return

        suggestion_msg = "Course not found. Did you mean:\n"
        emoji_list = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣"]
        for i, match in enumerate(closest_matches):
            suggestion_msg += f"- {emoji_list[i]} **{match}**: {courses[match]}\n"
        msg = await ctx.send(suggestion_msg)
        for emoji in emoji_list[: len(closest_matches)]:
            await msg.add_reaction(emoji)

        def check(reaction, user):
            return (
                user == ctx.author
                and str(reaction.emoji) in emoji_list
                and reaction.message.id == msg.id
            )

        try:
            reaction, _ = await self.bot.wait_for(
                "reaction_add", timeout=30.0, check=check
            )
            selected_index = emoji_list.index(str(reaction.emoji))
            selected_course = closest_matches[selected_index]
            embed = await self._get_course_details(selected_course)
            await msg.clear_reactions()
            await msg.edit(content=None, embed=embed)
        except asyncio.TimeoutError:
            await msg.clear_reactions()
            await msg.edit(content=suggestion_msg + "\n[Selection timed out]")
