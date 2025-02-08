"""Course Manager Cog for Redbot.

This cog allows users to join/leave course channels, refresh course data,
and view course details. It depends on CourseDataProxy from coursedata.py.
"""

import asyncio
import re
import logging
from datetime import timezone, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import discord
from redbot.core import commands, Config
from redbot.core.utils.chat_formatting import error, info, success, warning
from rapidfuzz import process

from .coursedata import CourseDataProxy

log = logging.getLogger("red.course_helper")
log.setLevel(logging.DEBUG)
if not log.handlers:
    log.addHandler(logging.StreamHandler())

# Regex pattern to normalize course codes.
COURSE_KEY_PATTERN = re.compile(
    r"^\s*([A-Za-z]+)[\s\-_]*(\d+(?:[A-Za-z\d]*\d+)?)([A-Za-z])?\s*$"
)
# Reaction options for interactive prompts.
REACTION_OPTIONS: List[str] = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "❌"]


class CourseManager(commands.Cog):
    """
    Manages course channels and details.

    Lookup logic:
      1. Normalize input (e.g. "socwork2cc3" becomes "SOCWORK-2CC3").
      2. If an exact match exists in the course listing, use it.
         If the API call fails for the perfect match, return immediately.
      3. Otherwise, if the input lacks a suffix, search for variant keys.
         - If exactly one variant exists, use it.
         - If multiple exist, prompt the user.
      4. Otherwise, fall back to fuzzy lookup (up to 5 suggestions).

    **Enabled Server Feature:**
      This cog functions only in servers that have been explicitly enabled.
      Administrators can run `course enable` and `course disable` to opt in or out.
    """

    def __init__(self, bot: commands.Bot) -> None:
        self.bot: commands.Bot = bot
        self.channel_permissions: discord.PermissionOverwrite = (
            discord.PermissionOverwrite.from_pair(
                discord.Permissions(446676945984), discord.Permissions(0)
            )
        )
        # Base category name for course channels.
        self.category_name: str = "COURSES"
        self.max_courses: int = 10
        self.logging_channel: Optional[discord.TextChannel] = None

        default_global: Dict[str, Any] = {
            "term_codes": {},
            "courses": {},
            "course_listings": {},
            "enabled_guilds": [],
        }
        self.config: Config = Config.get_conf(
            self, identifier=3720194665, force_registration=True
        )
        self.config.register_global(**default_global)
        self.course_data_proxy: CourseDataProxy = CourseDataProxy(self.config, log)

        self._prune_task: asyncio.Task = self.bot.loop.create_task(
            self._auto_prune_task()
        )
        log.debug("CourseManager initialized with max_courses=%s", self.max_courses)

    def cog_unload(self) -> None:
        """Cancel background tasks when the cog unloads."""
        log.debug("Unloading CourseManager cog; cancelling auto-prune task.")
        self._prune_task.cancel()

    async def cog_check(self, ctx: commands.Context) -> bool:
        if ctx.guild is None:
            return True
        if await ctx.bot.is_owner(ctx.author):
            return True
        if ctx.command.name in ("enable", "disable"):
            return True
        enabled = await self.config.enabled_guilds()
        if ctx.guild.id in enabled:
            return True
        else:
            await ctx.send(
                "Course Manager is not enabled in this server. "
                "An administrator can enable it with `course enable`."
            )
            return False

    async def _auto_prune_task(self) -> None:
        """Background task to auto-prune inactive course channels."""
        PRUNE_INTERVAL = 3600  # seconds
        PRUNE_THRESHOLD = timedelta(days=120)
        await self.bot.wait_until_ready()
        log.debug("Auto-prune task started.")
        enabled = await self.config.enabled_guilds()
        while not self.bot.is_closed():
            for guild in self.bot.guilds:
                if guild.id not in enabled:
                    continue
                for category in self.get_course_categories(guild):
                    for channel in category.channels:
                        if isinstance(channel, discord.TextChannel):
                            await self._prune_channel(
                                channel,
                                PRUNE_THRESHOLD,
                                "Auto-pruned due to inactivity.",
                            )
            await asyncio.sleep(PRUNE_INTERVAL)

    # --- New Enable/Disable Commands ---
    @commands.group(name="course", invoke_without_command=True)
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def course(self, ctx: commands.Context) -> None:
        """Main command group for course functionalities.

        Use `course enable` to enable or `course disable` to disable Course Manager in your server.
        """
        await ctx.send_help(self.course)

    @course.command(name="enable")
    @commands.guild_only()
    @commands.has_permissions(administrator=True)
    async def enable(self, ctx: commands.Context) -> None:
        """Enable Course Manager in your server."""
        enabled = await self.config.enabled_guilds()
        if ctx.guild.id in enabled:
            await ctx.send("Course Manager is already enabled in this server.")
        else:
            enabled.append(ctx.guild.id)
            await self.config.enabled_guilds.set(enabled)
            await ctx.send("Course Manager has been enabled in this server.")

    @course.command(name="disable")
    @commands.guild_only()
    @commands.has_permissions(administrator=True)
    async def disable(self, ctx: commands.Context) -> None:
        """Disable Course Manager in your server."""
        enabled = await self.config.enabled_guilds()
        if ctx.guild.id not in enabled:
            await ctx.send("Course Manager is already disabled in this server.")
        else:
            enabled.remove(ctx.guild.id)
            await self.config.enabled_guilds.set(enabled)
            await ctx.send("Course Manager has been disabled in this server.")

    # --- Course Key and Channel Helpers ---

    def _format_course_key(self, course_key_raw: str) -> Optional[str]:
        log.debug("Formatting course key: %s", course_key_raw)
        match = COURSE_KEY_PATTERN.match(course_key_raw)
        if not match:
            log.debug("Input '%s' does not match expected pattern.", course_key_raw)
            return None
        subject, number, suffix = match.groups()
        formatted = f"{subject.upper()}-{number.upper()}" + (
            suffix.upper() if suffix else ""
        )
        log.debug("Formatted course key: %s", formatted)
        return formatted

    def _get_channel_name(self, course_key: str) -> str:
        if course_key and course_key[-1].isalpha():
            base = course_key[:-1]
        else:
            base = course_key
        channel_name = base.lower()
        log.debug("Derived channel name: %s", channel_name)
        return channel_name

    def get_course_categories(
        self, guild: discord.Guild
    ) -> List[discord.CategoryChannel]:
        """Return all categories whose names start with the base course category."""
        base_upper = self.category_name.upper()
        return [
            cat for cat in guild.categories if cat.name.upper().startswith(base_upper)
        ]

    def get_category(self, guild: discord.Guild) -> Optional[discord.CategoryChannel]:
        """
        Return the base course category (exact match, case-insensitive) in the guild.
        This is used when creating a new course channel.
        """
        for cat in guild.categories:
            if cat.name.upper() == self.category_name.upper():
                return cat
        return None

    def get_course_channel(
        self, guild: discord.Guild, course_key: str
    ) -> Optional[discord.TextChannel]:
        """Return the course channel (by name) if it exists in any course category."""
        target_name = self._get_channel_name(course_key)
        for category in self.get_course_categories(guild):
            for channel in category.channels:
                if (
                    isinstance(channel, discord.TextChannel)
                    and channel.name == target_name
                ):
                    log.debug(
                        "Found course channel '%s' in guild %s",
                        channel.name,
                        guild.name,
                    )
                    return channel
        log.debug("Course channel '%s' not found in guild %s", target_name, guild.name)
        return None

    async def create_course_channel(
        self, guild: discord.Guild, category: discord.CategoryChannel, course_key: str
    ) -> discord.TextChannel:
        """Create a new course channel under the specified category."""
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
        Return a list of course channel names (uppercased) that the user can access.
        Searches all course categories.
        """
        courses = []
        for category in self.get_course_categories(guild):
            for channel in category.channels:
                if (
                    isinstance(channel, discord.TextChannel)
                    and channel.permissions_for(user).read_messages
                ):
                    courses.append(channel.name.upper())
        log.debug("User %s has access to courses: %s", user, courses)
        return courses

    # --- Course Data Lookup Logic ---

    def _find_variant_matches(self, base: str, listings: Dict[str, str]) -> List[str]:
        return [
            key for key in listings if key.startswith(base) and len(key) > len(base)
        ]

    async def _prompt_variant_selection(
        self, ctx: commands.Context, variants: List[str], listings: Dict[str, str]
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        prompt = "Multiple course variants found. Please choose one:\n"
        for i, key in enumerate(variants):
            prompt += f"{REACTION_OPTIONS[i]} **{key}**: {listings.get(key, '')}\n"
        prompt += f"{REACTION_OPTIONS[-1]} Cancel"
        msg = await ctx.send(prompt)
        for emoji in REACTION_OPTIONS[: len(variants)]:
            await msg.add_reaction(emoji)
        await msg.add_reaction(REACTION_OPTIONS[-1])
        reaction = await self._wait_for_reaction(ctx, msg, REACTION_OPTIONS)
        if reaction is None or str(reaction.emoji) == REACTION_OPTIONS[-1]:
            try:
                await msg.clear_reactions()
            except Exception:
                pass
            return None, None
        selected_index = REACTION_OPTIONS.index(str(reaction.emoji))
        candidate = variants[selected_index]
        data = await self.course_data_proxy.get_course_data(candidate)
        try:
            await msg.clear_reactions()
        except Exception:
            pass
        return candidate, data if data and data.get("course_data") else (None, None)

    async def _lookup_course_data(
        self, ctx: commands.Context, formatted: str
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        listings: Dict[str, str] = (await self.config.course_listings()).get(
            "courses", {}
        )
        if formatted in listings:
            data = await self.course_data_proxy.get_course_data(formatted)
            if data and data.get("course_data"):
                return formatted, data
            else:
                log.error("Failed to fetch fresh data for perfect match: %s", formatted)
                return formatted, None
        if not formatted[-1].isalpha():
            variants = self._find_variant_matches(formatted, listings)
            if variants:
                if len(variants) == 1:
                    candidate = variants[0]
                    data = await self.course_data_proxy.get_course_data(candidate)
                    if data and data.get("course_data"):
                        return candidate, data
                else:
                    candidate, data = await self._prompt_variant_selection(
                        ctx, variants, listings
                    )
                    if candidate:
                        return candidate, data
        candidate, data = await self._fallback_fuzzy_lookup(ctx, formatted)
        return candidate, data

    async def _fallback_fuzzy_lookup(
        self, ctx: commands.Context, formatted: str
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        listings: Dict[str, str] = (await self.config.course_listings()).get(
            "courses", {}
        )
        if not listings:
            log.debug("Course listings unavailable; cannot perform fuzzy lookup.")
            return None, None
        matches = process.extract(formatted, listings.keys(), limit=5, score_cutoff=70)
        if not matches:
            return None, None
        prompt = "Course not found. Did you mean:\n"
        options: List[str] = []
        for i, match in enumerate(matches):
            key = match[0]
            prompt += f"{REACTION_OPTIONS[i]} **{key}**: {listings.get(key, '')}\n"
            options.append(key)
        prompt += f"{REACTION_OPTIONS[-1]} Cancel"
        msg = await ctx.send(prompt)
        for emoji in REACTION_OPTIONS[: len(options)]:
            await msg.add_reaction(emoji)
        await msg.add_reaction(REACTION_OPTIONS[-1])
        reaction = await self._wait_for_reaction(ctx, msg, REACTION_OPTIONS)
        if reaction is None or str(reaction.emoji) == REACTION_OPTIONS[-1]:
            try:
                await msg.clear_reactions()
            except Exception:
                pass
            return None, None
        selected_index = REACTION_OPTIONS.index(str(reaction.emoji))
        selected = options[selected_index]
        data = await self.course_data_proxy.get_course_data(selected)
        try:
            await msg.clear_reactions()
        except Exception:
            pass
        if data and data.get("course_data"):
            return selected, data
        else:
            return None, None

    async def _wait_for_reaction(
        self, ctx: commands.Context, message: discord.Message, valid_emojis: List[str]
    ) -> Optional[discord.Reaction]:
        def check(reaction: discord.Reaction, user: discord.User) -> bool:
            return (
                user == ctx.author
                and str(reaction.emoji) in valid_emojis
                and reaction.message.id == message.id
            )

        try:
            reaction, _ = await self.bot.wait_for(
                "reaction_add", timeout=30.0, check=check
            )
            return reaction
        except asyncio.TimeoutError:
            log.debug("Reaction wait timed out for user %s", ctx.author)
            return None

    async def _get_course_details(
        self, ctx: commands.Context, course_code: str
    ) -> Optional[discord.Embed]:
        candidate, data = await self._lookup_course_data(ctx, course_code)
        if not candidate or not (data and data.get("course_data")):
            return None
        embed = self._create_course_embed(candidate, data)
        return embed

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
                    "Pruning channel '%s' in guild '%s' (last activity: %s)",
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
        fields = [
            ("Title", data_item.get("title", "")),
            ("Term", data_item.get("term_found", "")),
            ("Instructor", data_item.get("teacher", "")),
            ("Code", data_item.get("course_code", "")),
            ("Number", data_item.get("course_number", "")),
            ("Credits", data_item.get("credits", "")),
        ]
        for name, value in fields:
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

    # --- Commands ---

    @course.command(name="list")
    @commands.cooldown(1, 600, commands.BucketType.user)
    async def list_enrollments(self, ctx: commands.Context) -> None:
        log.debug("Listing courses for user %s in guild %s", ctx.author, ctx.guild.name)
        courses = self.get_user_courses(ctx.author, ctx.guild)
        if courses:
            await ctx.send(
                "You are enrolled in the following courses:\n" + "\n".join(courses)
            )
        else:
            await ctx.send("You are not enrolled in any courses.")

    @course.command()
    @commands.cooldown(1, 86400, commands.BucketType.user)
    async def refresh(self, ctx: commands.Context, *, course_code: str) -> None:
        log.debug("Refresh invoked for '%s'", course_code)
        formatted = self._format_course_key(course_code)
        if not formatted:
            await ctx.send(error(f"Invalid course code: {course_code}."))
            return
        candidate, data = await self._lookup_course_data(ctx, formatted)
        if not candidate or not (data and data.get("course_data")):
            await ctx.send(error(f"Failed to refresh data for {formatted}."))
            return
        async with self.config.courses() as courses:
            courses[candidate] = {"is_fresh": False}
        async with ctx.typing():
            data = await self.course_data_proxy.get_course_data(candidate)
        if data and data.get("course_data"):
            await ctx.send(
                success(f"Course data for {candidate} refreshed successfully.")
            )
        else:
            await ctx.send(error(f"Failed to refresh course data for {candidate}."))

    @course.command()
    @commands.cooldown(5, 28800, commands.BucketType.user)
    async def join(self, ctx: commands.Context, *, course_code: str) -> None:
        log.debug("%s attempting to join course: %s", ctx.author, course_code)
        formatted = self._format_course_key(course_code)
        if not formatted:
            await ctx.send(error(f"Invalid course code: {course_code}."))
            return
        async with ctx.typing():
            candidate, data = await self._lookup_course_data(ctx, formatted)
        if not candidate or not (data and data.get("course_data")):
            await ctx.send(error(f"No valid course data found for {formatted}."))
            return
        if candidate.upper() in self.get_user_courses(ctx.author, ctx.guild):
            await ctx.send(info(f"You are already enrolled in {candidate}."))
            return
        if len(self.get_user_courses(ctx.author, ctx.guild)) >= self.max_courses:
            await ctx.send(
                error(
                    f"You have reached the maximum limit of {self.max_courses} courses. Leave one to join another."
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
        channel = self.get_course_channel(ctx.guild, candidate)
        if not channel:
            log.debug(
                "Course channel for %s not found; creating new channel.", candidate
            )
            channel = await self.create_course_channel(ctx.guild, category, candidate)
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
            success(f"You have successfully joined {candidate}."), delete_after=120
        )
        if self.logging_channel:
            await self.logging_channel.send(f"{ctx.author} has joined {candidate}.")

    @course.command()
    @commands.cooldown(5, 28800, commands.BucketType.user)
    async def leave(self, ctx: commands.Context, *, course_code: str) -> None:
        log.debug("%s attempting to leave course: %s", ctx.author, course_code)
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

    @commands.admin()
    @course.command()
    async def delete(
        self, ctx: commands.Context, *, channel: discord.TextChannel
    ) -> None:
        log.debug("Admin %s attempting to delete channel %s", ctx.author, channel.name)
        if channel.category not in self.get_course_categories(ctx.guild):
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
        self.logging_channel = channel
        log.debug("Logging channel set to %s by admin %s", channel.name, ctx.author)
        await ctx.send(success(f"Logging channel set to {channel.mention}."))

    @course.command(name="details")
    @commands.cooldown(10, 600, commands.BucketType.guild)
    async def course_details(
        self, ctx: commands.Context, *, course_key_raw: str
    ) -> None:
        log.debug("Fetching details for '%s'", course_key_raw)
        formatted = self._format_course_key(course_key_raw)
        if not formatted:
            await ctx.send(
                error(
                    f"Invalid course code: {course_key_raw}. Use a format like 'MATH 1A03'."
                )
            )
            return
        embed = await self._get_course_details(ctx, formatted)
        if embed is None:
            await ctx.send(error(f"Course not found: {formatted}"))
        else:
            await ctx.send(embed=embed)

    # --- Developer Commands (Owner-only) ---

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
            enabled = await self.config.enabled_guilds()
            if guild.id not in enabled:
                continue
            for category in self.get_course_categories(guild):
                for channel in category.channels:
                    if isinstance(channel, discord.TextChannel):
                        pruned = await self._prune_channel(
                            channel,
                            PRUNE_THRESHOLD,
                            "Manually pruned due to inactivity.",
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
        # Clear both the individual course data and the course listings.
        await self.config.courses.set({})
        await self.config.course_listings.set({})
        log.debug("All course data and course listings cleared by %s", ctx.author)
        await ctx.send(
            warning(
                "All courses and course listings have been cleared from the config."
            )
        )

    @dev_course.command(name="list")
    async def list_courses(self, ctx: commands.Context) -> None:
        cfg = await self.config.courses.all()
        serialized = "\n".join(list(cfg))
        await ctx.send(serialized)

    @dev_course.command(name="listall")
    async def list_all_courses(self, ctx: commands.Context) -> None:
        cfg = await self.config.course_listings.all()
        if "courses" in cfg:
            courses = cfg["courses"]
            dtm = cfg["date_updated"]
            serialized_courses = "\n".join(list(courses.keys()))
            if len(serialized_courses) > 1500:
                serialized_courses = f"{serialized_courses[:1500]}..."
            await ctx.send(
                f"{len(cfg['courses'])} courses cached on {dtm}\n{serialized_courses}"
            )
        else:
            await ctx.send("Course list not found. Run populate command first.")

    @dev_course.command(name="populate")
    async def fetch_prefixes(self, ctx: commands.Context) -> None:
        course_count = await self.course_data_proxy.update_course_listing()
        if course_count and int(course_count) > 0:
            await ctx.send(info(f"Fetched and cached {course_count} courses"))
        else:
            await ctx.send(warning("0 courses fetched. Check console log"))


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(CourseManager(bot))
