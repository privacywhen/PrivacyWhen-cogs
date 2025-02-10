"""Course Manager Cog for Redbot.

This cog allows users to join/leave course channels, refresh course data,
and view course details. It depends on CourseDataProxy from coursedata.py.
"""

import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import discord
from rapidfuzz import process
from redbot.core import commands, Config
from redbot.core.utils.chat_formatting import error, info, success, warning

from .coursedata import CourseDataProxy

log = logging.getLogger("red.course_helper")
log.setLevel(logging.DEBUG)
if not log.handlers:
    log.addHandler(logging.StreamHandler())

COURSE_KEY_PATTERN = re.compile(
    r"^\s*([A-Za-z]+)[\s\-_]*(\d+(?:[A-Za-z\d]*\d+)?)([A-Za-z])?\s*$"
)
REACTION_OPTIONS: List[str] = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "❌"]


class CourseManager(commands.Cog):
    """
    Manages course channels and details.
    """

    def __init__(self, bot: commands.Bot) -> None:
        self.bot: commands.Bot = bot
        self.channel_permissions: discord.PermissionOverwrite = (
            discord.PermissionOverwrite.from_pair(
                discord.Permissions(446676945984), discord.Permissions(0)
            )
        )
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
        log.debug(f"CourseManager initialized with max_courses={self.max_courses}")

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
        await ctx.send(
            "Course Manager is not enabled in this server. "
            "An administrator can enable it with `course enable`."
        )
        return False

    async def _auto_prune_task(self) -> None:
        """Background task to auto-prune inactive course channels."""
        PRUNE_INTERVAL = 2628000  # seconds (monthly)
        PRUNE_THRESHOLD = timedelta(days=120)
        await self.bot.wait_until_ready()
        log.debug("Auto-prune task started.")
        enabled = await self.config.enabled_guilds()
        while not self.bot.is_closed():
            log.debug(f"Auto-prune cycle started at {datetime.now(timezone.utc)}")
            for guild in self.bot.guilds:
                if guild.id not in enabled:
                    log.debug(
                        f"Skipping guild {guild.name} as Course Manager is not enabled"
                    )
                    continue
                log.debug(f"Processing guild: {guild.name} for auto-pruning")
                for category in self.get_course_categories(guild):
                    log.debug(
                        f"Processing category: {category.name} in guild {guild.name}"
                    )
                    for channel in category.channels:
                        if isinstance(channel, discord.TextChannel):
                            pruned = await self._prune_channel(
                                channel,
                                PRUNE_THRESHOLD,
                                "Auto-pruned due to inactivity.",
                            )
                            if pruned:
                                log.debug(
                                    f"Channel {channel.name} in guild {guild.name} pruned during auto-prune cycle"
                                )
            log.debug(
                f"Auto-prune cycle complete. Sleeping for {PRUNE_INTERVAL} seconds."
            )
            await asyncio.sleep(PRUNE_INTERVAL)

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

    def _format_course_key(self, course_key_raw: str) -> Optional[str]:
        log.debug(f"Formatting course key: {course_key_raw}")
        match = COURSE_KEY_PATTERN.match(course_key_raw)
        if not match:
            log.debug(f"Input '{course_key_raw}' does not match expected pattern.")
            return None
        subject, number, suffix = match.groups()
        formatted = (
            f"{subject.upper()}-{number.upper()}{suffix.upper() if suffix else ''}"
        )
        log.debug(f"Formatted course key: {formatted}")
        return formatted

    def _get_channel_name(self, course_key: str) -> str:
        base = (
            course_key[:-1] if course_key and course_key[-1].isalpha() else course_key
        )
        channel_name = base.lower()
        log.debug(f"Derived channel name: {channel_name}")
        return channel_name

    def get_course_categories(
        self, guild: discord.Guild
    ) -> List[discord.CategoryChannel]:
        base_upper = self.category_name.upper()
        return [
            cat for cat in guild.categories if cat.name.upper().startswith(base_upper)
        ]

    def get_category(self, guild: discord.Guild) -> Optional[discord.CategoryChannel]:
        return next(
            (
                cat
                for cat in guild.categories
                if cat.name.upper() == self.category_name.upper()
            ),
            None,
        )

    def get_course_channel(
        self, guild: discord.Guild, course_key: str
    ) -> Optional[discord.TextChannel]:
        target_name = self._get_channel_name(course_key)
        for category in self.get_course_categories(guild):
            for channel in category.channels:
                if (
                    isinstance(channel, discord.TextChannel)
                    and channel.name == target_name
                ):
                    log.debug(
                        f"Found course channel '{channel.name}' in guild {guild.name}"
                    )
                    return channel
        log.debug(f"Course channel '{target_name}' not found in guild {guild.name}")
        return None

    async def create_course_channel(
        self, guild: discord.Guild, category: discord.CategoryChannel, course_key: str
    ) -> discord.TextChannel:
        """Create a new course channel under the specified category."""
        target_name = self._get_channel_name(course_key)
        log.debug(f"Creating channel '{target_name}' in guild {guild.name}")
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            guild.me: discord.PermissionOverwrite(administrator=True),
        }
        channel = await guild.create_text_channel(
            target_name, overwrites=overwrites, category=category
        )
        log.debug(f"Created channel '{channel.name}' in guild {guild.name}")
        return channel

    def get_user_courses(self, user: discord.Member, guild: discord.Guild) -> List[str]:
        courses = []
        for category in self.get_course_categories(guild):
            courses.extend(
                channel.name.upper()
                for channel in category.channels
                if isinstance(channel, discord.TextChannel)
                and channel.permissions_for(user).read_messages
            )
        log.debug(f"User {user} has access to courses: {courses}")
        return courses

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

        # 1. Check for an exact listing match
        if formatted in listings:
            data = await self.course_data_proxy.get_course_data(formatted)
            if data and data.get("course_data"):
                return formatted, data
            log.error(f"Failed to fetch fresh data for perfect match: {formatted}")
            return formatted, None

        # 2. If the code doesn't end with a letter, look for variants.
        if not formatted[-1].isalpha():
            if variants := self._find_variant_matches(formatted, listings):
                if len(variants) == 1:
                    candidate = variants[0]
                    data = await self.course_data_proxy.get_course_data(candidate)
                    if data and data.get("course_data"):
                        return candidate, data
                else:
                    candidate, data = await self._prompt_variant_selection(
                        ctx, variants, listings
                    )
                    return (candidate, data) if candidate else (None, None)
        # 3. Fallback to fuzzy lookup.
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
        return (selected, data) if data and data.get("course_data") else (None, None)

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
            log.debug(f"Reaction wait timed out for user {ctx.author}")
            return None

    async def _get_course_details(
        self, ctx: commands.Context, course_code: str
    ) -> Optional[discord.Embed]:
        candidate, data = await self._lookup_course_data(ctx, course_code)
        if not candidate or not data or not data.get("course_data"):
            return None
        return self._create_course_embed(candidate, data)

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
                    f"Pruning channel '{channel.name}' in guild '{channel.guild.name}' (last activity: {last_activity})"
                )
                await channel.delete(reason=reason)
                return True
        except Exception as e:
            log.error(
                f"Error pruning channel '{channel.name}' in guild '{channel.guild.name}': {e}"
            )
        return False

    def _create_course_embed(
        self, course_key: str, course_data: Dict[str, Any]
    ) -> discord.Embed:
        log.debug(f"Creating embed for course: {course_key}")
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

    @course.command(name="list")
    @commands.cooldown(1, 600, commands.BucketType.user)
    async def list_enrollments(self, ctx: commands.Context) -> None:
        log.debug(f"Listing courses for user {ctx.author} in guild {ctx.guild.name}")
        if courses := self.get_user_courses(ctx.author, ctx.guild):
            await ctx.send(
                "You are enrolled in the following courses:\n" + "\n".join(courses)
            )
        else:
            await ctx.send("You are not enrolled in any courses.")

    @course.command()
    @commands.cooldown(1, 86400, commands.BucketType.user)
    async def refresh(self, ctx: commands.Context, *, course_code: str) -> None:
        log.debug(f"Refresh invoked for '{course_code}'")
        formatted = self._format_course_key(course_code)
        if not formatted:
            await ctx.send(error(f"Invalid course code: {course_code}."))
            return
        candidate, data = await self._lookup_course_data(ctx, formatted)
        if not candidate or not data or not data.get("course_data"):
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
        log.debug(f"{ctx.author} attempting to join course: {course_code}")
        formatted = self._format_course_key(course_code)
        if not formatted:
            await ctx.send(error(f"Invalid course code: {course_code}."))
            return
        async with ctx.typing():
            # Pass auto_variant=True to automatically pick a variant.
            candidate, data = await self._lookup_course_data(
                ctx, formatted, auto_variant=True
            )
        if not candidate or not data or not data.get("course_data"):
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
                    f"Created category '{self.category_name}' in guild {ctx.guild.name}"
                )
            except discord.Forbidden:
                await ctx.send(
                    error("I don't have permission to create the courses category.")
                )
                return
        channel = self.get_course_channel(ctx.guild, candidate)
        if not channel:
            log.debug(
                f"Course channel for {candidate} not found; creating new channel."
            )
            channel = await self.create_course_channel(ctx.guild, category, candidate)
        try:
            await channel.set_permissions(
                ctx.author, overwrite=self.channel_permissions
            )
            log.debug(f"Permissions set for {ctx.author} on channel {channel.name}")
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
        log.debug(f"{ctx.author} attempting to leave course: {course_code}")
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
            log.debug(f"Removed permissions for {ctx.author} on channel {channel.name}")
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
        log.debug(f"Admin {ctx.author} attempting to delete channel {channel.name}")
        if channel.category not in self.get_course_categories(ctx.guild):
            await ctx.send(error(f"{channel.mention} is not a course channel."))
            return
        try:
            await channel.delete()
            log.debug(f"Channel {channel.name} deleted by admin {ctx.author}")
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
        log.debug(f"Logging channel set to {channel.name} by admin {ctx.author}")
        await ctx.send(success(f"Logging channel set to {channel.mention}."))

    @course.command(name="details")
    @commands.cooldown(10, 600, commands.BucketType.guild)
    async def course_details(
        self, ctx: commands.Context, *, course_key_raw: str
    ) -> None:
        log.debug(f"Fetching details for '{course_key_raw}'")
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

    @commands.is_owner()
    @commands.group(name="dc", invoke_without_command=True)
    async def dev_course(self, ctx: commands.Context) -> None:
        log.debug(f"Dev command group 'dev_course' invoked by {ctx.author}")
        await ctx.send_help(self.dev_course)

    @dev_course.command(name="term")
    async def set_term_codes(
        self, ctx: commands.Context, term_name: str, term_id: int
    ) -> None:
        async with self.config.term_codes() as term_codes:
            term_codes[term_name.lower()] = term_id
        log.debug(f"Set term code for {term_name} to {term_id}")
        await ctx.send(
            success(f"Term code for {term_name.capitalize()} set to: {term_id}")
        )

    @dev_course.command(name="clearstale")
    async def clear_stale_config(self, ctx: commands.Context) -> None:
        log.debug("Clearing stale config entries.")
        stale = []
        courses = await self.config.courses.all()
        for course_key in courses.keys():
            if not any(
                self.get_course_channel(guild, course_key) for guild in self.bot.guilds
            ):
                stale.append(course_key)
        for course_key in stale:
            await self.config.courses.clear_raw(course_key)
            log.debug(f"Cleared stale entry for course {course_key}")
        if stale:
            await ctx.send(success(f"Cleared stale config entries: {', '.join(stale)}"))
        else:
            await ctx.send(info("No stale course config entries found."))

    @dev_course.command(name="prune")
    async def manual_prune(self, ctx: commands.Context) -> None:
        log.debug(f"Manual prune triggered by {ctx.author}")
        pruned_channels = []
        PRUNE_THRESHOLD = timedelta(days=120)
        for guild in self.bot.guilds:
            enabled = await self.config.enabled_guilds()
            if guild.id not in enabled:
                log.debug(
                    f"Skipping guild {guild.name} as Course Manager is not enabled"
                )
                continue
            log.debug(f"Processing guild {guild.name} for manual pruning")
            for category in self.get_course_categories(guild):
                log.debug(f"Processing category {category.name} in guild {guild.name}")
                for channel in category.channels:
                    if isinstance(channel, discord.TextChannel):
                        if await self._prune_channel(
                            channel,
                            PRUNE_THRESHOLD,
                            "Manually pruned due to inactivity.",
                        ):
                            pruned_channels.append(f"{guild.name} - {channel.name}")
                            log.debug(
                                f"Channel {channel.name} in guild {guild.name} pruned manually"
                            )
        if pruned_channels:
            await ctx.send(success("Pruned channels:\n" + "\n".join(pruned_channels)))
        else:
            await ctx.send(info("No inactive channels to prune."))

    @dev_course.command(name="clearcourses")
    async def clear_courses(self, ctx: commands.Context) -> None:
        await self.config.courses.set({})
        await self.config.course_listings.set({})
        log.debug(f"All course data and course listings cleared by {ctx.author}")
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
