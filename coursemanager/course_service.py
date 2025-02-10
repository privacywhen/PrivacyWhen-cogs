import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import discord
from rapidfuzz import process
from redbot.core import Config, commands
from redbot.core.utils.chat_formatting import error, info, success, warning

from .course_data_proxy import CourseDataProxy
from .utils import (
    format_course_key,
    get_channel_name,
    prune_channel,
    get_categories_by_prefix,
)
from .constants import REACTION_OPTIONS

log = logging.getLogger("red.course_service")
log.setLevel(logging.DEBUG)
if not log.handlers:
    log.addHandler(logging.StreamHandler())


class CourseService:
    """Implements high-level course workflows."""

    def __init__(self, bot: commands.Bot, config: Config) -> None:
        self.bot: commands.Bot = bot
        self.config: Config = config
        self.category_name: str = "COURSES"
        self.max_courses: int = 10
        self.logging_channel: Optional[discord.TextChannel] = None
        self.channel_permissions: discord.PermissionOverwrite = (
            discord.PermissionOverwrite.from_pair(
                discord.Permissions(446676945984), discord.Permissions(0)
            )
        )
        self.course_data_proxy: CourseDataProxy = CourseDataProxy(self.config, log)

    async def _check_enabled(self, ctx: commands.Context) -> bool:
        """
        Helper method to check if Course Manager is enabled in the guild.
        If not enabled, it notifies the user and returns False.
        """
        enabled: List[int] = await self.config.enabled_guilds()
        if ctx.guild.id not in enabled:
            await ctx.send(
                error(
                    "Course Manager is disabled in this server. Please enable it using the 'course enable' command."
                )
            )
            return False
        return True

    async def enable(self, ctx: commands.Context) -> None:
        enabled: List[int] = await self.config.enabled_guilds()
        if ctx.guild.id in enabled:
            await ctx.send("Course Manager is already enabled in this server.")
        else:
            enabled.append(ctx.guild.id)
            await self.config.enabled_guilds.set(enabled)
            await ctx.send("Course Manager has been enabled in this server.")

    async def disable(self, ctx: commands.Context) -> None:
        enabled: List[int] = await self.config.enabled_guilds()
        if ctx.guild.id not in enabled:
            await ctx.send("Course Manager is already disabled in this server.")
        else:
            enabled.remove(ctx.guild.id)
            await self.config.enabled_guilds.set(enabled)
            await ctx.send("Course Manager has been disabled in this server.")

    def get_course_categories(
        self, guild: discord.Guild
    ) -> List[discord.CategoryChannel]:
        """
        Retrieve all course categories in the guild that match the configured category name.
        """
        return get_categories_by_prefix(guild, self.category_name)

    def get_category(self, guild: discord.Guild) -> Optional[discord.CategoryChannel]:
        """
        Retrieve the course category channel that exactly matches the configured category name.

        Args:
            guild (discord.Guild): The Discord guild.

        Returns:
            Optional[discord.CategoryChannel]: The matching category channel, or None if not found.
        """
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
        """
        Retrieve a course text channel by course key within the guild.

        Args:
            guild (discord.Guild): The Discord guild.
            course_key (str): The course key to search for.

        Returns:
            Optional[discord.TextChannel]: The course text channel if found; otherwise, None.
        """
        target_name = get_channel_name(course_key)
        channel = next(
            (
                ch
                for category in self.get_course_categories(guild)
                for ch in category.channels
                if isinstance(ch, discord.TextChannel) and ch.name == target_name
            ),
            None,
        )
        if channel:
            log.debug(f"Found course channel '{channel.name}' in guild {guild.name}")
        else:
            log.debug(f"Course channel '{target_name}' not found in guild {guild.name}")
        return channel

    async def create_course_channel(
        self, guild: discord.Guild, category: discord.CategoryChannel, course_key: str
    ) -> discord.TextChannel:
        """
        Create a new course text channel in the specified category.

        Args:
            guild (discord.Guild): The Discord guild.
            category (discord.CategoryChannel): The category in which to create the channel.
            course_key (str): The course key used to determine the channel name.

        Returns:
            discord.TextChannel: The newly created course text channel.
        """
        target_name = get_channel_name(course_key)
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
        """
        Retrieve a list of course channel names that the user can access in the guild.

        Args:
            user (discord.Member): The user whose course enrollments are being checked.
            guild (discord.Guild): The Discord guild.

        Returns:
            List[str]: A list of course channel names in uppercase.
        """
        courses = [
            channel.name.upper()
            for category in self.get_course_categories(guild)
            for channel in category.channels
            if isinstance(channel, discord.TextChannel)
            and channel.permissions_for(user).read_messages
        ]
        log.debug(f"User {user} has access to courses: {courses}")
        return courses

    def _find_variant_matches(self, base: str, listings: Dict[str, str]) -> List[str]:
        """
        Find course variants that start with the given base course code.

        Args:
            base (str): The base course code.
            listings (Dict[str, str]): The course listings.

        Returns:
            List[str]: A list of variant course keys.
        """
        return [
            key for key in listings if key.startswith(base) and len(key) > len(base)
        ]

    async def _prompt_variant_selection(
        self, ctx: commands.Context, variants: List[str], listings: Dict[str, str]
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """
        Prompt the user to select from multiple course variants.

        Args:
            ctx (commands.Context): The command context.
            variants (List[str]): A list of variant course keys.
            listings (Dict[str, str]): The course listings with additional information.

        Returns:
            Tuple[Optional[str], Optional[Dict[str, Any]]]: The selected course key and its data, or (None, None) if canceled.
        """
        options = [
            f"{REACTION_OPTIONS[i]} **{key}**: {listings.get(key, '')}"
            for i, key in enumerate(variants)
        ]
        prompt = (
            "Multiple course variants found. Please choose one:\n"
            + "\n".join(options)
            + f"\n{REACTION_OPTIONS[-1]} Cancel"
        )
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
        """
        Lookup course data using an exact match, variant matching, or fuzzy matching.

        Args:
            ctx (commands.Context): The command context.
            formatted (str): The formatted course key.

        Returns:
            Tuple[Optional[str], Optional[Dict[str, Any]]]: The course key and its data if found; otherwise, (None, None).
        """
        listings: Dict[str, str] = (await self.config.course_listings()).get(
            "courses", {}
        )
        if formatted in listings:
            data = await self.course_data_proxy.get_course_data(formatted)
            if data and data.get("course_data"):
                return formatted, data
            log.error(f"Failed to fetch fresh data for perfect match: {formatted}")
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
                    return (candidate, data) if candidate else (None, None)
        candidate, data = await self._fallback_fuzzy_lookup(ctx, formatted)
        return candidate, data

    async def _fallback_fuzzy_lookup(
        self, ctx: commands.Context, formatted: str
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """
        Perform a fuzzy lookup for a course key when an exact match is not found.

        Args:
            ctx (commands.Context): The command context.
            formatted (str): The formatted course key.

        Returns:
            Tuple[Optional[str], Optional[Dict[str, Any]]]: The selected course key and its data if found; otherwise, (None, None).
        """
        listings: Dict[str, str] = (await self.config.course_listings()).get(
            "courses", {}
        )
        if not listings:
            log.debug("Course listings unavailable; cannot perform fuzzy lookup.")
            return None, None
        matches = process.extract(formatted, listings.keys(), limit=5, score_cutoff=70)
        if not matches:
            return None, None
        options = [
            f"{REACTION_OPTIONS[i]} **{match[0]}**: {listings.get(match[0], '')}"
            for i, match in enumerate(matches)
        ]
        prompt = (
            "Course not found. Did you mean:\n"
            + "\n".join(options)
            + f"\n{REACTION_OPTIONS[-1]} Cancel"
        )
        msg = await ctx.send(prompt)
        for emoji in REACTION_OPTIONS[: len(matches)]:
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
        selected = matches[selected_index][0]
        data = await self.course_data_proxy.get_course_data(selected)
        try:
            await msg.clear_reactions()
        except Exception:
            pass
        return (selected, data) if data and data.get("course_data") else (None, None)

    async def _wait_for_reaction(
        self, ctx: commands.Context, message: discord.Message, valid_emojis: List[str]
    ) -> Optional[discord.Reaction]:
        """
        Wait for a reaction from the user on a specific message.

        Args:
            ctx (commands.Context): The command context.
            message (discord.Message): The message to monitor for reactions.
            valid_emojis (List[str]): A list of acceptable emoji strings.

        Returns:
            Optional[discord.Reaction]: The reaction received or None if timed out.
        """

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

    async def course_details(
        self, ctx: commands.Context, course_code: str
    ) -> Optional[discord.Embed]:
        # Optionally, you may check for enablement here too if details should be restricted.
        formatted = format_course_key(course_code)
        if not formatted:
            return None
        candidate, data = await self._lookup_course_data(ctx, formatted)
        if not candidate or not data or not data.get("course_data"):
            return None
        return self._create_course_embed(candidate, data)

    def _create_course_embed(
        self, course_key: str, course_data: Dict[str, Any]
    ) -> discord.Embed:
        """
        Create a Discord embed containing the course details.

        Args:
            course_key (str): The course key.
            course_data (Dict[str, Any]): The course data structure containing details.

        Returns:
            discord.Embed: The formatted embed with course details.
        """
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

    async def list_enrollments(self, ctx: commands.Context) -> None:
        if not await self._check_enabled(ctx):
            return
        courses = self.get_user_courses(ctx.author, ctx.guild)
        if courses:
            await ctx.send(
                "You are enrolled in the following courses:\n" + "\n".join(courses)
            )
        else:
            await ctx.send("You are not enrolled in any courses.")

    async def join_course(self, ctx: commands.Context, course_code: str) -> None:
        # Check if Course Manager is enabled for the guild.
        if not await self._check_enabled(ctx):
            return

        formatted = format_course_key(course_code)
        if not formatted:
            await ctx.send(error(f"Invalid course code: {course_code}."))
            return
        user_courses = self.get_user_courses(ctx.author, ctx.guild)
        channel = self.get_course_channel(ctx.guild, formatted)
        if channel:
            if formatted.upper() in user_courses:
                await ctx.send(info(f"You are already enrolled in {formatted}."))
                return
            if len(user_courses) >= self.max_courses:
                await ctx.send(
                    error(
                        f"You have reached the maximum limit of {self.max_courses} courses."
                    )
                )
                return
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
                success(f"You have successfully joined {formatted}."), delete_after=120
            )
            if self.logging_channel:
                await self.logging_channel.send(f"{ctx.author} has joined {formatted}.")
            return
        async with ctx.typing():
            candidate, data = await self._lookup_course_data(ctx, formatted)
        if not candidate or not data or not data.get("course_data"):
            await ctx.send(error(f"No valid course data found for {formatted}."))
            return
        user_courses = self.get_user_courses(ctx.author, ctx.guild)
        if candidate.upper() in user_courses:
            await ctx.send(info(f"You are already enrolled in {candidate}."))
            return
        if len(user_courses) >= self.max_courses:
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

    async def leave_course(self, ctx: commands.Context, course_code: str) -> None:
        # Check if Course Manager is enabled for the guild.
        if not await self._check_enabled(ctx):
            return

        formatted = format_course_key(course_code)
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

    async def admin_delete_channel(
        self, ctx: commands.Context, channel: discord.TextChannel
    ) -> None:
        # (Optionally, you could also check enablement here if desired.)
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

    async def set_logging(
        self, ctx: commands.Context, channel: discord.TextChannel
    ) -> None:
        self.logging_channel = channel
        log.debug(f"Logging channel set to {channel.name} by admin {ctx.author}")
        await ctx.send(success(f"Logging channel set to {channel.mention}."))

    async def set_term_code(
        self, ctx: commands.Context, term_name: str, term_id: int
    ) -> None:
        async with self.config.term_codes() as term_codes:
            term_codes[term_name.lower()] = term_id
        log.debug(f"Set term code for {term_name} to {term_id}")
        await ctx.send(
            success(f"Term code for {term_name.capitalize()} set to: {term_id}")
        )

    async def clear_stale_config(self, ctx: commands.Context) -> None:
        log.debug("Clearing stale config entries.")
        stale: List[str] = []
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

    async def manual_prune(self, ctx: commands.Context) -> None:
        log.debug(f"Manual prune triggered by {ctx.author}")
        pruned_channels: List[str] = []
        PRUNE_THRESHOLD = timedelta(days=120)
        for guild in self.bot.guilds:
            enabled: List[int] = await self.config.enabled_guilds()
            if guild.id not in enabled:
                log.debug(
                    f"Skipping guild {guild.name} as Course Manager is not enabled"
                )
                continue
            for category in self.get_course_categories(guild):
                for channel in category.channels:
                    if isinstance(channel, discord.TextChannel):
                        if await prune_channel(
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

    async def clear_courses(self, ctx: commands.Context) -> None:
        await self.config.courses.set({})
        await self.config.course_listings.set({})
        log.debug(f"All course data and course listings cleared by {ctx.author}")
        await ctx.send(
            warning(
                "All courses and course listings have been cleared from the config."
            )
        )

    async def list_courses(self, ctx: commands.Context) -> None:
        cfg = await self.config.courses.all()
        serialized = "\n".join(list(cfg))
        await ctx.send(serialized)

    async def list_all_courses(self, ctx: commands.Context) -> None:
        cfg = await self.config.course_listings.all()
        if "courses" in cfg:
            courses = cfg["courses"]
            dtm = cfg["date_updated"]
            serialized_courses = "\n".join(list(courses.keys()))
            if len(serialized_courses) > 1500:
                serialized_courses = f"{serialized_courses[:1500]}..."
            await ctx.send(
                f"{len(courses)} courses cached on {dtm}\n{serialized_courses}"
            )
        else:
            await ctx.send("Course list not found. Run populate command first.")

    async def populate_courses(self, ctx: commands.Context) -> None:
        course_count = await self.course_data_proxy.update_course_listing()
        if course_count and int(course_count) > 0:
            await ctx.send(info(f"Fetched and cached {course_count} courses"))
        else:
            await ctx.send(warning("0 courses fetched. Check console log"))

    async def refresh_course_data(
        self, ctx: commands.Context, course_code: str
    ) -> None:
        """
        Refresh course data for a given course.
        Marks the cached data as stale so that the next fetch returns fresh data.
        """
        if not await self._check_enabled(ctx):
            return

        formatted = format_course_key(course_code)
        if not formatted:
            await ctx.send(error(f"Invalid course code: {course_code}."))
            return

        # Mark the existing data as stale.
        async with self.config.courses() as courses:
            if formatted in courses:
                courses[formatted]["is_fresh"] = False
            else:
                await ctx.send(error(f"No existing data for course {formatted}."))
                return

        # Re-fetch data from the external API.
        data = await self.course_data_proxy.get_course_data(formatted)
        if data and data.get("course_data"):
            await ctx.send(success(f"Course data for {formatted} has been refreshed."))
        else:
            await ctx.send(error(f"Failed to refresh course data for {formatted}."))
