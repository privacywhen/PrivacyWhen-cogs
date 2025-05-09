"""Handle course data management, user access, and course channel clustering."""

from __future__ import annotations

import asyncio
import functools
import time
from typing import TYPE_CHECKING, Any, Callable, TypeVar

import discord
from redbot.core import Config, commands  # noqa: TC002
from redbot.core.utils.chat_formatting import error, info, pagify, success, warning
from redbot.core.utils.menus import menu

from .course_code import CourseCode
from .course_data_proxy import CourseDataProxy
from .logger_util import get_logger
from .utils import get_available_course_category, get_categories_by_prefix

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from .channel_service import ChannelService
    from .course_channel_clustering import CourseChannelClustering

log = get_logger(__name__)
T = TypeVar("T")


def requires_enabled(
    func: Callable[..., Coroutine[Any, Any, T]],
) -> Callable[..., Coroutine[Any, Any, T]]:
    """Ensure the course manager is enabled for the current server context."""

    @functools.wraps(func)
    async def wrapper(
        self: CourseService,
        ctx: commands.Context,
        *args: object,
        **kwargs: object,
    ) -> T:
        if not await self._check_enabled(ctx):
            return None
        return await func(self, ctx, *args, **kwargs)

    return wrapper


class CourseService:
    """Fetch, cache, and manage course data with optional term resolution."""

    def __init__(self, bot: commands.Bot, config: Config) -> None:
        """Initialize CourseService with bot, config, and necessary data."""
        self.bot: commands.Bot = bot
        self.config: Config = config
        self.lock = (
            asyncio.Lock()
        )  # Initialize a lock for the enabled_guilds configuration
        self.category_name: str = "COURSES"
        self.max_courses: int = 10
        self.logging_channel: discord.TextChannel | None = None
        self.course_data_proxy: CourseDataProxy = CourseDataProxy(self.config, log)
        self._listings_cache: dict[str, str] | None = None
        self._listings_cache_time: float = 0.0
        self._listings_ttl: float = 60.0

    async def _load_listings(self) -> dict[str, str]:
        """Load course listings from cache or config with TTL."""
        now = time.monotonic()
        if (
            self._listings_cache is not None
            and now - self._listings_cache_time < self._listings_ttl
        ):
            return self._listings_cache

        data = await self.config.course_listings()
        self._listings_cache = data.get("courses", {})
        self._listings_cache_time = now
        return self._listings_cache

    async def _prepare_course_access(
        self,
        ctx: commands.Context,
        course_code: str,
    ) -> tuple[discord.Guild, discord.Member, CourseCode | None]:
        """Validate user limit and resolve the raw course code."""
        guild = ctx.guild
        user = ctx.author
        if not await self._ensure_user_channel_limit_not_exceeded(ctx, user, guild):
            return guild, user, None
        course_obj = await self._resolve_course(ctx, course_code)
        return guild, user, course_obj

    async def _fetch_and_validate_course_data(
        self,
        ctx: commands.Context,
        course_obj: CourseCode,
    ) -> tuple[CourseCode | None, Any]:
        """Fetch detailed course data; notify user on failure."""
        async with ctx.typing():
            candidate_obj, data = await self._lookup_course_data(
                ctx,
                course_obj,
                already_resolved=True,
            )
        if not candidate_obj or not self._is_valid_course_data(data):
            await ctx.send(
                error(
                    f"No course data could be retrieved for {course_obj.canonical()}.",
                ),
            )
            return None, None
        return candidate_obj, data

    async def _get_or_create_channel(
        self,
        guild: discord.Guild,
        course_obj: CourseCode,
        category: discord.CategoryChannel,
    ) -> discord.TextChannel:
        """Return existing channel or create a new one under the given category."""
        if channel := self.get_course_channel(guild, course_obj):
            return channel
        return await self.create_course_channel(guild, category, course_obj)

    def _is_valid_course_data(self, data: object) -> bool:
        return bool(data and data.get("cached_course_data"))

    async def _check_enabled(self, ctx: commands.Context) -> bool:
        enabled_guilds: list[int] = await self.config.enabled_guilds()
        if ctx.guild.id not in enabled_guilds:
            await ctx.send(
                error(
                    "Course Manager is disabled. Enable it with the 'course' command.",
                ),
            )
            return False
        return True

    async def _update_enabled_status(
        self,
        ctx: commands.Context,
        *,
        enable: bool,
    ) -> None:
        async with (
            self.lock
        ):  # Acquire the lock to prevent concurrent modification of enabled_guilds
            async with self.config.enabled_guilds() as enabled_guilds:
                log.debug(f"Before update, Enabled guilds: {enabled_guilds}")
                if enable:
                    if ctx.guild.id in enabled_guilds:
                        await ctx.send(
                            "Course Manager is already enabled on this server.",
                        )
                        log.debug(
                            f"Guild {ctx.guild.id} already enabled Course Manager.",
                        )
                    else:
                        enabled_guilds.append(ctx.guild.id)
                        await ctx.send(
                            "Course Manager has been enabled on this server.",
                        )
                        log.debug(f"Guild {ctx.guild.id} enabled Course Manager.")
                elif ctx.guild.id not in enabled_guilds:
                    await ctx.send("Course Manager is already disabled on this server.")
                    log.debug(f"Guild {ctx.guild.id} already disabled Course Manager.")
                else:
                    enabled_guilds.remove(ctx.guild.id)
                    await ctx.send("Course Manager has been disabled on this server.")
                    log.debug(f"Guild {ctx.guild.id} disabled Course Manager.")
                # Confirm the update and ensure the list is saved
                log.debug(f"After update, Enabled guilds: {enabled_guilds}")

    async def enable(self, ctx: commands.Context) -> None:
        """Enable the course manager for the current guild."""
        await self._update_enabled_status(ctx, enable=True)

    async def disable(self, ctx: commands.Context) -> None:
        """Disable the course manager for the current guild."""
        await self._update_enabled_status(ctx, enable=False)

    def get_category(self, guild: discord.Guild) -> discord.CategoryChannel | None:
        """Return the configured course category if it exists."""
        category = next(
            (
                cat
                for cat in guild.categories
                if cat.name.upper() == self.category_name.upper()
            ),
            None,
        )
        log.debug(
            "Category %s %s in guild %s",
            category.name if category else self.category_name,
            "found" if category else "not found",
            guild.name,
        )
        return category

    def get_course_channel(
        self,
        guild: discord.Guild,
        course: CourseCode,
    ) -> discord.TextChannel | None:
        """Find an existing text channel for this CourseCode."""
        target_name = course.formatted_channel_name()
        channel = next(
            (
                ch
                for cat in get_categories_by_prefix(guild, self.category_name)
                for ch in cat.channels
                if isinstance(ch, discord.TextChannel) and ch.name == target_name
            ),
            None,
        )
        log.debug(
            "%s course channel '%s' in guild '%s'",
            "Found" if channel else "No",
            target_name,
            guild.name,
        )
        return channel

    async def create_course_channel(
        self,
        guild: discord.Guild,
        category: discord.CategoryChannel,
        course: CourseCode,
    ) -> discord.TextChannel:
        """Create a new course text channel under the given category."""
        target_name = course.formatted_channel_name()
        log.debug(
            "Creating channel '%s' in guild '%s' under category '%s'",
            target_name,
            guild.name,
            category.name,
        )
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            guild.me: discord.PermissionOverwrite(administrator=True),
        }
        channel = await guild.create_text_channel(
            target_name,
            overwrites=overwrites,
            category=category,
        )
        log.debug("Created channel '%s' in guild '%s'", channel.name, guild.name)
        return channel

    def _has_joined(self, user: discord.Member, channel: discord.TextChannel) -> bool:
        overwrite = channel.overwrites_for(user)
        return bool(overwrite.read_messages and overwrite.send_messages)

    def _user_already_joined(
        self,
        user: discord.Member,
        guild: discord.Guild,
        course: CourseCode,
    ) -> bool:
        return course.formatted_channel_name() in self.get_user_courses(user, guild)

    async def _handle_existing_channel(
        self,
        ctx: commands.Context,
        user: discord.Member,
        channel: discord.TextChannel,
        canonical: str,
    ) -> bool:
        """If the user already has access, inform them; otherwise grant it."""
        if self._has_joined(user, channel):
            await ctx.send(
                info(f"You are already a member of {canonical}."),
                delete_after=120,
            )
            return True
        return bool(await self._grant_access(ctx, channel, canonical))

    def get_user_courses(self, user: discord.Member, guild: discord.Guild) -> list[str]:
        """List the names of all course channels the user has joined."""
        joined = [
            ch.name
            for cat in get_categories_by_prefix(guild, self.category_name)
            for ch in cat.channels
            if isinstance(ch, discord.TextChannel) and self._has_joined(user, ch)
        ]
        log.debug("User '%s' has joined courses: %s", user, joined)
        return joined

    def _user_channel_limit_reached(
        self,
        user: discord.Member,
        guild: discord.Guild,
    ) -> bool:
        return len(self.get_user_courses(user, guild)) >= self.max_courses

    async def _ensure_user_channel_limit_not_exceeded(
        self,
        ctx: commands.Context,
        user: discord.Member,
        guild: discord.Guild,
    ) -> bool:
        if self._user_channel_limit_reached(user, guild):
            await ctx.send(
                error(
                    f"You have reached the {self.max_courses} course channel limit.",
                ),
            )
            return False
        return True

    async def _lookup_course_data(
        self,
        ctx: commands.Context,
        course: CourseCode,
        *,
        already_resolved: bool = False,
    ) -> tuple[CourseCode | None, Any]:
        """Fetch fresh or cached course data with optional resolver fallback."""
        canonical = course.canonical()
        log.debug("Looking up course data for '%s'", canonical)
        listings = await self._load_listings()

        if canonical in listings:
            data = await self.course_data_proxy.get_course_data(
                canonical,
                detailed=True,
            )
            if self._is_valid_course_data(data):
                return course, data
            log.error("Failed to fetch fresh data for '%s'", canonical)
            return course, None

        if not already_resolved:
            from .course_code_resolver import CourseCodeResolver

            resolver = CourseCodeResolver(
                listings,
                course_data_proxy=self.course_data_proxy,
            )
            return await resolver.resolve_course_code(ctx, course)

        log.debug("Already resolved; skipping further resolution for '%s'", canonical)
        return course, None

    async def _resolve_course(
        self,
        ctx: commands.Context,
        course_code: str,
    ) -> CourseCode | None:
        """Validate and normalize a raw course code via utils."""
        safe = discord.utils.escape_mentions(course_code)
        from .utils import validate_and_resolve_course_code

        course_obj = await validate_and_resolve_course_code(
            ctx,
            safe,
            await self._load_listings(),
            self.course_data_proxy,
        )
        if course_obj is None:
            await ctx.send(error(f"The course code '{safe}' is invalid."))
        return course_obj

    async def course_details(self, ctx: commands.Context, course_code: str) -> None:
        """Show an embed of detailed course information."""
        try:
            course_obj = await self._resolve_course(ctx, course_code)
            if course_obj is None:
                return
            data = await self.course_data_proxy.get_course_data(
                course_obj.canonical(),
                detailed=True,
            )
            if not self._is_valid_course_data(data):
                await ctx.send(
                    error("No course data could be retrieved for this course."),
                )
                return
            embed = self._create_course_embed(course_obj.canonical(), data)
            await ctx.send(embed=embed)
        except Exception:
            log.exception("Error retrieving course details for %s", course_code)
            await ctx.send(error("An error occurred while retrieving course details."))

    def _create_course_embed(
        self,
        course_key: str,
        course_data: dict[str, Any],
    ) -> discord.Embed:
        """Assemble a Discord Embed from course_data."""
        embed = discord.Embed(
            title=f"Course Details: {course_key}",
            color=discord.Color.green(),
        )
        data_item = (course_data.get("cached_course_data") or [{}])[0]
        embed.set_footer(
            text=f"Last updated: {course_data.get('last_updated', 'Unknown')}",
        )
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
        if desc := data_item.get("description"):
            embed.add_field(name="Description", value=desc, inline=False)
        if req := data_item.get("prerequisites"):
            embed.add_field(name="Prerequisite(s)", value=req, inline=True)
        if anti := data_item.get("antirequisites"):
            embed.add_field(name="Antirequisite(s)", value=anti, inline=True)
        return embed

    async def _update_channel_permissions(  # noqa: PLR0913
        self,
        ctx: commands.Context,
        channel: discord.TextChannel,
        user: discord.Member,
        overwrite: discord.PermissionOverwrite | None,
        success_msg: str,
        action: str,
    ) -> bool:
        """Set per-user permissions on a channel and log/reply on success or failure."""
        try:
            await channel.set_permissions(user, overwrite=overwrite)
            log.debug(
                "%s for %s on channel %s (ID: %s)",
                action,
                user,
                channel.name,
                channel.id,
            )
        except discord.Forbidden:
            log.exception(
                "Failed to %s for %s on channel %s",
                action.lower(),
                user,
                channel.name,
            )
            await ctx.send(
                error("Insufficient permissions to manage channel settings."),
            )
            return False

        await ctx.send(success(success_msg), delete_after=120)
        if self.logging_channel:
            await self.logging_channel.send(
                f"{user} has {action.lower()} {channel.name}.",
            )
        return True

    async def _grant_access(
        self,
        ctx: commands.Context,
        channel: discord.TextChannel,
        canonical: str,
    ) -> bool:
        """Grant the invoking user read/send access to a channel."""
        return await self._update_channel_permissions(
            ctx,
            channel,
            ctx.author,
            discord.PermissionOverwrite(read_messages=True, send_messages=True),
            f"Access granted: You have joined {canonical}.",
            "Granted access",
        )

    @requires_enabled
    async def grant_course_channel_access(
        self,
        ctx: commands.Context,
        course_code: str,
    ) -> None:
        """Grant the user access to a course channel, creating it if needed."""
        # Step 1: validate and resolve
        guild, user, course_obj = await self._prepare_course_access(ctx, course_code)
        if course_obj is None:
            return

        # Step 2: fetch & validate data
        candidate_obj, _ = await self._fetch_and_validate_course_data(ctx, course_obj)
        if candidate_obj is None:
            return

        # Step 3: pick or create category + channel
        category = await get_available_course_category(guild, self.category_name, ctx)
        if category is None:
            return
        channel = await self._get_or_create_channel(guild, candidate_obj, category)

        # Step 4: grant permissions
        await self._grant_access(ctx, channel, candidate_obj.canonical())

    @requires_enabled
    async def revoke_course_channel_access(
        self,
        ctx: commands.Context,
        course_code: str,
    ) -> None:
        """Command: revoke the user's access to a course channel."""
        try:
            guild = ctx.guild
            course_obj = await self._resolve_course(ctx, course_code)
            if course_obj is None:
                return
            canonical = course_obj.canonical()
            channel = self.get_course_channel(guild, course_obj)
            if not channel:
                await ctx.send(
                    error(f"You are not a member of the course channel {canonical}."),
                )
                return
            await self._update_channel_permissions(
                ctx,
                channel,
                ctx.author,
                None,
                f"You have left the course channel {canonical}.",
                "Removed permissions",
            )
        except Exception:
            log.exception("Error in revoke_course_channel_access")
            await ctx.send(error("An error occurred while revoking access."))

    async def set_logging(
        self,
        ctx: commands.Context,
        channel: discord.TextChannel,
    ) -> None:
        """Command: set a channel where join/leave events will be logged."""
        self.logging_channel = channel
        log.debug("Logging channel set to %s by %s", channel.name, ctx.author)
        await ctx.send(
            success(f"Logging channel has been updated to {channel.mention}."),
        )

    async def set_term_code(
        self,
        ctx: commands.Context,
        term_name: str,
        year: int,
        term_id: int,
    ) -> None:
        """Command: register a term-to-ID mapping."""
        term_key = f"{term_name.lower()}-{year}"
        async with self.config.term_codes() as term_codes:
            term_codes[term_key] = term_id
        log.debug("Set term code for %s to %s", term_key, term_id)
        await ctx.send(
            success(f"Term code for {term_name.capitalize()} {year} set to: {term_id}"),
        )

    async def list_all_courses(self, ctx: commands.Context) -> None:
        """Command: paging list of all cached course codes."""
        cfg = await self.config.course_listings.all()
        if courses := cfg.get("courses", {}):
            dtm = cfg.get("date_updated", "Unknown")
            serialized = "\n".join(courses.keys())
            pages = [
                f"{len(courses)} courses cached on {dtm}\n{p}"
                for p in pagify(serialized, page_length=1500)
            ]
            await menu(ctx, pages, timeout=60.0, user=ctx.author)
        else:
            await ctx.send(
                "No course list. Run 'populate' to fetch courses.",
            )

    async def populate_courses(self, ctx: commands.Context) -> None:
        """Command: fetch and cache the full list of courses."""
        count = await self.course_data_proxy.update_course_listing()
        self._listings_cache = None
        if count and int(count) > 0:
            await ctx.send(info(f"Successfully fetched and cached {count} courses"))
        else:
            await ctx.send(
                warning(
                    "No courses fetched. Check the console for details.",
                ),
            )

    async def _refresh_course_data_and_notify(
        self,
        ctx: commands.Context,
        course_obj: CourseCode,
    ) -> None:
        """Force refresh cache and notify the user."""
        canonical = course_obj.canonical()
        marked = await self.course_data_proxy.force_mark_stale(canonical, detailed=True)
        if not marked:
            await ctx.send(
                error(f"No detailed data exists for course {canonical} to refresh."),
            )
            return
        data = await self.course_data_proxy.get_course_data(canonical, detailed=True)
        if self._is_valid_course_data(data):
            await ctx.send(
                success(f"The course data for {canonical} has been refreshed."),
            )
        else:
            await ctx.send(
                error(f"Course data for {canonical} could not be refreshed."),
            )

    @requires_enabled
    async def refresh_course_data(
        self,
        ctx: commands.Context,
        course_code: str,
    ) -> None:
        """Command: refresh detailed course data on demand."""
        try:
            course_obj = await self._resolve_course(ctx, course_code)
            if course_obj is None:
                return
            await self._refresh_course_data_and_notify(ctx, course_obj)
        except Exception:
            log.exception("Error refreshing course data for %s", course_code)
            await ctx.send(error("There was an error refreshing the course data."))

    async def print_config(self, ctx: commands.Context) -> None:
        """Command: dump the current bot configuration to console."""
        config_data = await self.config.all()
        log.info("Config dump: %s", config_data)
        await ctx.send(info("Configuration has been printed to the console."))

    async def reset_config(self, ctx: commands.Context) -> None:
        """Command: clear all configuration settings."""
        await self.config.clear_all()
        await ctx.send(success("All configuration data has been cleared."))

    async def auto_course_clustering(
        self,
        channel_service: ChannelService,
        clustering: CourseChannelClustering,
        *,
        interval: int | None = None,
    ) -> None:
        """Background task: periodically recluster and move course channels."""
        await self.bot.wait_until_ready()
        iteration = 1
        log.info("Auto-course-clustering task started.")
        try:
            while not self.bot.is_closed():
                effective_interval = (
                    interval
                    if interval is not None
                    else await self.config.grouping_interval()
                )
                log.debug(
                    "Clustering cycle %d (interval=%ds)",
                    iteration,
                    effective_interval,
                )
                enabled = set(await self.config.enabled_guilds())

                for guild in self.bot.guilds:
                    if guild.id not in enabled:
                        continue
                    try:
                        course_users = await self.gather_course_user_data(guild)
                        mapping = clustering.cluster_courses(course_users)
                        await self.config.course_groups.set(mapping)
                        await channel_service.apply_category_mapping(guild, mapping)
                    except Exception:
                        log.exception(
                            "Error clustering guild %s (%s)",
                            guild.name,
                            guild.id,
                        )

                iteration += 1
                log.debug("Cycle complete; sleeping %ds", effective_interval)
                await asyncio.sleep(effective_interval)
        except asyncio.CancelledError:
            log.debug("Auto-course-clustering task cancelled.")
            raise
        except Exception:
            log.exception("Unexpected error in auto-course-clustering task.")

    async def gather_course_user_data(
        self,
        guild: discord.Guild,
        *,
        include_metadata: bool = False,
    ) -> dict[str, set[int]] | tuple[dict[str, set[int]], dict[str, dict[str, str]]]:
        """Collect course code to member ID mapping (with optional metadata)."""
        prefix = await self.config.course_category()
        users_raw: dict[str, set[int]] = {}
        meta_raw: dict[str, dict[str, str]] = {}

        for cat in get_categories_by_prefix(guild, prefix):
            for chan in cat.text_channels:
                try:
                    course = CourseCode(chan.name)
                except ValueError:
                    continue

                members = {
                    m.id
                    for m in chan.members
                    if not m.bot
                    and chan.permissions_for(m).read_messages
                    and chan.permissions_for(m).send_messages
                }
                if not members:
                    continue

                key = course.canonical()
                users_raw[key] = members
                meta_raw[key] = {"department": course.department}

        return (users_raw, meta_raw) if include_metadata else users_raw
