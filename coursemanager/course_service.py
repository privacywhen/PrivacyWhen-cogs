import time
from math import ceil
import functools
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple, TypeVar
import discord
from redbot.core import Config, commands
from redbot.core.utils.chat_formatting import error, info, success, warning, pagify
from redbot.core.utils.menus import menu
from .course_code import CourseCode
from .course_data_proxy import CourseDataProxy
from .logger_util import get_logger
from .utils import (
    get_categories_by_prefix,
    get_or_create_category,
    get_available_course_category,
)

log = get_logger("red.course.service")
T = TypeVar("T")


def requires_enabled(
    func: Callable[..., Coroutine[Any, Any, T]]
) -> Callable[..., Coroutine[Any, Any, T]]:
    @functools.wraps(func)
    async def wrapper(
        self: "CourseService", ctx: commands.Context, *args: Any, **kwargs: Any
    ) -> T:
        if not await self._check_enabled(ctx):
            return
        return await func(self, ctx, *args, **kwargs)

    return wrapper


class CourseService:
    def __init__(self, bot: commands.Bot, config: Config) -> None:
        self.bot: commands.Bot = bot
        self.config: Config = config
        self.category_name: str = "COURSES"
        self.max_courses: int = 10
        self.logging_channel: Optional[discord.TextChannel] = None
        self.course_data_proxy: CourseDataProxy = CourseDataProxy(self.config, log)
        self._listings_cache: Optional[Dict[str, str]] = None
        self._listings_cache_time: float = 0.0
        self._listings_ttl: float = 60.0

    async def _get_course_listings(self) -> Dict[str, str]:
        now: float = time.monotonic()
        if (
            self._listings_cache is not None
            and now - self._listings_cache_time < self._listings_ttl
        ):
            return self._listings_cache
        data = await self.config.course_listings()
        self._listings_cache = data.get("courses", {})
        self._listings_cache_time = now
        return self._listings_cache

    def _is_valid_course_data(self, data: Any) -> bool:
        return bool(data and data.get("cached_course_data"))

    async def _check_enabled(self, ctx: commands.Context) -> bool:
        enabled_guilds: List[int] = await self.config.enabled_guilds()
        if ctx.guild.id not in enabled_guilds:
            await ctx.send(
                error(
                    "Course Manager is disabled in this server. Please enable it using the 'course enable' command."
                )
            )
            return False
        return True

    async def _update_enabled_status(self, ctx: commands.Context, enable: bool) -> None:
        enabled_guilds: List[int] = await self.config.enabled_guilds()
        if enable:
            if ctx.guild.id in enabled_guilds:
                await ctx.send("Course Manager is already enabled in this server.")
            else:
                enabled_guilds.append(ctx.guild.id)
                await self.config.enabled_guilds.set(enabled_guilds)
                await ctx.send("Course Manager has been enabled in this server.")
        elif ctx.guild.id not in enabled_guilds:
            await ctx.send("Course Manager is already disabled in this server.")
        else:
            enabled_guilds.remove(ctx.guild.id)
            await self.config.enabled_guilds.set(enabled_guilds)
            await ctx.send("Course Manager has been disabled in this server.")

    async def enable(self, ctx: commands.Context) -> None:
        await self._update_enabled_status(ctx, True)

    async def disable(self, ctx: commands.Context) -> None:
        await self._update_enabled_status(ctx, False)

    def get_category(self, guild: discord.Guild) -> Optional[discord.CategoryChannel]:
        category = next(
            (
                cat
                for cat in guild.categories
                if cat.name.upper() == self.category_name.upper()
            ),
            None,
        )
        if category:
            log.debug(f"Found category '{category.name}' in guild '{guild.name}'")
        else:
            log.debug(
                f"No category '{self.category_name}' found in guild '{guild.name}'"
            )
        return category

    def get_course_channel(
        self, guild: discord.Guild, course: CourseCode
    ) -> Optional[discord.TextChannel]:
        target_name: str = course.formatted_channel_name()
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
            f"{'Found' if channel else 'No'} course channel '{target_name}' for course '{course.canonical()}' in guild '{guild.name}'"
        )
        return channel

    async def create_course_channel(
        self,
        guild: discord.Guild,
        category: discord.CategoryChannel,
        course: CourseCode,
    ) -> discord.TextChannel:
        target_name: str = course.formatted_channel_name()
        log.debug(
            f"Creating channel '{target_name}' in guild '{guild.name}' under category '{category.name}'"
        )
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            guild.me: discord.PermissionOverwrite(administrator=True),
        }
        channel: discord.TextChannel = await guild.create_text_channel(
            target_name, overwrites=overwrites, category=category
        )
        log.debug(f"Created channel '{channel.name}' in guild '{guild.name}'")
        return channel

    def _has_joined(self, user: discord.Member, channel: discord.TextChannel) -> bool:
        overwrite = channel.overwrites_for(user)
        return overwrite.read_messages is True and overwrite.send_messages is True

    def _user_already_joined(
        self, user: discord.Member, guild: discord.Guild, course: CourseCode
    ) -> bool:
        return course.formatted_channel_name() in self.get_user_courses(user, guild)

    async def _handle_existing_channel(
        self,
        ctx: commands.Context,
        user: discord.Member,
        channel: discord.TextChannel,
        canonical: str,
    ) -> bool:
        if self._has_joined(user, channel):
            await ctx.send(
                info(f"You are already joined in {canonical}."), delete_after=120
            )
            return True
        return bool(await self._grant_access(ctx, channel, canonical))

    def get_user_courses(self, user: discord.Member, guild: discord.Guild) -> List[str]:
        joined_courses = [
            channel.name
            for cat in get_categories_by_prefix(guild, self.category_name)
            for channel in cat.channels
            if isinstance(channel, discord.TextChannel)
            and self._has_joined(user, channel)
        ]
        log.debug(f"User '{user}' has joined courses: {joined_courses}")
        return joined_courses

    def _user_channel_limit_reached(
        self, user: discord.Member, guild: discord.Guild
    ) -> bool:
        return len(self.get_user_courses(user, guild)) >= self.max_courses

    async def _ensure_user_channel_limit_not_exceeded(
        self, ctx: commands.Context, user: discord.Member, guild: discord.Guild
    ) -> bool:
        if self._user_channel_limit_reached(user, guild):
            await ctx.send(
                error(
                    f"You have reached the maximum limit of {self.max_courses} course channels."
                )
            )
            return False
        return True

    async def _lookup_course_data(
        self, ctx: commands.Context, course: CourseCode, already_resolved: bool = False
    ) -> Tuple[Optional[CourseCode], Any]:
        canonical: str = course.canonical()
        log.debug(f"Looking up course data for '{canonical}'")
        listings = await self._get_course_listings()
        if canonical in listings:
            log.debug(f"Found perfect match for '{canonical}' in listings")
            data = await self.course_data_proxy.get_course_data(
                canonical, detailed=True
            )
            if self._is_valid_course_data(data):
                log.debug(f"Fresh data retrieved for '{canonical}'")
                return (course, data)
            log.error(f"Failed to fetch fresh data for '{canonical}'")
            return (course, None)
        if not already_resolved:
            from .course_code_resolver import CourseCodeResolver

            resolver = CourseCodeResolver(
                listings, course_data_proxy=self.course_data_proxy
            )
            resolved_course, data = await resolver.resolve_course_code(ctx, course)
            return (resolved_course, data)
        log.debug(
            "Course code already resolved; skipping further resolution and prompt."
        )
        return (course, None)

    async def _resolve_course(
        self, ctx: commands.Context, course_code: str
    ) -> Optional[CourseCode]:
        listings: Dict[str, str] = await self._get_course_listings()
        from .utils import validate_and_resolve_course_code

        course_obj: Optional[CourseCode] = await validate_and_resolve_course_code(
            ctx, course_code, listings, self.course_data_proxy
        )
        if course_obj is None:
            await ctx.send(error(f"Invalid course code: {course_code}."))
        return course_obj

    async def course_details(
        self, ctx: commands.Context, course_code: str
    ) -> Optional[discord.Embed]:
        try:
            course_obj: Optional[CourseCode] = await self._resolve_course(
                ctx, course_code
            )
            if course_obj is None:
                return None
            data = await self.course_data_proxy.get_course_data(
                course_obj.canonical(), detailed=True
            )
            if not self._is_valid_course_data(data):
                return None
            return self._create_course_embed(course_obj.canonical(), data)
        except Exception as exc:
            log.exception(f"Error retrieving course details for {course_code}: {exc}")
            await ctx.send(error("An error occurred while retrieving course details."))
            return None

    def _create_course_embed(
        self, course_key: str, course_data: Dict[str, Any]
    ) -> discord.Embed:
        log.debug(f"Creating embed for course: {course_key}")
        embed = discord.Embed(
            title=f"Course Details: {course_key}", color=discord.Color.green()
        )
        data_item = (course_data.get("cached_course_data") or [{}])[0]
        embed.set_footer(
            text=f"Last updated: {course_data.get('last_updated', 'Unknown')}"
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

    async def _update_channel_permissions(
        self,
        ctx: commands.Context,
        channel: discord.TextChannel,
        user: discord.Member,
        overwrite: Optional[discord.PermissionOverwrite],
        success_msg: str,
        action: str,
    ) -> bool:
        try:
            await channel.set_permissions(user, overwrite=overwrite)
            log.debug(f"{action} for {user} on channel {channel.name}")
        except discord.Forbidden as exc:
            log.error(
                f"Failed to {action.lower()} for {user} on channel {channel.name}: {exc}"
            )
            await ctx.send(
                error("I don't have permission to manage channel permissions.")
            )
            return False
        await ctx.send(success(success_msg), delete_after=120)
        if self.logging_channel:
            await self.logging_channel.send(
                f"{user} has {action.lower()} {channel.name}."
            )
        return True

    async def _grant_access(
        self, ctx: commands.Context, channel: discord.TextChannel, canonical: str
    ) -> bool:
        return await self._update_channel_permissions(
            ctx,
            channel,
            ctx.author,
            discord.PermissionOverwrite(read_messages=True, send_messages=True),
            f"You have successfully joined {canonical}.",
            "Granted access",
        )

    @requires_enabled
    async def grant_course_channel_access(
        self, ctx: commands.Context, course_code: str
    ) -> None:
        try:
            log.debug(
                f"[grant_course_channel_access] invoked by {ctx.author} in guild '{ctx.guild.name}' with course_code '{course_code}'"
            )
            guild: discord.Guild = ctx.guild
            user: discord.Member = ctx.author
            if not await self._ensure_user_channel_limit_not_exceeded(ctx, user, guild):
                return
            course_obj: Optional[CourseCode] = await self._resolve_course(
                ctx, course_code
            )
            if course_obj is None:
                return
            canonical: str = course_obj.canonical()
            log.debug(f"Parsed course code: canonical={canonical}")
            channel = self.get_course_channel(guild, course_obj)
            if channel and await self._handle_existing_channel(
                ctx, user, channel, canonical
            ):
                return
            log.debug(
                f"No existing channel for {canonical}. Proceeding with lookup and creation."
            )
            async with ctx.typing():
                candidate_obj, data = await self._lookup_course_data(
                    ctx, course_obj, already_resolved=True
                )
            if not candidate_obj or not self._is_valid_course_data(data):
                log.debug(f"Course data lookup failed for {canonical}.")
                await ctx.send(error(f"No valid course data found for {canonical}."))
                return
            if self._user_already_joined(user, guild, candidate_obj):
                log.debug(
                    f"User {user} already has access to {candidate_obj.canonical()} after lookup"
                )
                await ctx.send(
                    info(f"You are already joined in {candidate_obj.canonical()}."),
                    delete_after=120,
                )
                return
            if not await self._ensure_user_channel_limit_not_exceeded(ctx, user, guild):
                return
            category = await get_available_course_category(
                guild, self.category_name, ctx
            )
            if category is None:
                return
            channel = self.get_course_channel(guild, candidate_obj)
            if not channel:
                log.debug(f"Creating new channel for {candidate_obj.canonical()}")
                channel = await self.create_course_channel(
                    guild, category, candidate_obj
                )
            await self._grant_access(ctx, channel, candidate_obj.canonical())
        except Exception as exc:
            log.exception(f"Error in grant_course_channel_access: {exc}")
            await ctx.send(error("An error occurred while granting access."))

    @requires_enabled
    async def revoke_course_channel_access(
        self, ctx: commands.Context, course_code: str
    ) -> None:
        try:
            guild: discord.Guild = ctx.guild
            course_obj: Optional[CourseCode] = await self._resolve_course(
                ctx, course_code
            )
            if course_obj is None:
                return
            canonical: str = course_obj.canonical()
            channel = self.get_course_channel(guild, course_obj)
            if not channel:
                await ctx.send(error(f"You are not a member of {canonical}."))
                return
            await self._update_channel_permissions(
                ctx,
                channel,
                ctx.author,
                None,
                f"You have successfully left {canonical}.",
                "Removed permissions",
            )
        except Exception as exc:
            log.exception(f"Error in revoke_course_channel_access: {exc}")
            await ctx.send(error("An error occurred while revoking access."))

    async def set_logging(
        self, ctx: commands.Context, channel: discord.TextChannel
    ) -> None:
        self.logging_channel = channel
        log.debug(f"Logging channel set to {channel.name} by admin {ctx.author}")
        await ctx.send(success(f"Logging channel set to {channel.mention}."))

    async def set_term_code(
        self, ctx: commands.Context, term_name: str, year: int, term_id: int
    ) -> None:
        term_key = f"{term_name.lower()}-{year}"
        async with self.config.term_codes() as term_codes:
            term_codes[term_key] = term_id
        log.debug(f"Set term code for {term_key} to {term_id}")
        await ctx.send(
            success(f"Term code for {term_name.capitalize()} {year} set to: {term_id}")
        )

    async def list_all_courses(self, ctx: commands.Context) -> None:
        cfg = await self.config.course_listings.all()
        if courses := cfg.get("courses", {}):
            dtm = cfg.get("date_updated", "Unknown")
            serialized_courses = "\n".join(courses.keys())
            pages = [
                f"{len(courses)} courses cached on {dtm}\n{page}"
                for page in pagify(serialized_courses, page_length=1500)
            ]
            await menu(ctx, pages, timeout=60.0, user=ctx.author)
        else:
            await ctx.send("Course list not found. Run populate command first.")

    async def populate_courses(self, ctx: commands.Context) -> None:
        course_count = await self.course_data_proxy.update_course_listing()
        self._listings_cache = None
        if course_count and int(course_count) > 0:
            await ctx.send(info(f"Fetched and cached {course_count} courses"))
        else:
            await ctx.send(warning("0 courses fetched. Check console log"))

    async def _refresh_course_data_and_notify(
        self, ctx: commands.Context, course_obj: CourseCode
    ) -> None:
        canonical: str = course_obj.canonical()
        marked: bool = await self.course_data_proxy.force_mark_stale(
            canonical, detailed=True
        )
        if not marked:
            await ctx.send(error(f"No existing detailed data for course {canonical}."))
            return
        data: Any = await self.course_data_proxy.get_course_data(
            canonical, detailed=True
        )
        if self._is_valid_course_data(data):
            await ctx.send(success(f"Course data for {canonical} has been refreshed."))
        else:
            await ctx.send(error(f"Failed to refresh course data for {canonical}."))

    @requires_enabled
    async def refresh_course_data(
        self, ctx: commands.Context, course_code: str
    ) -> None:
        try:
            course_obj: Optional[CourseCode] = await self._resolve_course(
                ctx, course_code
            )
            if course_obj is None:
                return
            await self._refresh_course_data_and_notify(ctx, course_obj)
        except Exception as exc:
            log.exception(f"Error refreshing course data for {course_code}: {exc}")
            await ctx.send(error("An error occurred while refreshing course data."))

    async def print_config(self, ctx: commands.Context) -> None:
        config_data = await self.config.all()
        log.info(config_data)
        await ctx.send(info("Config printed to console."))

    async def reset_config(self, ctx: commands.Context) -> None:
        await self.config.clear_all()
        await ctx.send(success("All config data cleared."))
