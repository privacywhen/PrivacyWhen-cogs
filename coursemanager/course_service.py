from typing import Any, Dict, List, Optional, Tuple
import discord
from redbot.core import Config, commands
from redbot.core.utils.chat_formatting import error, info, success, warning, pagify
from redbot.core.utils.menus import menu, close_menu
from .course_data_proxy import CourseDataProxy
from .utils import get_categories_by_prefix, get_or_create_category, get_logger
from .constants import REACTION_OPTIONS
from .course_code import CourseCode

log = get_logger("red.course_service")


class CourseService:
    def __init__(self, bot: commands.Bot, config: Config) -> None:
        self.bot: commands.Bot = bot
        self.config: Config = config
        self.category_name: str = "COURSES"
        self.max_courses: int = 10
        self.logging_channel: Optional[discord.TextChannel] = None
        self.channel_permissions: discord.PermissionOverwrite = (
            discord.PermissionOverwrite(read_messages=True, send_messages=True)
        )
        self.course_data_proxy: CourseDataProxy = CourseDataProxy(self.config, log)

    async def _get_course_listings(self) -> Dict[str, str]:
        """
        Retrieve course listings from config and return the 'courses' dictionary.
        """
        return (await self.config.course_listings()).get("courses", {})

    def _is_valid_course_data(self, data: Any) -> bool:
        """
        Returns True if the provided course data is valid (i.e. has non-empty 'cached_course_data').
        """
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

    async def enable(self, ctx: commands.Context) -> None:
        enabled_guilds: List[int] = await self.config.enabled_guilds()
        if ctx.guild.id in enabled_guilds:
            await ctx.send("Course Manager is already enabled in this server.")
        else:
            enabled_guilds.append(ctx.guild.id)
            await self.config.enabled_guilds.set(enabled_guilds)
            await ctx.send("Course Manager has been enabled in this server.")

    async def disable(self, ctx: commands.Context) -> None:
        enabled_guilds: List[int] = await self.config.enabled_guilds()
        if ctx.guild.id not in enabled_guilds:
            await ctx.send("Course Manager is already disabled in this server.")
        else:
            enabled_guilds.remove(ctx.guild.id)
            await self.config.enabled_guilds.set(enabled_guilds)
            await ctx.send("Course Manager has been disabled in this server.")

    def get_course_categories(
        self, guild: discord.Guild
    ) -> List[discord.CategoryChannel]:
        categories = get_categories_by_prefix(guild, self.category_name)
        log.debug(
            f"Found {len(categories)} categories with prefix '{self.category_name}' in guild '{guild.name}'"
        )
        return categories

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
        target_name = course.formatted_channel_name()
        channel = next(
            (
                ch
                for cat in self.get_course_categories(guild)
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
        target_name = course.formatted_channel_name()
        log.debug(
            f"Attempting to create channel '{target_name}' in guild '{guild.name}' under category '{category.name}'"
        )
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            guild.me: discord.PermissionOverwrite(administrator=True),
        }
        channel = await guild.create_text_channel(
            target_name, overwrites=overwrites, category=category
        )
        log.debug(f"Created channel '{channel.name}' in guild '{guild.name}'")
        return channel

    def _has_joined(self, user: discord.Member, channel: discord.TextChannel) -> bool:
        overwrite = channel.overwrites_for(user)
        return overwrite.read_messages is True and overwrite.send_messages is True

    def get_user_courses(self, user: discord.Member, guild: discord.Guild) -> List[str]:
        joined_courses = [
            channel.name
            for category in self.get_course_categories(guild)
            for channel in category.channels
            if isinstance(channel, discord.TextChannel)
            and self._has_joined(user, channel)
        ]
        log.debug(f"User '{user}' has joined courses: {joined_courses}")
        return joined_courses

    async def _lookup_course_data(
        self, ctx: commands.Context, course: CourseCode
    ) -> Tuple[Optional[CourseCode], Any]:
        canonical = course.canonical()
        log.debug(f"Looking up course data for '{canonical}'")
        listings = await self._get_course_listings()
        if canonical in listings:
            log.debug(f"Found perfect match for '{canonical}' in listings")
            data = await self.course_data_proxy.get_course_data(canonical)
            if self._is_valid_course_data(data):
                log.debug(f"Fresh data retrieved for '{canonical}'")
                return (course, data)
            log.error(f"Failed to fetch fresh data for '{canonical}'")
            return (course, None)
        from .course_code_resolver import CourseCodeResolver

        resolver = CourseCodeResolver(
            listings, course_data_proxy=self.course_data_proxy
        )
        resolved_course, data = await resolver.resolve_course_code(ctx, course)
        return (resolved_course, data)

    async def course_details(
        self, ctx: commands.Context, course_code: str
    ) -> Optional[discord.Embed]:
        try:
            course_obj = CourseCode(course_code)
        except ValueError:
            return None
        data = await self.course_data_proxy.get_course_data(
            course_obj.canonical(), detailed=True
        )
        if not self._is_valid_course_data(data):
            return None
        return self._create_course_embed(course_obj.canonical(), data)

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

    async def grant_course_channel_access(
        self, ctx: commands.Context, course_code: str
    ) -> None:
        log.debug(
            f"[grant_course_channel_access] invoked by {ctx.author} in guild '{ctx.guild.name}' with course_code '{course_code}'"
        )
        if not await self._check_enabled(ctx):
            return
        try:
            course_obj = CourseCode(course_code)
        except ValueError:
            log.debug(f"Invalid course code formatting for input '{course_code}'")
            await ctx.send(error(f"Invalid course code: {course_code}."))
            return

        canonical = course_obj.canonical()
        log.debug(f"Parsed course code: canonical={canonical}")

        user_courses = self.get_user_courses(ctx.author, ctx.guild)
        log.debug(f"User {ctx.author} current joined courses: {user_courses}")
        if len(user_courses) >= self.max_courses:
            log.debug(
                f"User {ctx.author} reached max channels limit: {len(user_courses)}"
            )
            await ctx.send(
                error(
                    f"You have reached the maximum limit of {self.max_courses} course channels."
                )
            )
            return

        channel = self.get_course_channel(ctx.guild, course_obj)
        if channel:
            if channel.name in user_courses:
                log.debug(f"User {ctx.author} already has access to {canonical}")
                await ctx.send(
                    info(f"You are already joined in {canonical}."), delete_after=120
                )
                return
            if await self._grant_access(ctx, channel, canonical):
                return

        log.debug(
            f"No existing channel for {canonical}. Proceeding with lookup and creation."
        )
        async with ctx.typing():
            candidate_obj, data = await self._lookup_course_data(ctx, course_obj)
        if not candidate_obj or not self._is_valid_course_data(data):
            log.debug(f"Course data lookup failed for {canonical}.")
            await ctx.send(error(f"No valid course data found for {canonical}."))
            return

        user_courses = self.get_user_courses(ctx.author, ctx.guild)
        if candidate_obj.formatted_channel_name() in user_courses:
            log.debug(
                f"User {ctx.author} already has access to {candidate_obj.canonical()} after lookup"
            )
            await ctx.send(
                info(f"You are already joined in {candidate_obj.canonical()}."),
                delete_after=120,
            )
            return
        if len(user_courses) >= self.max_courses:
            log.debug(
                f"User {ctx.author} reached max channels limit after lookup: {len(user_courses)}"
            )
            await ctx.send(
                error(
                    f"You have reached the maximum limit of {self.max_courses} course channels."
                )
            )
            return

        category = self.get_category(ctx.guild)
        if category is None:
            log.debug("Course category not found. Attempting to create one.")
            category = await get_or_create_category(ctx.guild, self.category_name)
        if category is None:
            log.error(
                "Failed to find or create courses category. Check bot permissions."
            )
            await ctx.send(
                error("I don't have permission to create the courses category.")
            )
            return

        channel = self.get_course_channel(ctx.guild, candidate_obj)
        if not channel:
            log.debug(f"Creating new channel for {candidate_obj.canonical()}")
            channel = await self.create_course_channel(
                ctx.guild, category, candidate_obj
            )
        await self._grant_access(ctx, channel, candidate_obj.canonical())

    async def _grant_access(
        self, ctx: commands.Context, channel: discord.TextChannel, canonical: str
    ) -> bool:
        try:
            await channel.set_permissions(
                ctx.author, overwrite=self.channel_permissions
            )
            log.debug(f"Granted access for {ctx.author} on channel {channel.name}")
        except discord.Forbidden as e:
            log.error(
                f"Failed to set permissions for {ctx.author} on channel {channel.name}: {e}"
            )
            await ctx.send(
                error("I don't have permission to manage channel permissions.")
            )
            return False
        await ctx.send(
            success(f"You have successfully joined {canonical}."), delete_after=120
        )
        if self.logging_channel:
            await self.logging_channel.send(f"{ctx.author} has joined {canonical}.")
        return True

    async def revoke_course_channel_access(
        self, ctx: commands.Context, course_code: str
    ) -> None:
        if not await self._check_enabled(ctx):
            return
        try:
            course_obj = CourseCode(course_code)
        except ValueError:
            await ctx.send(error(f"Invalid course code: {course_code}."))
            return
        canonical = course_obj.canonical()
        channel = self.get_course_channel(ctx.guild, course_obj)
        if not channel:
            await ctx.send(error(f"You are not a member of {canonical}."))
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
            success(f"You have successfully left {canonical}."), delete_after=120
        )
        if self.logging_channel:
            await self.logging_channel.send(f"{ctx.author} has left {canonical}.")

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

    async def list_all_courses(self, ctx: commands.Context) -> None:
        cfg = await self.config.course_listings.all()
        if "courses" in cfg:
            courses = cfg["courses"]
            dtm = cfg["date_updated"]
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
        if course_count and int(course_count) > 0:
            await ctx.send(info(f"Fetched and cached {course_count} courses"))
        else:
            await ctx.send(warning("0 courses fetched. Check console log"))

    async def refresh_course_data(
        self, ctx: commands.Context, course_code: str
    ) -> None:
        if not await self._check_enabled(ctx):
            return
        try:
            course_obj = CourseCode(course_code)
        except ValueError:
            await ctx.send(error(f"Invalid course code: {course_code}."))
            return
        marked = await self.course_data_proxy.force_mark_stale(
            course_obj.canonical(), detailed=True
        )
        if not marked:
            await ctx.send(
                error(f"No existing detailed data for course {course_obj.canonical()}.")
            )
            return
        data = await self.course_data_proxy.get_course_data(
            course_obj.canonical(), detailed=True
        )
        if self._is_valid_course_data(data):
            await ctx.send(
                success(f"Course data for {course_obj.canonical()} has been refreshed.")
            )
        else:
            await ctx.send(
                error(f"Failed to refresh course data for {course_obj.canonical()}.")
            )

    async def _menu_select_option(
        self,
        ctx: commands.Context,
        options: List[Tuple[str, str]],
        prompt_prefix: str,
    ) -> Optional[str]:
        cancel_emoji = REACTION_OPTIONS[-1]
        limited_options = options[: len(REACTION_OPTIONS) - 1]
        option_lines = [
            f"{REACTION_OPTIONS[i]} **{option}**: {description}"
            for i, (option, description) in enumerate(limited_options)
        ]
        option_lines.append(f"{cancel_emoji} Cancel")
        prompt = f"{prompt_prefix}\n" + "\n".join(option_lines)
        log.debug(f"Prompting menu with:\n{prompt}")
        controls = {}

        def make_handler(emoji: str, opt: str):
            async def handler(
                ctx,
                pages,
                controls,
                message,
                page,
                timeout,
                reacted_emoji,
                *,
                user=None,
            ):
                log.debug(f"Option '{opt}' selected via emoji '{emoji}'")
                await close_menu(
                    ctx,
                    pages,
                    controls,
                    message,
                    page,
                    timeout,
                    reacted_emoji,
                    user=user,
                )
                return opt

            return handler

        emoji_to_option = {
            REACTION_OPTIONS[i]: option for i, (option, _) in enumerate(limited_options)
        }
        for emoji, opt in emoji_to_option.items():
            controls[emoji] = make_handler(emoji, opt)

        async def cancel_handler(
            pages, controls, message, page, timeout, emoji, *, user=None
        ):
            log.debug("User cancelled the menu")
            await close_menu(
                ctx, pages, controls, message, page, timeout, emoji, user=user
            )
            return None

        controls[cancel_emoji] = cancel_handler
        result = await menu(
            ctx, [prompt], controls=controls, timeout=30.0, user=ctx.author
        )
        log.debug(f"Menu selection result: {result}")
        return result
