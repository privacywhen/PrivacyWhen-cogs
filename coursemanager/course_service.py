# course_service.py
from typing import Any, Dict, List, Optional, Tuple
import discord
from rapidfuzz import process
from redbot.core import Config, commands
from redbot.core.utils.chat_formatting import error, info, success, warning, pagify
from redbot.core.utils.menus import menu
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
            discord.PermissionOverwrite.from_pair(
                discord.Permissions(446676945984), discord.Permissions(0)
            )
        )
        self.course_data_proxy: CourseDataProxy = CourseDataProxy(self.config, log)

    async def _check_enabled(self, ctx: commands.Context) -> bool:
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

    def get_course_categories(self, guild: discord.Guild) -> list:
        categories = get_categories_by_prefix(guild, self.category_name)
        log.debug(
            f"CourseService.get_course_categories: Found {len(categories)} categories with prefix '{self.category_name}' in guild '{guild.name}'"
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
            log.debug(
                f"CourseService.get_category: Found category '{category.name}' in guild '{guild.name}'"
            )
        else:
            log.debug(
                f"CourseService.get_category: No category '{self.category_name}' found in guild '{guild.name}'"
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
            f"CourseService.get_course_channel: {('Found' if channel else 'No')} course channel '{target_name}' for course '{course.canonical()}' in guild '{guild.name}'"
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
            f"CourseService.create_course_channel: Attempting to create channel '{target_name}' in guild '{guild.name}' under category '{category.name}'"
        )
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            guild.me: discord.PermissionOverwrite(administrator=True),
        }
        channel = await guild.create_text_channel(
            target_name, overwrites=overwrites, category=category
        )
        log.debug(
            f"CourseService.create_course_channel: Created channel '{channel.name}' in guild '{guild.name}'"
        )
        return channel

    def get_user_courses(self, user: discord.Member, guild: discord.Guild) -> list:
        courses = [
            channel.name
            for category in self.get_course_categories(guild)
            for channel in category.channels
            if isinstance(channel, discord.TextChannel)
            and channel.permissions_for(user).read_messages
        ]
        log.debug(
            f"CourseService.get_user_courses: User '{user}' has access to courses: {courses}"
        )
        return courses

    def _find_variant_matches(self, base: str, listings: dict) -> list:
        variants = [
            key for key in listings if key.startswith(base) and len(key) > len(base)
        ]
        log.debug(
            f"CourseService._find_variant_matches: For base '{base}', found variants: {variants}"
        )
        return variants

    async def _prompt_variant_selection(
        self, ctx: commands.Context, variants: list, listings: dict
    ) -> Tuple[Optional[CourseCode], Any]:
        options = [(variant, listings.get(variant, "")) for variant in variants]
        log.debug(
            f"CourseService._prompt_variant_selection: Options for selection: {options}"
        )
        result = await self._menu_select_option(
            ctx, options, "Multiple course variants found. Please choose one:"
        )
        log.debug(f"CourseService._prompt_variant_selection: User selected: {result}")
        if result is None:
            return (None, None)
        data = await self.course_data_proxy.get_course_data(result)
        log.debug(
            f"CourseService._prompt_variant_selection: Data valid for candidate '{result}': {bool(data and data.get('course_data'))}"
        )
        try:
            candidate_obj = CourseCode(result)
        except ValueError:
            candidate_obj = None
        return (
            (candidate_obj, data)
            if candidate_obj and data and data.get("course_data")
            else (None, None)
        )

    async def _lookup_course_data(
        self, ctx: commands.Context, course: CourseCode
    ) -> Tuple[Optional[CourseCode], Any]:
        canonical = course.canonical()
        log.debug(
            f"CourseService._lookup_course_data: Looking up course data for '{canonical}'"
        )
        listings: dict = (await self.config.course_listings()).get("courses", {})
        if canonical in listings:
            log.debug(
                f"CourseService._lookup_course_data: Found perfect match for '{canonical}' in listings"
            )
            data = await self.course_data_proxy.get_course_data(canonical)
            if data and data.get("course_data"):
                log.debug(
                    f"CourseService._lookup_course_data: Fresh data retrieved for '{canonical}'"
                )
                return (course, data)
            log.error(
                f"CourseService._lookup_course_data: Failed to fetch fresh data for '{canonical}'"
            )
            return (course, None)
        if not canonical[-1].isalpha():
            if variants := self._find_variant_matches(canonical, listings):
                if len(variants) == 1:
                    candidate = variants[0]
                    log.debug(
                        f"CourseService._lookup_course_data: Single variant '{candidate}' found for '{canonical}'"
                    )
                    data = await self.course_data_proxy.get_course_data(candidate)
                    if data and data.get("course_data"):
                        try:
                            candidate_obj = CourseCode(candidate)
                        except ValueError:
                            candidate_obj = None
                        return (candidate_obj, data)
                else:
                    candidate_obj, data = await self._prompt_variant_selection(
                        ctx, variants, listings
                    )
                    log.debug(
                        f"CourseService._lookup_course_data: Variant selection returned candidate '{candidate_obj}'"
                    )
                    return (candidate_obj, data) if candidate_obj else (None, None)
        log.debug(
            f"CourseService._lookup_course_data: Falling back to fuzzy lookup for '{canonical}'"
        )
        candidate, data = await self._fallback_fuzzy_lookup(ctx, canonical)
        log.debug(
            f"CourseService._lookup_course_data: Fuzzy lookup returned candidate '{candidate}'"
        )
        return (candidate, data)

    async def _fallback_fuzzy_lookup(
        self, ctx: commands.Context, canonical: str
    ) -> Tuple[Optional[CourseCode], Any]:
        listings: dict = (await self.config.course_listings()).get("courses", {})
        if not listings:
            log.debug(
                "CourseService._fallback_fuzzy_lookup: No course listings available for fuzzy lookup"
            )
            return (None, None)
        matches = process.extract(canonical, listings.keys(), limit=5, score_cutoff=70)
        log.debug(
            f"CourseService._fallback_fuzzy_lookup: Fuzzy matches for '{canonical}': {matches}"
        )
        if not matches:
            return (None, None)
        options = [(match[0], listings.get(match[0], "")) for match in matches]
        result = await self._menu_select_option(
            ctx, options, "Course not found. Did you mean:"
        )
        log.debug(
            f"CourseService._fallback_fuzzy_lookup: User selected '{result}' from fuzzy lookup"
        )
        data = await self.course_data_proxy.get_course_data(result)
        log.debug(
            f"CourseService._fallback_fuzzy_lookup: Retrieved course data for candidate '{result}': {bool(data and data.get('course_data'))}"
        )
        try:
            candidate_obj = CourseCode(result)
        except ValueError:
            candidate_obj = None
        return (
            (candidate_obj, data)
            if candidate_obj and data and data.get("course_data")
            else (None, None)
        )

    async def course_details(
        self, ctx: commands.Context, course_code: str
    ) -> Optional[discord.Embed]:
        try:
            course_obj = CourseCode(course_code)
        except ValueError:
            return None
        candidate, data = await self._lookup_course_data(ctx, course_obj)
        if not candidate or not data or (not data.get("course_data")):
            return None
        return self._create_course_embed(candidate.canonical(), data)

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

    async def grant_course_channel_access(
        self, ctx: commands.Context, course_code: str
    ) -> None:
        log.debug(
            f"grant_course_channel_access invoked by {ctx.author} in guild '{ctx.guild.name}' with course_code '{course_code}'"
        )
        if not await self._check_enabled(ctx):
            log.debug("Course Manager is disabled for this guild.")
            return
        try:
            course_obj = CourseCode(course_code)
        except ValueError:
            log.debug(f"Course code formatting failed for input '{course_code}'")
            await ctx.send(error(f"Invalid course code: {course_code}."))
            return
        canonical = course_obj.canonical()
        channel_name = course_obj.formatted_channel_name()
        log.debug(f"Parsed course code: canonical={canonical}, channel={channel_name}")
        user_courses = self.get_user_courses(ctx.author, ctx.guild)
        log.debug(f"User {ctx.author} current course channel accesses: {user_courses}")
        channel = self.get_course_channel(ctx.guild, course_obj)
        if channel:
            log.debug(f"Found existing course channel: {channel.name}")
            if channel.name in user_courses:
                log.debug(f"User {ctx.author} already has access to {canonical}")
                await ctx.send(
                    info(f"You are already joined in {canonical}."), delete_after=120
                )
                return
            if len(user_courses) >= self.max_courses:
                log.debug(
                    f"User {ctx.author} has reached the maximum channel access limit: {len(user_courses)}"
                )
                await ctx.send(
                    error(
                        f"You have reached the maximum limit of {self.max_courses} course channels."
                    )
                )
                return
            try:
                await channel.set_permissions(
                    ctx.author, overwrite=self.channel_permissions
                )
                log.debug(
                    f"Permissions successfully set for {ctx.author} on channel {channel.name}"
                )
            except discord.Forbidden as e:
                log.error(
                    f"Failed to set permissions for {ctx.author} on channel {channel.name}: {e}"
                )
                await ctx.send(
                    error("I don't have permission to manage channel permissions.")
                )
                return
            await ctx.send(
                success(f"You have successfully joined {canonical}."), delete_after=120
            )
            if self.logging_channel:
                await self.logging_channel.send(f"{ctx.author} has joined {canonical}.")
            log.debug("grant_course_channel_access completed using existing channel.")
            return
        log.debug(
            f"No existing course channel for {canonical}. Proceeding with lookup and potential creation."
        )
        async with ctx.typing():
            candidate_obj, data = await self._lookup_course_data(ctx, course_obj)
        log.debug(
            f"Lookup result: candidate = {candidate_obj}, data valid = {bool(data and data.get('course_data'))}"
        )
        if not candidate_obj or not data or (not data.get("course_data")):
            log.debug(
                f"Course data lookup failed for {canonical}. Candidate: {candidate_obj}, data: {data}"
            )
            await ctx.send(error(f"No valid course data found for {canonical}."))
            return
        user_courses = self.get_user_courses(ctx.author, ctx.guild)
        log.debug(
            f"User {ctx.author} current course channel accesses after lookup: {user_courses}"
        )
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
                f"User {ctx.author} has reached the maximum channel access limit: {len(user_courses)} after lookup"
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
        log.debug(f"Using course category: {category.name}")
        channel = self.get_course_channel(ctx.guild, candidate_obj)
        if not channel:
            log.debug(
                f"Course channel for {candidate_obj.canonical()} not found; creating a new channel."
            )
            channel = await self.create_course_channel(
                ctx.guild, category, candidate_obj
            )
            log.debug(f"New course channel created: {channel.name}")
        try:
            await channel.set_permissions(
                ctx.author, overwrite=self.channel_permissions
            )
            log.debug(
                f"Permissions successfully set for {ctx.author} on channel {channel.name}"
            )
        except discord.Forbidden as e:
            log.error(
                f"Failed to set permissions for {ctx.author} on channel {channel.name}: {e}"
            )
            await ctx.send(
                error("I don't have permission to manage channel permissions.")
            )
            return
        await ctx.send(
            success(f"You have successfully joined {candidate_obj.canonical()}."),
            delete_after=120,
        )
        if self.logging_channel:
            await self.logging_channel.send(
                f"{ctx.author} has joined {candidate_obj.canonical()}."
            )
        log.debug(
            f"grant_course_channel_access completed successfully for user {ctx.author} for {candidate_obj.canonical()}"
        )

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

    async def admin_delete_channel(
        self, ctx: commands.Context, channel: discord.TextChannel
    ) -> None:
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
        stale.extend(
            (
                course_key
                for course_key in courses.keys()
                if not any(
                    (
                        self.get_course_channel(guild, CourseCode(course_key))
                        for guild in self.bot.guilds
                        if CourseCode(course_key)
                    )
                )
            )
        )
        for course_key in stale:
            await self.config.courses.clear_raw(course_key)
            log.debug(f"Cleared stale entry for course {course_key}")
        if stale:
            await ctx.send(success(f"Cleared stale config entries: {', '.join(stale)}"))
        else:
            await ctx.send(info("No stale course config entries found."))

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
        canonical = course_obj.canonical()
        async with self.config.courses() as courses:
            if canonical in courses:
                courses[canonical]["is_fresh"] = False
            else:
                await ctx.send(error(f"No existing data for course {canonical}."))
                return
        data = await self.course_data_proxy.get_course_data(canonical)
        if data and data.get("course_data"):
            await ctx.send(success(f"Course data for {canonical} has been refreshed."))
        else:
            await ctx.send(error(f"Failed to refresh course data for {canonical}."))

    async def _menu_select_option(
        self, ctx: commands.Context, options: list, prompt_prefix: str
    ) -> Optional[str]:
        cancel_emoji = REACTION_OPTIONS[-1]
        emoji_to_option = {}
        option_lines = []
        for i, (option, description) in enumerate(options):
            emoji = REACTION_OPTIONS[i] if i < len(REACTION_OPTIONS) - 1 else None
            if not emoji:
                break
            emoji_to_option[emoji] = option
            option_lines.append(f"{emoji} **{option}**: {description}")
        option_lines.append(f"{cancel_emoji} Cancel")
        prompt = f"{prompt_prefix}\n" + "\n".join(option_lines)
        log.debug(f"CourseService._menu_select_option: Prompting menu with:\n{prompt}")
        controls = {}
        for emoji, opt in emoji_to_option.items():

            async def handler(
                ctx,
                pages,
                controls,
                message,
                page,
                timeout,
                emoji,
                *,
                opt=opt,
                user=None,
            ):
                log.debug(
                    f"CourseService._menu_select_option.handler: Option '{opt}' selected via emoji '{emoji}'"
                )
                return opt

            controls[emoji] = handler

        async def cancel_handler(
            ctx, pages, controls, message, page, timeout, emoji, *, user=None
        ):
            log.debug(
                "CourseService._menu_select_option.cancel_handler: User cancelled the menu"
            )
            return None

        controls[cancel_emoji] = cancel_handler
        result = await menu(
            ctx, [prompt], controls=controls, timeout=30.0, user=ctx.author
        )
        log.debug(f"CourseService._menu_select_option: Menu selection result: {result}")
        return result
