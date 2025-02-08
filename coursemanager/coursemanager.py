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

# New regex pattern to normalize course codes.
# It accepts inputs such as "socwork2cc3", "socwork 2cc3", "socwork-2cc3",
# or with a trailing suffix like "math 2xx3 a" and returns codes like "SOCWORK-2CC3" or "MATH-2XX3A".
COURSE_KEY_PATTERN = re.compile(
    r"^\s*([A-Za-z]+)[\s\-_]*(\d+(?:[A-Za-z\d]*\d+)?)([A-Za-z])?\s*$"
)
# Reaction options for interactive prompts.
REACTION_OPTIONS: List[str] = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "❌"]  # Last is cancel


class CourseManager(commands.Cog):
    """
    Manages course channels and details.

    Lookup logic:
      1. Normalize input (e.g. "socwork2cc3" becomes "SOCWORK-2CC3").
      2. If an exact match exists in the course listing, use it.
         If the API call fails for the perfect match (e.g. returns HTTP 500),
         return immediately (so the user is informed the course is not available).
      3. Otherwise, if the input lacks a suffix, search for variant keys.
         - If exactly one variant exists, use it.
         - If multiple exist, prompt the user.
      4. Otherwise, fall back to fuzzy lookup (up to 5 suggestions).
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

    async def _auto_prune_task(self) -> None:
        """Background task to auto-prune inactive course channels."""
        PRUNE_INTERVAL = 3600  # seconds
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

    # --- Course Key and Channel Helpers ---

    def _format_course_key(self, course_key_raw: str) -> Optional[str]:
        """
        Normalize a course code.

        Accepts input such as:
            "sOcWoRk2cc3", "socwork2cc3", "socwork 2cc3", "socwork-2cc3",
            "math 2xx3 a"
        and returns a standardized code like:
            "SOCWORK-2CC3" or "MATH-2XX3A"
        """
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
        """
        Derive a channel name from the course code.
        Channels are lowercase and drop any suffix.
        """
        if course_key and course_key[-1].isalpha():
            base = course_key[:-1]
        else:
            base = course_key
        channel_name = base.lower()
        log.debug("Derived channel name: %s", channel_name)
        return channel_name

    def get_category(self, guild: discord.Guild) -> Optional[discord.CategoryChannel]:
        """Return the category in the guild matching self.category_name."""
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
        """Return the course channel (by name) if it exists in the guild."""
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
        """
        Create a new course channel under the specified category.
        (Channel names are lowercase.)
        """
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

    # --- Course Data Lookup Logic ---

    def _find_variant_matches(self, base: str, listings: Dict[str, str]) -> List[str]:
        """
        Return a list of keys in listings that start with the base key and have an extra letter suffix.
        """
        return [
            key for key in listings if key.startswith(base) and len(key) > len(base)
        ]

    async def _prompt_variant_selection(
        self, ctx: commands.Context, variants: List[str], listings: Dict[str, str]
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """
        Prompt the user to choose among multiple variant keys using reaction options.
        """
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
        """
        Layered lookup for course data:
          1. If an exact match exists in the course listing, use it.
             If the API call fails for the perfect match, return immediately.
          2. Otherwise, if no suffix was provided, look for variant keys.
             - If exactly one variant is found, use it.
             - If multiple, prompt the user.
          3. Otherwise, fall back to fuzzy lookup (up to 5 suggestions).
        """
        listings: Dict[str, str] = (await self.config.course_listings()).get(
            "courses", {}
        )

        # Step 1: Exact match
        if formatted in listings:
            data = await self.course_data_proxy.get_course_data(formatted)
            if data and data.get("course_data"):
                return formatted, data
            else:
                log.error("Failed to fetch fresh data for perfect match: %s", formatted)
                return formatted, None

        # Step 2: If input lacks a suffix, check for variants.
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

        # Step 3: Fuzzy lookup fallback.
        candidate, data = await self._fallback_fuzzy_lookup(ctx, formatted)
        return candidate, data

    async def _fallback_fuzzy_lookup(
        self, ctx: commands.Context, formatted: str
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """
        Perform fuzzy lookup using the full course listing.
        Present up to 5 suggestions for user selection.
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
        """
        Wait for a valid reaction from the command author on the given message.
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
            log.debug("Reaction wait timed out for user %s", ctx.author)
            return None

    async def _get_course_details(
        self, ctx: commands.Context, course_code: str
    ) -> Optional[discord.Embed]:
        """
        Retrieve course details as a Discord embed.
        Returns None if no course data is found.
        """
        candidate, data = await self._lookup_course_data(ctx, course_code)
        if not candidate or not (data and data.get("course_data")):
            return None
        embed = self._create_course_embed(candidate, data)
        return embed

    async def _prune_channel(
        self, channel: discord.TextChannel, threshold: timedelta, reason: str
    ) -> bool:
        """
        Delete the channel if its last activity exceeds the threshold.
        """
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
        """
        Build and return a Discord embed with course details.
        """
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

    @commands.group(invoke_without_command=True)
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def course(self, ctx: commands.Context) -> None:
        """Main command group for course functionalities."""
        log.debug("Course command group invoked by %s", ctx.author)
        await ctx.send_help(self.course)

    @course.command(name="list")
    @commands.cooldown(1, 600, commands.BucketType.user)
    async def list_enrollments(self, ctx: commands.Context) -> None:
        """List all course channels the user is currently enrolled in."""
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
        """
        Force refresh the course data for a specified course.
        Example: !course refresh MATH 1A03
        """
        formatted = self._format_course_key(course_code)
        log.debug("Refresh invoked for '%s' (formatted: %s)", course_code, formatted)
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
        """
        Leave a course channel by removing your permission override.
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

    @commands.admin()
    @course.command()
    async def delete(
        self, ctx: commands.Context, *, channel: discord.TextChannel
    ) -> None:
        """
        Delete a course channel (admin-only).
        """
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
        """
        Set the logging channel for join/leave notifications (admin-only).
        """
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
        embed = await self._get_course_details(ctx, formatted)
        if embed is None:
            await ctx.send(error(f"Course not found: {formatted}"))
        else:
            await ctx.send(embed=embed)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(CourseManager(bot))
