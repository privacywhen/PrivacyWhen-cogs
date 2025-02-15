import asyncio
from typing import Optional

import discord
from redbot.core import Config, commands
from redbot.core.utils.chat_formatting import error, info, success, warning
from redbot.core.utils.menus import menu

from .channel_service import ChannelService
from .constants import GLOBAL_DEFAULTS
from .course_service import CourseService
from .logger_util import get_logger

log = get_logger("red.course_channel_cog")


class CourseChannelCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot: commands.Bot = bot
        self.config: Config = Config.get_conf(
            self, identifier=42043360, force_registration=True
        )
        # Register global defaults from constants
        self.config.register_global(**GLOBAL_DEFAULTS)
        self.channel_service: ChannelService = ChannelService(bot, self.config)
        self.course_service: CourseService = CourseService(bot, self.config)
        # Start the background task for auto-pruning channels
        self._prune_task: Optional[asyncio.Task] = asyncio.create_task(
            self.channel_service.auto_channel_prune()
        )
        log.debug("CourseChannelCog initialized.")

    async def cog_check(self, ctx: commands.Context) -> bool:
        """
        Ensure that Course Manager commands can only be executed if enabled in the guild.
        """
        if ctx.guild is None:
            return True
        # Check if command belongs to 'course' group and is not one of the allowed commands when disabled
        if ctx.command.qualified_name.lower().startswith(
            "course"
        ) and ctx.command.name.lower() not in {"enable", "disable", "course"}:
            enabled = await self.config.enabled_guilds()
            if ctx.guild.id not in enabled:
                await ctx.send(
                    error(
                        "Course Manager is disabled in this server. Please enable it using `course enable`."
                    )
                )
                return False
        return True

    def cog_unload(self) -> None:
        """Cleanup tasks and resources when the cog is unloaded."""
        log.debug("Unloading CourseChannelCog; cancelling background tasks.")
        if self._prune_task:
            self._prune_task.cancel()
        # Close the course data proxy session asynchronously
        self.bot.loop.create_task(self.course_service.course_data_proxy.close())

    @commands.group(name="course", invoke_without_command=True, case_insensitive=True)
    async def course(self, ctx: commands.Context) -> None:
        """Display help for course commands."""
        await ctx.send_help(self.course)

    @course.command(name="join")
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def join_course(self, ctx: commands.Context, *, course_code: str) -> None:
        """Join a course channel."""
        await self.course_service.grant_course_channel_access(ctx, course_code)

    @course.command(name="leave")
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def leave_course(self, ctx: commands.Context, *, course_code: str) -> None:
        """Leave a course channel."""
        await self.course_service.revoke_course_channel_access(ctx, course_code)

    @course.command(name="details")
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def course_details(self, ctx: commands.Context, *, course_code: str) -> None:
        """Get details of a course."""
        embed = await self.course_service.course_details(ctx, course_code)
        if embed is None:
            await ctx.send(error(f"Course not found: {course_code}"))
        else:
            await ctx.send(embed=embed)

    @commands.admin()
    @course.command(name="setlogging")
    async def set_logging(
        self, ctx: commands.Context, channel: discord.TextChannel
    ) -> None:
        """Set the logging channel for course events."""
        await self.course_service.set_logging(ctx, channel)

    @commands.is_owner()
    @commands.group(name="dc", invoke_without_command=True)
    async def dev_course(self, ctx: commands.Context) -> None:
        """Developer commands for Course Manager."""
        await ctx.send_help(self.dev_course)

    @dev_course.command(name="enable")
    @commands.guild_only()
    @commands.has_permissions(administrator=True)
    async def enable(self, ctx: commands.Context) -> None:
        """Enable Course Manager in the server."""
        await self.course_service.enable(ctx)

    @dev_course.command(name="disable")
    @commands.guild_only()
    @commands.has_permissions(administrator=True)
    async def disable(self, ctx: commands.Context) -> None:
        """Disable Course Manager in the server."""
        await self.course_service.disable(ctx)

    @dev_course.command(name="term")
    async def set_term_code(
        self, ctx: commands.Context, term_name: str, year: int, term_id: int
    ) -> None:
        """Set term code for a given term."""
        term_key = f"{term_name.lower()}-{year}"
        async with self.config.term_codes() as term_codes:
            term_codes[term_key] = term_id
        log.debug(f"Set term code for {term_key} to {term_id}")
        await ctx.send(
            success(f"Term code for {term_name.capitalize()} {year} set to: {term_id}")
        )

    @dev_course.command(name="populate")
    async def populate_courses(self, ctx: commands.Context) -> None:
        """Fetch and cache courses data."""
        await self.course_service.populate_courses(ctx)

    @dev_course.command(name="listall")
    async def list_all_courses(self, ctx: commands.Context) -> None:
        """List all cached courses."""
        await self.course_service.list_all_courses(ctx)

    @dev_course.command(name="refresh")
    async def refresh_course(self, ctx: commands.Context, *, course_code: str) -> None:
        """Refresh data for a specific course."""
        await self.course_service.refresh_course_data(ctx, course_code)

    @dev_course.command(name="printconfig")
    async def print_config(self, ctx: commands.Context) -> None:
        """Print the current configuration to the console."""
        print(await self.config.all())
        await ctx.send(info("Config printed to console."))

    @dev_course.command(name="clearall")
    async def reset_config(self, ctx: commands.Context) -> None:
        """Clear all configuration data."""
        await self.config.clear_all()
        await ctx.send(success("All config data cleared."))

    @dev_course.command(name="setdefaultcategory")
    async def set_default_category(
        self, ctx: commands.Context, *, category_name: str
    ) -> None:
        """Set the default category for courses."""
        await self.channel_service.set_default_category(ctx, category_name)
        await ctx.send(success(f"Default category set to **{category_name}**"))
