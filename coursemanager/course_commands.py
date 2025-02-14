"""
Module defining the CourseChannelCog and associated commands.
"""

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
    """
    Redbot cog for managing course channels and course-related commands.
    """

    def __init__(self, bot: commands.Bot) -> None:
        """
        Initialize the CourseChannelCog.

        Args:
            bot (commands.Bot): The bot instance.
        """
        self.bot: commands.Bot = bot
        self.config: Config = Config.get_conf(
            self, identifier=42043360, force_registration=True
        )
        self.config.register_global(**GLOBAL_DEFAULTS)
        self.channel_service: ChannelService = ChannelService(bot, self.config)
        self.course_service: CourseService = CourseService(bot, self.config)
        self._prune_task: Optional[asyncio.Task] = asyncio.create_task(
            self.channel_service.auto_channel_prune()
        )
        log.debug("CourseChannelCog initialized.")

    async def cog_check(self, ctx: commands.Context) -> bool:
        """
        Global check for commands in this cog.

        Args:
            ctx (commands.Context): The command context.

        Returns:
            bool: True if the command can be processed, otherwise False.
        """
        if ctx.guild is None:
            return True
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
        """
        Unload the cog and cancel background tasks.
        """
        log.debug("Unloading CourseChannelCog; cancelling background tasks.")
        if self._prune_task:
            self._prune_task.cancel()
        self.bot.loop.create_task(self.course_service.course_data_proxy.close())

    @commands.group(name="course", invoke_without_command=True)
    async def course(self, ctx: commands.Context) -> None:
        """
        Base command for course management.
        """
        await ctx.send_help(self.course)

    @course.command(name="join")
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def join_course(self, ctx: commands.Context, *, course_code: str) -> None:
        """
        Join a course channel.

        Args:
            ctx (commands.Context): The command context.
            course_code (str): The course code to join.
        """
        await self.course_service.grant_course_channel_access(ctx, course_code)

    @course.command(name="leave")
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def leave_course(self, ctx: commands.Context, *, course_code: str) -> None:
        """
        Leave a course channel.

        Args:
            ctx (commands.Context): The command context.
            course_code (str): The course code to leave.
        """
        await self.course_service.revoke_course_channel_access(ctx, course_code)

    @course.command(name="details")
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def course_details(self, ctx: commands.Context, *, course_code: str) -> None:
        """
        Get details for a course.

        Args:
            ctx (commands.Context): The command context.
            course_code (str): The course code to get details for.
        """
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
        """
        Set the logging channel.

        Args:
            ctx (commands.Context): The command context.
            channel (discord.TextChannel): The text channel for logging.
        """
        await self.course_service.set_logging(ctx, channel)

    @commands.is_owner()
    @commands.group(name="dc", invoke_without_command=True)
    async def dev_course(self, ctx: commands.Context) -> None:
        """
        Developer commands for course management.
        """
        await ctx.send_help(self.dev_course)

    @dev_course.command(name="enable")
    @commands.guild_only()
    @commands.has_permissions(administrator=True)
    async def enable(self, ctx: commands.Context) -> None:
        """
        Enable Course Manager in the server.

        Args:
            ctx (commands.Context): The command context.
        """
        await self.course_service.enable(ctx)

    @dev_course.command(name="disable")
    @commands.guild_only()
    @commands.has_permissions(administrator=True)
    async def disable(self, ctx: commands.Context) -> None:
        """
        Disable Course Manager in the server.

        Args:
            ctx (commands.Context): The command context.
        """
        await self.course_service.disable(ctx)

    @dev_course.command(name="term")
    async def set_term_code(
        self, ctx: commands.Context, term_name: str, term_id: int
    ) -> None:
        """
        Set the term code for a given term.

        Args:
            ctx (commands.Context): The command context.
            term_name (str): The name of the term.
            term_id (int): The term identifier.
        """
        await self.course_service.set_term_code(ctx, term_name, term_id)

    @dev_course.command(name="populate")
    async def populate_courses(self, ctx: commands.Context) -> None:
        """
        Populate the course listings.
        """
        await self.course_service.populate_courses(ctx)

    @dev_course.command(name="listall")
    async def list_all_courses(self, ctx: commands.Context) -> None:
        """
        List all cached courses.
        """
        await self.course_service.list_all_courses(ctx)

    @dev_course.command(name="refresh")
    async def refresh_course(self, ctx: commands.Context, *, course_code: str) -> None:
        """
        Refresh course data for a specific course.

        Args:
            ctx (commands.Context): The command context.
            course_code (str): The course code to refresh.
        """
        await self.course_service.refresh_course_data(ctx, course_code)

    @dev_course.command(name="printconfig")
    async def print_config(self, ctx: commands.Context) -> None:
        """
        Print the current configuration to the console.
        """
        print(await self.config.all())
        await ctx.send(info("Config printed to console."))

    @dev_course.command(name="clearall")
    async def reset_config(self, ctx: commands.Context) -> None:
        """
        Clear all configuration data.
        """
        await self.config.clear_all()
        await ctx.send(success("All config data cleared."))

    @dev_course.command(name="setdefaultcategory")
    async def set_default_category(
        self, ctx: commands.Context, *, category_name: str
    ) -> None:
        """
        Set the default courses category.

        Args:
            ctx (commands.Context): The command context.
            category_name (str): The name of the default category.
        """
        await self.channel_service.set_default_category(ctx, category_name)
        await ctx.send(success(f"Default category set to **{category_name}**"))
