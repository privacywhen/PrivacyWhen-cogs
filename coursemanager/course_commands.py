"""Module: CourseChannelCog.

Module defines the `CourseChannelCog` class, which handles course management commands
for a Discord bot using Redbot. The cog supports course-specific functionalities such as
joining or leaving courses, displaying course details, and configuring logging channels.
Also handles the clustering of course channels and provides commands for developers to
enable or disable the course manager, manage term codes, and more.

Dependencies:
- redbot.core: Provides core functionality for commands, config, and context handling.
- discord: Used for interacting with Discord's API, e.g. creating and managing channels.
- ChannelService: Manages the creation, modification, and deletion of channels.
- CourseService: Handles management of course data and user access to course channels.
- CourseChannelClustering: Groups courses into categories based on clustering logic.
"""

from __future__ import annotations

import asyncio
import functools
from typing import TYPE_CHECKING, Any, Callable, TypeVar

import discord  # noqa: TC002
from redbot.core import Config, app_commands, commands
from redbot.core.utils.chat_formatting import error, success

from .channel_service import ChannelService
from .constants import GLOBAL_DEFAULTS, GROUPING_INTERVAL
from .course_channel_clustering import CourseChannelClustering
from .course_service import CourseService
from .logger_util import get_logger

if TYPE_CHECKING:
    from collections.abc import Coroutine

log = get_logger(__name__)
T = TypeVar("T")


def handle_command_errors(
    func: Callable[..., Coroutine[Any, Any, T]],
) -> Callable[..., Coroutine[Any, Any, T]]:
    """Handle errors in commands.

    Catches any exceptions raised in the command function, logs them,
    and sends a user-friendly error message to the context.

    Args:
        func (Callable[..., Coroutine[Any, Any, T]]): Command function to be wrapped.

    Returns:
        Callable[..., Coroutine[Any, Any, T]]: Wrapped command function error handling.

    """

    @functools.wraps(func)
    async def wrapper(
        self: commands.Cog,
        ctx: commands.Context,
        *args: object,
        **kwargs: object,
    ) -> T:
        try:
            return await func(self, ctx, *args, **kwargs)
        except Exception:
            log.exception("Error in command '%s'", func.__name__)
            await ctx.send(error("An unexpected error occurred."))

    return wrapper


class CourseChannelCog(commands.Cog):
    """Cog managing course commands, logging channels, and course channel clustering and pruning.

    Attributes:
        bot (commands.Bot): The instance of the bot.
        config (Config): The configuration object for persistent storage.
        channel_service (ChannelService): The service responsible for managing channels.
        course_service (CourseService): The service responsible for managing courses.
        clustering (CourseChannelClustering): The clustering logic for course channels.
        _prune_task (asyncio.Task | None): The task responsible for pruning channels.
        _cluster_task (asyncio.Task | None): The task responsible for clustering course channels.

    """

    def __init__(self, bot: commands.Bot) -> None:
        """Initialize the `CourseChannelCog` class with the given bot instance.

        Args:
            bot (commands.Bot): The bot instance to associate with the cog.

        """
        self.bot: commands.Bot = bot
        self.config: Config = Config.get_conf(
            self,
            identifier=42043360,
            force_registration=True,
        )
        self.config.register_global(**GLOBAL_DEFAULTS)
        self.channel_service: ChannelService = ChannelService(bot, self.config)
        self.course_service: CourseService = CourseService(bot, self.config)
        self.clustering = CourseChannelClustering(
            grouping_threshold=GLOBAL_DEFAULTS.get("grouping_threshold", 2),
            category_prefix=GLOBAL_DEFAULTS.get("course_category", "COURSES"),
        )
        self._prune_task: asyncio.Task | None = asyncio.create_task(
            self.channel_service.auto_channel_prune(),
        )
        self._cluster_task: asyncio.Task | None = asyncio.create_task(
            self.course_service.auto_course_clustering(
                self.channel_service,
                self.clustering,
                interval=GROUPING_INTERVAL,
            ),
        )
        log.debug("CourseChannelCog initialized.")

    async def cog_check(self, ctx: commands.Context) -> bool:
        """Check if a command is allowed to run, depending on the state of the server.

        Args:
            ctx (commands.Context): The context for the command to check.

        Returns:
            bool: Whether the command is allowed to run in the given context.

        """
        if ctx.guild is None:
            return True
        if ctx.command.qualified_name.lower().startswith(
            "course",
        ) and ctx.command.name.lower() not in {
            "enable",
            "disable",
            "course",
        }:
            enabled = await self.config.enabled_guilds()
            if ctx.guild.id not in enabled:
                await ctx.send(
                    error(
                        "The Course Manager is currently disabled in this server. Please enable it using the `/course enable` command.",
                    ),
                )
                return False
        return True

    def cog_unload(self) -> None:
        """Unload the cog, canceling background tasks and cleaning up resources."""
        log.debug("Unloading CourseChannelCog; cancelling background tasks.")
        for task in (self._prune_task, getattr(self, "_cluster_task", None)):
            if task:
                task.cancel()

        try:
            asyncio.get_event_loop().create_task(
                self.course_service.course_data_proxy.close(),
            )
        except Exception:
            log.exception("Error during CourseDataProxy shutdown")

    @commands.hybrid_group(
        name="course",
        invoke_without_command=True,
        case_insensitive=True,
    )
    async def course(self, ctx: commands.Context) -> None:
        """Provide help for the `course` command group.

        Args:
            ctx (commands.Context): The context for the command.

        """
        await ctx.send_help(ctx.command)

    @course.command(name="join")
    @commands.cooldown(1, 5, commands.BucketType.user)
    @app_commands.describe(course_code="The course code you wish to join")
    @handle_command_errors
    async def join_course(self, ctx: commands.Context, *, course_code: str) -> None:
        """Grant access to a course channel for the user.

        Command allows users to join a course channel based on the provided course code.

        Args:
            ctx (commands.Context): The context for the command.
            course_code (str): The course code the user wishes to join.

        """
        await self.course_service.grant_course_channel_access(ctx, course_code)

    @course.command(name="leave")
    @commands.cooldown(1, 5, commands.BucketType.user)
    @app_commands.describe(course_code="The course code you wish to leave")
    @handle_command_errors
    async def leave_course(self, ctx: commands.Context, *, course_code: str) -> None:
        """Revoke access to a course channel for the user.

        This command allows users to leave a course channel based on the provided course code.

        Args:
            ctx (commands.Context): The context for the command.
            course_code (str): The course code the user wishes to leave.

        """
        await self.course_service.revoke_course_channel_access(ctx, course_code)

    @course.command(name="details")
    @commands.cooldown(1, 5, commands.BucketType.user)
    @app_commands.describe(course_code="The course code to view details for")
    @handle_command_errors
    async def course_details(self, ctx: commands.Context, *, course_code: str) -> None:
        """Show details for the specified course.

        This command provides detailed information about a course based on the course code.

        Args:
            ctx (commands.Context): The context for the command.
            course_code (str): The course code to retrieve details for.

        """
        await self.course_service.course_details(ctx, course_code)

    @course.command(name="setlogging")
    @commands.admin()
    @app_commands.describe(channel="The text channel to set as logging channel")
    @handle_command_errors
    async def set_logging(
        self,
        ctx: commands.Context,
        channel: discord.TextChannel,
    ) -> None:
        """Set the specified channel as the logging channel for course-related activities.

        This command configures the bot to send course-related logs to the given text channel.

        Args:
            ctx (commands.Context): The context for the command.
            channel (discord.TextChannel): The text channel to set as the logging channel.

        """
        await self.course_service.set_logging(ctx, channel)

    @commands.is_owner()
    @commands.group(name="dc", invoke_without_command=True)
    async def dev_course(self, ctx: commands.Context) -> None:
        """Provide help for the 'dc' command group.

        This command serves as the entry point for the 'dev_course' command group.
        When invoked without subcommands, it sends help information for the group.

        Args:
            ctx (commands.Context): The context for the command.

        """
        await ctx.send_help(ctx.command)

    @dev_course.command(name="enable")
    @commands.guild_only()
    @commands.has_permissions(administrator=True)
    @handle_command_errors
    async def enable(self, ctx: commands.Context) -> None:
        """Enable the course manager for the current server.

        This command enables the course manager system on the current server, allowing
        users to access course channels and related functionality.

        Args:
            ctx (commands.Context): The context for the command.

        """
        await self.course_service.enable(ctx)

    @dev_course.command(name="disable")
    @commands.guild_only()
    @commands.has_permissions(administrator=True)
    @handle_command_errors
    async def disable(self, ctx: commands.Context) -> None:
        """Disable the course manager for the current server.

        This command disables the course manager system on the current server, preventing
        users from interacting with course channels.

        Args:
            ctx (commands.Context): The context for the command.

        """
        await self.course_service.disable(ctx)

    @dev_course.command(name="term")
    @handle_command_errors
    async def set_term_code(
        self,
        ctx: commands.Context,
        term_name: str,
        year: int,
        term_id: int,
    ) -> None:
        """Set the term code for a specific term.

        This command updates the term information, allowing users to assign a term
        to a course based on the term name, year, and term ID.

        Args:
            ctx (commands.Context): The context for the command.
            term_name (str): The name of the term (e.g., Spring, Fall).
            year (int): The year of the term.
            term_id (int): The identifier for the term.

        """
        await self.course_service.set_term_code(ctx, term_name, year, term_id)

    @dev_course.command(name="populate")
    @handle_command_errors
    async def populate_courses(self, ctx: commands.Context) -> None:
        """Populate the course database with new courses.

        This command adds new courses to the database by gathering the required
        course information and populating the system.

        Args:
            ctx (commands.Context): The context for the command.

        """
        await self.course_service.populate_courses(ctx)

    @dev_course.command(name="listall")
    @handle_command_errors
    async def list_all_courses(self, ctx: commands.Context) -> None:
        """List all available courses.

        This command retrieves and displays a list of all courses in the system.

        Args:
            ctx (commands.Context): The context for the command.

        """
        await self.course_service.list_all_courses(ctx)

    @dev_course.command(name="refresh")
    @handle_command_errors
    async def refresh_course(self, ctx: commands.Context, *, course_code: str) -> None:
        """Refresh the data for a specific course.

        This command updates the course data for the specified course code.

        Args:
            ctx (commands.Context): The context for the command.
            course_code (str): The course code to refresh.

        """
        await self.course_service.refresh_course_data(ctx, course_code)

    @dev_course.command(name="printconfig")
    @handle_command_errors
    async def print_config(self, ctx: commands.Context) -> None:
        """Print the current configuration of the course system.

        This command shows the current configuration settings for the course manager.

        Args:
            ctx (commands.Context): The context for the command.

        """
        await self.course_service.print_config(ctx)

    @dev_course.command(name="clearall")
    @handle_command_errors
    async def reset_config(self, ctx: commands.Context) -> None:
        """Reset the course configuration to its default settings.

        This command clears all custom configurations and restores the default settings.

        Args:
            ctx (commands.Context): The context for the command.

        """
        await self.course_service.reset_config(ctx)

    @dev_course.command(name="setdefaultcategory")
    @handle_command_errors
    async def set_default_category(
        self,
        ctx: commands.Context,
        *,
        category_name: str,
    ) -> None:
        """Set the default category for course channels.

        This command configures the default category to which new course channels
        will be assigned.

        Args:
            ctx (commands.Context): The context for the command.
            category_name (str): The name of the default category to set.

        """
        await self.channel_service.set_default_category(ctx, category_name)
        await ctx.send(success(f"Default category set to **{category_name}**"))

    @dev_course.command(name="recluster")
    @commands.guild_only()
    @commands.has_permissions(administrator=True)
    @handle_command_errors
    async def recluster(self, ctx: commands.Context) -> None:
        """Recompute clustering and move course channels into their new categories.

        This command triggers the recomputation of course channel clusters based on
        the current user groupings and applies the new category mapping.

        Args:
            ctx (commands.Context): The context for the command.

        """
        guild = ctx.guild

        # 1) Gather current membership
        course_users = await self.course_service.gather_course_user_data(guild)

        # 2) Compute new grouping
        mapping = self.clustering.cluster_courses(course_users)

        # 3) Persist and apply
        await self.config.course_groups.set(mapping)
        await self.channel_service.apply_category_mapping(guild, mapping)

        await ctx.send(
            success(
                "Reclustered and moved course channels according to clusters.",
            ),
        )
