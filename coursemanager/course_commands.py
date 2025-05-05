import asyncio
import functools
from typing import Any, Callable, Coroutine, Optional, TypeVar

import discord
from redbot.core import Config, commands, app_commands
from redbot.core.utils.chat_formatting import error, info, success, warning

from coursemanager.course_code import CourseCode
from coursemanager.utils import get_categories_by_prefix

from .channel_service import ChannelService
from .constants import GLOBAL_DEFAULTS
from .course_service import CourseService
from .course_channel_clustering import CourseChannelClustering
from .logger_util import get_logger

log = get_logger("red.course_channel_cog")
T = TypeVar("T")


def handle_command_errors(
    func: Callable[..., Coroutine[Any, Any, T]],
) -> Callable[..., Coroutine[Any, Any, T]]:
    @functools.wraps(func)
    async def wrapper(self: Any, ctx: commands.Context, *args: Any, **kwargs: Any) -> T:
        try:
            return await func(self, ctx, *args, **kwargs)
        except Exception as exc:
            log.exception(f"Error in command '{func.__name__}': {exc}")
            await ctx.send(error("An unexpected error occurred."))

    return wrapper


class CourseChannelCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot: commands.Bot = bot
        self.config: Config = Config.get_conf(
            self, identifier=42043360, force_registration=True
        )
        self.config.register_global(**GLOBAL_DEFAULTS)
        self.channel_service: ChannelService = ChannelService(bot, self.config)
        self.course_service: CourseService = CourseService(bot, self.config)
        self.clustering = CourseChannelClustering(
            grouping_threshold=GLOBAL_DEFAULTS.get("grouping_threshold", 2),
            category_prefix=GLOBAL_DEFAULTS.get("course_category", "COURSES"),
        )
        self._prune_task: Optional[asyncio.Task] = asyncio.create_task(
            self.channel_service.auto_channel_prune()
        )
        log.debug("CourseChannelCog initialized.")

    async def cog_check(self, ctx: commands.Context) -> bool:
        if ctx.guild is None:
            return True
        if ctx.command.qualified_name.lower().startswith(
            "course"
        ) and ctx.command.name.lower() not in {
            "enable",
            "disable",
            "course",
        }:
            enabled = await self.config.enabled_guilds()
            if ctx.guild.id not in enabled:
                await ctx.send(
                    error(
                        "The Course Manager is currently disabled in this server. Please enable it using the `/course enable` command."
                    )
                )
                return False
        return True

    def cog_unload(self) -> None:
        log.debug("Unloading CourseChannelCog; cancelling background tasks.")
        if self._prune_task:
            self._prune_task.cancel()
        try:
            asyncio.create_task(self.course_service.course_data_proxy.close())
        except Exception as exc:
            log.exception(f"Error during closing course_data_proxy: {exc}")

    @commands.hybrid_group(
        name="course", invoke_without_command=True, case_insensitive=True
    )
    async def course(self, ctx: commands.Context) -> None:
        await ctx.send_help(ctx.command)

    @course.command(name="join")
    @commands.cooldown(1, 5, commands.BucketType.user)
    @app_commands.describe(course_code="The course code you wish to join")
    @handle_command_errors
    async def join_course(self, ctx: commands.Context, *, course_code: str) -> None:
        await self.course_service.grant_course_channel_access(ctx, course_code)

    @course.command(name="leave")
    @commands.cooldown(1, 5, commands.BucketType.user)
    @app_commands.describe(course_code="The course code you wish to leave")
    @handle_command_errors
    async def leave_course(self, ctx: commands.Context, *, course_code: str) -> None:
        await self.course_service.revoke_course_channel_access(ctx, course_code)

    @course.command(name="details")
    @commands.cooldown(1, 5, commands.BucketType.user)
    @app_commands.describe(course_code="The course code to view details for")
    @handle_command_errors
    async def course_details(self, ctx: commands.Context, *, course_code: str) -> None:
        await self.course_service.course_details(ctx, course_code)

    @course.command(name="setlogging")
    @commands.admin()
    @app_commands.describe(channel="The text channel to set as logging channel")
    @handle_command_errors
    async def set_logging(
        self, ctx: commands.Context, channel: discord.TextChannel
    ) -> None:
        await self.course_service.set_logging(ctx, channel)

    @commands.is_owner()
    @commands.group(name="dc", invoke_without_command=True)
    async def dev_course(self, ctx: commands.Context) -> None:
        await ctx.send_help(ctx.command)

    @dev_course.command(name="enable")
    @commands.guild_only()
    @commands.has_permissions(administrator=True)
    @handle_command_errors
    async def enable(self, ctx: commands.Context) -> None:
        await self.course_service.enable(ctx)

    @dev_course.command(name="disable")
    @commands.guild_only()
    @commands.has_permissions(administrator=True)
    @handle_command_errors
    async def disable(self, ctx: commands.Context) -> None:
        await self.course_service.disable(ctx)

    @dev_course.command(name="term")
    @handle_command_errors
    async def set_term_code(
        self, ctx: commands.Context, term_name: str, year: int, term_id: int
    ) -> None:
        await self.course_service.set_term_code(ctx, term_name, year, term_id)

    @dev_course.command(name="populate")
    @handle_command_errors
    async def populate_courses(self, ctx: commands.Context) -> None:
        await self.course_service.populate_courses(ctx)

    @dev_course.command(name="listall")
    @handle_command_errors
    async def list_all_courses(self, ctx: commands.Context) -> None:
        await self.course_service.list_all_courses(ctx)

    @dev_course.command(name="refresh")
    @handle_command_errors
    async def refresh_course(self, ctx: commands.Context, *, course_code: str) -> None:
        await self.course_service.refresh_course_data(ctx, course_code)

    @dev_course.command(name="printconfig")
    @handle_command_errors
    async def print_config(self, ctx: commands.Context) -> None:
        await self.course_service.print_config(ctx)

    @dev_course.command(name="clearall")
    @handle_command_errors
    async def reset_config(self, ctx: commands.Context) -> None:
        await self.course_service.reset_config(ctx)

    @dev_course.command(name="setdefaultcategory")
    @handle_command_errors
    async def set_default_category(
        self, ctx: commands.Context, *, category_name: str
    ) -> None:
        await self.channel_service.set_default_category(ctx, category_name)
        await ctx.send(success(f"Default category set to **{category_name}**"))

    @dev_course.command(name="cluster")
    @commands.guild_only()
    @commands.is_owner()
    @handle_command_errors
    async def manual_cluster(self, ctx: commands.Context) -> None:
        """Manually trigger course clustering from live channel membership."""
        guild = ctx.guild
        prefix = await self.config.course_category()

        course_users: dict[str, set[int]] = {}
        course_metadata: dict[str, dict[str, str]] = {}

        # Build the user‑course map
        for category in get_categories_by_prefix(guild, prefix):
            for channel in category.text_channels:  # 1️⃣ not async
                try:
                    course = CourseCode(channel.name)
                except ValueError:  # skip non‑course channels
                    continue

                if users := {
                    m.id
                    for m in channel.members
                    if not m.bot
                    and channel.permissions_for(m).read_messages
                    and channel.permissions_for(m).send_messages
                }:
                    key = course.canonical()  # 2️⃣ stable key
                    course_users[key] = users
                    course_metadata[key] = {"department": course.department}

        if not course_users:
            await ctx.send(error("No course membership data found."))
            return

        mapping = self.clustering.cluster_courses(course_users, course_metadata)

        # Optional: persist mapping to Config here
        # await self.config.guild(guild).course_mapping.set(mapping)

        preview = "\n".join(f"{k}: {v}" for k, v in list(mapping.items())[:10])
        await ctx.send(success(f"Clustering complete. Sample result:\n```{preview}```"))
