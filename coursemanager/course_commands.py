import asyncio
import functools
from typing import Any, Callable, Coroutine, Optional, TypeVar

import discord
from redbot.core import Config, app_commands, commands
from redbot.core.utils.chat_formatting import error, success

from .channel_service import ChannelService
from .constants import GLOBAL_DEFAULTS, GROUPING_INTERVAL
from .course_channel_clustering import CourseChannelClustering
from .course_service import CourseService
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
        self._cluster_task: Optional[asyncio.Task] = asyncio.create_task(
            self.course_service.auto_course_clustering(
                self.channel_service,
                self.clustering,
                interval=GROUPING_INTERVAL,
            )
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
        for task in (self._prune_task, getattr(self, "_cluster_task", None)):
            if task:
                task.cancel()

        try:
            asyncio.get_event_loop().create_task(
                self.course_service.course_data_proxy.close()
            )
        except Exception as exc:
            log.exception(f"Error during CourseDataProxy shutdown: {exc}")

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
        """Manually trigger course clustering using live Discord state."""
        guild = ctx.guild

        # Gather user-sets and metadata in one shot (string-keyed)
        (
            course_users_raw,
            course_metadata_raw,
        ) = await self.course_service.gather_course_user_data(
            guild, include_metadata=True
        )
        if not course_users_raw:
            await ctx.send(error("No course membership data found."))
            return

        # Convert course codes to deterministic ints
        sorted_codes = sorted(course_users_raw)
        code_to_id: dict[str, int] = {
            code: idx for idx, code in enumerate(sorted_codes, start=1)
        }
        id_to_code: dict[int, str] = {v: k for k, v in code_to_id.items()}

        course_users: dict[int, set[int]] = {
            code_to_id[code]: users for code, users in course_users_raw.items()
        }
        course_metadata: dict[int, dict[str, str]] = {
            code_to_id[code]: meta for code, meta in course_metadata_raw.items()
        }

        # Run clustering
        mapping_int = self.clustering.cluster_courses(course_users, course_metadata)

        # Convert result back to readable course codes
        mapping: dict[str, str] = {
            id_to_code[course_id]: category
            for course_id, category in mapping_int.items()
        }

        # Show a sample of the result
        preview = "\n".join(f"{k}: {v}" for k, v in list(mapping.items())[:10])
        await ctx.send(
            success(f"Clustering complete. Sample result (first 10):\n```{preview}```")
        )

    @dev_course.command(name="recluster")
    @commands.guild_only()
    @commands.has_permissions(administrator=True)
    @handle_command_errors
    async def recluster(self, ctx: commands.Context) -> None:
        """
        Recompute clustering and move course channels into their new categories.
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
                "Reclustered and moved course channels according to the latest clusters."
            )
        )
