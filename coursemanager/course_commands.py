import asyncio
from collections import defaultdict
import functools
from typing import Any, Callable, Coroutine, Optional, TypeVar

import discord
from redbot.core import Config, commands, app_commands
from redbot.core.utils.chat_formatting import error, info, success, warning
from redbot.core.utils.menus import menu

from .channel_service import ChannelService
from .constants import GLOBAL_DEFAULTS
from .course_service import CourseService
from .course_channel_clustering import CourseChannelClustering
from .logger_util import get_logger

log = get_logger("red.course_channel_cog")
T = TypeVar("T")


def handle_command_errors(
    func: Callable[..., Coroutine[Any, Any, T]]
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
            max_category_channels=50,
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
                        "Course Manager is disabled in this server. Please enable it using `/course enable`."
                    )
                )
                return False
        return True

    def cog_unload(self) -> None:
        log.debug("Unloading CourseChannelCog; cancelling background tasks.")
        if self._prune_task:
            self._prune_task.cancel()
        asyncio.create_task(self.course_service.course_data_proxy.close())

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

    ###TEMP TESTING###
    @dev_course.command(name="testclusteringreal")
    @commands.is_owner()
    @handle_command_errors
    async def test_clustering_real(self, ctx: commands.Context) -> None:
        """
        Run the clustering algorithm on real course channel data from your guild.

        This command:
          1. Finds categories using the course category prefix from config.
          2. For each text channel, collects IDs of members (excluding bots) who can read it.
          3. Tries to parse each channel name as a CourseCode to extract metadata.
          4. Runs the clustering algorithm on the collected data.
          5. Outputs detailed stats, the final course-to-category mapping, and
             a new channel ordering per category with indications as to why a channel
             may have been moved (e.g. if a cluster was split due to channel count limits).

        Additional logging is emitted to the console (with summaries only) to assist with debugging.
        """
        import time
        from json import dumps
        from typing import Dict, List, Set, DefaultDict
        from discord import TextChannel
        from .course_code import CourseCode

        def safe_dumps(obj) -> str:
            # Recursively convert sets to lists for JSON serialization.
            def convert(o):
                if isinstance(o, set):
                    return list(o)
                elif isinstance(o, dict):
                    return {k: convert(v) for k, v in o.items()}
                elif isinstance(o, list):
                    return [convert(i) for i in o]
                return o

            return dumps(convert(obj), indent=2, sort_keys=True)

        def paginate_output(
            lines: List[str], header: str = "```python\n", footer: str = "\n```"
        ) -> List[str]:
            """Breaks the output into chunks that are less than 2000 characters."""
            messages = []
            current_chunk = header
            for line in lines:
                if len(current_chunk) + len(line) + len(footer) + 1 > 1900:
                    current_chunk += footer
                    messages.append(current_chunk)
                    current_chunk = header + line + "\n"
                else:
                    current_chunk += line + "\n"
            if current_chunk.strip() != header.strip():
                current_chunk += footer
                messages.append(current_chunk)
            return messages

        # Additional logging to console.
        log.debug("Starting real-data clustering test command.")
        output_lines: List[str] = []
        output_lines.append("===== Real Data Clustering Test =====")
        overall_start = time.time()

        # Step 1: Collect channel data.
        try:
            guild = ctx.guild
            config_prefix: str = await self.config.course_category()
            output_lines.append(
                f"Using course category prefix from config: '{config_prefix}'"
            )
            log.debug(
                "Retrieving categories with prefix '%s' in guild '%s'.",
                config_prefix,
                guild.name,
            )
            from .utils import get_categories_by_prefix

            categories = get_categories_by_prefix(guild, config_prefix)
            if not categories:
                msg = (
                    f"No categories found with prefix '{config_prefix}' in this guild."
                )
                output_lines.append(msg)
                log.error(msg)
                for msg_chunk in paginate_output(output_lines):
                    await ctx.send(msg_chunk)
                return

            channels_processed = 0
            channels_failed_parse = 0
            course_users: Dict[int, Set[int]] = {}  # channel ID -> set of member IDs
            course_metadata: Dict[int, Dict[str, str]] = {}  # channel ID -> metadata

            for category in categories:
                log.debug("Processing category '%s'.", category.name)
                for channel in category.channels:
                    if not isinstance(channel, TextChannel):
                        continue
                    channels_processed += 1
                    # Collect member IDs (exclude bots)
                    member_ids = {
                        member.id
                        for member in guild.members
                        if not member.bot
                        and channel.permissions_for(member).read_messages
                    }
                    if member_ids:
                        course_users[channel.id] = member_ids
                    # Try to parse the channel name as a CourseCode.
                    try:
                        course_obj = CourseCode(channel.name)
                        # We store department and also the original channel name.
                        course_metadata[channel.id] = {
                            "department": course_obj.department,
                            "name": channel.name,
                        }
                        log.debug(
                            "Channel '%s' parsed successfully (department: %s).",
                            channel.name,
                            course_obj.department,
                        )
                    except Exception as parse_exc:
                        channels_failed_parse += 1
                        course_metadata[channel.id] = {"name": channel.name}
                        log.warning(
                            "Failed to parse channel name '%s': %s",
                            channel.name,
                            parse_exc,
                        )

            output_lines.append(
                f"Step 1: Processed {channels_processed} channels across {len(categories)} categories."
            )
            output_lines.append(
                f"         Valid parsing: {channels_processed - channels_failed_parse}"
            )
            output_lines.append(f"         Failed parsing: {channels_failed_parse}")
            output_lines.append(
                f"         Channels with user data: {len(course_users)}"
            )
            log.debug("Step 1 complete: Data collection finished.")
        except Exception as e:
            msg = f"Step 1 Error: {e}"
            output_lines.append(msg)
            log.exception(msg)
            for msg_chunk in paginate_output(output_lines):
                await ctx.send(msg_chunk)
            return

        # Step 2: Instantiate clustering instance.
        try:
            grouping_threshold = await self.config.grouping_threshold()
            # Use constant value for max_category_channels (since self.config.get(...) isn't registered).
            max_cat_channels = 50
            clustering_instance = self.clustering.__class__(
                grouping_threshold=grouping_threshold,
                max_category_channels=max_cat_channels,
                category_prefix=config_prefix,
                adaptive_threshold=False,
            )
            output_lines.append("Step 2: Clustering instance created.")
            log.debug(
                "Clustering instance created with grouping_threshold=%s, max_category_channels=%s",
                grouping_threshold,
                max_cat_channels,
            )
        except Exception as e:
            msg = f"Step 2 Error: {e}"
            output_lines.append(msg)
            log.exception(msg)
            for msg_chunk in paginate_output(output_lines):
                await ctx.send(msg_chunk)
            return

        # Step 3: Run clustering.
        try:
            cluster_start = time.time()
            mapping = clustering_instance.cluster_courses(course_users, course_metadata)
            cluster_time = time.time() - cluster_start
            output_lines.append(
                f"Step 3: Clustering executed in {cluster_time:.4f} seconds."
            )
            output_lines.append(
                "  Final Course-to-Category Mapping (showing sample keys):"
            )
            sample_keys = list(mapping.keys())[:10]
            output_lines.append(
                safe_dumps(
                    {"sample_mapping_keys": sample_keys, "total_keys": len(mapping)}
                )
            )
            log.debug("Clustering complete. Mapping sample keys: %s", sample_keys)
        except Exception as e:
            msg = f"Step 3 Error: {e}"
            output_lines.append(msg)
            log.exception(msg)
            for msg_chunk in paginate_output(output_lines):
                await ctx.send(msg_chunk)
            return

        # Step 4: Additional statistics.
        try:
            norm_users = clustering_instance._normalize_course_users(course_users)
            norm_metadata = clustering_instance._normalize_course_metadata(
                course_metadata
            )
            graph = clustering_instance._build_graph(norm_users, norm_metadata)
            clusters = clustering_instance._perform_clustering(graph)
            cluster_sizes = [len(cluster) for cluster in clusters]
            evaluation = clustering_instance.evaluate_clusters(graph, clusters)
            output_lines.append("Step 4: Additional Statistics:")
            output_lines.append(
                f"  Graph: {graph.number_of_nodes()} nodes, {graph.number_of_edges()} edges"
            )
            output_lines.append(f"  Cluster sizes: {cluster_sizes}")
            output_lines.append(f"  Evaluation Metrics: {safe_dumps(evaluation)}")
            log.debug(
                "Graph: %s nodes, %s edges",
                graph.number_of_nodes(),
                graph.number_of_edges(),
            )
            log.debug("Cluster sizes: %s", cluster_sizes)
            log.debug("Evaluation metrics: %s", evaluation)
        except Exception as e:
            msg = f"Step 4 Error: {e}"
            output_lines.append(msg)
            log.exception(msg)

        # Step 5: Compute and output the new channel ordering per category.
        try:
            # The mapping from _map_clusters_to_categories is our channel ordering.
            # We group channels by category label.
            new_ordering: DefaultDict[str, List[int]] = defaultdict(list)
            for channel_id, category_label in mapping.items():
                new_ordering[category_label].append(channel_id)
            # For clarity, sort each category's channel list numerically.
            for cat in new_ordering:
                new_ordering[cat].sort()
            output_lines.append("Step 5: New Channel Ordering per Category:")
            for cat, channels in sorted(new_ordering.items()):
                note = ""
                # If the category label has a dash, it indicates a split due to max channel limit.
                if "-" in cat:
                    note = " (cluster split due to channel limit)"
                output_lines.append(f"  {cat}:{note} {channels}")
            log.debug("New channel ordering computed: %s", dict(new_ordering))
        except Exception as e:
            msg = f"Step 5 Error: {e}"
            output_lines.append(msg)
            log.exception(msg)

        overall_time = time.time() - overall_start
        output_lines.append(f"\nTotal Execution Time: {overall_time:.4f} seconds")
        output_lines.append("===================================")
        log.debug("Test clustering command completed in %.4f seconds.", overall_time)

        # Paginate and send output.
        for msg_chunk in paginate_output(output_lines):
            await ctx.send(msg_chunk)
