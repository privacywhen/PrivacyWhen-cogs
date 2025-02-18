import asyncio
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
    @dev_course.command(name="testclustering")
    @commands.is_owner()
    @handle_command_errors
    async def test_clustering(self, ctx: commands.Context) -> None:
        """
        Verbosely test the clustering functionality from course_channel_clustering.py.

        This command performs the following steps:
          1. Creates dummy course user and metadata inputs.
          2. Normalizes the data.
          3. Builds an overlap graph.
          4. Performs clustering.
          5. Evaluates clusters (calculates modularity).
          6. Maps courses to category labels.
          7. Reports detailed intermediate and final results.
        """
        import time
        from json import dumps

        def format_dict(d: dict) -> str:
            return dumps(d, indent=2, sort_keys=True)

        def format_list(l: list) -> str:
            return dumps(l, indent=2)

        final_output = []  # Collects output for final reporting
        final_output.append("===== Test Clustering Output =====\n")

        overall_start = time.time()

        # Step 1: Dummy Data Creation
        try:
            dummy_course_users = {
                "101": {1, 2, 3},
                "102": {2, 3, 4},
                "103": {5},
                "104": {6, 7},
                "105": {7, 8},
            }
            dummy_course_metadata = {
                "101": {"department": "CS"},
                "102": {"department": "CS"},
                "103": {"department": "MATH"},
                "104": {"department": "CS"},
                "105": {"department": "MATH"},
            }
            final_output.append("Step 1: Dummy data created successfully.")
        except Exception as e:
            final_output.append(f"Step 1 Error: {e}")
            await ctx.send("\n".join(final_output))
            return

        # Step 2: Instantiate Clustering Instance with Custom Test Parameters
        try:
            clustering_instance = self.clustering.__class__(
                grouping_threshold=2,
                max_category_channels=2,  # force splitting clusters if needed
                category_prefix="TEST",
                adaptive_threshold=False,
            )
            final_output.append(
                "Step 2: Clustering instance created with test parameters."
            )
        except Exception as e:
            final_output.append(f"Step 2 Error: {e}")
            await ctx.send("\n".join(final_output))
            return

        # Step 3: Data Normalization
        try:
            norm_start = time.time()
            normalized_course_users = clustering_instance._normalize_course_users(
                dummy_course_users
            )
            normalized_course_metadata = clustering_instance._normalize_course_metadata(
                dummy_course_metadata
            )
            norm_time = time.time() - norm_start
            final_output.append(
                f"Step 3: Normalization complete in {norm_time:.4f} seconds."
            )
            final_output.append("  Normalized Course Users:")
            final_output.append(format_dict(normalized_course_users))
            final_output.append("  Normalized Course Metadata:")
            final_output.append(format_dict(normalized_course_metadata))
        except Exception as e:
            final_output.append(f"Step 3 Error: {e}")
            await ctx.send("\n".join(final_output))
            return

        # Step 4: Graph Building
        try:
            graph_start = time.time()
            graph = clustering_instance._build_graph(
                normalized_course_users, normalized_course_metadata
            )
            graph_time = time.time() - graph_start
            final_output.append(f"Step 4: Graph built in {graph_time:.4f} seconds.")
            final_output.append(
                f"  Graph Nodes: {graph.number_of_nodes()} nodes, {graph.number_of_edges()} edges"
            )
            # Provide additional graph details
            node_degrees = dict(graph.degree())
            final_output.append("  Node Degrees:")
            final_output.append(format_dict(node_degrees))
            edges = list(graph.edges(data=True))
            final_output.append("  Graph Edges with Weights:")
            final_output.append(
                format_list(
                    [
                        {"source": u, "target": v, "weight": data.get("weight")}
                        for u, v, data in edges
                    ]
                )
            )
        except Exception as e:
            final_output.append(f"Step 4 Error: {e}")
            await ctx.send("\n".join(final_output))
            return

        # Step 5: Clustering
        try:
            cluster_start = time.time()
            clusters = clustering_instance._perform_clustering(graph)
            cluster_time = time.time() - cluster_start
            final_output.append(
                f"Step 5: Clustering complete in {cluster_time:.4f} seconds."
            )
            clusters_formatted = [sorted(list(cluster)) for cluster in clusters]
            final_output.append("  Clusters (raw sets):")
            final_output.append(format_list(clusters_formatted))
        except Exception as e:
            final_output.append(f"Step 5 Error: {e}")
            await ctx.send("\n".join(final_output))
            return

        # Step 6: Evaluation (Modularity)
        try:
            eval_start = time.time()
            evaluation_metrics = clustering_instance.evaluate_clusters(graph, clusters)
            eval_time = time.time() - eval_start
            final_output.append(
                f"Step 6: Evaluation complete in {eval_time:.4f} seconds."
            )
            final_output.append("  Evaluation Metrics:")
            final_output.append(format_dict(evaluation_metrics))
        except Exception as e:
            final_output.append(f"Step 6 Error: {e}")
            await ctx.send("\n".join(final_output))
            return

        # Step 7: Mapping Courses to Category Labels
        try:
            mapping_start = time.time()
            mapping = clustering_instance._map_clusters_to_categories(clusters)
            mapping_time = time.time() - mapping_start
            final_output.append(
                f"Step 7: Mapping complete in {mapping_time:.4f} seconds."
            )
            final_output.append("  Final Course-to-Category Mapping:")
            final_output.append(format_dict(mapping))
        except Exception as e:
            final_output.append(f"Step 7 Error: {e}")
            await ctx.send("\n".join(final_output))
            return

        overall_time = time.time() - overall_start
        final_output.append(f"\nTotal Execution Time: {overall_time:.4f} seconds")
        final_output.append("===================================")

        # Send output as a formatted code block
        output_message = "```python\n" + "\n".join(final_output) + "\n```"
        await ctx.send(output_message)
