import asyncio
from datetime import datetime, timedelta, timezone
from itertools import combinations
from typing import Dict, List, Optional, Tuple
import discord
import networkx as nx
import community as community_louvain
from redbot.core import Config
from redbot.core.utils.chat_formatting import error, pagify
from redbot.core.utils.menus import menu
from .utils import (
    get_categories_by_prefix,
    get_or_create_category,
    get_logger,
)
from .course_code import CourseCode


log = get_logger("red.channel_service")


class ChannelService:
    def __init__(self, bot: discord.Client, config: Config) -> None:
        self.bot: discord.Client = bot
        self.config: Config = config

    async def set_default_category(
        self, ctx: discord.ext.commands.Context, category_name: str
    ) -> None:
        await self.config.default_category.set(category_name)
        log.debug(f"Default category set to {category_name}")

    async def create_channel(
        self,
        ctx: discord.ext.commands.Context,
        channel_name: str,
        category: Optional[discord.CategoryChannel] = None,
    ) -> None:
        guild: discord.Guild = ctx.guild
        if category is None:
            default_cat_name: str = await self.config.default_category()
            category = await get_or_create_category(guild, default_cat_name)
        if category is None:
            await ctx.send(
                error("I do not have permission to create the default category.")
            )
            return
        try:
            channel = await guild.create_text_channel(channel_name, category=category)
            await ctx.send(
                f"Channel {channel.mention} created in category **{category.name}**."
            )
        except discord.Forbidden:
            await ctx.send(
                error("I do not have permission to create a channel in that category.")
            )

    async def delete_channel(
        self, ctx: discord.ext.commands.Context, channel: discord.TextChannel
    ) -> None:
        try:
            await channel.delete()
            await ctx.send(f"Channel **{channel.name}** deleted.")
        except discord.Forbidden:
            await ctx.send(error("I do not have permission to delete that channel."))

    async def list_channels(
        self,
        ctx: discord.ext.commands.Context,
        category: Optional[discord.CategoryChannel] = None,
    ) -> None:
        if category:
            channels = category.channels
            title = f"Channels in category **{category.name}**:"
        else:
            guild: discord.Guild = ctx.guild
            channels = guild.text_channels
            title = "Text channels in this server:"
        if channels:
            channel_list = "\n".join(channel.name for channel in channels)
            pages = [
                f"**{title}**\n{page}"
                for page in pagify(channel_list, page_length=1900)
            ]
            await menu(ctx, pages, timeout=60.0, user=ctx.author)
        else:
            await ctx.send("No channels found.")

    async def set_channel_permission(
        self,
        ctx: discord.ext.commands.Context,
        channel: discord.TextChannel,
        member: discord.Member,
        allow: bool,
    ) -> None:
        try:
            if allow:
                overwrite = discord.PermissionOverwrite(
                    read_messages=True, send_messages=True
                )
            else:
                overwrite = discord.PermissionOverwrite(read_messages=False)
            action = "granted" if allow else "removed"
            await channel.set_permissions(member, overwrite=overwrite)
            await ctx.send(
                f"Permissions for {member.mention} {action} in {channel.mention}."
            )
        except discord.Forbidden:
            await ctx.send(
                error("I do not have permission to manage channel permissions.")
            )

    async def dynamic_grouping_task(self) -> None:
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                await self.compute_course_groupings()
            except asyncio.CancelledError:
                log.info("Dynamic grouping task cancelled.")
                break
            except Exception:
                log.exception("Error computing course groupings")
            interval: int = await self.config.grouping_interval()
            await asyncio.sleep(interval)

    async def compute_course_groupings(self) -> None:
        new_groupings: Dict[int, Dict[str, List[str]]] = {}
        base: str = await self.config.course_category()
        for guild in self.bot.guilds:
            enrollments = await self._gather_enrollments(guild, base)
            if not enrollments:
                continue

            edge_counts = self._compute_edge_counts(enrollments)
            courses_set = {
                course for courses in enrollments.values() for course in courses
            }
            G = nx.Graph()
            G.add_nodes_from(courses_set)

            grouping_threshold: int = await self.config.grouping_threshold()
            for (course1, course2), weight in edge_counts.items():
                if weight >= grouping_threshold:
                    G.add_edge(course1, course2, weight=weight)

            if G.number_of_edges() > 0:
                partition: Dict[str, int] = community_louvain.best_partition(
                    G, weight="weight"
                )
            else:
                partition = {
                    node: int(node) if node.isdigit() else node for node in G.nodes()
                }
            communities: Dict[str, List[str]] = {}
            for course, community_id in partition.items():
                communities.setdefault(str(community_id), []).append(course)
            new_groupings[guild.id] = communities

            target_mapping = self._compute_target_mapping(communities, base)
            await self._assign_channels_to_categories(guild, target_mapping, base)
            log.debug(f"Guild {guild.id}: target mapping: {target_mapping}")

        await self.config.course_groups.set(new_groupings)
        log.debug(f"Updated course groups: {new_groupings}")

    def _compute_edge_counts(
        self, enrollments: Dict[int, List[str]]
    ) -> Dict[Tuple[str, str], int]:
        edge_counter: Dict[Tuple[str, str], int] = {}
        for courses in enrollments.values():
            unique_courses = sorted(set(courses))
            for course_pair in combinations(unique_courses, 2):
                edge_counter[course_pair] = edge_counter.get(course_pair, 0) + 1
        return edge_counter

    async def _gather_enrollments(
        self, guild: discord.Guild, base: str
    ) -> Dict[int, List[str]]:
        enrollments: Dict[int, List[str]] = {}
        course_categories = get_categories_by_prefix(guild, base)
        for category in course_categories:
            for channel in category.channels:
                if isinstance(channel, discord.TextChannel):

                    try:
                        course_obj = CourseCode(channel.name)
                        normalized = course_obj.formatted_channel_name()
                    except ValueError:
                        normalized = channel.name()

                    async for msg in channel.history(limit=10):
                        if not msg.author.bot:
                            enrollments.setdefault(channel.id, []).append(normalized)
                            break
        return enrollments

    def _compute_target_mapping(
        self, communities: Dict[str, List[str]], base: str
    ) -> Dict[str, str]:
        target_mapping: Dict[str, str] = {}
        for community_id, courses in communities.items():
            target_category = f"{base}-{community_id}" if community_id != "0" else base
            for course in courses:
                target_mapping[course] = target_category
        return target_mapping

    async def _assign_channels_to_categories(
        self, guild: discord.Guild, target_mapping: Dict[str, str], base: str
    ) -> None:
        course_categories = get_categories_by_prefix(guild, base)
        for category in course_categories:
            for channel in category.channels:
                if not isinstance(channel, discord.TextChannel):
                    continue
                try:
                    course_obj = CourseCode(channel.name)
                    course_code = course_obj.formatted_channel_name()
                except ValueError:
                    course_code = channel.name()

                if course_code in target_mapping:
                    target_cat_name = target_mapping[course_code]
                    current_cat = channel.category
                    if current_cat is None or current_cat.name != target_cat_name:
                        target_category = await get_or_create_category(
                            guild, target_cat_name
                        )
                        if target_category is not None:
                            try:
                                await channel.edit(category=target_category)
                                log.debug(
                                    f"Moved channel {channel.name} to category {target_cat_name}"
                                )
                            except discord.Forbidden:
                                log.error(
                                    f"No permission to move channel {channel.name} in guild {guild.name}"
                                )

    async def channel_prune_helper(
        self,
        guild: discord.Guild,
        channel: discord.TextChannel,
        prune_threshold: timedelta,
    ) -> None:
        now = datetime.now(timezone.utc)
        last_activity = None

        # Use channel.last_message if available and its author is not a bot.
        if channel.last_message and not channel.last_message.author.bot:
            last_activity = channel.last_message.created_at
            log.debug(f"Using channel.last_message for {channel.name}: {last_activity}")
        else:
            # Retrieve the number of messages to check from the config.
            prune_history_limit: int = await self.config.channel_prune_history_limit()
            async for message in channel.history(limit=prune_history_limit):
                if not message.author.bot:
                    last_activity = message.created_at
                    log.debug(
                        f"Found non-bot message in {channel.name} at {last_activity}"
                    )
                    break

        # If no non-bot message is found, fall back on the channel's creation date.
        if last_activity is None:
            last_activity = channel.created_at
            log.debug(
                f"No non-bot messages found in {channel.name}. Using channel.created_at: {last_activity}"
            )

        inactivity_duration = now - last_activity
        log.debug(
            f"Channel '{channel.name}' inactivity duration: {inactivity_duration}"
        )

        # Delete the channel if it has been inactive longer than the threshold.
        if inactivity_duration > prune_threshold:
            log.info(
                f"Pruning channel '{channel.name}' in guild '{guild.name}'. "
                f"Inactive for {inactivity_duration} (threshold: {prune_threshold})."
            )
            try:
                # Instead of retrieving the reason from config, we now hardcode it.
                await channel.delete(reason="Auto-pruned due to inactivity.")
            except Exception as e:
                log.exception(
                    f"Failed to delete channel '{channel.name}' in guild '{guild.name}': {e}"
                )

    async def auto_channel_prune(self) -> None:
        # Retrieve the inactivity threshold (in days) from config and convert to a timedelta.
        prune_threshold_days: int = await self.config.prune_threshold_days()
        prune_threshold = timedelta(days=prune_threshold_days)
        # Retrieve the prune interval from the config.
        prune_interval: int = await self.config.channel_prune_interval()

        await self.bot.wait_until_ready()
        log.debug("Auto-channel-prune task started.")

        while not self.bot.is_closed():
            log.debug(
                f"Auto-channel-prune cycle started at {datetime.now(timezone.utc)}"
            )
            enabled_guilds: List[int] = await self.config.enabled_guilds()

            # Iterate over each guild where the course manager is enabled.
            for guild in self.bot.guilds:
                if guild.id not in enabled_guilds:
                    continue

                base_category: str = await self.config.course_category()
                # For each category matching the course prefix...
                for category in get_categories_by_prefix(guild, base_category):
                    for channel in category.channels:
                        if not isinstance(channel, discord.TextChannel):
                            continue

                        await self.channel_prune_helper(guild, channel, prune_threshold)

            log.debug(
                f"Auto-channel-prune cycle complete. Sleeping for {prune_interval} seconds."
            )
            await asyncio.sleep(prune_interval)
