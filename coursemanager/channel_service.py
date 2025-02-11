import asyncio
from datetime import datetime, timedelta, timezone
from itertools import combinations
from typing import Any, Dict, List, Optional, Tuple
import discord
import networkx as nx
import community as community_louvain
from redbot.core import Config
from redbot.core.utils.chat_formatting import error
from .utils import (
    prune_channel,
    get_categories_by_prefix,
    get_or_create_category,
    get_logger,
)

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
            await ctx.send(f"**{title}**\n{channel_list}")
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
        course_categories = self._get_course_categories(guild, base)
        for category in course_categories:
            for channel in category.channels:
                if isinstance(channel, discord.TextChannel):
                    async for msg in channel.history(limit=10):
                        if not msg.author.bot:
                            enrollments.setdefault(channel.id, []).append(
                                channel.name.upper()
                            )
                            break
        return enrollments

    def _get_course_categories(
        self, guild: discord.Guild, base: str
    ) -> List[discord.CategoryChannel]:
        return get_categories_by_prefix(guild, base)

    def _compute_target_mapping(
        self, communities: Dict[str, List[str]], base: str
    ) -> Dict[str, str]:
        target_mapping: Dict[str, str] = {}
        for community_id, courses in communities.items():
            target_category = f"{base}-{community_id}" if community_id != "0" else base
            for course in courses:
                target_mapping[course.upper()] = target_category
        return target_mapping

    async def _assign_channels_to_categories(
        self, guild: discord.Guild, target_mapping: Dict[str, str], base: str
    ) -> None:
        course_categories = self._get_course_categories(guild, base)
        for category in course_categories:
            for channel in category.channels:
                if not isinstance(channel, discord.TextChannel):
                    continue
                course_code = channel.name.upper()
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

    async def auto_prune_task(self) -> None:
        prune_threshold_days: int = await self.config.prune_threshold_days()
        PRUNE_THRESHOLD = timedelta(days=prune_threshold_days)
        PRUNE_INTERVAL = 2628000  # seconds (approx. one month)
        await self.bot.wait_until_ready()
        log.debug("Auto-prune task started.")
        while not self.bot.is_closed():
            log.debug(f"Auto-prune cycle started at {datetime.now(timezone.utc)}")
            enabled_guilds = await self.config.enabled_guilds()
            for guild in self.bot.guilds:
                if guild.id not in enabled_guilds:
                    continue
                base_category: str = await self.config.course_category()
                for category in self._get_course_categories(guild, base_category):
                    for channel in category.channels:
                        if isinstance(channel, discord.TextChannel):
                            pruned = await prune_channel(
                                channel,
                                PRUNE_THRESHOLD,
                                reason="Auto-pruned due to inactivity.",
                            )
                            if pruned:
                                log.debug(
                                    f"Channel {channel.name} in guild {guild.name} pruned during auto-prune cycle"
                                )
            log.debug(
                f"Auto-prune cycle complete. Sleeping for {PRUNE_INTERVAL} seconds."
            )
            await asyncio.sleep(PRUNE_INTERVAL)
