"""
Module for managing channel operations and dynamic grouping of course channels.

This module implements the ChannelService class that handles channel creation,
deletion, permission updates, dynamic grouping of course channels, and auto-pruning
of inactive channels. Error handling uses detailed exception logging for better
diagnosis.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from itertools import combinations
from typing import Any, Dict, List, Optional, Tuple

import discord
import networkx as nx
import community as community_louvain
from redbot.core import Config
from redbot.core.utils.chat_formatting import error

log = logging.getLogger("red.channel_service")
log.setLevel(logging.DEBUG)
if not log.handlers:
    log.addHandler(logging.StreamHandler())


class ChannelService:
    """
    Manages channel operations and dynamic grouping of course channels.

    Attributes:
        bot (discord.Client): The Discord bot client instance.
        config (Config): The Redbot config instance for storing and retrieving settings.
    """

    def __init__(self, bot: discord.Client, config: Config) -> None:
        """
        Initialize ChannelService.

        Args:
            bot (discord.Client): The Discord client instance (bot).
            config (Config): The Redbot config instance.
        """
        self.bot: discord.Client = bot
        self.config: Config = config

    async def set_default_category(
        self, ctx: discord.ext.commands.Context, category_name: str
    ) -> None:
        """
        Set the default category name in the bot's config.

        Args:
            ctx (discord.ext.commands.Context): The command invocation context.
            category_name (str): The name of the default category to set.
        """
        await self.config.default_category.set(category_name)
        log.debug(f"Default category set to {category_name}")

    async def create_channel(
        self,
        ctx: discord.ext.commands.Context,
        channel_name: str,
        category: Optional[discord.CategoryChannel] = None,
    ) -> None:
        """
        Create a new text channel in a specified or default category.

        Args:
            ctx (discord.ext.commands.Context): The command invocation context.
            channel_name (str): The name of the channel to create.
            category (Optional[discord.CategoryChannel], optional): The category in which to create the channel.
                If not provided, the configured default category is used.
        """
        guild: discord.Guild = ctx.guild
        if category is None:
            default_cat_name: str = await self.config.default_category()
            category = discord.utils.get(guild.categories, name=default_cat_name)

        if category is None:
            try:
                category = await guild.create_category(default_cat_name)
                log.debug(f"Created default category: {default_cat_name}")
            except discord.Forbidden:
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
        """
        Delete a specified text channel.

        Args:
            ctx (discord.ext.commands.Context): The command invocation context.
            channel (discord.TextChannel): The channel to delete.
        """
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
        """
        List text channels in a specified category or across the server if none is provided.

        Args:
            ctx (discord.ext.commands.Context): The command invocation context.
            category (Optional[discord.CategoryChannel], optional): The category to list channels from.
                If not provided, lists all text channels in the guild.
        """
        guild: discord.Guild = ctx.guild
        if category:
            channels = category.channels
            title = f"Channels in category **{category.name}**:"
        else:
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
        """
        Grant or remove read/send permissions for a member in a given channel.

        Args:
            ctx (discord.ext.commands.Context): The command invocation context.
            channel (discord.TextChannel): The channel where permissions will be set.
            member (discord.Member): The member whose permissions will be updated.
            allow (bool): Whether to grant or remove permissions.
        """
        try:
            overwrite = (
                discord.PermissionOverwrite(read_messages=allow, send_messages=allow)
                if allow
                else discord.PermissionOverwrite(read_messages=False)
            )
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
        """
        Background task that periodically computes dynamic groupings of course channels.

        It waits for the bot to be ready, then runs until the bot is closed, pausing
        for a configured interval between computations.
        """
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
        """
        Compute natural course groupings based on enrollment overlap and reassign channels accordingly.

        The logic creates a graph of course channels, adds edges where courses co-occur,
        and then uses a community detection algorithm to group channels together.
        """
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

            # Create and populate a graph with course co-occurrence edges
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
                # Fallback: assign each channel its own group if no edges
                partition = {
                    node: str(int(node)) if node.isdigit() else node
                    for node in G.nodes()
                }

            # Convert partition structure into {community_id -> [courses...]}
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
        """
        Compute the co-occurrence frequency of course pairs.

        Args:
            enrollments (Dict[int, List[str]]): Mapping of channel IDs to a list of courses in each channel.

        Returns:
            Dict[Tuple[str, str], int]: A dictionary where the key is a tuple of two courses and
            the value is how many channels they co-occur in.
        """
        edge_counter: Dict[Tuple[str, str], int] = {}
        for courses in enrollments.values():
            for course_pair in combinations(sorted(set(courses)), 2):
                edge_counter[course_pair] = edge_counter.get(course_pair, 0) + 1
        return edge_counter

    async def _gather_enrollments(
        self, guild: discord.Guild, base: str
    ) -> Dict[int, List[str]]:
        """
        Gather user enrollments by examining the most recent message in each course channel.

        Args:
            guild (discord.Guild): The guild in which to gather enrollments.
            base (str): The base name that identifies course categories.

        Returns:
            Dict[int, List[str]]: A mapping of channel IDs to a list of uppercase course names.
        """
        enrollments: Dict[int, List[str]] = {}
        course_categories = self._get_course_categories(guild, base)

        for category in course_categories:
            for channel in category.channels:
                if isinstance(channel, discord.TextChannel):
                    async for _ in channel.history(limit=1):
                        enrollments.setdefault(channel.id, []).append(
                            channel.name.upper()
                        )
                        break
        return enrollments

    def _get_course_categories(
        self, guild: discord.Guild, base: str
    ) -> List[discord.CategoryChannel]:
        """
        Retrieve all category channels that match the base name (case-insensitive).

        Args:
            guild (discord.Guild): The guild to search.
            base (str): The base string used to identify course categories.

        Returns:
            List[discord.CategoryChannel]: A list of matching category channels.
        """
        return [
            cat for cat in guild.categories if cat.name.upper().startswith(base.upper())
        ]

    def _compute_target_mapping(
        self, communities: Dict[str, List[str]], base: str
    ) -> Dict[str, str]:
        """
        Compute a mapping of course codes to target category names.

        Args:
            communities (Dict[str, List[str]]): Mapping of community IDs to lists of course codes.
            base (str): The base category name.

        Returns:
            Dict[str, str]: Mapping of uppercase course codes to the target category name.
        """
        target_mapping: Dict[str, str] = {}
        for community_id, courses in communities.items():
            target_category = f"{base}-{community_id}" if community_id != "0" else base
            for course in courses:
                target_mapping[course.upper()] = target_category
        return target_mapping

    async def _assign_channels_to_categories(
        self, guild: discord.Guild, target_mapping: Dict[str, str], base: str
    ) -> None:
        """
        Move each existing course channel into its target category.

        Args:
            guild (discord.Guild): The guild where channels are located.
            target_mapping (Dict[str, str]): Mapping of uppercase course codes to target category names.
            base (str): The base category name used to identify course categories.
        """

        async def get_or_create_category(
            g: discord.Guild, category_name: str
        ) -> Optional[discord.CategoryChannel]:
            cat = discord.utils.get(g.categories, name=category_name)
            if cat is None:
                try:
                    cat = await g.create_category(category_name)
                    log.debug(f"Created category {category_name} in guild {g.name}")
                except discord.Forbidden:
                    log.error(
                        f"No permission to create category {category_name} in guild {g.name}"
                    )
                    return None
            return cat

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
                                    f"No permission to move channel {channel.name} "
                                    f"in guild {guild.name}"
                                )

    async def auto_prune_task(self) -> None:
        """
        Background task to auto-prune inactive course channels.

        Uses a configured inactivity threshold in days. Only processes
        guilds that have the Course Manager enabled. Runs on a monthly interval.
        """
        prune_threshold_days: int = await self.config.prune_threshold_days()
        PRUNE_THRESHOLD = timedelta(days=prune_threshold_days)
        PRUNE_INTERVAL = 2628000  # ~1 month in seconds

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
                            pruned = await self._prune_channel(
                                channel,
                                PRUNE_THRESHOLD,
                                reason="Auto-pruned due to inactivity.",
                            )
                            if pruned:
                                log.debug(
                                    f"Channel {channel.name} in guild {guild.name} "
                                    f"pruned during auto-prune cycle"
                                )

            log.debug(
                f"Auto-prune cycle complete. Sleeping for {PRUNE_INTERVAL} seconds."
            )
            await asyncio.sleep(PRUNE_INTERVAL)

    async def _prune_channel(
        self, channel: discord.TextChannel, threshold: timedelta, reason: str
    ) -> bool:
        """
        Prune (delete) a channel if its last user activity exceeds a given threshold.

        Args:
            channel (discord.TextChannel): The channel to evaluate.
            threshold (timedelta): The maximum allowed inactivity duration.
            reason (str): Reason provided for the channel deletion.

        Returns:
            bool: True if the channel was pruned, otherwise False.
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
                    f"Pruning channel '{channel.name}' in guild '{channel.guild.name}' "
                    f"(last activity: {last_activity})"
                )
                await channel.delete(reason=reason)
                return True
        except Exception:
            log.exception(
                f"Error pruning channel '{channel.name}' in guild '{channel.guild.name}'"
            )
        return False
