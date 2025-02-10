"""Channel Manager Cog for Redbot.

This cog handles Discord channel management tasks such as creating, deleting,
listing, and pruning channels, as well as managing channel permissions.
It has been extended to include dynamic grouping of course channels into
multiple categories (e.g. COURSES, COURSES-1, COURSES-2, etc.) based on
student enrollments and natural course co-occurrence.
"""

import asyncio
import itertools
import logging
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

import discord
import networkx as nx
import community as community_louvain  # Requires the python-louvain package
from redbot.core import commands, Config
from redbot.core.utils.chat_formatting import error, info, success, warning

log = logging.getLogger("red.channel_manager")
log.setLevel(logging.DEBUG)
if not log.handlers:
    log.addHandler(logging.StreamHandler())

DEFAULT_GLOBAL: Dict[str, object] = {
    "default_category": "CHANNELS",
    "prune_threshold_days": 30,
    "grouping_threshold": 2,  # Minimum co-occurrence count to add an edge.
    "grouping_interval": 3600,  # Refresh groupings every hour.
    "course_groups": {},  # Stores the computed natural groupings per guild.
    "course_category": "COURSES",  # Base name for course categories.
}


class ChannelManager(commands.Cog):
    """
    Cog for managing Discord channels.

    Features:
      • Set a default category for channel creation.
      • Create and delete channels.
      • List channels and prune inactive channels.
      • Manage channel permissions.
      • **Dynamic Grouping:** Periodically reassign course channels into multiple
        categories based on natural groupings computed from user enrollments.
    """

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.config: Config = Config.get_conf(
            self, identifier=987654321, force_registration=True
        )
        self.config.register_global(**DEFAULT_GLOBAL)
        log.debug("ChannelManager initialized.")

        # Start the background dynamic grouping task.
        self._grouping_task = self.bot.loop.create_task(self._dynamic_grouping_task())

    def cog_unload(self) -> None:
        """Cancel background tasks when the cog unloads."""
        log.debug("Unloading ChannelManager cog; cancelling grouping task.")
        self._grouping_task.cancel()

    @commands.command()
    async def setdefaultcategory(
        self, ctx: commands.Context, *, category_name: str
    ) -> None:
        """Set the default category name for channel creation.
        Example: !setdefaultcategory MyChannels
        """
        await self.config.default_category.set(category_name)
        await ctx.send(success(f"Default category set to **{category_name}**"))

    @commands.command()
    async def createchannel(
        self,
        ctx: commands.Context,
        channel_name: str,
        category: Optional[discord.CategoryChannel] = None,
    ) -> None:
        """Create a new text channel in the specified category or the default category.
        Example: !createchannel my-new-channel
        """
        if category is None:
            default_cat_name: str = await self.config.default_category()
            category = discord.utils.get(ctx.guild.categories, name=default_cat_name)
        if category is None:
            try:
                category = await ctx.guild.create_category(default_cat_name)
                log.debug(f"Created default category: {default_cat_name}")
            except discord.Forbidden:
                await ctx.send(
                    error("I do not have permission to create the default category.")
                )
                return
        try:
            channel = await ctx.guild.create_text_channel(
                channel_name, category=category
            )
            await ctx.send(
                success(
                    f"Channel {channel.mention} created in category **{category.name}**."
                )
            )
        except discord.Forbidden:
            await ctx.send(
                error("I do not have permission to create a channel in that category.")
            )

    @commands.command()
    async def deletechannel(
        self, ctx: commands.Context, channel: discord.TextChannel
    ) -> None:
        """Delete the specified text channel.
        Example: !deletechannel #my-old-channel
        """
        try:
            await channel.delete()
            await ctx.send(success(f"Channel **{channel.name}** deleted."))
        except discord.Forbidden:
            await ctx.send(error("I do not have permission to delete that channel."))

    @commands.command()
    async def listchannels(
        self,
        ctx: commands.Context,
        *,
        category: Optional[discord.CategoryChannel] = None,
    ) -> None:
        """List text channels in the specified category, or across the server if none is provided.
        Example: !listchannels or !listchannels MyCategory
        """
        if category:
            channels = category.channels
            title = f"Channels in category **{category.name}**:"
        else:
            channels = ctx.guild.text_channels
            title = "Text channels in this server:"
        if channels:
            channel_list = "\n".join(channel.name for channel in channels)
            await ctx.send(f"**{title}**\n{channel_list}")
        else:
            await ctx.send(info("No channels found."))

    async def _prune_channel(
        self, channel: discord.TextChannel, threshold: timedelta, reason: str
    ) -> bool:
        log.debug(f"Starting to check channel {channel.name} for pruning")
        try:
            last_user_message: Optional[discord.Message] = None
            log.debug(f"Fetching message history for channel {channel.name}")
            async for msg in channel.history(limit=10):
                if not msg.author.bot:
                    last_user_message = msg
                    log.debug(
                        f"Found last user message in channel {channel.name} at {last_user_message.created_at}"
                    )
                    break
            last_activity = (
                last_user_message.created_at
                if last_user_message
                else channel.created_at
            )
            if not last_user_message:
                log.debug(
                    f"No user messages found in channel {channel.name}; using channel creation time: {channel.created_at}"
                )
            inactivity_duration = datetime.now(timezone.utc) - last_activity
            log.debug(
                f"Channel {channel.name} inactivity duration: {inactivity_duration}"
            )
            if inactivity_duration > threshold:
                log.info(
                    f"Pruning channel '{channel.name}' in guild '{channel.guild.name}' (last activity: {last_activity}, inactivity duration: {inactivity_duration})"
                )
                await channel.delete(reason=reason)
                log.debug(f"Channel {channel.name} successfully pruned")
                return True
            else:
                log.debug(
                    f"Channel {channel.name} is active (inactivity duration {inactivity_duration} is within threshold {threshold})"
                )
        except Exception as e:
            log.error(
                f"Error pruning channel '{channel.name}' in guild '{channel.guild.name}': {e}"
            )
        return False

    @commands.command()
    async def setchannelperm(
        self,
        ctx: commands.Context,
        channel: discord.TextChannel,
        member: discord.Member,
        allow: bool,
    ) -> None:
        """Set or remove permissions for a member in a specified channel.
        Usage: !setchannelperm #channel @member True/False
        Example: !setchannelperm #general @User True
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
                success(
                    f"Permissions for {member.mention} {action} in {channel.mention}."
                )
            )
        except discord.Forbidden:
            await ctx.send(
                error("I do not have permission to manage channel permissions.")
            )

    async def _dynamic_grouping_task(self) -> None:
        """Background task that periodically recomputes natural course groupings,
        then reassigns course channels into appropriately named categories.
        """
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                await self.compute_course_groupings()
            except asyncio.CancelledError:
                log.info("Dynamic grouping task cancelled.")
                break
            except Exception as e:
                log.error(f"Error computing course groupings: {e}")
            interval: int = await self.config.grouping_interval()
            await asyncio.sleep(interval)

    async def compute_course_groupings(self) -> None:
        """
        Computes natural course groupings for each guild based on user enrollments,
        then reassigns course channels to categories so that no category exceeds 50 channels.
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
            G = nx.Graph()
            G.add_nodes_from(courses_set)
            grouping_threshold: int = await self.config.grouping_threshold()
            for (course1, course2), weight in edge_counts.items():
                if weight >= grouping_threshold:
                    G.add_edge(course1, course2, weight=weight)

            partition: Dict[str, int] = (
                community_louvain.best_partition(G, weight="weight")
                if G.number_of_edges() > 0
                else {node: node for node in G.nodes()}
            )
            communities: Dict[str, List[str]] = {}
            for course, community_id in partition.items():
                communities.setdefault(str(community_id), []).append(course)
            new_groupings[guild.id] = communities

            target_mapping = self._compute_target_mapping(communities, base)
            await self._assign_channels_to_categories(guild, target_mapping, base)

            log.debug(f"Guild {guild.id}: target mapping: {target_mapping}")

        await self.config.course_groups.set(new_groupings)
        log.debug(f"Updated course groups: {new_groupings}")

    async def _assign_channels_to_categories(
        self, guild: discord.Guild, target_mapping: Dict[str, str], base: str
    ) -> None:
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
                                    f"No permission to move channel {channel.name} in guild {guild.name}"
                                )


def setup(bot: commands.Bot) -> None:
    bot.add_cog(ChannelManager(bot))
