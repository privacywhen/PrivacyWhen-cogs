"""Channel Manager Cog for Redbot.

This cog handles Discord channel management tasks such as creating, deleting,
listing, and pruning channels, as well as managing channel permissions.
It has been extended to include dynamic grouping of course channels into
multiple categories (e.g. COURSES, COURSES-1, COURSES-2, etc.) based on
student enrollments and natural course co-occurrence.
"""

import asyncio
import logging
import itertools
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

import discord
from redbot.core import commands, Config
from redbot.core.utils.chat_formatting import error, info, success, warning

# Additional imports for dynamic grouping.
import networkx as nx
import community as community_louvain  # Requires the python-louvain package

log = logging.getLogger("red.channel_manager")
log.setLevel(logging.DEBUG)
if not log.handlers:
    log.addHandler(logging.StreamHandler())

DEFAULT_GLOBAL = {
    "default_category": "CHANNELS",
    "prune_threshold_days": 30,
    # Keys for dynamic grouping:
    "grouping_threshold": 2,  # Minimum co-occurrence count to add an edge.
    "grouping_interval": 3600,  # Refresh groupings every 3600 seconds (1 hour).
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
        categories (COURSES, COURSES-1, etc.) based on natural groupings computed
        from user enrollments. This ensures that courses that are often co-enrolled
        appear together—and no category exceeds 50 channels.
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
                    log.debug("Created default category: %s", default_cat_name)
                except discord.Forbidden:
                    await ctx.send(
                        error(
                            "I do not have permission to create the default category."
                        )
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

    @commands.command()
    async def prunechannels(
        self, ctx: commands.Context, days: Optional[int] = None
    ) -> None:
        """Prune channels that have been inactive for a specified number of days.
        If no value is provided, the default from config is used.
        Only channels in the default category are pruned.
        Example: !prunechannels 14
        """
        if days is None:
            days = await self.config.prune_threshold_days()
        threshold = timedelta(days=days)
        default_cat_name: str = await self.config.default_category()
        category = discord.utils.get(ctx.guild.categories, name=default_cat_name)
        if category is None:
            await ctx.send(info("Default category not found."))
            return

        pruned: List[str] = []
        now = datetime.now(timezone.utc)
        for channel in category.channels:
            if not isinstance(channel, discord.TextChannel):
                continue
            last_message_time = channel.created_at
            try:
                async for msg in channel.history(limit=1):
                    last_message_time = msg.created_at
            except Exception as e:
                log.debug("Error fetching history for channel %s: %s", channel.name, e)
            if now - last_message_time > threshold:
                try:
                    await channel.delete(reason="Pruned due to inactivity.")
                    pruned.append(channel.name)
                except Exception as e:
                    log.error("Failed to prune channel %s: %s", channel.name, e)
        if pruned:
            await ctx.send(success(f"Pruned channels: {', '.join(pruned)}"))
        else:
            await ctx.send(info("No channels pruned."))

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
            if allow:
                overwrite = discord.PermissionOverwrite(
                    read_messages=True, send_messages=True
                )
                action = "granted"
            else:
                overwrite = discord.PermissionOverwrite(read_messages=False)
                action = "removed"
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

    # --- Enhanced Dynamic Grouping Functionality ---

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
                log.error("Error computing course groupings: %s", e)
            interval = await self.config.grouping_interval()
            await asyncio.sleep(interval)

    async def compute_course_groupings(self) -> None:
        """
        Computes natural course groupings for each guild based on user enrollments,
        then reassigns course channels to categories so that no category exceeds 50 channels.

        Steps:
          1. Gather enrollment data from all course channels (from all categories whose names
             start with the base course category).
          2. Compute pairwise co-occurrence counts.
          3. Build a weighted graph and apply the Louvain algorithm.
          4. For each natural community, split the courses into chunks of up to 50 and assign
             a target category name (the first chunk uses the base name; subsequent chunks use a suffix).
          5. Reassign channels to their target categories.
          6. Save the natural grouping result in config.
        """
        new_groupings: Dict[int, Dict[str, List[str]]] = {}
        base = await self.config.course_category()

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
            grouping_threshold = await self.config.grouping_threshold()
            for (course1, course2), weight in edge_counts.items():
                if weight >= grouping_threshold:
                    G.add_edge(course1, course2, weight=weight)

            if G.number_of_edges() > 0:
                partition = community_louvain.best_partition(G, weight="weight")
            else:
                partition = {node: node for node in G.nodes()}
            communities: Dict[str, List[str]] = {}
            for course, community_id in partition.items():
                communities.setdefault(str(community_id), []).append(course)
            new_groupings[guild.id] = communities

            target_mapping = self._compute_target_mapping(communities, base)
            await self._assign_channels_to_categories(guild, target_mapping, base)

            log.debug("Guild %s: target mapping: %s", guild.id, target_mapping)

        await self.config.course_groups.set(new_groupings)
        log.debug("Updated course groups: %s", new_groupings)

    async def _gather_enrollments(
        self, guild: discord.Guild, base: str
    ) -> Dict[int, Set[str]]:
        """
        Scan all course channels (from categories whose names start with the base)
        in the given guild and build a mapping from user IDs to the set of course codes.
        """
        enrollments: Dict[int, Set[str]] = {}
        course_categories = self._get_course_categories(guild, base)
        for category in course_categories:
            for channel in category.channels:
                if not isinstance(channel, discord.TextChannel):
                    continue
                course_code = channel.name.upper()
                for member in channel.members:
                    enrollments.setdefault(member.id, set()).add(course_code)
        log.debug("Guild %s: Collected enrollments: %s", guild.id, enrollments)
        return enrollments

    def _get_course_categories(
        self, guild: discord.Guild, base: str
    ) -> List[discord.CategoryChannel]:
        base_upper = base.upper()
        return [
            cat for cat in guild.categories if cat.name.upper().startswith(base_upper)
        ]

    def _compute_edge_counts(
        self, enrollments: Dict[int, Set[str]]
    ) -> Dict[Tuple[str, str], int]:
        counter = Counter()
        for courses in enrollments.values():
            sorted_courses = sorted(courses)
            for course_pair in itertools.combinations(sorted_courses, 2):
                counter[course_pair] += 1
        log.debug("Computed edge counts: %s", dict(counter))
        return dict(counter)

    def _compute_target_mapping(
        self, communities: Dict[str, List[str]], base: str
    ) -> Dict[str, str]:
        """
        Given natural communities (community id -> list of course codes), compute a mapping
        from each course code to its target category name, ensuring that no category has more
        than 50 channels.
        """
        target_mapping: Dict[str, str] = {}
        category_counter = 0
        sorted_communities = sorted(
            communities.items(), key=lambda item: len(item[1]), reverse=True
        )
        for comm_id, course_list in sorted_communities:
            course_list_sorted = sorted(course_list)
            for i in range(0, len(course_list_sorted), 50):
                chunk = course_list_sorted[i : i + 50]
                if category_counter == 0:
                    target_category = base
                else:
                    target_category = f"{base}-{category_counter}"
                for course in chunk:
                    target_mapping[course] = target_category
                category_counter += 1
        log.debug("Computed target mapping: %s", target_mapping)
        return target_mapping

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
                    log.debug("Created category %s in guild %s", category_name, g.name)
                except discord.Forbidden:
                    log.error(
                        "No permission to create category %s in guild %s",
                        category_name,
                        g.name,
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
                                    "Moved channel %s to category %s",
                                    channel.name,
                                    target_cat_name,
                                )
                            except discord.Forbidden:
                                log.error(
                                    "No permission to move channel %s in guild %s",
                                    channel.name,
                                    guild.name,
                                )

    # --- Admin Commands for Grouping Settings & Manual Refresh ---

    @commands.command()
    async def coursegroups(self, ctx: commands.Context) -> None:
        """Display the latest computed natural course groupings for this guild."""
        groups = await self.config.course_groups()
        guild_groups = groups.get(ctx.guild.id)
        if not guild_groups:
            await ctx.send(info("No course groupings available for this guild."))
            return

        msg = "**Course Groupings (Natural Communities):**\n"
        for community_id, courses in guild_groups.items():
            courses_list = ", ".join(sorted(courses))
            msg += f"**Community {community_id}:** {courses_list}\n"
        await ctx.send(msg)

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def setgroupingthreshold(self, ctx: commands.Context, threshold: int) -> None:
        """Set the minimum co-occurrence count for grouping courses together.
        Example: !setgroupingthreshold 3
        """
        await self.config.grouping_threshold.set(threshold)
        await ctx.send(success(f"Grouping threshold set to {threshold}."))

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def setgroupinginterval(self, ctx: commands.Context, interval: int) -> None:
        """Set the grouping refresh interval (in seconds).
        Example: !setgroupinginterval 1800
        """
        await self.config.grouping_interval.set(interval)
        await ctx.send(success(f"Grouping interval set to {interval} seconds."))

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def setcoursecategory(
        self, ctx: commands.Context, *, category_name: str
    ) -> None:
        """Set the base category name used for course channels.
        Example: !setcoursecategory COURSES
        """
        await self.config.course_category.set(category_name)
        await ctx.send(
            success(f"Course channel base category set to **{category_name}**.")
        )

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def refreshcoursegroups(self, ctx: commands.Context) -> None:
        """Manually trigger a refresh of course groupings and channel reassignments."""
        try:
            await self.compute_course_groupings()
            await ctx.send(
                success(
                    "Course groupings refreshed and channels reassigned successfully."
                )
            )
        except Exception as e:
            log.error("Error refreshing course groups: %s", e)
            await ctx.send(error("Failed to refresh course groupings."))


def setup(bot: commands.Bot) -> None:
    bot.add_cog(ChannelManager(bot))
