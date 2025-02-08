"""Channel Manager Cog for Redbot.

This cog handles Discord channel management tasks such as creating, deleting,
listing, and pruning channels, as well as managing channel permissions.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import discord
from redbot.core import commands, Config
from redbot.core.utils.chat_formatting import error, info, success, warning

log = logging.getLogger("red.channel_manager")
log.setLevel(logging.DEBUG)
if not log.handlers:
    log.addHandler(logging.StreamHandler())

DEFAULT_GLOBAL = {
    "default_category": "CHANNELS",
    "prune_threshold_days": 30,
}


class ChannelManager(commands.Cog):
    """
    Cog for managing Discord channels.

    Features:
      • Set a default category for channel creation.
      • Create new text channels.
      • Delete text channels.
      • List channels in a given category or server-wide.
      • Prune inactive channels using a configurable threshold.
      • Set or remove channel permissions for members.
    """

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.config: Config = Config.get_conf(
            self, identifier=987654321, force_registration=True
        )
        self.config.register_global(**DEFAULT_GLOBAL)
        log.debug("ChannelManager initialized.")

    @commands.command()
    async def setdefaultcategory(
        self, ctx: commands.Context, *, category_name: str
    ) -> None:
        """
        Set the default category name for channel creation.
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
        """
        Create a new text channel in the specified category or the default category.
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
        """
        Delete the specified text channel.
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
        """
        List text channels in the specified category, or across the server if none is provided.
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
        """
        Prune channels that have been inactive for a specified number of days.
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
            except Exception:
                pass
            if now - last_message_time > threshold:
                try:
                    await channel.delete(reason="Pruned due to inactivity.")
                    pruned.append(channel.name)
                except Exception:
                    pass
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
        """
        Set or remove permissions for a member in a specified channel.
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


def setup(bot: commands.Bot) -> None:
    bot.add_cog(ChannelManager(bot))
