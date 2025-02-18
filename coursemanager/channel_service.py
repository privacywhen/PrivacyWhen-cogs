import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import discord
from redbot.core import Config, commands
from redbot.core.utils.chat_formatting import error, pagify, success
from redbot.core.utils.menus import menu
from .logger_util import get_logger, log_entry_exit
from .utils import get_categories_by_prefix, get_or_create_category

log = get_logger("red.channel_service")


class ChannelService:
    def __init__(self, bot: commands.Bot, config: Config) -> None:
        self.bot: commands.Bot = bot
        self.config: Config = config

    async def set_default_category(
        self, ctx: commands.Context, category_name: str
    ) -> None:
        await self.config.default_category.set(category_name)
        log.debug(f"Default category set to {category_name}")

    async def create_channel(
        self,
        ctx: commands.Context,
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
                success(
                    f"Channel {channel.mention} created in category **{category.name}**."
                )
            )
        except discord.Forbidden as e:
            log.exception(
                f"Permission error while creating channel '{channel_name}': {e}"
            )
            await ctx.send(
                error("I do not have permission to create a channel in that category.")
            )
        except Exception as e:
            log.exception(
                f"Unexpected error while creating channel '{channel_name}': {e}"
            )
            await ctx.send(
                error("An unexpected error occurred while creating the channel.")
            )

    async def channel_prune_helper(
        self,
        guild: discord.Guild,
        channel: discord.TextChannel,
        prune_threshold: timedelta,
    ) -> None:
        now: datetime = datetime.now(timezone.utc)
        last_activity: Optional[datetime] = None
        if (last_msg := channel.last_message) and (not last_msg.author.bot):
            last_activity = last_msg.created_at
            log.debug(f"Using channel.last_message for {channel.name}: {last_activity}")
        else:
            prune_history_limit: int = await self.config.channel_prune_history_limit()
            async for message in channel.history(limit=prune_history_limit):
                if not message.author.bot:
                    last_activity = message.created_at
                    log.debug(
                        f"Found non-bot message in {channel.name} at {last_activity}"
                    )
                    break
        if last_activity is None:
            last_activity = channel.created_at
            log.debug(
                f"No non-bot messages found in {channel.name}. Using channel.created_at: {last_activity}"
            )
        inactivity_duration: timedelta = now - last_activity
        log.debug(
            f"Channel '{channel.name}' inactivity duration: {inactivity_duration}"
        )
        if inactivity_duration > prune_threshold:
            log.info(
                f"Pruning channel '{channel.name}' in guild '{guild.name}'. Inactive for {inactivity_duration} (threshold: {prune_threshold})."
            )
            try:
                await channel.delete(reason="Auto-pruned due to inactivity.")
            except Exception as e:
                log.exception(
                    f"Failed to delete channel '{channel.name}' in guild '{guild.name}': {e}"
                )

    async def auto_channel_prune(self) -> None:
        prune_threshold_days: int = await self.config.prune_threshold_days()
        prune_threshold: timedelta = timedelta(days=prune_threshold_days)
        prune_interval: int = await self.config.channel_prune_interval()
        await self.bot.wait_until_ready()
        log.debug("Auto-channel-prune task started.")
        try:
            while not self.bot.is_closed():
                current_time = datetime.now(timezone.utc)
                log.debug(f"Auto-channel-prune cycle started at {current_time}")
                enabled_guilds: List[int] = await self.config.enabled_guilds()
                for guild in self.bot.guilds:
                    if guild.id not in enabled_guilds:
                        continue
                    base_category: str = await self.config.course_category()
                    for category in get_categories_by_prefix(guild, base_category):
                        for channel in category.channels:
                            if not isinstance(channel, discord.TextChannel):
                                continue
                            try:
                                await self.channel_prune_helper(
                                    guild, channel, prune_threshold
                                )
                            except Exception as e:
                                log.exception(
                                    f"Error pruning channel '{channel.name}' in guild '{guild.name}': {e}"
                                )
                log.debug(
                    f"Auto-channel-prune cycle complete. Sleeping for {prune_interval} seconds."
                )
                await asyncio.sleep(prune_interval)
        except asyncio.CancelledError:
            log.debug("Auto-channel-prune task cancelled.")
            raise
        except Exception as e:
            log.exception(f"Unexpected error in auto-channel-prune task: {e}")
