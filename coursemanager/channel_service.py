import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import discord
from redbot.core import Config, commands
from redbot.core.utils.chat_formatting import error, success

from .logger_util import get_logger
from .utils import get_categories_by_prefix, get_or_create_category, utcnow

log = get_logger("red.channel_service")


class ChannelService:
    def __init__(self, bot: commands.Bot, config: Config) -> None:
        self.bot: commands.Bot = bot
        self.config: Config = config

    async def set_default_category(
        self, ctx: commands.Context, category_name: str
    ) -> None:
        log.debug(f"Attempting to set default category to {category_name}")
        try:
            await self.config.default_category.set(category_name)
            log.debug(f"Default category set to {category_name}")
        except Exception as exc:
            log.exception(f"Error setting default category to {category_name}: {exc}")
            await ctx.send(error("Unable to set the default category."))

    async def create_channel(
        self,
        ctx: commands.Context,
        channel_name: str,
        category: Optional[discord.CategoryChannel] = None,
    ) -> None:
        guild: discord.Guild = ctx.guild
        log.debug(
            f"Starting channel creation for '{channel_name}' in guild '{guild.id}'"
        )
        if category is None:
            try:
                default_cat_name: str = await self.config.default_category()
                category = await get_or_create_category(guild, default_cat_name)
            except Exception as exc:
                log.exception(
                    f"Error retrieving default category for guild '{guild.id}': {exc}"
                )
                await ctx.send(error("Unable to retrieve the default category."))
                return
        if category is None:
            await ctx.send(
                error("Insufficient permissions to create the default category.")
            )
            return
        try:
            channel = await guild.create_text_channel(channel_name, category=category)
            log.debug(f"Channel '{channel.name}' created in category '{category.name}'")
            await ctx.send(
                success(
                    f"Channel {channel.mention} created in category **{category.name}**."
                )
            )
        except discord.Forbidden as exc:
            log.exception(
                f"Permission error while creating channel '{channel_name}' in guild '{guild.id}': {exc}"
            )
            await ctx.send(
                error("Insufficient permissions to create a channel in that category.")
            )
        except Exception as exc:
            log.exception(
                f"Unexpected error while creating channel '{channel_name}' in guild '{guild.id}': {exc}"
            )
            await ctx.send(
                error("An unexpected error occurred during channel creation.")
            )

    async def channel_prune_helper(
        self,
        guild: discord.Guild,
        channel: discord.TextChannel,
        prune_threshold: timedelta,
    ) -> None:
        now = utcnow()
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
                f"Pruning channel '{channel.name}' in guild '{guild.name}' (ID: {guild.id}). "
                f"Inactive for {inactivity_duration} (threshold: {prune_threshold})."
            )
            try:
                await channel.delete(reason="Auto-pruned due to inactivity.")
                log.debug(f"Channel '{channel.name}' pruned successfully.")
            except discord.Forbidden as exc:
                log.exception(
                    f"Permission error while deleting channel '{channel.name}' in guild '{guild.id}': {exc}"
                )
            except Exception as exc:
                log.exception(
                    f"Failed to delete channel '{channel.name}' in guild '{guild.id}': {exc}"
                )

    async def auto_channel_prune(self) -> None:
        prune_threshold_days: int = await self.config.prune_threshold_days()
        prune_threshold: timedelta = timedelta(days=prune_threshold_days)
        prune_interval: int = await self.config.channel_prune_interval()
        await self.bot.wait_until_ready()
        log.debug("Auto-channel-prune task started.")
        iteration = 1
        try:
            while not self.bot.is_closed():
                current_time = utcnow()
                log.debug(
                    f"Auto-channel-prune cycle {iteration} started at {current_time}"
                )
                enabled_guilds: List[int] = await self.config.enabled_guilds()
                for guild in self.bot.guilds:
                    if guild.id not in enabled_guilds:
                        continue
                    base_category: str = await self.config.course_category()
                    for category in get_categories_by_prefix(guild, base_category):
                        for channel in filter(
                            lambda ch: isinstance(ch, discord.TextChannel),
                            category.channels,
                        ):
                            try:
                                await self.channel_prune_helper(
                                    guild, channel, prune_threshold
                                )
                            except Exception as exc:
                                log.exception(
                                    f"Error pruning channel '{channel.name}' in guild '{guild.id}': {exc}"
                                )
                log.debug(
                    f"Auto-channel-prune cycle {iteration} complete. Sleeping for {prune_interval} seconds."
                )
                iteration += 1
                await asyncio.sleep(prune_interval)
        except asyncio.CancelledError:
            log.debug("Auto-channel-prune task cancelled.")
            raise
        except Exception as exc:
            log.exception(f"Unexpected error in auto-channel-prune task: {exc}")

    async def apply_category_mapping(
        self, guild: discord.Guild, mapping: Dict[int, str]
    ) -> None:
        """
        Move existing course channels into the categories defined by `mapping`.
        mapping: course_id → category_name (e.g. 101 → "COURSES-2").
        """
        # Base prefix to discover existing course categories
        base_prefix: str = await self.config.course_category()

        # Iterate all current course categories
        for category in get_categories_by_prefix(guild, base_prefix):
            for channel in category.channels:
                if not isinstance(channel, discord.TextChannel):
                    continue
                # Extract numeric course_id from channel name: 'cse-101' → 101
                try:
                    course_id = int(channel.name.split("-")[-1])
                except ValueError:
                    continue
                target_category = mapping.get(course_id)
                # Skip if no mapping or already in correct category
                if not target_category or category.name == target_category:
                    continue
                # Ensure the target category exists (creates it if needed)
                new_cat = await get_or_create_category(guild, target_category)
                if new_cat:
                    await channel.edit(category=new_cat)
                    log.info(f"Moved '{channel.name}' → '{target_category}'")
