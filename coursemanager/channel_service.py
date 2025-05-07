from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Iterable, Sequence

import discord
from redbot.core import Config, commands  # noqa: TC002
from redbot.core.utils.chat_formatting import error, success

from .constants import RATE_LIMIT_DELAY
from .course_code import CourseCode
from .logger_util import get_logger
from .utils import get_categories_by_prefix, get_or_create_category, nat_key, utcnow

log = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Module-level helpers
# --------------------------------------------------------------------------- #
async def _safe_sleep(seconds: float) -> None:
    """Wrap asyncio.sleep to simplify testing."""
    if seconds > 0:
        await asyncio.sleep(seconds)


def _iter_course_text_channels(
    guild: discord.Guild,
    prefix: str,
) -> Iterable[discord.TextChannel]:
    """Yield all text channels in categories whose name starts with *prefix*."""
    for category in get_categories_by_prefix(guild, prefix):
        for channel in category.channels:
            if isinstance(channel, discord.TextChannel):
                yield channel


class ChannelService:
    """Manage creation, sorting, and pruning of course channels/categories."""

    RATE_LIMIT_DELAY: float = RATE_LIMIT_DELAY

    # --------------------------------------------------------------------- #
    # Construction
    # --------------------------------------------------------------------- #
    def __init__(self, bot: commands.Bot, config: Config) -> None:
        self.bot: commands.Bot = bot
        self.config: Config = config

    # --------------------------------------------------------------------- #
    # Time hook (for testability)
    # --------------------------------------------------------------------- #
    def _now(self) -> datetime:
        """Return current UTC datetime (hookable in tests)."""
        return utcnow()

    # --------------------------------------------------------------------- #
    # Category resolution
    # --------------------------------------------------------------------- #
    async def _resolve_default_category(
        self,
        guild: discord.Guild,
    ) -> discord.CategoryChannel | None:
        """Fetch or create the guild's default category.

        Returns None on failure or missing permissions.
        """
        try:
            name: str = await self.config.default_category()
            return await get_or_create_category(guild, name)
        except Exception:
            log.exception("Unable to resolve default category for guild %s", guild.id)
            return None

    # --------------------------------------------------------------------- #
    # Public commands
    # --------------------------------------------------------------------- #
    async def set_default_category(
        self,
        ctx: commands.Context,
        category_name: str,
    ) -> None:
        """Persist *category_name* as the default for new channels."""
        log.debug("Setting default category → %s", category_name)
        try:
            await self.config.default_category.set(category_name)
        except Exception:
            log.exception("Failed to set default category")
            await ctx.send(error("Unable to set the default category."))
        else:
            await ctx.send(success(f"Default category set to **{category_name}**."))

    async def create_channel(
        self,
        ctx: commands.Context,
        channel_name: str,
        category: discord.CategoryChannel | None = None,
    ) -> None:
        """Create a text channel named *channel_name*, optionally under *category*."""
        guild = ctx.guild
        if guild is None:
            await ctx.send(error("This command must be used in a server."))
            return

        log.debug("Begin channel creation '%s' in guild %s", channel_name, guild.id)

        if category is None:
            category = await self._resolve_default_category(guild)
        if category is None:
            await ctx.send(error("Cannot retrieve or create the default category."))
            return

        try:
            channel = await guild.create_text_channel(
                channel_name,
                category=category,
            )
        except discord.Forbidden:
            log.exception("Forbidden creating text channel '%s'", channel_name)
            await ctx.send(error("Insufficient permissions to create that channel."))
        except Exception:
            log.exception("Unexpected error creating text channel '%s'", channel_name)
            await ctx.send(error("An unexpected error occurred during creation."))
        else:
            log.info(
                "Channel '%s' created in category '%s'",
                channel.name,
                category.name,
            )
            await ctx.send(
                success(
                    f"Channel {channel.mention} created in category **{category.name}**.",
                ),
            )

    # --------------------------------------------------------------------- #
    # Prune helpers
    # --------------------------------------------------------------------- #
    async def _compute_last_activity(
        self,
        channel: discord.TextChannel,
    ) -> datetime:
        """Return timestamp of last non-bot message or channel creation time."""
        if (msg := channel.last_message) and not msg.author.bot:
            return msg.created_at

        limit = await self.config.channel_prune_history_limit()
        async for message in channel.history(limit=limit):
            if not message.author.bot:
                return message.created_at

        return channel.created_at

    async def channel_prune_helper(
        self,
        guild: discord.Guild,
        channel: discord.TextChannel,
        prune_threshold: timedelta,
    ) -> None:
        """Delete *channel* if inactivity (since last student message) exceeds *prune_threshold*."""
        now = self._now()
        last_activity = await self._compute_last_activity(channel)
        inactivity = now - last_activity

        if inactivity <= prune_threshold:
            return

        log.info(
            "Pruning '%s' in guild %s (inactive %s > %s)",
            channel.name,
            guild.id,
            inactivity,
            prune_threshold,
        )
        try:
            await channel.delete(reason="Auto-pruned due to inactivity.")
        except discord.Forbidden:
            log.exception(
                "Forbidden deleting channel '%s' in guild %s",
                channel.name,
                guild.id,
            )
        except Exception:
            log.exception(
                "Failed deleting channel '%s' in guild %s",
                channel.name,
                guild.id,
            )

    async def auto_channel_prune(self) -> None:
        """Background task: periodically prune inactive course channels."""
        prune_threshold = timedelta(days=await self.config.prune_threshold_days())
        prune_interval = await self.config.channel_prune_interval()

        await self.bot.wait_until_ready()
        log.debug("Auto-prune task started (interval %s s)", prune_interval)

        iteration = 1
        try:
            while not self.bot.is_closed():
                enabled = set(await self.config.enabled_guilds())
                log.debug("Prune cycle #%d - enabled guilds: %s", iteration, enabled)

                for guild in self.bot.guilds:
                    if guild.id not in enabled:
                        continue
                    base_prefix = await self.config.course_category()
                    for channel in _iter_course_text_channels(guild, base_prefix):
                        # Exceptions are handled inside channel_prune_helper
                        await self.channel_prune_helper(guild, channel, prune_threshold)

                iteration += 1
                log.debug("Prune cycle complete; sleeping %s s", prune_interval)
                await _safe_sleep(prune_interval)
        except asyncio.CancelledError:
            log.debug("Auto-prune task cancelled")
            raise
        except Exception:
            log.exception("Unexpected error in auto-prune loop")

    # --------------------------------------------------------------------- #
    # Category mapping & ordering
    # --------------------------------------------------------------------- #
    async def apply_category_mapping(
        self,
        guild: discord.Guild,
        mapping: dict[str, str],
    ) -> None:
        """Move course text channels into the categories defined by *mapping*.

        Keys are canonical course codes.
        """
        base_prefix = await self.config.course_category()

        # 1. Move each channel into its mapped category (creating it if needed)
        for channel in _iter_course_text_channels(guild, base_prefix):
            try:
                code = CourseCode(channel.name).canonical()
            except ValueError:
                continue

            target = mapping.get(code)
            current_cat = channel.category.name if channel.category else None
            if not target or current_cat == target:
                continue

            new_cat = await get_or_create_category(guild, target)
            if new_cat:
                await channel.edit(category=new_cat)
                log.info("Moved '%s' → '%s'", channel.name, target)
                await _safe_sleep(self.RATE_LIMIT_DELAY)

        # 2. Clean up any empty, unmapped categories
        desired = set(mapping.values())
        for cat in get_categories_by_prefix(guild, base_prefix):
            # If this category is not in our current mapping and has no text channels, delete it
            if cat.name not in desired and not any(
                isinstance(ch, discord.TextChannel) for ch in cat.channels
            ):
                try:
                    await cat.delete(reason="Cleaning up stale course category")
                    log.info("Deleted stale empty category '%s'", cat.name)
                    await _safe_sleep(self.RATE_LIMIT_DELAY)
                except discord.Forbidden:
                    log.warning("No permission to delete stale category '%s'", cat.name)
                except Exception:
                    log.exception("Failed to delete stale category '%s'", cat.name)

    async def _reorder_course_categories(
        self,
        guild: discord.Guild,
        prefix: str,
    ) -> None:
        """Ensure course categories appear alphabetically after other categories."""
        non_course = [
            c for c in guild.categories if not c.name.upper().startswith(prefix)
        ]
        course_cats = sorted(
            (c for c in guild.categories if c.name.upper().startswith(prefix)),
            key=lambda c: nat_key(c.name),
        )
        desired: Sequence[discord.CategoryChannel] = (*non_course, *course_cats)
        for idx, cat in enumerate(desired):
            if cat.position != idx:
                await cat.edit(position=idx)
                await _safe_sleep(self.RATE_LIMIT_DELAY)
