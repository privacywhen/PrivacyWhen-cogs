"""Manage creation, pruning, and ordering of course channels/categories."""

from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING

import discord
from redbot.core import Config, commands  # noqa: TC002
from redbot.core.utils.chat_formatting import error, success

from .constants import RATE_LIMIT_DELAY
from .course_code import CourseCode
from .logger_util import get_logger
from .utils import get_categories_by_prefix, get_or_create_category, utcnow

if TYPE_CHECKING:
    from collections.abc import Iterable

log = get_logger(__name__)

class MappingResult(Enum):
    SUCCESS = "success"
    SUCCESS_WITH_WARNINGS = "success_with_warnings"
    PARTIAL = "partial"
    FAILED = "failed"


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
    _re_num = re.compile(r"(\d+|\D+)")

    def __init__(self, bot: commands.Bot, config: Config) -> None:
        """Initialize ChannelService with a Bot instance and its Config."""
        self.bot: commands.Bot = bot
        self.config: Config = config

    def _now(self) -> datetime:
        """Return current UTC datetime (hookable in tests)."""
        return utcnow()

    async def _resolve_default_category(
        self,
        guild: discord.Guild,
    ) -> discord.CategoryChannel | None:
        """Fetch or create the guild's default category.

        Returns None on failure or missing permissions.
        """
        name: str | None = None
        try:
            name = await self.config.default_category()
            return await get_or_create_category(guild, name)
        except Exception:
            log.exception(
                "Unable to resolve default category %r for guild %s",
                name,
                guild.id,
            )
            return None

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
            await ctx.send(
                success(
                    f"Default category set to **{category_name}**.",
                ),
            )

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
        except discord.HTTPException:
            log.exception("HTTP error creating text channel '%s'", channel_name)
            await ctx.send(error("An unexpected error occurred during creation."))
        else:
            log.info(
                "Channel '%s' created in category '%s'",
                channel.name,
                category.name,
            )
            await ctx.send(
                success(
                    f"Created channel {channel.mention} in **{category.name}**.",
                ),
            )

    async def _compute_last_activity(
        self,
        channel: discord.TextChannel,
    ) -> datetime | None:
        """Return last user activity, or None when bounded history is inconclusive."""
        if (msg := channel.last_message) and not msg.author.bot:
            return msg.created_at

        limit = await self.config.channel_prune_history_limit()
        scanned = 0
        async for message in channel.history(limit=limit):
            scanned += 1
            if not message.author.bot:
                return message.created_at

        if scanned >= limit:
            return None  # Older user messages may be hidden behind bot messages.
        return channel.created_at

    async def channel_prune_helper(
        self,
        guild: discord.Guild,
        channel: discord.TextChannel,
        prune_threshold: timedelta,
    ) -> None:
        """Delete *channel* if inactivity exceeds *prune_threshold*."""
        now = self._now()
        last_activity = await self._compute_last_activity(channel)
        if last_activity is None:
            return
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
        except discord.HTTPException:
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
                    try:
                        base_prefix = await self.config.course_category()
                        for channel in _iter_course_text_channels(guild, base_prefix):
                            try:
                                await self.channel_prune_helper(guild, channel, prune_threshold)
                            except Exception:
                                log.exception("Failed to inspect channel in guild %s", guild.id)
                    except Exception:
                        log.exception("Failed to enumerate course channels in guild %s", guild.id)

                iteration += 1
                log.debug("Prune cycle complete; sleeping %s s", prune_interval)
                await _safe_sleep(prune_interval)
        except asyncio.CancelledError:
            log.debug("Auto-prune task cancelled")
            raise
        except Exception:
            log.exception("Unexpected error in auto-prune loop")

    async def _move_channels(
        self,
        guild: discord.Guild,
        mapping: dict[str, str],
    ) -> tuple[int, int]:
        """Return required and applied channel counts for the mapping."""
        base_prefix = await self.config.course_category()
        required = applied = 0
        encountered: set[str] = set()
        for channel in _iter_course_text_channels(guild, base_prefix):
            try:
                code = CourseCode(channel.name).canonical()
            except ValueError:
                continue

            target = mapping.get(code)
            if not target:
                continue
            encountered.add(code)
            required += 1
            current_cat = channel.category.name if channel.category else None
            if current_cat == target:
                applied += 1
                continue

            try:
                new_cat = await get_or_create_category(guild, target)
                if new_cat is None:
                    log.error("Cannot move channel %s in guild %s: category %s unavailable", channel.name, guild.id, target)
                    continue
                await channel.edit(category=new_cat)
                applied += 1
                log.info("Moved '%s' → '%s'", channel.name, target)
                await _safe_sleep(self.RATE_LIMIT_DELAY)
            except Exception:
                log.exception("Failed moving channel %s in guild %s to %s", channel.name, guild.id, target)
        required += len(mapping.keys() - encountered)
        return required, applied

    @staticmethod
    def _nat_key(s: str) -> list:
        return [
            int(tok) if tok.isdigit() else tok.casefold()
            for tok in ChannelService._re_num.findall(s)
        ]

    async def _sort_category_channels(self, category: discord.CategoryChannel) -> None:
        text_channels = [
            c for c in category.channels if isinstance(c, discord.TextChannel)
        ]
        sorted_chans = sorted(text_channels, key=lambda c: self._nat_key(c.name))
        for idx, chan in enumerate(sorted_chans):
            if chan.position != idx:
                await chan.edit(position=idx)
                await asyncio.sleep(self.RATE_LIMIT_DELAY)

    async def _reorder_course_categories(
        self,
        guild: discord.Guild,
        base_prefix: str,
    ) -> None:
        """Reorder course categories below non-course categories in the guild."""
        non_course = [
            c for c in guild.categories if not c.name.upper().startswith(base_prefix)
        ]
        course_cats = sorted(
            (c for c in guild.categories if c.name.upper().startswith(base_prefix)),
            key=lambda c: self._nat_key(c.name),  # Using natural sorting
        )
        desired = (*non_course, *course_cats)

        # Reorder categories to maintain proper order
        for idx, cat in enumerate(desired):
            if cat.position != idx:
                await cat.edit(position=idx)
                await _safe_sleep(self.RATE_LIMIT_DELAY)

    async def _cleanup_categories(
        self,
        guild: discord.Guild,
        mapping: dict[str, str],
    ) -> bool:
        """Delete empty obsolete categories; report whether cleanup completed."""
        base_prefix = await self.config.course_category()
        desired = set(mapping.values())
        completed = True
        for cat in get_categories_by_prefix(guild, base_prefix):
            if cat.name not in desired and not any(
                isinstance(ch, discord.TextChannel) for ch in cat.channels
            ):
                try:
                    await cat.delete(reason="Cleaning up stale course category")
                    log.info("Deleted stale empty category '%s'", cat.name)
                    await _safe_sleep(self.RATE_LIMIT_DELAY)
                except discord.Forbidden:
                    log.warning("No permission to delete stale category '%s'", cat.name)
                    completed = False
                except discord.HTTPException:
                    log.exception("Failed to delete stale category '%s'", cat.name)
                    completed = False
        return completed

    async def apply_category_mapping(
        self,
        guild: discord.Guild,
        mapping: dict[str, str],
    ) -> MappingResult:
        """Apply required moves, then best-effort cleanup and ordering."""
        try:
            required, applied = await self._move_channels(guild, mapping)
        except Exception:
            log.exception("Failed during channel move phase for guild %s", guild.id)
            return MappingResult.FAILED
        if applied < required:
            log.warning("Category mapping incomplete in guild %s (%s/%s channels applied)", guild.id, applied, required)
            return MappingResult.PARTIAL if applied else MappingResult.FAILED

        warnings = False
        try:
            warnings = not await self._cleanup_categories(guild, mapping)
        except Exception:
            log.exception("Failed during cleanup phase for guild %s", guild.id)
            warnings = True

        try:
            base_prefix = await self.config.course_category()
            await self._reorder_course_categories(guild, base_prefix)
        except Exception:
            log.exception("Failed to reorder course categories in guild %s", guild.id)
            warnings = True
        return MappingResult.SUCCESS_WITH_WARNINGS if warnings else MappingResult.SUCCESS
