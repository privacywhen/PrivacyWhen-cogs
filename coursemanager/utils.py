from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import discord
from redbot.core.utils.chat_formatting import error

from .constants import MAX_CATEGORY_CHANNELS
from .course_code import CourseCode
from .course_code_resolver import CourseCodeResolver
from .logger_util import get_logger

if TYPE_CHECKING:
    from redbot.core import commands

    from .course_data_proxy import CourseDataProxy

log = get_logger(__name__)

# Maximum number of alternate category suffixes to try
ALT_CATEGORY_LIMIT: int = 100


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def get_categories_by_prefix(
    guild: discord.Guild,
    prefix: str,
) -> list[discord.CategoryChannel]:
    matching: list[discord.CategoryChannel] = [
        cat for cat in guild.categories if cat.name.upper().startswith(prefix.upper())
    ]
    log.debug(
        "get_categories_by_prefix: Found %d categories in guild %s with prefix %s",
        len(matching),
        guild.name,
        prefix,
    )
    return matching


async def get_or_create_category(
    guild: discord.Guild,
    category_name: str,
) -> discord.CategoryChannel | None:
    category = discord.utils.get(guild.categories, name=category_name)
    if category is None:
        try:
            category = await guild.create_category(category_name)
            log.debug(
                "get_or_create_category: Created category %s in guild %s",
                category_name,
                guild.name,
            )
        except discord.Forbidden:
            log.exception(
                "get_or_create_category: No permission to create category %s in guild %s",
                category_name,
                guild.name,
            )
            return None
    else:
        log.debug(
            "get_or_create_category: Found existing category %s in guild %s",
            category_name,
            guild.name,
        )
    return category


async def get_available_course_category(
    guild: discord.Guild,
    base_name: str,
    ctx: commands.Context,
    max_channels: int = MAX_CATEGORY_CHANNELS,
) -> discord.CategoryChannel | None:
    category = discord.utils.get(guild.categories, name=base_name)
    if category is None:
        category = await get_or_create_category(guild, base_name)
    if category is None:
        await ctx.send(
            error("Insufficient permissions to create the courses category."),
        )
        return None
    if len(category.channels) < max_channels:
        return category
    for i in range(2, ALT_CATEGORY_LIMIT):
        alt_name = f"{base_name}-{i}"
        alt_category = discord.utils.get(guild.categories, name=alt_name)
        if alt_category is None:
            alt_category = await get_or_create_category(guild, alt_name)
        if alt_category is None:
            await ctx.send(
                error(f"Insufficient permissions to create the category '{alt_name}'."),
            )
            return None
        if len(alt_category.channels) < max_channels:
            return alt_category
    await ctx.send(
        error("All course categories have reached the maximum channel limit."),
    )
    return None


async def validate_and_resolve_course_code(
    ctx: commands.Context,
    raw_input: str,
    listings: dict[str, Any],
    course_data_proxy: CourseDataProxy,
) -> CourseCode | None:
    resolver = CourseCodeResolver(listings, course_data_proxy=course_data_proxy)
    try:
        course_obj: CourseCode = CourseCode(raw_input)
    except ValueError:
        log.debug(
            "Failed to parse '%s' using CourseCode. Attempting to resolve using CourseCodeResolver.",
            raw_input,
        )
        resolved, _ = await resolver.fallback_fuzzy_lookup(ctx, raw_input.strip())
        if not resolved:
            return None
        course_obj = resolved
    else:
        resolved, _ = await resolver.resolve_course_code(ctx, course_obj)
        if resolved:
            course_obj = resolved
    return course_obj


_RE_NUM = re.compile(r"(\d+|\D+)")


def nat_key(s: str) -> list[int | str]:
    """Split digits and non-digit segments for natural sorting."""
    return [int(tok) if tok.isdigit() else tok.casefold() for tok in _RE_NUM.findall(s)]
