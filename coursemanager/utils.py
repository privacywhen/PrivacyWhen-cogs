from typing import Any, Dict, List, Optional

import discord
from redbot.core import commands

from .course_code import CourseCode
from .course_code_resolver import CourseCodeResolver
from .course_data_proxy import CourseDataProxy
from .logger_util import get_logger
from redbot.core.utils.chat_formatting import error
from datetime import datetime, timezone

log = get_logger("red.utils")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def get_categories_by_prefix(
    guild: discord.Guild, prefix: str
) -> List[discord.CategoryChannel]:
    matching: List[discord.CategoryChannel] = [
        cat for cat in guild.categories if cat.name.upper().startswith(prefix.upper())
    ]
    log.debug(
        f"get_categories_by_prefix: Found {len(matching)} categories in guild '{guild.name}' with prefix '{prefix}'"
    )
    return matching


async def get_or_create_category(
    guild: discord.Guild, category_name: str
) -> Optional[discord.CategoryChannel]:
    category = discord.utils.get(guild.categories, name=category_name)
    if category is None:
        try:
            category = await guild.create_category(category_name)
            log.debug(
                f"get_or_create_category: Created category '{category_name}' in guild '{guild.name}'"
            )
        except discord.Forbidden:
            log.error(
                f"get_or_create_category: No permission to create category '{category_name}' in guild '{guild.name}'"
            )
            return None
    else:
        log.debug(
            f"get_or_create_category: Found existing category '{category_name}' in guild '{guild.name}'"
        )
    return category


async def get_available_course_category(
    guild: discord.Guild, base_name: str, ctx: commands.Context, max_channels: int = 50
) -> Optional[discord.CategoryChannel]:
    category = discord.utils.get(guild.categories, name=base_name)
    if category is None:
        category = await get_or_create_category(guild, base_name)
    if category is None:
        await ctx.send(
            error("Insufficient permissions to create the courses category.")
        )
        return None
    if len(category.channels) < max_channels:
        return category
    for i in range(2, 100):
        alt_name = f"{base_name}-{i}"
        alt_category = discord.utils.get(guild.categories, name=alt_name)
        if alt_category is None:
            alt_category = await get_or_create_category(guild, alt_name)
        if alt_category is None:
            await ctx.send(
                error(f"Insufficient permissions to create the category '{alt_name}'.")
            )
            return None
        if len(alt_category.channels) < max_channels:
            return alt_category
    await ctx.send(
        error("All course categories have reached the maximum channel limit.")
    )
    return None


async def validate_and_resolve_course_code(
    ctx: commands.Context,
    raw_input: str,
    listings: Dict[str, Any],
    course_data_proxy: CourseDataProxy,
) -> Optional[CourseCode]:
    resolver = CourseCodeResolver(listings, course_data_proxy=course_data_proxy)
    try:
        course_obj: CourseCode = CourseCode(raw_input)
    except ValueError:
        log.debug(
            f"Failed to parse '{raw_input}' using CourseCode. Attempting to resolve using CourseCodeResolver."
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
