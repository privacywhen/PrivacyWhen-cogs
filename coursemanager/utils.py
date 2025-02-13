from typing import Any, Dict, List, Optional, Tuple
import discord
from redbot.core import commands
from .course_code import CourseCode
from .course_code_resolver import CourseCodeResolver
from .course_data_proxy import CourseDataProxy
from .logger_util import get_logger, log_entry_exit

log = get_logger("red.utils")


def get_categories_by_prefix(
    guild: discord.Guild, prefix: str
) -> List[discord.CategoryChannel]:
    matching = [
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


async def validate_and_resolve_course_code(
    ctx: commands.Context,
    raw_input: str,
    listings: Dict[str, Any],
    course_data_proxy: CourseDataProxy,
) -> Optional["CourseCode"]:
    from .course_code import CourseCode

    resolver = CourseCodeResolver(listings, course_data_proxy=course_data_proxy)
    try:
        course_obj = CourseCode(raw_input)
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
