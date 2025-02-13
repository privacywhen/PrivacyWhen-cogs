from typing import Any, Dict, Optional, List
import discord
import logging
from .course_code import CourseCode
from .course_code_resolver import CourseCodeResolver
from .course_data_proxy import CourseDataProxy


def get_logger(name: str, level: int = logging.DEBUG) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("[%(levelname)s] %(name)s: %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


logger = get_logger(__name__)


def get_categories_by_prefix(
    guild: discord.Guild, prefix: str
) -> List[discord.CategoryChannel]:
    matching = [
        cat for cat in guild.categories if cat.name.upper().startswith(prefix.upper())
    ]
    logger.debug(
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
            logger.debug(
                f"get_or_create_category: Created category '{category_name}' in guild '{guild.name}'"
            )
        except discord.Forbidden:
            logger.error(
                f"get_or_create_category: No permission to create category '{category_name}' in guild '{guild.name}'"
            )
            return None
    else:
        logger.debug(
            f"get_or_create_category: Found existing category '{category_name}' in guild '{guild.name}'"
        )
    return category


async def validate_and_resolve_course_code(
    ctx: commands.Context,
    raw_input: str,
    listings: Dict[str, Any],
    course_data_proxy: CourseDataProxy,
) -> Optional[CourseCode]:
    """
    Validate and resolve a course code string into a canonical CourseCode instance.

    This function attempts to parse the provided `raw_input` using the standard
    CourseCode parser. If parsing fails (for example due to formatting issues),
    it uses CourseCodeResolver's fallback fuzzy lookup to find a match.
    Even if the initial parsing succeeds, it runs the resolver to handle any
    variant resolution. This guarantees that the returned CourseCode is always
    properly formatted.

    Args:
        ctx (commands.Context): The command context.
        raw_input (str): The raw course code provided by the user.
        listings (Dict[str, Any]): The dictionary of course listings.
        course_data_proxy (CourseDataProxy): Instance to assist with online lookups.

    Returns:
        Optional[CourseCode]: A canonical CourseCode instance if resolution
                              succeeds, or None if no valid code can be found.
    """
    resolver = CourseCodeResolver(listings, course_data_proxy=course_data_proxy)

    try:
        # Attempt standard parsing.
        course_obj = CourseCode(raw_input)
    except ValueError:
        logger.debug(
            f"Failed to parse '{raw_input}' using CourseCode. Attempting to resolve using CourseCodeResolver."
        )
        # Fallback: try to resolve the raw input.
        resolved, _ = await resolver.fallback_fuzzy_lookup(ctx, raw_input)
        if not resolved:
            return None
        course_obj = resolved
    else:
        # Even if parsing succeeds, use the resolver to check for variants.
        resolved, _ = await resolver.resolve_course_code(ctx, course_obj)
        if resolved:
            course_obj = resolved

    return course_obj
