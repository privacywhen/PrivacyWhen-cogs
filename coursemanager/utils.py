from typing import Any, Dict, Optional, List, Tuple
import discord
from redbot.core import commands
from .course_code_resolver import CourseCodeResolver
from .course_data_proxy import CourseDataProxy
from .logger_util import get_logger

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
    from .course_code import CourseCode

    resolver = CourseCodeResolver(listings, course_data_proxy=course_data_proxy)

    try:
        # Attempt standard parsing.
        course_obj = CourseCode(raw_input)
    except ValueError:
        log.debug(
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


async def menu_select_option(
    ctx: commands.Context, options: List[Tuple[str, str]], prompt_prefix: str
) -> Optional[str]:
    from redbot.core.utils.menus import menu, close_menu
    from .constants import REACTION_OPTIONS

    cancel_emoji = REACTION_OPTIONS[-1]
    limited_options = options[: len(REACTION_OPTIONS) - 1]
    option_lines = [
        f"{REACTION_OPTIONS[i]} **{option}**: {description}"
        for i, (option, description) in enumerate(limited_options)
    ]
    option_lines.append(f"{cancel_emoji} Cancel")
    prompt = f"{prompt_prefix}\n" + "\n".join(option_lines)

    log.debug(f"Prompting menu with:\n{prompt}")
    controls = {}

    def make_handler(emoji: str, opt: str):
        async def handler(
            ctx, pages, controls, message, page, timeout, reacted_emoji, *, user=None
        ):
            log.debug(f"Option '{opt}' selected via emoji '{emoji}'")
            await close_menu(
                ctx, pages, controls, message, page, timeout, reacted_emoji, user=user
            )
            return opt

        return handler

    emoji_to_option = {
        REACTION_OPTIONS[i]: option for i, (option, _) in enumerate(limited_options)
    }
    for emoji, opt in emoji_to_option.items():
        controls[emoji] = make_handler(emoji, opt)

    async def cancel_handler(
        pages, controls, message, page, timeout, emoji, *, user=None
    ):
        log.debug("User cancelled the menu")
        await close_menu(ctx, pages, controls, message, page, timeout, emoji, user=user)
        return None

    controls[cancel_emoji] = cancel_handler
    result = await menu(ctx, [prompt], controls=controls, timeout=30.0, user=ctx.author)
    log.debug(f"Menu selection result: {result}")
    return result
