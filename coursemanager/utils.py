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


async def menu_select_option(
    ctx: commands.Context, options: List[Tuple[str, str]], prompt_prefix: str
) -> Optional[str]:
    from redbot.core.utils.menus import menu, close_menu
    from .constants import REACTION_OPTIONS

    cancel_emoji = REACTION_OPTIONS[-1]
    max_options = len(REACTION_OPTIONS) - 1
    # Limit the options to available reaction slots (reserve one for cancel)
    limited_options = options[:max_options]

    # Build the prompt with emoji labels and descriptions.
    option_lines = [
        f"{REACTION_OPTIONS[i]} **{option}**: {description}"
        for i, (option, description) in enumerate(limited_options)
    ]
    option_lines.append(f"{cancel_emoji} Cancel")
    prompt = f"{prompt_prefix}\n" + "\n".join(option_lines)
    log.debug(f"Prompting menu with:\n{prompt}")

    # Create handler functions for each reaction.
    def create_handler(selected_option: str, emoji: str):
        async def handler(
            ctx, pages, controls, message, page, timeout, reacted_emoji, *, user=None
        ):
            log.debug(f"Option '{selected_option}' selected via emoji '{emoji}'")
            await close_menu(
                ctx, pages, controls, message, page, timeout, reacted_emoji, user=user
            )
            return selected_option

        return handler

    # Map each reaction emoji to its corresponding handler.
    controls: Dict[str, Any] = {
        emoji: create_handler(option, emoji)
        for emoji, (option, _) in zip(REACTION_OPTIONS, limited_options)
    }

    async def cancel_handler(
        ctx, pages, controls, message, page, timeout, emoji, *, user=None
    ):
        log.debug("User cancelled the menu")
        await close_menu(ctx, pages, controls, message, page, timeout, emoji, user=user)
        return None

    controls[cancel_emoji] = cancel_handler

    result = await menu(ctx, [prompt], controls=controls, timeout=30.0, user=ctx.author)
    log.debug(f"Menu selection result: {result}")
    return result
