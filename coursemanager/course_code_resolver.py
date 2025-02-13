from typing import Any, List, Optional, Tuple
from rapidfuzz import process
from redbot.core import commands
from .course_code import CourseCode
from .constants import REACTION_OPTIONS
from .utils import get_logger
from redbot.core.utils.menus import menu, close_menu

log = get_logger("red.course_code_resolver")


class CourseCodeResolver:
    def __init__(self, course_listings: dict, course_data_proxy: Any = None):
        self.course_listings = course_listings
        self.course_data_proxy = course_data_proxy

    def find_variant_matches(self, base: str) -> List[str]:
        variants = [
            key
            for key in self.course_listings
            if key.startswith(base) and len(key) > len(base)
        ]
        log.debug(f"For base '{base}', found variant matches: {variants}")
        return variants

    async def prompt_variant_selection(
        self, ctx: commands.Context, variants: List[str], listings: dict
    ) -> Optional[str]:
        options = [(variant, listings.get(variant, "")) for variant in variants]
        log.debug(f"Prompting variant selection with options: {options}")
        result = await self._menu_select_option(
            ctx, options, "Multiple course variants found. Please choose one:"
        )
        log.debug(f"User selected variant: {result}")
        return result

    async def fallback_fuzzy_lookup(
        self, ctx: commands.Context, canonical: str
    ) -> Tuple[Optional[CourseCode], Optional[dict]]:
        matches = process.extract(
            canonical, list(self.course_listings.keys()), limit=5, score_cutoff=70
        )
        log.debug(f"Fuzzy matches for '{canonical}': {matches}")
        if not matches:
            return (None, None)
        selected = await self.prompt_variant_selection(
            ctx, [match[0] for match in matches], self.course_listings
        )
        if selected:
            try:
                candidate_obj = CourseCode(selected)
            except ValueError:
                candidate_obj = None
            data = self.course_listings.get(selected)
            return (candidate_obj, data) if candidate_obj else (None, None)
        return (None, None)

    async def resolve_course_code(
        self, ctx: commands.Context, course: CourseCode
    ) -> Tuple[Optional[CourseCode], Optional[dict]]:
        canonical = course.canonical()
        if canonical in self.course_listings:
            return (course, self.course_listings[canonical])
        if variants := self.find_variant_matches(canonical):
            if len(variants) == 1:
                try:
                    candidate_obj = CourseCode(variants[0])
                except ValueError:
                    candidate_obj = None
                data = self.course_listings.get(variants[0])
                return (candidate_obj, data) if candidate_obj else (None, None)
            else:
                selected = await self.prompt_variant_selection(
                    ctx, variants, self.course_listings
                )
                if selected:
                    try:
                        candidate_obj = CourseCode(selected)
                    except ValueError:
                        candidate_obj = None
                    data = self.course_listings.get(selected)
                    return (candidate_obj, data) if candidate_obj else (None, None)
        return await self.fallback_fuzzy_lookup(ctx, canonical)

    async def _menu_select_option(
        self, ctx: commands.Context, options: List[Tuple[str, str]], prompt_prefix: str
    ) -> Optional[str]:
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
                ctx,
                pages,
                controls,
                message,
                page,
                timeout,
                reacted_emoji,
                *,
                user=None,
            ):
                log.debug(f"Option '{opt}' selected via emoji '{emoji}'")
                await close_menu(
                    ctx,
                    pages,
                    controls,
                    message,
                    page,
                    timeout,
                    reacted_emoji,
                    user=user,
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
            await close_menu(
                ctx, pages, controls, message, page, timeout, emoji, user=user
            )
            return None

        controls[cancel_emoji] = cancel_handler
        result = await menu(
            ctx, [prompt], controls=controls, timeout=30.0, user=ctx.author
        )
        log.debug(f"Menu selection result: {result}")
        return result
