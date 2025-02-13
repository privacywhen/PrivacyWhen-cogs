from typing import Any, Dict, List, Optional, Tuple, Callable
from rapidfuzz import process
from redbot.core import commands
from .course_code import CourseCode
from .logger_util import get_logger
from redbot.core.utils.menus import menu, close_menu
from .constants import REACTION_OPTIONS

log = get_logger("red.course_code_resolver")
FuzzyMatch = Tuple[str, CourseCode, float]


class CourseCodeResolver:
    FUZZY_LIMIT: int = 5
    FUZZY_SCORE_CUTOFF: int = 70
    SCORE_MARGIN: int = 10

    def __init__(
        self, course_listings: Dict[str, Any], course_data_proxy: Any = None
    ) -> None:
        self.course_listings = course_listings
        self.course_data_proxy = course_data_proxy
        # Note: Instead of caching the keys at initialization,
        # we now recompute them on each fuzzy lookup to avoid stale data.
        # self._listing_keys = list(course_listings.keys())

    def find_variant_matches(self, canonical: str) -> List[str]:
        variants = [
            key
            for key in self.course_listings
            if key.startswith(canonical) and len(key) > len(canonical)
        ]
        log.debug(f"For canonical '{canonical}', found variant matches: {variants}")
        return variants

    async def prompt_variant_selection(
        self, ctx: commands.Context, variants: List[str]
    ) -> Optional[str]:
        # Build options as tuples of (course_key, description)
        options = [
            (variant, self.course_listings.get(variant, "")) for variant in variants
        ]
        log.debug(f"Prompting variant selection with options: {options}")
        return await self._menu_select_option(
            ctx, options, "Multiple course variants found. Please choose one:"
        )

    def _parse_course_code(self, raw: str) -> Optional[CourseCode]:
        try:
            return CourseCode(raw)
        except ValueError:
            log.error(f"Invalid course code format: {raw}")
            return None

    async def fallback_fuzzy_lookup(
        self, ctx: commands.Context, canonical: str
    ) -> Tuple[Optional[CourseCode], Optional[Dict[str, Any]]]:
        # Recompute keys each time to ensure they’re up to date
        keys_list = list(self.course_listings.keys())
        all_matches = process.extract(
            canonical,
            keys_list,
            limit=self.FUZZY_LIMIT,
            score_cutoff=self.FUZZY_SCORE_CUTOFF,
        )
        log.debug(f"Fuzzy matches for '{canonical}': {all_matches}")

        valid_matches: List[FuzzyMatch] = []
        for candidate, score, _ in all_matches:
            if candidate_obj := self._parse_course_code(candidate):
                valid_matches.append((candidate, candidate_obj, score))
            else:
                log.debug(f"Candidate '{candidate}' failed parsing and is skipped.")

        if not valid_matches:
            log.debug("No valid candidates after filtering fuzzy matches.")
            return (None, None)

        # Sort matches by score (highest first)
        valid_matches.sort(key=lambda x: x[2], reverse=True)
        best_candidate, best_obj, best_score = valid_matches[0]
        log.debug(
            f"Best valid fuzzy match for '{canonical}': {best_candidate} with score {best_score}"
        )

        # Auto-select if only one candidate or the best score is decisively better
        if len(valid_matches) == 1 or (
            len(valid_matches) > 1
            and valid_matches[0][2] - valid_matches[1][2] >= self.SCORE_MARGIN
        ):
            selected_candidate = best_candidate
            selected_obj = best_obj
            log.debug(
                f"Auto-selected candidate '{selected_candidate}' (score: {best_score})"
            )
        else:
            # Prompt the user to select among close matches.
            candidate_options = [candidate for candidate, _, _ in valid_matches]
            selected_candidate = await self.prompt_variant_selection(
                ctx, candidate_options
            )
            if not selected_candidate:
                log.debug("No selection made by the user during fuzzy lookup prompt.")
                return (None, None)
            # Try to find the already parsed CourseCode in our matches.
            selected_obj = next(
                (
                    obj
                    for candidate, obj, _ in valid_matches
                    if candidate == selected_candidate
                ),
                self._parse_course_code(selected_candidate),
            )
            if selected_obj is None:
                log.debug(
                    f"Failed to parse the selected candidate '{selected_candidate}' after user selection."
                )
                return (None, None)

        data = self.course_listings.get(selected_candidate)
        return (selected_obj, data)

    async def resolve_course_code(
        self, ctx: commands.Context, course: CourseCode
    ) -> Tuple[Optional[CourseCode], Optional[Dict[str, Any]]]:
        canonical = course.canonical()
        log.debug(f"Resolving course code for canonical: {canonical}")

        if canonical in self.course_listings:
            log.debug(f"Exact match found for '{canonical}'.")
            return (course, self.course_listings[canonical])

        if variants := self.find_variant_matches(canonical):
            # Auto-select if only one variant; otherwise prompt.
            selected_code = (
                variants[0]
                if len(variants) == 1
                else await self.prompt_variant_selection(ctx, variants)
            )
            if not selected_code:
                log.debug("No variant selected by the user.")
                return (None, None)
            candidate_obj = self._parse_course_code(selected_code)
            if candidate_obj is None:
                log.debug(f"Failed to parse selected variant '{selected_code}'.")
                return (None, None)
            log.debug(f"Variant '{selected_code}' selected and parsed successfully.")
            data = self.course_listings.get(selected_code)
            return (candidate_obj, data)

        log.debug("No variants found; proceeding with fuzzy lookup.")
        return await self.fallback_fuzzy_lookup(ctx, canonical)

    async def _menu_select_option(
        self, ctx: commands.Context, options: List[Tuple[str, str]], prompt_prefix: str
    ) -> Optional[str]:
        return await CourseCodeResolver.interactive_course_selector(
            ctx, options, prompt_prefix
        )

    @staticmethod
    async def interactive_course_selector(
        ctx: commands.Context, options: List[Tuple[str, str]], prompt_prefix: str
    ) -> Optional[str]:
        cancel_emoji = REACTION_OPTIONS[-1]
        max_options = len(REACTION_OPTIONS) - 1
        limited_options = options[:max_options]

        # Build the display lines for each option.
        option_lines = [
            f"{REACTION_OPTIONS[i]} **{option}**: {description}"
            for i, (option, description) in enumerate(limited_options)
        ]
        option_lines.append(f"{cancel_emoji} Cancel")
        prompt = f"{prompt_prefix}\n" + "\n".join(option_lines)
        log.debug(f"Prompting menu with:\n{prompt}")

        def create_handler(selected_option: str, emoji: str) -> Callable[..., Any]:
            async def handler(
                ctx: commands.Context,
                pages: List[str],
                controls: Dict[str, Any],
                message: Any,
                page: int,
                timeout: float,
                reacted_emoji: str,
                *,
                user: Optional[Any] = None,
            ) -> str:
                log.debug(f"Option '{selected_option}' selected via emoji '{emoji}'")
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
                return selected_option

            return handler

        controls: Dict[str, Any] = {
            emoji: create_handler(option, emoji)
            for emoji, (option, _) in zip(REACTION_OPTIONS, limited_options)
        }

        async def cancel_handler(
            ctx: commands.Context,
            pages: List[str],
            controls: Dict[str, Any],
            message: Any,
            page: int,
            timeout: float,
            emoji: str,
            *,
            user: Optional[Any] = None,
        ) -> None:
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
