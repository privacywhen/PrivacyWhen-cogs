from typing import Any, Callable, Dict, List, Optional, Tuple

from rapidfuzz import process
from redbot.core import commands
from redbot.core.utils.menus import menu, close_menu

from .constants import REACTION_OPTIONS
from .course_code import CourseCode
from .logger_util import get_logger, log_entry_exit

FuzzyMatch = Tuple[str, CourseCode, float]
log = get_logger("red.course_code_resolver")


class CourseCodeResolver:
    FUZZY_LIMIT: int = 5
    FUZZY_SCORE_CUTOFF: int = 70
    SCORE_MARGIN: int = 10

    def __init__(
        self, course_listings: Dict[str, Any], course_data_proxy: Any = None
    ) -> None:
        self.course_listings: Dict[str, Any] = course_listings
        self.course_data_proxy: Any = course_data_proxy

    def find_variant_matches(self, canonical: str) -> List[str]:
        """
        Find course code variants that start with the canonical code and are longer.
        """
        variants: List[str] = [
            key
            for key in self.course_listings
            if key.startswith(canonical) and len(key) > len(canonical)
        ]
        log.debug(f"For canonical '{canonical}', found variant matches: {variants}")
        return variants

    async def prompt_variant_selection(
        self, ctx: commands.Context, variants: List[str]
    ) -> Optional[str]:
        """
        Prompt the user to select from multiple course code variants.
        """
        options: List[Tuple[str, Any]] = [
            (variant, self.course_listings.get(variant, "")) for variant in variants
        ]
        log.debug(f"Prompting variant selection with options: {options}")
        return await CourseCodeResolver.interactive_course_selector(
            ctx, options, "Multiple course variants found. Please choose one:"
        )

    def _parse_course_code(self, raw: str) -> Optional[CourseCode]:
        """
        Attempt to parse a raw course code string into a CourseCode object.
        """
        try:
            return CourseCode(raw)
        except ValueError:
            log.error(f"Invalid course code format: {raw}")
            return None

    async def fallback_fuzzy_lookup(
        self, ctx: commands.Context, canonical: str
    ) -> Tuple[Optional[CourseCode], Optional[Dict[str, Any]]]:
        """
        Use fuzzy matching to find the closest course code match when an exact match is not found.
        """
        keys_list: List[str] = list(self.course_listings.keys())
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
        valid_matches.sort(key=lambda x: x[2], reverse=True)
        best_candidate, best_obj, best_score = valid_matches[0]
        log.debug(
            f"Best valid fuzzy match for '{canonical}': {best_candidate} with score {best_score}"
        )
        if len(valid_matches) == 1 or (
            len(valid_matches) > 1
            and valid_matches[0][2] - valid_matches[1][2] >= self.SCORE_MARGIN
        ):
            # Auto-select if the top match is significantly better
            selected_candidate: str = best_candidate
            selected_obj: CourseCode = best_obj
            log.debug(
                f"Auto-selected candidate '{selected_candidate}' (score: {best_score})"
            )
        else:
            candidate_options: List[str] = [
                candidate for candidate, _, _ in valid_matches
            ]
            selected_candidate = await self.prompt_variant_selection(
                ctx, candidate_options
            )
            if not selected_candidate:
                log.debug("No selection made by the user during fuzzy lookup prompt.")
                return (None, None)
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
        data: Any = self.course_listings.get(selected_candidate)
        return (selected_obj, data)

    async def resolve_course_code(
        self, ctx: commands.Context, course: CourseCode
    ) -> Tuple[Optional[CourseCode], Optional[Dict[str, Any]]]:
        """
        Resolve the course code using exact match, variant matching, or fuzzy lookup.
        """
        canonical: str = course.canonical()
        log.debug(f"Resolving course code for canonical: {canonical}")
        if canonical in self.course_listings:
            log.debug(f"Exact match found for '{canonical}'.")
            return (course, self.course_listings[canonical])
        if variants := self.find_variant_matches(canonical):
            selected_code: Optional[str] = (
                variants[0]
                if len(variants) == 1
                else await self.prompt_variant_selection(ctx, variants)
            )
            if not selected_code:
                log.debug("No variant selected by the user.")
                return (None, None)
            candidate_obj: Optional[CourseCode] = self._parse_course_code(selected_code)
            if candidate_obj is None:
                log.debug(f"Failed to parse selected variant '{selected_code}'.")
                return (None, None)
            log.debug(f"Variant '{selected_code}' selected and parsed successfully.")
            data = self.course_listings.get(selected_code)
            return (candidate_obj, data)
        log.debug("No variants found; proceeding with fuzzy lookup.")
        return await self.fallback_fuzzy_lookup(ctx, canonical)

    @staticmethod
    async def interactive_course_selector(
        ctx: commands.Context, options: List[Tuple[str, str]], prompt_prefix: str
    ) -> Optional[str]:
        """
        Display an interactive menu to the user and return their selection.
        """
        ctx._menu_call_count = getattr(ctx, "_menu_call_count", 0) + 1
        log.debug(
            f"Interactive menu call count for this context: {ctx._menu_call_count}"
        )
        cancel_emoji: str = REACTION_OPTIONS[-1]
        max_options: int = len(REACTION_OPTIONS) - 1
        limited_options: List[Tuple[str, str]] = options[:max_options]
        option_lines = [
            f"{REACTION_OPTIONS[i]} **{option}**: {description}"
            for i, (option, description) in enumerate(limited_options)
        ]
        option_lines.append(f"{cancel_emoji} Cancel")
        prompt: str = f"{prompt_prefix}\n" + "\n".join(option_lines)
        log.debug(f"Prompting menu with:\n{prompt}")

        def _make_menu_handler(
            return_value: str, emoji: str, is_cancel: bool = False
        ) -> Callable[..., Any]:
            async def handler(
                ctx: commands.Context,
                pages: List[str],
                controls: dict,
                message: Any,
                page: int,
                timeout: float,
                reacted_emoji: str,
                *,
                user: Optional[Any] = None,
            ) -> str:
                if is_cancel:
                    log.debug("User cancelled the menu")
                else:
                    log.debug(f"Option '{return_value}' selected via emoji '{emoji}'")
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
                return "CANCELLED" if is_cancel else return_value

            return handler

        from typing import Callable, Dict

        controls: Dict[str, Callable[..., Any]] = {
            emoji: _make_menu_handler(option, emoji)
            for emoji, (option, _) in zip(REACTION_OPTIONS, limited_options)
        }
        controls[cancel_emoji] = _make_menu_handler(
            "CANCELLED", cancel_emoji, is_cancel=True
        )
        result: Optional[str] = await menu(
            ctx, [prompt], controls=controls, timeout=30.0, user=ctx.author
        )
        log.debug(f"Menu selection result: {result}")
        if result == "CANCELLED":
            log.debug("User cancellation detected. Exiting menu without re-prompt.")
            return None
        return result
