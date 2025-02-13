from typing import Any, Dict, List, Optional, Tuple
from rapidfuzz import process
from redbot.core import commands
from .course_code import CourseCode
from .logger_util import get_logger, log_entry_exit

log = get_logger("red.course_code_resolver")


class CourseCodeResolver:
    FUZZY_LIMIT: int = 5
    FUZZY_SCORE_CUTOFF: int = 70

    def __init__(
        self, course_listings: Dict[str, Any], course_data_proxy: Any = None
    ) -> None:
        self.course_listings = course_listings
        self.course_data_proxy = course_data_proxy

    @log_entry_exit(log)
    def find_variant_matches(self, canonical: str) -> List[str]:
        variants = [
            key
            for key in self.course_listings
            if key.startswith(canonical) and len(key) > len(canonical)
        ]
        log.debug(f"For canonical '{canonical}', found variant matches: {variants}")
        return variants

    @log_entry_exit(log)
    async def prompt_variant_selection(
        self, ctx: commands.Context, variants: List[str]
    ) -> Optional[str]:
        options = [
            (variant, self.course_listings.get(variant, "")) for variant in variants
        ]
        log.debug(f"Prompting variant selection with options: {options}")
        selected = await self._menu_select_option(
            ctx, options, "Multiple course variants found. Please choose one:"
        )
        log.debug(f"User selected variant: {selected}")
        return selected

    def _parse_course_code(self, raw: str) -> Optional[CourseCode]:
        try:
            return CourseCode(raw)
        except ValueError:
            log.error(f"Invalid course code format: {raw}")
            return None

    @log_entry_exit(log)
    async def fallback_fuzzy_lookup(
        self, ctx: commands.Context, canonical: str
    ) -> Tuple[Optional[CourseCode], Optional[Dict[str, Any]]]:
        keys_list = list(self.course_listings.keys())
        matches = process.extract(
            canonical,
            keys_list,
            limit=self.FUZZY_LIMIT,
            score_cutoff=self.FUZZY_SCORE_CUTOFF,
        )
        log.debug(f"Fuzzy matches for '{canonical}': {matches}")
        if not matches:
            log.debug(f"No fuzzy matches found for '{canonical}'.")
            return (None, None)
        matched_codes = [match[0] for match in matches]
        if len(matched_codes) == 1:
            selected = matched_codes[0]
            log.debug(f"Only one fuzzy match found: {selected}")
        else:
            selected = await self.prompt_variant_selection(ctx, matched_codes)
        if not selected:
            return (None, None)
        candidate_obj = self._parse_course_code(selected)
        if candidate_obj is None:
            log.debug(f"Failed to parse selected fuzzy match '{selected}'.")
            return (None, None)
        data = self.course_listings.get(selected)
        return (candidate_obj, data)

    @log_entry_exit(log)
    async def resolve_course_code(
        self, ctx: commands.Context, course: CourseCode
    ) -> Tuple[Optional[CourseCode], Optional[Dict[str, Any]]]:
        canonical = course.canonical()
        log.debug(f"Resolving course code for canonical: {canonical}")
        if canonical in self.course_listings:
            log.debug(f"Exact match found for '{canonical}'.")
            return (course, self.course_listings[canonical])
        if variants := self.find_variant_matches(canonical):
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

    @log_entry_exit(log)
    async def _menu_select_option(
        self, ctx: commands.Context, options: List[Tuple[str, str]], prompt_prefix: str
    ) -> Optional[str]:
        from .utils import menu_select_option

        return await menu_select_option(ctx, options, prompt_prefix)
