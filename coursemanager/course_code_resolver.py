from typing import Any, Dict, List, Optional, Tuple
from rapidfuzz import process
from redbot.core import commands
from .course_code import CourseCode
from .logger_util import get_logger, log_entry_exit

log = get_logger("red.course_code_resolver")


class CourseCodeResolver:
    # Class-level constants to allow easy tuning of fuzzy matching parameters.
    FUZZY_LIMIT: int = 5
    FUZZY_SCORE_CUTOFF: int = 70

    def __init__(
        self, course_listings: Dict[str, Any], course_data_proxy: Any = None
    ) -> None:
        """
        Initialize the CourseCodeResolver with a dictionary of course listings.

        :param course_listings: Mapping of course codes to associated data.
        :param course_data_proxy: Optional proxy for course data; reserved for future use.
        """
        self.course_listings = course_listings
        self.course_data_proxy = course_data_proxy

    @log_entry_exit(log)
    def find_variant_matches(self, base: str) -> List[str]:
        """
        Find course code variants that start with the given base but are longer.

        :param base: The base course code.
        :return: A list of variant course codes.
        """
        variants = [
            key
            for key in self.course_listings
            if key.startswith(base) and len(key) > len(base)
        ]
        log.debug(f"For base '{base}', found variant matches: {variants}")
        return variants

    @log_entry_exit(log)
    async def prompt_variant_selection(
        self, ctx: commands.Context, variants: List[str]
    ) -> Optional[str]:
        """
        Prompt the user to select one of the provided variant course codes.

        :param ctx: The command context.
        :param variants: List of variant course codes.
        :return: The selected course code or None if cancelled.
        """
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
        """
        Attempt to parse a raw course code string into a CourseCode object.

        :param raw: The raw course code.
        :return: A CourseCode object if successful; otherwise, None.
        """
        try:
            return CourseCode(raw)
        except ValueError:
            log.error(f"Invalid course code format: {raw}")
            return None

    @log_entry_exit(log)
    async def fallback_fuzzy_lookup(
        self, ctx: commands.Context, canonical: str
    ) -> Tuple[Optional[CourseCode], Optional[Dict[str, Any]]]:
        """
        Perform a fuzzy lookup for a course code when no exact or variant matches exist.

        :param ctx: The command context.
        :param canonical: The canonical course code string.
        :return: Tuple of (CourseCode, associated data) or (None, None) if not found.
        """
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
            return None, None

        matched_codes = [match[0] for match in matches]
        selected = await self.prompt_variant_selection(ctx, matched_codes)
        if not selected:
            return None, None

        candidate_obj = self._parse_course_code(selected)
        if candidate_obj is None:
            return None, None

        data = self.course_listings.get(selected)
        return candidate_obj, data

    @log_entry_exit(log)
    async def resolve_course_code(
        self, ctx: commands.Context, course: CourseCode
    ) -> Tuple[Optional[CourseCode], Optional[Dict[str, Any]]]:
        """
        Resolve a CourseCode object to its canonical version and retrieve associated data.

        Resolution process:
          1. Check for an exact match using the canonical form.
          2. Look for variants:
             - If one variant exists, use it.
             - If multiple variants exist, prompt the user.
          3. If no variants are found, perform a fuzzy lookup.

        :param ctx: The command context.
        :param course: The CourseCode object to resolve.
        :return: Tuple of (resolved CourseCode, associated data) or (None, None) if unresolved.
        """
        canonical = course.canonical()
        log.debug(f"Resolving course code for canonical: {canonical}")

        # 1. Exact match
        if canonical in self.course_listings:
            log.debug(f"Exact match found for '{canonical}'.")
            return course, self.course_listings[canonical]

        # 2. Look for variants
        variants = self.find_variant_matches(canonical)
        if variants:
            selected_code = (
                variants[0]
                if len(variants) == 1
                else await self.prompt_variant_selection(ctx, variants)
            )
            if not selected_code:
                log.debug("No variant selected by the user.")
                return None, None

            candidate_obj = self._parse_course_code(selected_code)
            if candidate_obj is None:
                log.debug(f"Failed to parse selected variant '{selected_code}'.")
                return None, None

            log.debug(f"Variant '{selected_code}' selected and parsed successfully.")
            data = self.course_listings.get(selected_code)
            return candidate_obj, data

        # 3. Fallback to fuzzy lookup
        log.debug("No variants found; proceeding with fuzzy lookup.")
        return await self.fallback_fuzzy_lookup(ctx, canonical)

    @log_entry_exit(log)
    async def _menu_select_option(
        self, ctx: commands.Context, options: List[Tuple[str, str]], prompt_prefix: str
    ) -> Optional[str]:
        """
        Delegate to the utility function for menu selection.

        :param ctx: The command context.
        :param options: List of tuples (course code, description).
        :param prompt_prefix: Prompt message.
        :return: The course code selected by the user, or None if cancelled.
        """
        from .utils import menu_select_option

        return await menu_select_option(ctx, options, prompt_prefix)
