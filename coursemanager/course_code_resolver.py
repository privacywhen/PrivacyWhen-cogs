# course_code_resolver.py
from typing import Any, Dict, List, Optional, Tuple
from rapidfuzz import process
from redbot.core import commands
from .course_code import CourseCode
from .logger_util import get_logger, log_entry_exit

log = get_logger("red.course_code_resolver")


class CourseCodeResolver:
    FUZZY_LIMIT: int = 5
    FUZZY_SCORE_CUTOFF: int = 70
    # Minimum score difference required for auto-selection over the next candidate.
    SCORE_MARGIN: int = 10

    def __init__(
        self, course_listings: Dict[str, Any], course_data_proxy: Any = None
    ) -> None:
        self.course_listings = course_listings
        self.course_data_proxy = course_data_proxy

    @log_entry_exit(log)
    def find_variant_matches(self, canonical: str) -> List[str]:
        """
        Find all keys in the course listings that start with the canonical course code
        and are longer than the canonical version.
        """
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
        """
        Prompt the user to choose a variant among the provided options.
        """
        # Prepare (option, description) tuples for the menu prompt.
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
        Resolve a course code using fuzzy matching with the following steps:
          1. Retrieve fuzzy matches using RapidFuzz.
          2. Filter out candidates that fail to parse.
          3. If only one valid candidate exists, return it.
          4. If multiple valid candidates exist:
              - Auto-select the best if its score exceeds the runner-up by SCORE_MARGIN.
              - Otherwise, prompt the user to choose among valid options.
        """
        keys_list = list(self.course_listings.keys())
        # Retrieve fuzzy matches (up to FUZZY_LIMIT) meeting the minimum score cutoff.
        all_matches = process.extract(
            canonical,
            keys_list,
            limit=self.FUZZY_LIMIT,
            score_cutoff=self.FUZZY_SCORE_CUTOFF,
        )
        log.debug(f"Fuzzy matches for '{canonical}': {all_matches}")
        if not all_matches:
            log.debug(f"No fuzzy matches found for '{canonical}'.")
            return (None, None)

        # Filter out candidates that cannot be parsed successfully.
        valid_matches: List[Tuple[str, CourseCode, float]] = []
        for candidate, score, _ in all_matches:
            candidate_obj = self._parse_course_code(candidate)
            if candidate_obj:
                valid_matches.append((candidate, candidate_obj, score))
            else:
                log.debug(f"Candidate '{candidate}' failed parsing and is skipped.")

        if not valid_matches:
            log.debug("No valid candidates after filtering fuzzy matches.")
            return (None, None)

        # Sort the valid matches in descending order by score.
        valid_matches.sort(key=lambda x: x[2], reverse=True)
        best_candidate, best_obj, best_score = valid_matches[0]
        log.debug(
            f"Best valid fuzzy match for '{canonical}': {best_candidate} with score {best_score}"
        )

        # Decide based on the number of valid candidates.
        if len(valid_matches) == 1:
            # Only one valid candidate remains.
            selected = best_candidate
            log.debug(f"Only one valid fuzzy match found: {selected}")
        else:
            # More than one valid candidate exists.
            second_score = valid_matches[1][2]
            log.debug(f"Second best score for '{canonical}' is {second_score}")
            if best_score - second_score >= self.SCORE_MARGIN:
                # Top candidate is clearly superior.
                selected = best_candidate
                log.debug(
                    f"Auto-selected best fuzzy match '{selected}' with score {best_score} "
                    f"(margin {best_score - second_score} >= {self.SCORE_MARGIN})."
                )
            else:
                # Scores are close; prompt the user using only valid candidate strings.
                candidate_options = [candidate for candidate, _, _ in valid_matches]
                selected = await self.prompt_variant_selection(ctx, candidate_options)
                if not selected:
                    log.debug(
                        "No selection made by the user during fuzzy lookup prompt."
                    )
                    return (None, None)

        # Return the parsed candidate and its associated data.
        candidate_obj = self._parse_course_code(selected)
        if candidate_obj is None:
            log.debug(
                f"Failed to parse the selected candidate '{selected}' after user selection."
            )
            return (None, None)
        data = self.course_listings.get(selected)
        return (candidate_obj, data)

    @log_entry_exit(log)
    async def resolve_course_code(
        self, ctx: commands.Context, course: CourseCode
    ) -> Tuple[Optional[CourseCode], Optional[Dict[str, Any]]]:
        """
        Resolve a course code by checking for:
          - An exact match in the course listings.
          - Variants that start with the canonical code.
          - Fuzzy matches as a last resort.
        """
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
