from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Callable, Mapping, Sequence

from rapidfuzz import process
from redbot.core.utils.menus import close_menu, menu

from .constants import FUZZY_LIMIT, FUZZY_SCORE_CUTOFF, REACTION_OPTIONS, SCORE_MARGIN
from .course_code import CourseCode
from .logger_util import get_logger

if TYPE_CHECKING:
    from redbot.core import commands

log = get_logger(__name__)


# Type aliases
CourseListing = Mapping[str, Any]
FuzzyMatch = tuple[str, CourseCode, float]


class CourseCodeResolver:
    """Resolve user-supplied course codes to canonical records."""

    def __init__(
        self,
        course_listings: CourseListing,
        course_data_proxy: Any | None = None,
    ) -> None:
        self.course_listings = course_listings
        self.course_data_proxy = course_data_proxy

    def find_variant_matches(self, canonical: str) -> list[str]:
        """Return codes extending the canonical prefix."""
        variants = [
            key
            for key in self.course_listings
            if key.startswith(canonical) and len(key) > len(canonical)
        ]
        log.debug("Variants for %s → %s", canonical, variants)
        return variants

    async def prompt_variant_selection(
        self,
        ctx: commands.Context,
        variants: Sequence[str],
    ) -> str | None:
        """Prompt user to choose from variants."""
        options = [(v, self.course_listings.get(v, "")) for v in variants]
        return await self.interactive_course_selector(
            ctx,
            options,
            "Multiple course variants found. Select the correct one:",
        )

    @staticmethod
    def _parse_course_code(raw: str) -> CourseCode | None:
        """Parse raw string into CourseCode or return None."""
        try:
            return CourseCode(raw)
        except ValueError:
            log.debug("Invalid course code format: %s", raw)
            return None

    def _filter_and_sort_fuzzy(
        self,
        matches: Sequence[tuple[str, int, Any]],
    ) -> list[FuzzyMatch]:
        """Filter and sort fuzzy matches."""
        valid: list[FuzzyMatch] = []
        for cand, score, _ in matches:
            if (obj := self._parse_course_code(cand)) is not None:
                valid.append((cand, obj, score))
            else:
                log.debug("Skipping unparseable candidate %s", cand)
        return sorted(valid, key=lambda t: t[2], reverse=True)

    async def fallback_fuzzy_lookup(
        self,
        ctx: commands.Context,
        canonical: str,
    ) -> tuple[CourseCode | None, Any | None]:
        """Locate best match via fuzzy lookup, with retry."""
        if not self.course_listings:
            log.debug("No listings available for fuzzy lookup")
            return None, None

        raw_matches = None
        for attempt in range(2):
            try:
                raw_matches = process.extract(
                    canonical,
                    list(self.course_listings),
                    limit=FUZZY_LIMIT,
                    score_cutoff=FUZZY_SCORE_CUTOFF,
                )
                break
            except Exception:
                if attempt == 0:
                    log.warning(
                        "Fuzzy extraction failed (attempt 1) for %s, retrying",
                        canonical,
                    )
                    await asyncio.sleep(1)
                else:
                    log.exception(
                        "Fuzzy extraction failed after retry for %s",
                        canonical,
                    )
                    return None, None

        log.debug("Raw fuzzy matches for %s → %s", canonical, raw_matches)
        valid = self._filter_and_sort_fuzzy(raw_matches or [])

        if not valid:
            log.debug("No valid fuzzy candidates after parsing")
            return None, None

        if len(valid) == 1 or (
            len(valid) > 1 and valid[0][2] - valid[1][2] >= SCORE_MARGIN
        ):
            selected_code, selected_obj, score = valid[0]
            log.debug("Auto-selected %s (score %s)", selected_code, score)
        else:
            choices = [code for code, _, _ in valid]
            selected_code = await self.prompt_variant_selection(ctx, choices)
            if selected_code is None:
                return None, None
            selected_obj = next(
                (obj for code, obj, _ in valid if code == selected_code),
                self._parse_course_code(selected_code),
            )
            if selected_obj is None:
                log.debug("Failed to parse user-selected candidate %s", selected_code)
                return None, None

        data = self.course_listings.get(selected_code)
        return selected_obj, data

    async def resolve_course_code(
        self,
        ctx: commands.Context,
        course: CourseCode | None,
    ) -> tuple[CourseCode | None, Any | None]:
        """Resolve a CourseCode to listing data."""
        if course is None:
            log.debug("resolve_course_code called with None")
            return None, None

        canonical = course.canonical()
        if canonical in self.course_listings:
            return course, self.course_listings[canonical]

        if variants := self.find_variant_matches(canonical):
            sel = (
                variants[0]
                if len(variants) == 1
                else await self.prompt_variant_selection(ctx, variants)
            )
            if sel is None:
                return None, None
            obj = self._parse_course_code(sel)
            return obj, (self.course_listings.get(sel) if obj else None)

        log.debug("No variants found for %s; invoking fuzzy lookup", canonical)
        return await self.fallback_fuzzy_lookup(ctx, canonical)

    @staticmethod
    def _build_menu_controls(
        options: Sequence[tuple[str, str]],
    ) -> dict[str, Callable[..., Any]]:
        """Build emoji controls for a menu."""
        controls: dict[str, Callable[..., Any]] = {}
        cancel = REACTION_OPTIONS[-1]
        limited = options[: len(REACTION_OPTIONS) - 1]

        def make_handler(value: str, *, is_cancel: bool = False) -> Callable[..., Any]:
            async def handler(
                ctx_: commands.Context,
                pages: list[str],
                controls: dict,
                message: Any,
                page: int,
                timeout: float,
                reacted_emoji: str,
                *,
                user: Any | None = None,
            ) -> str:
                await close_menu(
                    ctx_,
                    pages,
                    controls,
                    message,
                    page,
                    timeout,
                    reacted_emoji,
                    user=user,
                )
                return "CANCELLED" if is_cancel else value

            return handler

        for idx, (opt, _) in enumerate(limited):
            emoji = REACTION_OPTIONS[idx]
            controls[emoji] = make_handler(opt)

        controls[cancel] = make_handler("CANCELLED", is_cancel=True)
        return controls

    @staticmethod
    async def interactive_course_selector(
        ctx: commands.Context,
        options: Sequence[tuple[str, str]],
        prompt_prefix: str,
    ) -> str | None:
        """Display reaction menu and return choice."""
        if not options:
            log.debug("interactive_course_selector called with no options")
            return None

        lines = [
            f"{REACTION_OPTIONS[i]} **{opt}**: {desc}"
            for i, (opt, desc) in enumerate(options[: len(REACTION_OPTIONS) - 1])
        ]
        lines.append(f"{REACTION_OPTIONS[-1]} Cancel")
        prompt = f"{prompt_prefix}\n" + "\n".join(lines)

        controls = CourseCodeResolver._build_menu_controls(options)
        try:
            result = await menu(
                ctx,
                [prompt],
                controls=controls,
                timeout=30.0,
                user=ctx.author,
            )
        except Exception:
            log.exception("Error during interactive menu selection")
            return None

        return None if result == "CANCELLED" else result
