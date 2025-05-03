import asyncio
import logging
import random
import re
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from math import floor
from time import time
from typing import Any, Dict, List, Optional, Pattern, Tuple

from aiohttp import (
    ClientConnectionError,
    ClientResponseError,
    ClientSession,
    ClientTimeout,
)
from bs4 import BeautifulSoup, Tag
from redbot.core import Config

from .course_code import CourseCode
from .logger_util import get_logger
from .utils import utcnow

log = get_logger("red.course_data_proxy")

# ────────────────────────────── constants ───────────────────────────── #

CACHE_STALE_DAYS_BASIC: int = 90
CACHE_PURGE_DAYS: int = 180

TERM_NAMES: List[str] = ["winter", "spring", "fall"]

URL_BASE: str = (
    "https://mytimetable.mcmaster.ca/api/class-data"
    "?term={term}&course_0_0={course_key}&t={time_code}&e={entropy}"
)
LISTING_URL: str = (
    "https://mytimetable.mcmaster.ca/api/courses/suggestions?"
    "cams=MCMSTiMCMST_MCMSTiSNPOL_MCMSTiMHK_MCMSTiCON_MCMSTiOFF"
    "&course_add=*&page_num=-1"
)

MAX_ATTEMPTS: int = 2  # 1 initial request + 1 retry
BASE_DELAY_SECONDS: float = 2
XML_PARSER: str = "lxml-xml"
BR_TAG_REGEX: Pattern[str] = re.compile(r"<br\s*/?>", flags=re.IGNORECASE)

# Hint‑parsing regexes (compiled once)
PATTERN_TERM_YEAR: Pattern[str] = re.compile(r"\b(Winter|Spring|Fall)\s+(\d{4})", re.I)
PATTERN_YEAR_TERM: Pattern[str] = re.compile(r"(\d{4})\s+(Winter|Spring|Fall)", re.I)
PATTERN_TERM_ONLY: Pattern[str] = re.compile(r"\b(Winter|Spring|Fall)\s+only", re.I)

# ──────────────────────────── helper utils ──────────────────────────── #


def extract_caption_hints(caption: str) -> List[Tuple[str, Optional[int]]]:
    """Return parsed (season, year) tuples from a fuzzy‑lookup caption."""
    hints: List[Tuple[str, Optional[int]]] = [
        (term.lower(), int(year)) for term, year in PATTERN_TERM_YEAR.findall(caption)
    ]
    hints.extend(
        (term.lower(), int(year)) for year, term in PATTERN_YEAR_TERM.findall(caption)
    )
    hints.extend((term.lower(), None) for term in PATTERN_TERM_ONLY.findall(caption))
    return hints


def guess_year_for_term(term: str, current_year: int) -> int:
    """Infer a calendar year for captions such as “Fall only”."""
    month_today = utcnow().month
    if term == "winter":
        return current_year + (1 if month_today >= 10 else 0)
    if term == "spring":
        return current_year + (1 if month_today >= 5 else 0)
    if term == "fall":
        return current_year if month_today >= 8 else current_year - 1
    return current_year


# ──────────────────────────── proxy class ──────────────────────────── #


class CourseDataProxy:  # pylint: disable=too-many-public-methods
    """Scrapes McMaster timetable data and caches it via Red‑DiscordBot Config."""

    def __init__(self, config: Config, logger: logging.Logger) -> None:
        self.config: Config = config
        self.log: logging.Logger = logger
        self._session: Optional[ClientSession] = None
        self.log.debug("CourseDataProxy initialized.")

    # ───────────────────── session management ───────────────────── #

    async def _get_session(self) -> ClientSession:
        """Lazily create or return the shared aiohttp session."""
        if not self._session or self._session.closed:
            self._session = ClientSession(
                timeout=ClientTimeout(connect=10, sock_read=10)
            )
            self.log.debug("Created new HTTP session.")
        return self._session

    async def close(self) -> None:
        """Close the session (original logic)."""
        if self._session:
            await self._session.close()
            self._session = None
            self.log.debug("HTTP session closed.")

    # ───────────────────────── public API ───────────────────────── #

    async def get_course_data(
        self,
        course_code: str,
        *,
        hints: Optional[List[Tuple[str, Optional[int]]]] = None,
        detailed: bool = False,
    ) -> Dict[str, Any]:
        department, course_number, suffix_str = self._get_course_keys(course_code)
        cache_key = "detailed" if detailed else "basic"
        now_iso = utcnow().isoformat()

        courses_cache: Dict[str, Any] = await self.config.courses()
        cached_entry = self._get_cache_entry(
            courses_cache, department, course_number, suffix_str, cache_key
        )
        freshness_threshold = CACHE_PURGE_DAYS if detailed else CACHE_STALE_DAYS_BASIC

        if cached_entry and not self._is_stale(
            cached_entry.get("last_updated", ""), freshness_threshold
        ):
            self.log.debug("Using cached %s data for %s", cache_key, course_code)
            return cached_entry

        self.log.debug("Fetching %s data for %s", cache_key, course_code)
        soup, error_message = await self._fetch_course_online(course_code, hints=hints)
        if not soup:
            self.log.error(
                "Error fetching %s data for %s: %s",
                cache_key,
                course_code,
                error_message,
            )
            if detailed and (
                basic_entry := self._get_cache_entry(
                    courses_cache, department, course_number, suffix_str, "basic"
                )
            ):
                self.log.debug("Falling back to basic data for %s", course_code)
                return basic_entry
            return {}

        processed_courses = self._process_course_data(soup)
        canonical_code = CourseCode(course_code).canonical()
        new_cache_value: Dict[str, Any] = {
            "cached_course_data": processed_courses,
            "last_updated": now_iso,
        }
        if not detailed:
            new_cache_value["available_terms"] = (
                await self._determine_term_order_refined(canonical_code)
            )

        await self._update_cache_entry(
            department, course_number, suffix_str, cache_key, new_cache_value
        )
        self.log.debug("Fetched and cached %s data for %s", cache_key, course_code)
        return new_cache_value

    # ───────────────────── internal – term order ──────────────────── #

    def _term_order_from_hints(
        self, hints: List[Tuple[str, Optional[int]]]
    ) -> List[Tuple[str, int]]:
        current_year = utcnow().year
        ordered_terms: List[Tuple[str, int]] = []
        seen_pairs: set = set()

        for term, year in hints:
            resolved_year = year or guess_year_for_term(term, current_year)
            term_year_pair = (term, resolved_year)
            if term_year_pair not in seen_pairs:
                ordered_terms.append(term_year_pair)
                seen_pairs.add(term_year_pair)
        return ordered_terms

    async def _determine_term_order_refined(
        self, normalized_course: Optional[str] = None
    ) -> List[Tuple[str, int]]:
        candidate_term: Optional[Tuple[str, int]] = None
        if normalized_course:
            candidate_term = await self._extract_term_from_listing(normalized_course)

        month_today = utcnow().month
        year_today = utcnow().year

        def year_for_term(term: str) -> int:
            if term == "winter":
                return year_today + (1 if month_today >= 10 else 0)
            if term == "spring":
                return year_today + (1 if month_today >= 5 else 0)
            if term == "fall":
                return year_today if month_today >= 8 else year_today - 1
            return year_today

        refined_terms = [(term, year_for_term(term)) for term in TERM_NAMES]

        if candidate_term and candidate_term in refined_terms:
            refined_terms.remove(candidate_term)
        if candidate_term:
            refined_terms.insert(0, candidate_term)

        self.log.debug("Refined term order computed: %s", refined_terms)
        return refined_terms

    def _determine_term_order_fallback(self) -> List[Tuple[str, int]]:
        current_year = utcnow().year
        fallback_terms = [(term, current_year) for term in TERM_NAMES]
        self.log.debug("Fallback term order computed: %s", fallback_terms)
        return fallback_terms

    # ─────────────────────── HTTP orchestration ────────────────────── #

    async def _fetch_course_online(
        self,
        course_code: str,
        *,
        hints: Optional[List[Tuple[str, Optional[int]]]] = None,
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        normalized_code = CourseCode(course_code).canonical()
        self.log.debug("Fetching online data for %s", normalized_code)

        term_order = (
            self._term_order_from_hints(hints)
            if hints
            else await self._determine_term_order_refined(normalized_code)
        )
        self.log.debug("Primary term order: %s", term_order)

        soup, error_message = await self._fetch_data_with_retries(
            term_order, normalized_code
        )
        if soup or (error_message and "not found" in error_message.lower()):
            return soup, error_message

        # refined lookup failed – use brute‑force fallback
        self.log.debug("Primary lookup failed; falling back to default term list.")
        fallback_soup, fallback_error = await self._fetch_data_with_retries(
            self._determine_term_order_fallback(), normalized_code
        )
        return fallback_soup, fallback_error

    async def _fetch_data_with_retries(
        self, term_order: List[Tuple[str, int]], normalized_course: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        last_error_message: Optional[str] = None
        consecutive_500_errors = 0

        for term, year in term_order:
            term_key = f"{term}-{year}"
            term_id = await self._get_term_id(term_key)
            if not term_id:
                self.log.debug("No term ID for %s; skipping", term_key)
                continue

            url = self._build_url(term_id, normalized_course)
            soup_response, error_message = await self._retry_request(url)

            if soup_response:
                return soup_response, None

            if error_message:
                last_error_message = error_message
                if "500" in last_error_message:
                    consecutive_500_errors += 1
                    if consecutive_500_errors >= 2:
                        self.log.error("Two consecutive 500s; bailing early.")
                        break
                else:
                    consecutive_500_errors = 0

        return None, last_error_message or "Unknown error while fetching course data."

    # ──────────────────────── request helpers ──────────────────────── #

    def _build_url(self, term_id: int, normalized_course: str) -> str:
        time_code, entropy_component = self._generate_time_code()
        generated_url = URL_BASE.format(
            term=term_id,
            course_key=normalized_course,
            time_code=time_code,
            entropy=entropy_component,
        )
        if log.isEnabledFor(logging.DEBUG):
            self.log.debug("Generated URL: %s", generated_url)
        return generated_url

    @staticmethod
    def _generate_time_code() -> Tuple[int, int]:
        time_code = floor(time() / 60) % 1000
        entropy_component = time_code % 3 + time_code % 39 + time_code % 42
        return time_code, entropy_component

    async def _retry_request(
        self, url: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        for attempt_index in range(MAX_ATTEMPTS):
            if attempt_index:
                delay_seconds = BASE_DELAY_SECONDS * 2 ** (
                    attempt_index - 1
                ) + random.uniform(0, BASE_DELAY_SECONDS)
                await asyncio.sleep(delay_seconds)

            self.log.debug("Attempt %d for URL: %s", attempt_index + 1, url)
            soup_response, error_message = await self._fetch_single_attempt(url)

            if soup_response or (
                error_message and "not found" in error_message.lower()
            ):
                return soup_response, error_message

            # Non‑500 logical errors — don’t retry further
            if error_message and "500" not in error_message:
                return None, error_message

        return None, error_message  # type: ignore[name-defined]

    async def _fetch_single_attempt(
        self, url: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        session = await self._get_session()
        try:
            async with session.get(url) as response:
                if response.status == 500:
                    return None, "HTTP 500"
                if response.status != 200:
                    return None, f"HTTP {response.status}"

                html_content = await response.text()
                soup = BeautifulSoup(html_content, XML_PARSER)
                error_tag = soup.find("error")
                if error_tag:
                    return None, error_tag.text.strip() or "unknown remote error"
                return soup, None
        except (
            ClientResponseError,
            ClientConnectionError,
            asyncio.TimeoutError,
        ) as exception_instance:
            self.log.exception("HTTP error: %s", exception_instance)
            return None, "network error"
        except Exception as exception_instance:  # pylint: disable=broad-except
            self.log.exception("Unexpected error: %s", exception_instance)
            return None, "unexpected error"

    # ─────────────────────── term‑id helpers ─────────────────────── #

    @lru_cache(maxsize=128)
    async def _get_term_id(self, term_key: str) -> Optional[int]:  # type: ignore
        term_codes = await self.config.term_codes()
        return term_codes.get(term_key.lower())

    # ───────────────────────── caching utils ───────────────────────── #

    @staticmethod
    def _get_course_keys(course_code: str) -> Tuple[str, str, str]:
        course = CourseCode(course_code)
        return course.department, course.code, course.suffix or "__nosuffix__"

    @staticmethod
    def _get_cache_entry(
        courses: Dict[str, Any],
        department: str,
        code: str,
        suffix: str,
        key: str,
    ) -> Optional[Dict[str, Any]]:
        return courses.get(department, {}).get(code, {}).get(suffix, {}).get(key)

    async def _update_cache_entry(
        self,
        department: str,
        code: str,
        suffix: str,
        key: str,
        value: Dict[str, Any],
    ) -> None:
        async with self.config.courses() as courses_update:
            department_dict = courses_update.setdefault(department, {})
            course_dict = department_dict.setdefault(code, {})
            suffix_dict = course_dict.setdefault(suffix, {})
            suffix_dict[key] = value

    @staticmethod
    def _is_stale(last_updated_str: str, threshold_days: int) -> bool:
        try:
            last_updated_dt = datetime.fromisoformat(last_updated_str)
            if not last_updated_dt.tzinfo:
                last_updated_dt = last_updated_dt.replace(tzinfo=timezone.utc)
            return utcnow() - last_updated_dt > timedelta(days=threshold_days)
        except ValueError:
            return True

    # ─────────────────────── parsing functions ─────────────────────── #

    @staticmethod
    def _get_tag_attr(tag: Optional[Tag], attribute: str, default: str = "") -> str:
        return tag.get(attribute, default) if tag else default

    def _process_course_data(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        course_tags = soup.find_all("course")
        self.log.debug("Processing soup: found %d course entries.", len(course_tags))
        processed_course_list: List[Dict[str, Any]] = []
        for course_tag in course_tags:
            offering_tag = course_tag.find("offering")
            title = self._get_tag_attr(offering_tag, "title")
            description, prerequisites, antirequisites = self._parse_offering(
                offering_tag
            )
            selection_tag = course_tag.find("selection")
            credits = self._get_tag_attr(selection_tag, "credits")
            teacher = self._get_tag_attr(course_tag.find("block"), "teacher")
            processed_course_list.append(
                {
                    "title": title,
                    "term_found": self._get_tag_attr(course_tag.find("term"), "v"),
                    "teacher": teacher,
                    "course_code": course_tag.get("code", ""),
                    "course_number": course_tag.get("number", ""),
                    "credits": credits,
                    "description": description,
                    "prerequisites": prerequisites,
                    "antirequisites": antirequisites,
                }
            )
        return processed_course_list

    def _parse_offering(self, offering: Optional[Tag]) -> Tuple[str, str, str]:
        description = prerequisites = antirequisites = ""
        if not offering:
            return description, prerequisites, antirequisites

        if raw_description := offering.get("desc", ""):
            lines = [
                line.strip()
                for line in BR_TAG_REGEX.split(raw_description)
                if line.strip()
            ]
            if lines:
                description = lines[0]
            for line in lines:
                lower_line = line.lower()
                if lower_line.startswith("prerequisite"):
                    prerequisites = line.split(":", 1)[1].strip() if ":" in line else ""
                elif lower_line.startswith("antirequisite"):
                    antirequisites = (
                        line.split(":", 1)[1].strip() if ":" in line else ""
                    )
        return description, prerequisites, antirequisites

    async def update_course_listing(self) -> Optional[str]:
        self.log.debug("Retrieving full course listings")
        soup, error_message = await self._fetch_course_listings()
        if soup:
            course_listing = self._process_course_listing(soup)
            await self.config.course_listings.set(
                {"courses": course_listing, "date_updated": utcnow().isoformat()}
            )
            self.log.debug("Fetched and cached %d courses", len(course_listing))
            return str(len(course_listing))
        if error_message:
            self.log.error("Error fetching course list: %s", error_message)
            return "0"
        return None

    async def _fetch_course_listings(
        self,
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        return await self._fetch_single_attempt(LISTING_URL)

    def _process_course_listing(self, soup: BeautifulSoup) -> Dict[str, str]:
        course_tags = soup.find_all("rs")
        self.log.debug("Processing soup: found %d listing entries.", len(course_tags))
        listing_dict: Dict[str, str] = {}
        for course_tag in course_tags:
            raw_code = course_tag.text.strip()
            try:
                canonical_code = CourseCode(raw_code).canonical()
            except ValueError:
                self.log.exception("Invalid course code: %s", raw_code)
                continue
            course_info = BR_TAG_REGEX.sub(" ", course_tag.get("info", ""))
            listing_dict[canonical_code] = course_info
        return listing_dict

    async def force_mark_stale(self, course_code: str, detailed: bool = True) -> bool:
        department, course_number, suffix_str = self._get_course_keys(course_code)
        cache_key = "detailed" if detailed else "basic"
        courses_cache = await self.config.courses()
        if entry := self._get_cache_entry(
            courses_cache, department, course_number, suffix_str, cache_key
        ):
            entry["last_updated"] = "1970-01-01T00:00:00"
            await self._update_cache_entry(
                department, course_number, suffix_str, cache_key, entry
            )
            self.log.debug("Marked %s data for %s as stale.", cache_key, course_code)
            return True
        return False

    async def _extract_term_from_listing(
        self, normalized_course: str
    ) -> Optional[Tuple[str, int]]:
        listings_cache = await self.config.course_listings()
        listing_info = listings_cache.get("courses", {}).get(normalized_course)
        if not listing_info:
            return None

        candidate_pairs = {
            (match.group("term").lower(), int(match.group("year")))
            for match in PATTERN_TERM_YEAR.finditer(listing_info)
        } | {
            (match.group("term").lower(), int(match.group("year")))
            for match in PATTERN_YEAR_TERM.finditer(listing_info)
        }

        if not candidate_pairs:
            return None

        term_priority = {"winter": 1, "spring": 2, "fall": 3}
        current_year = utcnow().year
        future_candidates = [
            pair for pair in candidate_pairs if pair[1] >= current_year
        ]
        chosen_pair = min(
            future_candidates or candidate_pairs,
            key=lambda pair: (pair[1], term_priority.get(pair[0], 99)),
        )
        self.log.debug(
            "Extracted term from listing for %s: %s", normalized_course, chosen_pair
        )
        return chosen_pair
