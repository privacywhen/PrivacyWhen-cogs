# course_data_proxy.py
import asyncio
import logging
import random
import re
from math import floor
from time import time
from datetime import date, datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup, Tag
from redbot.core import Config
from aiohttp import (
    ClientConnectionError,
    ClientResponseError,
    ClientSession,
    ClientTimeout,
)
from .logger_util import get_logger, log_entry_exit
from .course_code import CourseCode

log = get_logger("red.course_data_proxy")


class CourseDataProxy:
    _CACHE_STALE_DAYS_BASIC: int = 90
    _CACHE_PURGE_DAYS: int = 180
    _TERM_NAMES: List[str] = ["winter", "spring", "fall"]
    # Updated URL template: the year is removed from the URL lookup.
    _URL_BASE: str = (
        "https://mytimetable.mcmaster.ca/api/class-data?term={term}&course_0_0={course_key}&t={t}&e={e}"
    )
    _LISTING_URL: str = (
        "https://mytimetable.mcmaster.ca/api/courses/suggestions?cams="
        "MCMSTiMCMST_MCMSTiSNPOL_MCMSTiMHK_MCMSTiCON_MCMSTiOFF&course_add=*&page_num=-1"
    )
    _MAX_RETRIES: int = 1
    _BASE_DELAY: float = 2
    _PARSER: str = "lxml-xml"
    _BR_REGEX = re.compile(r"<br\s*/?>", flags=re.IGNORECASE)

    def __init__(self, config: Config, logger: logging.Logger) -> None:
        self.config: Config = config
        self.log: logging.Logger = logger
        self.session: Optional[ClientSession] = None
        self.log.debug("CourseDataProxy initialized.")

    async def _get_session(self) -> ClientSession:
        if self.session is None or self.session.closed:
            self.session = ClientSession(timeout=ClientTimeout(total=15))
            self.log.debug("Created new HTTP session.")
        return self.session

    def _get_course_keys(self, course_code: str) -> Tuple[str, str, str]:
        course_obj: CourseCode = CourseCode(course_code)
        department: str = course_obj.department
        code: str = course_obj.code
        suffix: str = course_obj.suffix or "__nosuffix__"
        return (department, code, suffix)

    def _get_cache_entry(
        self, courses: Dict[str, Any], department: str, code: str, suffix: str, key: str
    ) -> Optional[Dict[str, Any]]:
        return courses.get(department, {}).get(code, {}).get(suffix, {}).get(key)

    async def _update_cache_entry(
        self, department: str, code: str, suffix: str, key: str, value: Dict[str, Any]
    ) -> None:
        async with self.config.courses() as courses_update:
            dept = courses_update.setdefault(department, {})
            course_dict = dept.setdefault(code, {})
            suffix_dict = course_dict.setdefault(suffix, {})
            suffix_dict[key] = value

    def _is_stale(self, last_updated_str: str, threshold_days: int) -> bool:
        try:
            last_updated = datetime.fromisoformat(last_updated_str)
            return datetime.now(timezone.utc) - last_updated > timedelta(
                days=threshold_days
            )
        except Exception as e:
            self.log.exception(f"Error checking staleness: {e}")
            return True

    async def get_course_data(
        self, course_code: str, detailed: bool = False
    ) -> Dict[str, Any]:
        department, code, suffix = self._get_course_keys(course_code)
        now: datetime = datetime.now(timezone.utc)
        courses: Dict[str, Any] = await self.config.courses()
        key: str = "detailed" if detailed else "basic"
        cached = self._get_cache_entry(courses, department, code, suffix, key)
        threshold: int = (
            self._CACHE_PURGE_DAYS if detailed else self._CACHE_STALE_DAYS_BASIC
        )
        if cached and (not self._is_stale(cached.get("last_updated", ""), threshold)):
            self.log.debug(f"Using cached {key} data for {course_code}")
            return cached
        self.log.debug(f"Fetching {key} data for {course_code}")
        soup, error_msg = await self._fetch_course_online(course_code)
        if soup:
            processed_data = self._process_course_data(soup)
            normalized: str = CourseCode(course_code).canonical()
            new_entry = {
                "cached_course_data": processed_data,
                "last_updated": now.isoformat(),
            }
            if not detailed:
                new_entry["available_terms"] = await self._determine_term_order_refined(
                    normalized
                )
            await self._update_cache_entry(department, code, suffix, key, new_entry)
            self.log.debug(f"Fetched and cached {key} data for {course_code}")
            return new_entry
        else:
            self.log.error(f"Error fetching {key} data for {course_code}: {error_msg}")
            if basic := self._get_cache_entry(
                courses, department, code, suffix, "basic"
            ):
                if detailed:
                    self.log.debug(f"Falling back to basic data for {course_code}")
                    return basic
            return {}

    async def _fetch_course_online(
        self, course_code: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        normalized: str = CourseCode(course_code).canonical()
        self.log.debug(f"Fetching online data for {normalized}")
        # Use the refined term order (leveraging cached course listings when available)
        refined_terms: List[Tuple[str, int]] = await self._determine_term_order_refined(
            normalized
        )
        self.log.debug(f"Refined term order: {refined_terms}")
        soup, error_message = await self._fetch_data_with_retries(
            refined_terms, normalized
        )
        if soup:
            return soup, None
        self.log.debug(
            "Refined term lookup failed; falling back to brute-force term lookup."
        )
        # Fallback: use the fixed term names with the current year.
        fallback_terms: List[Tuple[str, int]] = self._determine_term_order_fallback()
        self.log.debug(f"Fallback term order: {fallback_terms}")
        soup, error_message = await self._fetch_data_with_retries(
            fallback_terms, normalized
        )
        return (soup, None) if soup else (None, error_message)

    async def _extract_term_from_listing(
        self, normalized_course: str
    ) -> Optional[Tuple[str, int]]:
        """
        Leverage cached course listings to extract term information.
        Handles various formats such as:
          - "Winter 2025, 2025 Winter only"
          - "Winter 2025 only"
          - "2024 Fall only"
          - "2024 Fall, Winter 2025, and 2025 Winter only"
          - "Not available in any term"
        Returns a candidate tuple (term, year) if found.
        """
        listings: Dict[str, Any] = await self.config.course_listings()
        courses_listing: Dict[str, Any] = listings.get("courses", {})
        course_info: Optional[str] = courses_listing.get(normalized_course)
        if not course_info:
            return None

        # Define regex patterns for both "Term Year" and "Year Term"
        pattern_term_year = re.compile(
            r"\b(?P<term>Winter|Spring|Fall)\s+(?P<year>\d{4})\b", re.IGNORECASE
        )
        pattern_year_term = re.compile(
            r"\b(?P<year>\d{4})\s+(?P<term>Winter|Spring|Fall)\b", re.IGNORECASE
        )

        candidates: List[Tuple[str, int]] = []

        for match in pattern_term_year.finditer(course_info):
            term = match.group("term").lower()
            year = int(match.group("year"))
            candidates.append((term, year))

        for match in pattern_year_term.finditer(course_info):
            term = match.group("term").lower()
            year = int(match.group("year"))
            candidates.append((term, year))

        # Remove duplicates
        candidates = list(set(candidates))
        if not candidates:
            return None

        # Define an order for terms (lower value means higher priority)
        term_priority = {"winter": 1, "spring": 2, "fall": 3}
        current_year: int = datetime.now(timezone.utc).year

        # Prefer candidates with a year greater than or equal to current year.
        future_candidates = [cand for cand in candidates if cand[1] >= current_year]
        if future_candidates:
            chosen = min(
                future_candidates, key=lambda x: (x[1], term_priority.get(x[0], 99))
            )
        else:
            chosen = min(candidates, key=lambda x: (x[1], term_priority.get(x[0], 99)))

        self.log.debug(f"Extracted term from listing for {normalized_course}: {chosen}")
        return chosen

    async def _determine_term_order_refined(
        self, normalized_course: Optional[str] = None
    ) -> List[Tuple[str, int]]:
        """
        Compute a refined term order by incorporating a year component based on heuristics.
        If a term is extracted from cached course listings, that candidate is prioritized.
        """
        candidate: Optional[Tuple[str, int]] = None
        if normalized_course:
            candidate = await self._extract_term_from_listing(normalized_course)
        today: date = datetime.now(timezone.utc).date()
        current_year: int = today.year
        refined: List[Tuple[str, int]] = []
        for term in self._TERM_NAMES:
            if term.lower() == "winter":
                year: int = current_year + 1 if today.month >= 10 else current_year
            elif term.lower() == "spring":
                year = current_year + 1 if today.month >= 5 else current_year
            elif term.lower() == "fall":
                year = current_year if today.month >= 8 else current_year - 1
            else:
                year = current_year
            refined.append((term, year))
        if candidate:
            if candidate in refined:
                refined.remove(candidate)
            refined.insert(0, candidate)
        self.log.debug(f"Refined term order computed: {refined}")
        return refined

    def _determine_term_order_fallback(self) -> List[Tuple[str, int]]:
        """Fallback term order: use the fixed term names with the current year."""
        current_year: int = datetime.now(timezone.utc).year
        fallback: List[Tuple[str, int]] = [
            (term, current_year) for term in self._TERM_NAMES
        ]
        self.log.debug(f"Fallback term order computed: {fallback}")
        return fallback

    async def _fetch_data_with_retries(
        self, term_order: List[Tuple[str, int]], normalized_course: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        for term, year in term_order:
            term_key: str = f"{term}-{year}"
            term_id: Optional[int] = await self._get_term_id(term_key)
            if not term_id:
                self.log.debug(f"Term ID not found for {term_key}")
                continue
            self.log.debug(f"Using term '{term}' with year {year} and ID {term_id}")
            url: str = self._build_url(term_id, normalized_course, year)
            self.log.debug(f"Built URL: {url}")
            soup, error_message = await self._retry_request(url)
            if soup:
                self.log.debug(f"Successfully fetched data for term {term_key}")
                return (soup, None)
            if error_message and "not found" in error_message.lower():
                self.log.debug(
                    f"Error indicates not found for term {term_key}: {error_message}"
                )
                return (None, error_message)
        return (None, "Error: Max retries reached while fetching course data.")

    async def _get_term_id(self, term_key: str) -> Optional[int]:
        self.log.debug(f"Retrieving term ID for: {term_key}")
        term_codes: Dict[str, Any] = await self.config.term_codes()
        term_id = term_codes.get(term_key.lower())
        self.log.debug(f"Term ID for {term_key}: {term_id}")
        return term_id

    def _build_url(self, term_id: int, normalized_course: str, year: int) -> str:
        # Note: the year is used only to determine the correct term id and is not included in the URL.
        t, e = self._generate_time_code()
        url: str = self._URL_BASE.format(
            term=term_id, course_key=normalized_course, t=t, e=e
        )
        self.log.debug(f"Generated URL with t={t}, e={e}: {url}")
        return url

    def _generate_time_code(self) -> Tuple[int, int]:
        t: int = floor(time() / 60) % 1000
        e: int = t % 3 + t % 39 + t % 42
        self.log.debug(f"Generated time codes: t={t}, e={e}")
        return (t, e)

    async def _retry_request(
        self, url: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        for attempt in range(self._MAX_RETRIES):
            self.log.debug(f"Attempt {attempt + 1} for URL: {url}")
            try:
                soup, error_message = await self._fetch_single_attempt(url)
                if soup:
                    self.log.debug(f"Successfully fetched data from {url}")
                    return (soup, None)
                elif error_message:
                    self.log.debug(f"Received error: {error_message}")
                    if "not found" in error_message.lower():
                        self.log.error(f"Course not found for URL: {url}")
                        return (None, error_message)
            except (
                ClientResponseError,
                ClientConnectionError,
                asyncio.TimeoutError,
            ) as error:
                self.log.exception(f"HTTP error during fetch from {url}: {error}")
                if attempt == self._MAX_RETRIES - 1:
                    return (None, "Error: Issue occurred while fetching course data.")
            delay: float = self._BASE_DELAY * 2**attempt + random.uniform(
                0, self._BASE_DELAY
            )
            self.log.debug(f"Retrying in {delay:.2f} seconds...")
            await asyncio.sleep(delay)
        self.log.error(f"Max retries reached for {url}")
        return (None, "Error: Max retries reached while fetching course data.")

    async def _fetch_single_attempt(
        self, url: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        self.log.debug(f"HTTP GET: {url}")
        session: ClientSession = await self._get_session()
        try:
            async with session.get(url) as response:
                self.log.debug(f"Response {response.status} from URL: {url}")
                if response.status == 500:
                    return (None, "Error: HTTP 500")
                if response.status != 200:
                    return (None, f"Error: HTTP {response.status}")
                content: str = await response.text()
                soup: BeautifulSoup = BeautifulSoup(content, self._PARSER)
                error_tag = soup.find("error")
                if not error_tag:
                    self.log.debug(f"No error tag in response for {url}")
                    return (soup, None)
                error_message: str = error_tag.text.strip()
                self.log.debug(f"Error tag found: {error_message}")
                return (None, error_message or None)
        except (ClientResponseError, ClientConnectionError, asyncio.TimeoutError) as e:
            self.log.exception(f"HTTP error during GET from {url}: {e}")
            return (None, f"HTTP error: {e}")
        except Exception as e:
            self.log.exception(f"Unexpected error during HTTP GET from {url}: {e}")
            return (None, f"Unexpected error: {e}")

    def _process_course_data(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        courses = soup.find_all("course")
        self.log.debug(f"Processing soup: found {len(courses)} course entries.")
        processed_courses: List[Dict[str, Any]] = []
        for course in courses:
            offering = course.find("offering")
            title = offering.get("title", "") if offering else ""
            description, prerequisites, antirequisites = self._parse_offering(offering)
            selection = course.find("selection")
            credits = selection.get("credits", "") if selection else ""
            term_elem = course.find("term")
            term_found = term_elem.get("v", "") if term_elem else ""
            block = course.find("block")
            teacher = block.get("teacher", "") if block else ""
            processed_courses.append(
                {
                    "title": title,
                    "term_found": term_found,
                    "teacher": teacher,
                    "course_code": course.get("code", ""),
                    "course_number": course.get("number", ""),
                    "credits": credits,
                    "description": description,
                    "prerequisites": prerequisites,
                    "antirequisites": antirequisites,
                }
            )
        return processed_courses

    def _parse_offering(self, offering: Optional[Tag]) -> Tuple[str, str, str]:
        description, prerequisites, antirequisites = ("", "", "")
        if not offering:
            return (description, prerequisites, antirequisites)
        if raw_description := offering.get("desc", ""):
            desc_lines = [
                line.strip()
                for line in self._BR_REGEX.split(raw_description)
                if line.strip()
            ]
            if desc_lines:
                description = desc_lines[0]
            for line in desc_lines:
                lower_line = line.lower()
                if lower_line.startswith("prerequisite"):
                    prerequisites = line.split(":", 1)[1].strip() if ":" in line else ""
                elif lower_line.startswith("antirequisite"):
                    antirequisites = (
                        line.split(":", 1)[1].strip() if ":" in line else ""
                    )
        return (description, prerequisites, antirequisites)

    async def update_course_listing(self) -> Optional[str]:
        self.log.debug("Retrieving full course listings")
        soup, error_msg = await self._fetch_course_listings()
        if soup:
            processed_listing = self._process_course_listing(soup)
            new_data = {
                "courses": processed_listing,
                "date_updated": datetime.now(timezone.utc).isoformat(),
            }
            await self.config.course_listings.set(new_data)
            self.log.debug(f"Fetched and cached {len(processed_listing)} courses")
            return str(len(processed_listing))
        elif error_msg:
            self.log.error(f"Error fetching course list: {error_msg}")
            return "0"
        return None

    async def _fetch_course_listings(
        self,
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        url: str = self._LISTING_URL
        try:
            soup, error_message = await self._fetch_single_attempt(url)
            if soup:
                self.log.debug(f"Successfully fetched listing data from {url}")
                return (soup, None)
            elif error_message:
                self.log.debug(f"Received error: {error_message}")
                return (None, error_message)
        except (ClientResponseError, ClientConnectionError) as error:
            self.log.exception(f"Exception during fetch from {url}: {error}")
            return (None, "Error: Issue occurred while fetching course data.")
        return (None, "Unknown error while fetching course listings.")

    def _process_course_listing(self, soup: BeautifulSoup) -> Dict[str, str]:
        courses = soup.find_all("rs")
        self.log.debug(f"Processing soup: found {len(courses)} course listing entries.")
        courses_dict: Dict[str, str] = {}
        for course in courses:
            raw_course_code: str = course.text.strip()
            try:
                normalized_course_code = CourseCode(raw_course_code).canonical()
            except ValueError:
                self.log.exception(f"Invalid course code format: {raw_course_code}")
                continue
            course_info: str = self._BR_REGEX.sub(" ", course.get("info", ""))
            courses_dict[normalized_course_code] = course_info
        return courses_dict

    async def force_mark_stale(self, course_code: str, detailed: bool = True) -> bool:
        department, code, suffix = self._get_course_keys(course_code)
        key: str = "detailed" if detailed else "basic"
        courses = await self.config.courses()
        if entry := self._get_cache_entry(courses, department, code, suffix, key):
            entry["last_updated"] = "1970-01-01T00:00:00"
            await self._update_cache_entry(department, code, suffix, key, entry)
            self.log.debug(f"Marked {key} data for {course_code} as stale.")
            return True
        return False

    async def close(self) -> None:
        if self.session:
            await self.session.close()
            self.session = None
            self.log.debug("HTTP session closed.")
