import asyncio
import logging
import random
import re
from math import floor
from time import time
from datetime import date, datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
from bs4 import BeautifulSoup
from redbot.core import Config
from aiohttp import (
    ClientConnectionError,
    ClientResponseError,
    ClientSession,
    ClientTimeout,
)
from .utils import get_logger
from .course_code import CourseCode

log = get_logger("red.course_data_proxy")


class CourseDataProxy:
    # Cache thresholds (in days)
    _CACHE_STALE_DAYS_BASIC: int = 90  # Basic data stale after ~3 months
    _CACHE_EXPIRY_DAYS: int = 240
    _CACHE_PURGE_DAYS: int = 180  # Purge any data older than 6 months

    _TERM_NAMES: List[str] = ["winter", "spring", "fall"]
    _URL_BASE: str = (
        "https://mytimetable.mcmaster.ca/api/class-data?term={term}&course_0_0={course_key}&t={t}&e={e}"
    )
    _LISTING_URL: str = (
        "https://mytimetable.mcmaster.ca/api/courses/suggestions?cams=MCMSTiMCMST_MCMSTiSNPOL_MCMSTiMHK_MCMSTiCON_MCMSTiOFF&course_add=*&page_num=-1"
    )
    _MAX_RETRIES: int = 3
    _BASE_DELAY: float = 2
    _PARSER: str = "lxml-xml"

    def __init__(self, config: Config, logger: logging.Logger) -> None:
        self.config: Config = config
        self.log: logging.Logger = logger
        self.session: Optional[ClientSession] = None
        # Optional: in-memory secondary index for composite keys if needed in the future.
        self._in_memory_index: Optional[Dict[str, Tuple[str, str, str]]] = None
        self.log.debug("CourseDataProxy initialized.")

    async def _get_session(self) -> ClientSession:
        if self.session is None or self.session.closed:
            self.session = ClientSession(timeout=ClientTimeout(total=15))
        return self.session

    def _get_course_keys(self, course_code: str) -> Tuple[str, str, str]:
        """
        Extracts the department, course code, and suffix.
        If no suffix is provided, returns "__nosuffix__" as the suffix.
        """
        course_obj = CourseCode(course_code)
        department = course_obj.department
        code = course_obj.code
        suffix = course_obj.suffix or "__nosuffix__"
        return department, code, suffix

    def _get_cache_entry(
        self, courses: Dict[str, Any], department: str, code: str, suffix: str, key: str
    ) -> Optional[Dict[str, Any]]:
        """
        Helper to retrieve a subkey (basic/detailed) from the nested courses config.
        """
        return courses.get(department, {}).get(code, {}).get(suffix, {}).get(key)

    async def _update_cache_entry(
        self, department: str, code: str, suffix: str, key: str, value: Dict[str, Any]
    ) -> None:
        """
        Helper to update a subkey (basic/detailed) in the nested courses config.
        """
        async with self.config.courses() as courses_update:
            dept = courses_update.get(department, {})
            course_dict = dept.get(code, {})
            suffix_dict = course_dict.get(suffix, {})
            suffix_dict[key] = value
            course_dict[suffix] = suffix_dict
            dept[code] = course_dict
            courses_update[department] = dept

    def _is_stale(self, last_updated_str: str, threshold_days: int) -> bool:
        """
        Returns True if the given ISO timestamp is older than threshold_days.
        """
        try:
            last_updated = datetime.fromisoformat(last_updated_str)
            return (datetime.now(timezone.utc) - last_updated) > timedelta(
                days=threshold_days
            )
        except Exception as e:
            self.log.exception(f"Error checking staleness: {e}")
            return True

    async def get_course_data(
        self, course_code: str, detailed: bool = False
    ) -> Dict[str, Any]:
        """
        Retrieves course data from the cache (or online if missing/stale).
        Data is stored in a 3-level hierarchy: Department → Course Code → Suffix,
        with subkeys "basic" and "detailed". For basic data, metadata includes available terms
        and a last_updated timestamp.
        """
        department, code, suffix = self._get_course_keys(course_code)
        now = datetime.now(timezone.utc)
        courses = await self.config.courses()

        key = "detailed" if detailed else "basic"
        cached = self._get_cache_entry(courses, department, code, suffix, key)
        threshold = self._CACHE_PURGE_DAYS if detailed else self._CACHE_STALE_DAYS_BASIC

        if cached and not self._is_stale(cached.get("last_updated", ""), threshold):
            self.log.debug(f"Using cached {key} data for {course_code}")
            return cached

        self.log.debug(f"Fetching {key} data for {course_code}")
        soup, error_msg = await self._fetch_course_online(course_code)
        if soup:
            processed_data = self._process_course_data(soup)
            if detailed:
                new_entry = {
                    "cached_course_data": processed_data,
                    "last_updated": now.isoformat(),
                }
            else:
                new_entry = {
                    "cached_course_data": processed_data,
                    "available_terms": self._determine_term_order(),
                    "last_updated": now.isoformat(),
                }

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
        """Fetch course data online by iterating over term IDs."""
        normalized = CourseCode(course_code).canonical()
        self.log.debug(f"Fetching online data for {normalized}")
        term_order = self._determine_term_order()
        self.log.debug(f"Term order: {term_order}")
        soup, error_message = await self._fetch_data_with_retries(
            term_order, normalized
        )
        return (soup, None) if soup else (None, error_message)

    def _determine_term_order(self) -> List[str]:
        today = date.today()
        current_term_index = (today.month - 1) // 4
        term_order = (
            self._TERM_NAMES[current_term_index:]
            + self._TERM_NAMES[:current_term_index]
        )
        self.log.debug(f"Date: {today}, term order: {term_order}")
        return term_order

    async def _fetch_data_with_retries(
        self, term_order: List[str], normalized_course: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        url: Optional[str] = None
        for term_name in term_order:
            term_id = await self._get_term_id(term_name)
            if not term_id:
                self.log.debug(f"Term ID not found for term: {term_name}")
                continue
            self.log.debug(f"Using term '{term_name}' with ID {term_id}")
            url = self._build_url(term_id, normalized_course)
            self.log.debug(f"Built URL: {url}")
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
                            self.log.error(f"Course not found: {normalized_course}")
                            return (None, error_message)
                except (
                    ClientResponseError,
                    ClientConnectionError,
                    asyncio.TimeoutError,
                ) as error:
                    self.log.exception(f"HTTP error during fetch from {url}")
                    if attempt == self._MAX_RETRIES - 1:
                        return (
                            None,
                            "Error: Issue occurred while fetching course data.",
                        )
                delay = self._BASE_DELAY * 2**attempt + random.uniform(
                    0, self._BASE_DELAY
                )
                self.log.debug(f"Retrying in {delay:.2f} seconds...")
                await asyncio.sleep(delay)
        if url:
            self.log.error(f"Max retries reached for {url}")
        return (None, "Error: Max retries reached while fetching course data.")

    async def _get_term_id(self, term_name: str) -> Optional[int]:
        self.log.debug(f"Retrieving term ID for: {term_name}")
        term_codes: Dict[str, Any] = await self.config.term_codes()
        term_id = term_codes.get(term_name.lower())
        self.log.debug(f"Term ID for {term_name}: {term_id}")
        return term_id

    def _build_url(self, term_id: int, normalized_course: str) -> str:
        t, e = self._generate_time_code()
        url = self._URL_BASE.format(
            term=term_id, course_key=normalized_course, t=t, e=e
        )
        self.log.debug(f"Generated URL with t={t}, e={e}: {url}")
        return url

    def _generate_time_code(self) -> Tuple[int, int]:
        t: int = floor(time() / 60) % 1000
        e: int = t % 3 + t % 39 + t % 42
        self.log.debug(f"Generated time codes: t={t}, e={e}")
        return (t, e)

    async def _fetch_single_attempt(
        self, url: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        self.log.debug(f"HTTP GET: {url}")
        session = await self._get_session()
        try:
            async with session.get(url) as response:
                self.log.debug(f"Response {response.status} from URL: {url}")
                if response.status == 500:
                    return (None, "Error: HTTP 500")
                if response.status != 200:
                    return (None, f"Error: HTTP {response.status}")
                content = await response.text()
                soup = BeautifulSoup(content, self._PARSER)
                error_tag = soup.find("error")
                if not error_tag:
                    self.log.debug(f"No error tag in response for {url}")
                    return (soup, None)
                error_message = error_tag.text.strip() if error_tag else ""
                self.log.debug(f"Error tag found: {error_message}")
                return (None, error_message or None)
        except (ClientResponseError, ClientConnectionError, asyncio.TimeoutError) as e:
            self.log.exception(f"HTTP error during GET from {url}")
            return (None, f"HTTP error: {e}")
        except Exception as e:
            self.log.exception(f"Unexpected error during HTTP GET from {url}")
            return (None, f"Unexpected error: {e}")

    def _process_course_data(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        courses = soup.find_all("course")
        self.log.debug(f"Processing soup: found {len(courses)} course entries.")
        processed_courses: List[Dict[str, Any]] = []
        for course in courses:
            offering = course.find("offering")
            title = offering.get("title", "") if offering else ""
            desc_attr = offering.get("desc", "") if offering else ""
            description = ""
            prerequisites = ""
            antirequisites = ""
            if desc_attr:
                desc_parts = [
                    part.strip()
                    for part in re.split("<br\\s*/?>", desc_attr)
                    if part.strip()
                ]
                if desc_parts:
                    description = desc_parts[0]
                for part in desc_parts:
                    lower = part.lower()
                    if lower.startswith("prerequisite"):
                        prerequisites = (
                            part.split(":", 1)[1].strip() if ":" in part else ""
                        )
                    elif lower.startswith("antirequisite"):
                        antirequisites = (
                            part.split(":", 1)[1].strip() if ":" in part else ""
                        )
            selection = course.find("selection")
            credits = selection.get("credits", "") if selection else ""
            term_found = course.find("term").get("v", "") if course.find("term") else ""
            teacher = ""
            block = course.find("block")
            if block:
                teacher = block.get("teacher", "")
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
        url = self._LISTING_URL
        try:
            soup, error_message = await self._fetch_single_attempt(url)
            if soup:
                self.log.debug(f"Successfully fetched listing data from {url}")
                return (soup, None)
            elif error_message:
                self.log.debug(f"Received error: {error_message}")
                return (None, error_message)
        except (ClientResponseError, ClientConnectionError) as error:
            self.log.exception(f"Exception during fetch from {url}")
            return (None, "Error: Issue occurred while fetching course data.")
        return (None, None)

    def _process_course_listing(self, soup: BeautifulSoup) -> Dict[str, str]:
        courses = soup.find_all("rs")
        self.log.debug(f"Processing soup: found {len(courses)} course listing entries.")
        courses_dict: Dict[str, str] = {}
        for course in courses:
            raw_course_code = course.text.strip()
            try:
                normalized_course_code = CourseCode(raw_course_code).canonical()
            except ValueError:
                self.log.exception(f"Invalid course code format: {raw_course_code}")
                continue
            course_info = course.get("info", "").replace("<br/>", " ")
            courses_dict[normalized_course_code] = course_info
        return courses_dict

    async def force_mark_stale(self, course_code: str, detailed: bool = True) -> bool:
        """
        Force-marks a course's cached data as stale by setting its last_updated to a very old timestamp.
        Returns True if the entry existed and was updated.
        """
        department, code, suffix = self._get_course_keys(course_code)
        key = "detailed" if detailed else "basic"
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
            self.log.debug("HTTP session closed.")
