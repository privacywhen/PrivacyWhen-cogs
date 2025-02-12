"""
course_data_proxy.py

This module provides a proxy for retrieving and caching course data from an external API.
It handles fetching, caching, and parsing of course information using BeautifulSoup and aiohttp.
"""

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
    """
    A proxy for fetching and caching course data.

    This class handles retrieving course data from an external API, parsing the data,
    caching it, and providing methods to update or mark cached entries as stale.
    """

    # Caching thresholds and URL constants
    _CACHE_STALE_DAYS_BASIC: int = 90
    _CACHE_EXPIRY_DAYS: int = 240
    _CACHE_PURGE_DAYS: int = 180
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
        """
        Initialize the CourseDataProxy with a configuration and logger.

        :param config: The Redbot configuration instance.
        :param logger: A logging.Logger instance.
        """
        self.config: Config = config
        self.log: logging.Logger = logger
        self.session: Optional[ClientSession] = None
        self.log.debug("CourseDataProxy initialized.")

    async def _get_session(self) -> ClientSession:
        """
        Get or create an aiohttp ClientSession.

        :return: A ClientSession instance.
        """
        if self.session is None or self.session.closed:
            self.session = ClientSession(timeout=ClientTimeout(total=15))
            self.log.debug("Created new HTTP session.")
        return self.session

    def _get_course_keys(self, course_code: str) -> Tuple[str, str, str]:
        """
        Extract course keys (department, code, suffix) from the given course code.

        :param course_code: The raw course code string.
        :return: A tuple (department, code, suffix).
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
        Retrieve a specific cache entry from the nested courses configuration.

        :param courses: The courses configuration dictionary.
        :param department: The department key.
        :param code: The course code.
        :param suffix: The course suffix.
        :param key: The specific cache key ("basic" or "detailed").
        :return: The cached entry if present, otherwise None.
        """
        return courses.get(department, {}).get(code, {}).get(suffix, {}).get(key)

    async def _update_cache_entry(
        self, department: str, code: str, suffix: str, key: str, value: Dict[str, Any]
    ) -> None:
        """
        Update the cache entry for a given course.

        Uses setdefault to simplify updating nested dictionaries.

        :param department: The department key.
        :param code: The course code.
        :param suffix: The course suffix.
        :param key: The cache key ("basic" or "detailed").
        :param value: The new value to set.
        """
        async with self.config.courses() as courses_update:
            dept = courses_update.setdefault(department, {})
            course_dict = dept.setdefault(code, {})
            suffix_dict = course_dict.setdefault(suffix, {})
            suffix_dict[key] = value

    def _is_stale(self, last_updated_str: str, threshold_days: int) -> bool:
        """
        Determine whether a cached entry is stale based on its timestamp.

        :param last_updated_str: The ISO formatted last updated timestamp.
        :param threshold_days: The number of days before the cache is considered stale.
        :return: True if stale, False otherwise.
        """
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
        """
        Retrieve course data, either from cache or by fetching online.

        :param course_code: The course code to retrieve data for.
        :param detailed: Whether to retrieve detailed data.
        :return: A dictionary with course data.
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
            new_entry = {
                "cached_course_data": processed_data,
                "last_updated": now.isoformat(),
            }
            if not detailed:
                new_entry["available_terms"] = self._determine_term_order()
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
        """
        Fetch course data from the online API.

        :param course_code: The course code to fetch.
        :return: A tuple of (BeautifulSoup object or None, error message or None).
        """
        normalized = CourseCode(course_code).canonical()
        self.log.debug(f"Fetching online data for {normalized}")
        term_order = self._determine_term_order()
        self.log.debug(f"Term order: {term_order}")
        soup, error_message = await self._fetch_data_with_retries(
            term_order, normalized
        )
        return (soup, None) if soup else (None, error_message)

    def _determine_term_order(self) -> List[str]:
        """
        Determine the order of terms based on the current date.

        :return: A list of term names in order.
        """
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
        """
        Attempt to fetch data with retries for each term in the order.

        :param term_order: List of term names to try.
        :param normalized_course: The normalized course code.
        :return: A tuple of (BeautifulSoup object or None, error message or None).
        """
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
                        return soup, None
                    elif error_message:
                        self.log.debug(f"Received error: {error_message}")
                        if "not found" in error_message.lower():
                            self.log.error(f"Course not found: {normalized_course}")
                            return None, error_message
                except (
                    ClientResponseError,
                    ClientConnectionError,
                    asyncio.TimeoutError,
                ) as error:
                    self.log.exception(f"HTTP error during fetch from {url}")
                    if attempt == self._MAX_RETRIES - 1:
                        return None, "Error: Issue occurred while fetching course data."
                delay = self._BASE_DELAY * (2**attempt) + random.uniform(
                    0, self._BASE_DELAY
                )
                self.log.debug(f"Retrying in {delay:.2f} seconds...")
                await asyncio.sleep(delay)
        if url:
            self.log.error(f"Max retries reached for {url}")
        return None, "Error: Max retries reached while fetching course data."

    async def _get_term_id(self, term_name: str) -> Optional[int]:
        """
        Retrieve the term ID from the configuration.

        :param term_name: The term name.
        :return: The term ID if found, else None.
        """
        self.log.debug(f"Retrieving term ID for: {term_name}")
        term_codes: Dict[str, Any] = await self.config.term_codes()
        term_id = term_codes.get(term_name.lower())
        self.log.debug(f"Term ID for {term_name}: {term_id}")
        return term_id

    def _build_url(self, term_id: int, normalized_course: str) -> str:
        """
        Build the API URL for fetching course data.

        :param term_id: The term identifier.
        :param normalized_course: The normalized course code.
        :return: The complete URL as a string.
        """
        t, e = self._generate_time_code()
        url = self._URL_BASE.format(
            term=term_id, course_key=normalized_course, t=t, e=e
        )
        self.log.debug(f"Generated URL with t={t}, e={e}: {url}")
        return url

    def _generate_time_code(self) -> Tuple[int, int]:
        """
        Generate time-based codes used in URL construction.

        :return: A tuple (t, e) of integer time codes.
        """
        t: int = floor(time() / 60) % 1000
        e: int = t % 3 + t % 39 + t % 42
        self.log.debug(f"Generated time codes: t={t}, e={e}")
        return t, e

    async def _fetch_single_attempt(
        self, url: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """
        Perform a single HTTP GET attempt for the specified URL.

        :param url: The URL to fetch.
        :return: A tuple of (BeautifulSoup object or None, error message or None).
        """
        self.log.debug(f"HTTP GET: {url}")
        session = await self._get_session()
        try:
            async with session.get(url) as response:
                self.log.debug(f"Response {response.status} from URL: {url}")
                if response.status == 500:
                    return None, "Error: HTTP 500"
                if response.status != 200:
                    return None, f"Error: HTTP {response.status}"
                content = await response.text()
                soup = BeautifulSoup(content, self._PARSER)
                error_tag = soup.find("error")
                if not error_tag:
                    self.log.debug(f"No error tag in response for {url}")
                    return soup, None
                error_message = error_tag.text.strip()
                self.log.debug(f"Error tag found: {error_message}")
                return None, error_message or None
        except (ClientResponseError, ClientConnectionError, asyncio.TimeoutError) as e:
            self.log.exception(f"HTTP error during GET from {url}")
            return None, f"HTTP error: {e}"
        except Exception as e:
            self.log.exception(f"Unexpected error during HTTP GET from {url}")
            return None, f"Unexpected error: {e}"

    def _process_course_data(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Parse the BeautifulSoup object to extract course data.

        :param soup: BeautifulSoup object containing course data.
        :return: A list of dictionaries, each representing a course.
        """
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

    def _parse_offering(
        self, offering: Optional["bs4.element.Tag"]
    ) -> Tuple[str, str, str]:
        """
        Parse an offering element to extract description, prerequisites, and antirequisites.

        :param offering: BeautifulSoup Tag representing the offering.
        :return: A tuple (description, prerequisites, antirequisites)
        """
        description, prerequisites, antirequisites = "", "", ""
        if not offering:
            return description, prerequisites, antirequisites

        raw_description = offering.get("desc", "")
        if raw_description:
            # Split the raw description by <br> tags (case-insensitive)
            desc_lines = [
                line.strip()
                for line in re.split(r"<br\s*/?>", raw_description, flags=re.IGNORECASE)
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
        return description, prerequisites, antirequisites

    async def update_course_listing(self) -> Optional[str]:
        """
        Update the course listings by fetching data from the external API.

        :return: The number of courses processed as a string, or "0" on error.
        """
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
        """
        Fetch course listings from the external API.

        :return: A tuple of (BeautifulSoup object or None, error message or None).
        """
        url = self._LISTING_URL
        try:
            soup, error_message = await self._fetch_single_attempt(url)
            if soup:
                self.log.debug(f"Successfully fetched listing data from {url}")
                return soup, None
            elif error_message:
                self.log.debug(f"Received error: {error_message}")
                return None, error_message
        except (ClientResponseError, ClientConnectionError) as error:
            self.log.exception(f"Exception during fetch from {url}")
            return None, "Error: Issue occurred while fetching course data."
        return None, None

    def _process_course_listing(self, soup: BeautifulSoup) -> Dict[str, str]:
        """
        Process the course listings data from BeautifulSoup.

        :param soup: BeautifulSoup object containing course listing entries.
        :return: A dictionary mapping normalized course codes to course information.
        """
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
        Force a cached course entry to be marked as stale.

        :param course_code: The course code to mark.
        :param detailed: Whether to mark the detailed data.
        :return: True if an entry was marked stale, False otherwise.
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
        """
        Close the HTTP session.
        """
        if self.session:
            await self.session.close()
            self.log.debug("HTTP session closed.")
