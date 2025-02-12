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
    # Basic data is considered stale after 90 days; purge any data older than 180 days.
    _CACHE_STALE_DAYS_BASIC: int = 90  # ~3 months
    _CACHE_EXPIRY_DAYS: int = 240
    _CACHE_PURGE_DAYS: int = 180  # 6+ months
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
        self.log.debug("CourseDataProxy initialized.")

    async def _get_session(self) -> ClientSession:
        if self.session is None or self.session.closed:
            self.session = ClientSession(timeout=ClientTimeout(total=15))
        return self.session

    def _get_course_keys(self, course_code: str) -> Tuple[str, str, str]:
        """
        Extract the three keys: department, course code, and suffix.
        If no suffix is provided, use the token "__nosuffix__".
        """
        course_obj = CourseCode(course_code)
        department = course_obj.department
        code = course_obj.code
        suffix = course_obj.suffix if course_obj.suffix else "__nosuffix__"
        return department, code, suffix

    async def get_course_data(
        self, course_code: str, detailed: bool = False
    ) -> Dict[str, Any]:
        """
        Retrieves course data using a 3-level hierarchical cache:
          Department → Course Code → Suffix.
        Data is stored under a subkey:
          - "basic" for term-start (basic) data including available terms and a "last_updated" timestamp.
          - "detailed" for on-demand detailed lookups.
        If cached data is missing or stale (basic data older than 90 days, or detailed data older than 180 days),
        the code fetches fresh data online.
        """
        department, code, suffix = self._get_course_keys(course_code)
        now = datetime.now(timezone.utc)
        courses = await self.config.courses()
        dept_entry: Dict[str, Any] = courses.get(department, {})
        course_entry: Dict[str, Any] = dept_entry.get(code, {})
        data_entry: Dict[str, Any] = course_entry.get(suffix, {})

        if detailed:
            detailed_data = data_entry.get("detailed")
            if detailed_data:
                try:
                    last_updated = datetime.fromisoformat(
                        detailed_data.get("last_updated")
                    )
                    if now - last_updated < timedelta(days=self._CACHE_PURGE_DAYS):
                        self.log.debug(f"Using cached detailed data for {course_code}")
                        return detailed_data
                except Exception as e:
                    self.log.exception(
                        f"Error parsing detailed last_updated timestamp for {course_code}: {e}"
                    )
            self.log.debug(f"Fetching detailed data for {course_code}")
            soup, error_msg = await self._fetch_course_online(course_code)
            if soup:
                processed_data = self._process_course_data(soup)
                new_detailed = {"data": processed_data, "last_updated": now.isoformat()}
                # Update the nested cache while preserving any basic data.
                async with self.config.courses() as courses_update:
                    dept = courses_update.get(department, {})
                    course_dict = dept.get(code, {})
                    suffix_dict = course_dict.get(suffix, {})
                    suffix_dict["detailed"] = new_detailed
                    course_dict[suffix] = suffix_dict
                    dept[code] = course_dict
                    courses_update[department] = dept
                self.log.debug(f"Fetched and cached detailed data for {course_code}")
                return new_detailed
            else:
                self.log.error(
                    f"Error fetching detailed data for {course_code}: {error_msg}"
                )
                # Fallback to basic data if available
                basic_data = data_entry.get("basic")
                if basic_data:
                    self.log.debug(f"Falling back to basic data for {course_code}")
                    return basic_data
                return {}
        else:
            basic_data = data_entry.get("basic")
            if basic_data:
                try:
                    last_updated = datetime.fromisoformat(
                        basic_data.get("last_updated")
                    )
                    if now - last_updated < timedelta(
                        days=self._CACHE_STALE_DAYS_BASIC
                    ):
                        self.log.debug(f"Using cached basic data for {course_code}")
                        return basic_data
                except Exception as e:
                    self.log.exception(
                        f"Error parsing basic last_updated timestamp for {course_code}: {e}"
                    )
            self.log.debug(f"Fetching basic data for {course_code}")
            soup, error_msg = await self._fetch_course_online(course_code)
            if soup:
                processed_data = self._process_course_data(soup)
                new_basic = {
                    "data": processed_data,
                    "available_terms": self._determine_term_order(),
                    "last_updated": now.isoformat(),
                }
                async with self.config.courses() as courses_update:
                    dept = courses_update.get(department, {})
                    course_dict = dept.get(code, {})
                    suffix_dict = course_dict.get(suffix, {})
                    suffix_dict["basic"] = new_basic
                    course_dict[suffix] = suffix_dict
                    dept[code] = course_dict
                    courses_update[department] = dept
                self.log.debug(f"Fetched and cached basic data for {course_code}")
                return new_basic
            elif error_msg:
                self.log.error(
                    f"Error fetching basic data for {course_code}: {error_msg}"
                )
                return {}
            return {}

    async def _fetch_course_online(
        self, course_code: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """Fetches course data online by trying multiple term IDs in order."""
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

    async def close(self) -> None:
        if self.session:
            await self.session.close()
            self.log.debug("HTTP session closed.")
