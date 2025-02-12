import asyncio
import logging
import re
from math import floor
from time import time
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from bs4 import BeautifulSoup
from redbot.core import Config
from aiohttp import (
    ClientConnectionError,
    ClientResponseError,
    ClientSession,
)
from .utils import get_logger
from .course_code import CourseCode

log = get_logger("red.course_data_proxy")


class CourseDataProxy:
    _CACHE_STALE_DAYS: int = 120
    _CACHE_EXPIRY_DAYS: int = 240
    _TERM_NAMES: List[str] = ["winter", "spring", "fall"]
    _URL_BASE: str = (
        "https://mytimetable.mcmaster.ca/api/class-data?term={term}&course_0_0={course_key}&t={t}&e={e}"
    )
    _LISTING_URL: str = (
        "https://mytimetable.mcmaster.ca/api/courses/suggestions?"
        "cams=MCMSTiMCMST_MCMSTiSNPOL_MCMSTiMHK_MCMSTiCON_MCMSTiOFF&course_add=*&page_num=-1"
    )

    def __init__(self, bot, config: Config, logger: logging.Logger) -> None:
        self.bot = bot
        self.config: Config = config
        self.log: logging.Logger = logger
        self.log.debug("CourseDataProxy initialized with built-in HTTP session.")

    async def _get_session(self) -> ClientSession:
        return self.bot.http_session

    async def get_course_data(self, course_code: str) -> Dict[str, Any]:
        # Assume course_code is well-formed; get its canonical representation.
        normalized = CourseCode(course_code).canonical()
        self.log.debug(f"Retrieving course data for {normalized}")
        courses: Dict[str, Any] = await self.config.courses()
        course_data = courses.get(normalized)
        if not course_data or not course_data.get("is_fresh", False):
            self.log.debug(
                f"Course data missing/stale for {normalized}; fetching online."
            )
            soup, error_msg = await self._fetch_course_online(normalized)
            if soup:
                processed_data = self._process_course_data(soup)
                new_data = {
                    "course_data": processed_data,
                    "date_added": datetime.now(timezone.utc).isoformat(),
                    "is_fresh": True,
                }
                async with self.config.courses() as courses_update:
                    courses_update[normalized] = new_data
                self.log.debug(f"Fetched and cached data for {normalized}")
                course_data = new_data
            elif error_msg:
                self.log.error(f"Error fetching data for {normalized}: {error_msg}")
                return {}
        else:
            self.log.debug(f"Using cached data for {normalized}")
        return course_data or {}

    async def _fetch_course_online(
        self, normalized_course: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        self.log.debug(f"Fetching online data for {normalized_course}")
        term_order = self._determine_term_order()
        self.log.debug(f"Term order: {term_order}")
        soup, error_message = await self._fetch_data_with_retries(
            term_order, normalized_course
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
        max_retries: int = 3
        retry_delay: int = 5
        url: Optional[str] = None
        for term_name in term_order:
            term_id = await self._get_term_id(term_name)
            if not term_id:
                self.log.debug(f"Term ID not found for term: {term_name}")
                continue
            self.log.debug(f"Using term '{term_name}' with ID {term_id}")
            url = self._build_url(term_id, normalized_course)
            self.log.debug(f"Built URL: {url}")
            for retry_count in range(max_retries):
                self.log.debug(f"Attempt {retry_count + 1} for URL: {url}")
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
                        self.log.debug(f"Retrying in {retry_delay} seconds...")
                        await asyncio.sleep(retry_delay)
                except (
                    ClientResponseError,
                    ClientConnectionError,
                    asyncio.TimeoutError,
                ) as error:
                    self.log.exception(f"HTTP error during fetch from {url}")
                    if retry_count == max_retries - 1:
                        return (
                            None,
                            "Error: Issue occurred while fetching course data.",
                        )
                    self.log.debug(f"Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
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
        self, url: str, content_type: str = "xml"
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
                soup = BeautifulSoup(content, content_type)
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
                    for part in re.split(r"<br\s*/?>", desc_attr)
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
            # Assume external data is well-formed.
            normalized_course_code = CourseCode(raw_course_code).canonical()
            course_info = course.get("info", "").replace("<br/>", " ")
            courses_dict[normalized_course_code] = course_info
        return courses_dict
