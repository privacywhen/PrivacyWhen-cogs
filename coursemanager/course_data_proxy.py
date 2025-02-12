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
    A proxy for fetching and caching course data from an external timetable API.
    Implements caching with staleness and expiry checks, as well as robust retry logic.
    """

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
    _MAX_RETRIES: int = 3
    _BASE_DELAY: float = 2  # base delay (in seconds) for exponential backoff
    _PARSER: str = "lxml-xml"

    def __init__(self, config: Config, logger: logging.Logger) -> None:
        """
        Initialize the CourseDataProxy.
        """
        self.config: Config = config
        self.log: logging.Logger = logger
        self.session: Optional[ClientSession] = None
        self.log.debug("CourseDataProxy initialized.")

    async def _get_session(self) -> ClientSession:
        """
        Retrieve or create a new aiohttp ClientSession.
        """
        if self.session is None or self.session.closed:
            self.session = ClientSession(timeout=ClientTimeout(total=15))
        return self.session

    async def get_course_data(self, course_code: str) -> Dict[str, Any]:
        """
        Retrieve course data from the cache or fetch it online if missing/stale.
        """
        normalized = CourseCode(course_code).canonical()
        self.log.debug(f"Retrieving course data for {normalized}")
        courses: Dict[str, Any] = await self.config.courses()
        course_data = courses.get(normalized)
        now = datetime.now(timezone.utc)

        if course_data:
            try:
                date_added = datetime.fromisoformat(course_data.get("date_added"))
            except Exception as e:
                self.log.exception(f"Error parsing date for {normalized}: {e}")
                date_added = now
            age = now - date_added
            if age > timedelta(days=self._CACHE_EXPIRY_DAYS):
                self.log.debug(
                    f"Cached data for {normalized} expired (age: {age}). Fetching new data."
                )
                course_data = None
            elif age > timedelta(days=self._CACHE_STALE_DAYS):
                self.log.debug(f"Cached data for {normalized} is stale (age: {age}).")
                # Optionally, trigger a background refresh here.
        if not course_data or not course_data.get("is_fresh", False):
            self.log.debug(
                f"Course data missing or not fresh for {normalized}; fetching online."
            )
            soup, error_msg = await self._fetch_course_online(normalized)
            if soup:
                processed_data = self._process_course_data(soup)
                new_data = {
                    "course_data": processed_data,
                    "date_added": now.isoformat(),
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
        """
        Fetch course data online by trying different term options.
        """
        self.log.debug(f"Fetching online data for {normalized_course}")
        term_order = self._determine_term_order()
        self.log.debug(f"Term order: {term_order}")
        soup, error_message = await self._fetch_data_with_retries(
            term_order, normalized_course
        )
        return (soup, None) if soup else (None, error_message)

    def _determine_term_order(self) -> List[str]:
        """
        Determine the term order to try based on the current date.
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
        Attempt to fetch data for the given course with exponential backoff and added jitter.
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
                # Add jitter to the exponential backoff delay.
                delay = self._BASE_DELAY * (2**attempt) + random.uniform(
                    0, self._BASE_DELAY
                )
                self.log.debug(f"Retrying in {delay:.2f} seconds...")
                await asyncio.sleep(delay)
        if url:
            self.log.error(f"Max retries reached for {url}")
        return (None, "Error: Max retries reached while fetching course data.")

    async def _get_term_id(self, term_name: str) -> Optional[int]:
        """
        Retrieve the term ID from the configuration.
        """
        self.log.debug(f"Retrieving term ID for: {term_name}")
        term_codes: Dict[str, Any] = await self.config.term_codes()
        term_id = term_codes.get(term_name.lower())
        self.log.debug(f"Term ID for {term_name}: {term_id}")
        return term_id

    def _build_url(self, term_id: int, normalized_course: str) -> str:
        """
        Build the URL for fetching course data.
        """
        t, e = self._generate_time_code()
        url = self._URL_BASE.format(
            term=term_id, course_key=normalized_course, t=t, e=e
        )
        self.log.debug(f"Generated URL with t={t}, e={e}: {url}")
        return url

    def _generate_time_code(self) -> Tuple[int, int]:
        """
        Generate time-based codes for the API URL.
        """
        t: int = floor(time() / 60) % 1000
        e: int = t % 3 + t % 39 + t % 42
        self.log.debug(f"Generated time codes: t={t}, e={e}")
        return (t, e)

    async def _fetch_single_attempt(
        self, url: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """
        Perform a single HTTP GET request and parse the response.
        """
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
        """
        Extract course data from the BeautifulSoup object.
        """
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
                # Use a raw string for the regex pattern.
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
        """
        Retrieve and update the full course listings.
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
        """
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
        """
        Extract course listings from the BeautifulSoup object.
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

    async def close(self) -> None:
        """
        Close the aiohttp session.
        """
        if self.session:
            await self.session.close()
            self.log.debug("HTTP session closed.")
