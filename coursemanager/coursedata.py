"""Course Data Proxy for Redbot.

This module handles fetching and caching course data from an external API.
Cached data is stored in the configuration under the global key 'courses'.
"""

import asyncio
import re
from math import floor
from time import time
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup
from redbot.core import Config
from logging import Logger
from aiohttp import (
    ClientConnectionError,
    ClientResponseError,
    ClientSession,
    ClientTimeout,
)


class CourseDataProxy:
    """
    Handles fetching and caching of course data from an external API.
    Cached data is stored in config under the global key 'courses'.
    """

    _CACHE_STALE_DAYS: int = 120
    _CACHE_EXPIRY_DAYS: int = 240
    _TERM_NAMES: List[str] = ["winter", "spring", "fall"]
    _URL_BASE: str = (
        "https://mytimetable.mcmaster.ca/api/class-data?"
        "term={term}&course_0_0={course_key_formatted}&t={t}&e={e}"
    )
    _LISTING_URL: str = (
        "https://mytimetable.mcmaster.ca/api/courses/suggestions?"
        "cams=MCMSTiMCMST_MCMSTiSNPOL_MCMSTiMHK_MCMSTiCON_MCMSTiOFF&course_add=*"
        "&page_num=-1"
    )

    def __init__(self, config: Config, log: Logger) -> None:
        """Initialize the proxy with the bot's Config instance."""
        self.config: Config = config
        self.log: Logger = log
        self.log.debug("CourseDataProxy initialized with config: %s", config)

    async def get_course_data(self, course_key_formatted: str) -> Dict[str, Any]:
        """
        Retrieve course data from config if available and fresh.
        Otherwise, fetch it from the external API, cache it, and return the data.
        """
        self.log.debug("Retrieving course data for %s", course_key_formatted)
        courses: Dict[str, Any] = await self.config.courses()
        course_data = courses.get(course_key_formatted)
        if not course_data or not course_data.get("is_fresh", False):
            self.log.debug(
                "Course data missing/stale for %s; fetching online.",
                course_key_formatted,
            )
            soup, error_msg = await self._fetch_course_online(course_key_formatted)
            if soup:
                processed_data = self._process_course_data(soup)
                new_data = {
                    "course_data": processed_data,
                    "date_added": date.today().isoformat(),
                    "is_fresh": True,
                }
                async with self.config.courses() as courses_update:
                    courses_update[course_key_formatted] = new_data
                self.log.debug(
                    "Fetched and cached data for %s: %s", course_key_formatted, new_data
                )
                course_data = new_data
            elif error_msg:
                self.log.error(
                    "Error fetching data for %s: %s", course_key_formatted, error_msg
                )
                return {}
        else:
            self.log.debug("Using cached data for %s", course_key_formatted)
        return course_data or {}

    async def _fetch_course_online(
        self, course_key_formatted: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """Fetch course data from the external API."""
        self.log.debug("Fetching online data for %s", course_key_formatted)
        term_order = self._determine_term_order()
        self.log.debug("Term order: %s", term_order)
        soup, error_message = await self._fetch_data_with_retries(
            term_order, course_key_formatted
        )
        return (soup, None) if soup else (None, error_message)

    def _determine_term_order(self) -> List[str]:
        """Determine a prioritized list of term names based on the current date."""
        now = date.today()
        current_term_index = (now.month - 1) // 4
        term_order = (
            self._TERM_NAMES[current_term_index:]
            + self._TERM_NAMES[:current_term_index]
        )
        self.log.debug("Date: %s, term order: %s", now, term_order)
        return term_order

    async def _fetch_data_with_retries(
        self, term_order: List[str], course_key_formatted: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        max_retries = 1
        retry_delay = 5
        url: Optional[str] = None
        for term_name in term_order:
            term_id = await self._get_term_id(term_name)
            if not term_id:
                self.log.debug("Term ID not found for term: %s", term_name)
                continue
            self.log.debug("Using term '%s' with ID %s", term_name, term_id)
            url = self._build_url(term_id, course_key_formatted)
            self.log.debug("Built URL: %s", url)
            for retry_count in range(max_retries):
                self.log.debug("Attempt %s for URL: %s", retry_count + 1, url)
                try:
                    soup, error_message = await self._fetch_single_attempt(url)
                    if soup:
                        self.log.debug("Successfully fetched data from %s", url)
                        return soup, None
                    elif error_message:
                        self.log.debug("Received error: %s", error_message)
                        if "not found" in error_message.lower():
                            self.log.error("Course not found: %s", course_key_formatted)
                            return None, error_message
                        if retry_count == max_retries - 1:
                            return None, error_message
                        self.log.debug("Retrying in %s seconds...", retry_delay)
                        await asyncio.sleep(retry_delay)
                except (
                    ClientResponseError,
                    ClientConnectionError,
                    asyncio.TimeoutError,
                ) as error:
                    self.log.error("Exception during fetch from %s: %s", url, error)
                    if retry_count == max_retries - 1:
                        return None, "Error: Issue occurred while fetching course data."
                    self.log.debug("Retrying in %s seconds...", retry_delay)
                    await asyncio.sleep(retry_delay)
        if url:
            self.log.error("Max retries reached for %s", url)
        return None, "Error: Max retries reached while fetching course data."

    async def _get_term_id(self, term_name: str) -> Optional[int]:
        """Retrieve the term code from the configuration."""
        self.log.debug("Retrieving term ID for: %s", term_name)
        term_codes: Dict[str, Any] = await self.config.term_codes()
        term_id = term_codes.get(term_name)
        self.log.debug("Term ID for %s: %s", term_name, term_id)
        return term_id

    def _build_url(self, term_id: int, course_key_formatted: str) -> str:
        """Build the URL for the course data API request."""
        t, e = self._generate_time_code()
        url = self._URL_BASE.format(
            term=term_id, course_key_formatted=course_key_formatted, t=t, e=e
        )
        self.log.debug("Generated URL with t=%s, e=%s: %s", t, e, url)
        return url

    def _generate_time_code(self) -> Tuple[int, int]:
        t = floor(time() / 60) % 1000
        e = t % 3 + t % 39 + t % 42
        self.log.debug("Generated time codes: t=%s, e=%s", t, e)
        return t, e

    async def _fetch_single_attempt(
        self, url: str, content_type: str = "xml"
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        self.log.debug("HTTP GET: %s", url)
        timeout = ClientTimeout(total=15)
        try:
            async with ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    self.log.debug("Response %s from URL: %s", response.status, url)
                    if response.status == 500:
                        return None, "Error: HTTP 500"
                    if response.status != 200:
                        return None, f"Error: HTTP {response.status}"
                    content = await response.text()
                    soup = BeautifulSoup(content, content_type)
                    if not (error_tag := soup.find("error")):
                        self.log.debug("No error tag in response for %s", url)
                        return soup, None
                    error_message = error_tag.text.strip()
                    self.log.debug("Error tag found: %s", error_message)
                    return None, error_message or None
        except Exception as e:
            self.log.error("Exception during HTTP GET from %s: %s", url, e)
            return None, str(e)

    def _process_course_data(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Parse the BeautifulSoup object to extract course data."""
        courses = soup.find_all("course")
        self.log.debug("Processing soup: found %s course entries.", len(courses))
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
        """
        Retrieve and overwrite the full course listing.
        Returns the number of courses found as a string.
        """
        self.log.debug("Retrieving full course listings")
        soup, error_msg = await self._fetch_course_listings()
        if soup:
            processed_listing = self._process_course_listing(soup)
            new_data = {
                "courses": processed_listing,
                "date_updated": date.today().isoformat(),
            }
            await self.config.course_listings.set(new_data)
            self.log.debug("Fetched and cached %s courses", len(processed_listing))
            return str(len(processed_listing))
        elif error_msg:
            self.log.error("Error fetching course list: %s", error_msg)
            return "0"
        return None

    async def _fetch_course_listings(
        self,
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        url = self._LISTING_URL
        try:
            soup, error_message = await self._fetch_single_attempt(url)
            if soup:
                self.log.debug("Successfully fetched listing data from %s", url)
                return soup, None
            elif error_message:
                self.log.debug("Received error: %s", error_message)
                return None, error_message
        except (ClientResponseError, ClientConnectionError) as error:
            self.log.error("Exception during fetch from %s: %s", url, error)
            return None, "Error: Issue occurred while fetching course data."
        return None, None

    def _process_course_listing(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Parse the course listing from the BeautifulSoup object."""
        courses = soup.find_all("rs")
        self.log.debug(
            "Processing soup: found %s course listing entries.", len(courses)
        )
        courses_dict: Dict[str, str] = {}
        for course in courses:
            course_code = course.text.upper()
            course_name = course.get("info").replace("<br/>", " ")
            courses_dict[course_code] = course_name
        return courses_dict
