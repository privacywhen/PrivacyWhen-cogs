from collections import namedtuple
from datetime import datetime, timedelta
from math import floor
from re import findall, sub
from time import time
from typing import Dict, List, Optional, Tuple

from aiohttp import ClientConnectionError, ClientResponseError, ClientSession
from bs4 import BeautifulSoup
from discord import Guild
from redbot.core import Config, commands


class CourseDataHandler(commands.Cog):
    """Handles course cache and online course verification."""
    CACHE_STALE_DAYS = 120
    CACHE_EXPIRY_DAYS = 240
    TERM_NAMES = ["winter", "spring", "fall"]
    URL_BASE = "https://mytimetable.mcmaster.ca/getclassdata.jsp?term={term}&course_0_0={course_str}&t={t}&e={e}"
    CacheCheckResult = namedtuple('CacheCheckResult', ['course_data', 'is_stale'])

    def __init__(self, bot):
        """Initialize the CourseDataHandler class."""
        self.bot = bot
        self.config = Config.get_conf(self.bot, identifier=3720194665, force_registration=True)
        self.config.register_global(courses={}, term_codes={})
        self.session = ClientSession()

    async def close_session(self):
        """Close the aiohttp session."""
        await self.session.close()
    
    def cog_unload(self):
        """Close the aiohttp session when the cog is unloaded."""
        self.bot.loop.create_task(self.close_session())

    async def fetch_course_cache(self, course_str: str, ctx=None) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """Fetch course data from the cache, if available. Otherwise, fetch from the online source."""
        course_data, is_stale = await self.check_course_cache(course_str)

        if course_data is None or is_stale:
            fetched_course_data = await self.fetch_and_process_course_data(course_str, ctx)

            if fetched_course_data:
                await self.update_cache(course_str, fetched_course_data)
                course_data = fetched_course_data

            if is_stale:
                course_data_note = {"note": "The returned data may be out of date as it is older than 4 months and could not be updated from the online source."}
                if isinstance(course_data, list):
                    course_data.append(course_data_note)
                else:
                    course_data = [course_data, course_data_note['note']]

        if isinstance(course_data, list):
            # Return the first item in the list as the soup object
            soup = course_data[0] if course_data else None
            # Return the second item in the list as the error message string
            error_message = course_data[1] if len(course_data) > 1 else None
            return soup, error_message
        else:
            # If course_data is not a list, return it as the soup object and None as the error message
            return course_data, None

    async def fetch_and_process_course_data(self, course_str: str, ctx) -> list:
        """Fetch course data from the online source and process it."""
        soup, error_message = await self.fetch_course_online(course_str)

        if soup is not None:
            course_data = self.process_soup_content(soup)
            return course_data
        else:
            if error_message is not None and ctx is not None:
                await ctx.send(f"Error: {error_message}")
            return []

    async def update_cache(self, course_str: str, course_data: list) -> None:
        """Update the course cache with the new course data."""
        course_key = course_str
        now = datetime.utcnow()
        expiry = (now + timedelta(days=self.CACHE_EXPIRY_DAYS)).isoformat()
        async with self.config.courses() as courses:
            courses[course_key] = {"expiry": expiry, "data": course_data}

    async def check_course_cache(self, course_str: str) -> Tuple[Optional[BeautifulSoup], bool]:
        """Check if the course data is in the cache and if it is still valid."""
        courses = await self.config.courses()
        course_key = course_str

        if course_key in courses:
            expiry = datetime.fromisoformat(courses[course_key]["expiry"])
            stale_time = expiry - timedelta(days=self.CACHE_STALE_DAYS)
            now = datetime.utcnow()
            if now < expiry:
                return courses[course_key]["data"], now >= stale_time
            del courses[course_key]
            await self.config.courses.set(courses)

        return None, False

    async def term_codes(self, ctx, term_name: str, term_id: int):
        """Set the term code for the specified term."""
        async with self.config.term_codes() as term_codes:
            term_codes[term_name] = term_id
        await ctx.send(f"Term code for {term_name.capitalize()} has been set to: {term_id}")

    def current_term(self) -> str:
        """Determine the current term based on the current month."""
        now = datetime.utcnow()
        if 1 <= now.month <= 4:
            term = self.TERM_NAMES[0]
        elif 5 <= now.month <= 8:
            term = self.TERM_NAMES[1]
        else:
            term = self.TERM_NAMES[2]
        return term

    def generate_time_code(self) -> Tuple[int, int]:
        """Generate a time code for use in the query."""
        t = floor(time() / 60) % 1000
        e = t % 3 + t % 39 + t % 42
        return t, e

    async def get_term_id(self, term_name: str) -> int:
        term_codes = await self.config.term_codes()
        term_id = term_codes.get(term_name, None)
        return term_id

    async def fetch_course_online(self, course_str: str) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """
        Fetch course data from the online source.

        :param course_str: The formatted course string.
        :return: A tuple containing the BeautifulSoup object and an error message, if any.
        """
        current_term = self.current_term()
        term_order = self.TERM_NAMES[self.TERM_NAMES.index(current_term):] + self.TERM_NAMES[:self.TERM_NAMES.index(current_term)]

        soup = None
        error_message = None

        for term_name in term_order:
            term_id = await self.get_term_id(term_name)
            if term_id is None:
                continue

            t, e = self.generate_time_code()
            url = self.URL_BASE.format(term=term_id, course_str=course_str, t=t, e=e)

            try:
                async with self.session.get(url) as response:
                    if response.status != 200:
                        continue
                    content = await response.text()
                    soup = BeautifulSoup(content, "xml")
                    error_tag = soup.find("error")
                    error_message = error_tag.text if error_tag else None
                    if error_message is None:
                        break
            except ClientResponseError as error:
                print(f"Error fetching course data: {error}")
                error_message = "Error: An issue occurred while fetching the course data."
                break
            except ClientConnectionError as error:
                print(f"Error connecting to server: {error}")
                error_message = "Error: An issue occurred while connecting to the server."
                break

        return soup, error_message

    def create_course_info(self) -> Dict:
        """Create a dictionary containing the course data."""
        return {
            "course": "",
            "section": "",
            "teacher": "",
            "location": "",
            "campus": "",
            "courseKey": "",
            "prerequisites": "",
            "antirequisites": "",
            "notes": "",
            "term_found": "",
            "description": "",
            "title": "",
            "type": "", 
        }

    def process_soup_content(self, soup: BeautifulSoup) -> List[Dict]:
        """
        Process the BeautifulSoup content to extract course data.

        :param soup: BeautifulSoup object containing the course data.
        :return: A list of dictionaries containing the processed course data.
        """
        course_data = []

        for course in soup.find_all("course"):
            course_info = self.create_course_info()
            offering = course.find("offering")
            if offering:
                course_info["title"] = offering["title"]
                course_info["courseKey"] = offering["key"]
                desc = offering.get("desc", "")

                prereq_info = findall(r'Prerequisite\(s\):(.+?)(Antirequisite\(s\):|Not open to|$)', desc)
                course_info["prerequisites"] = prereq_info[0][0].strip() if prereq_info else ""

                antireq_info = findall(r'Antirequisite\(s\):(.+?)(Not open to|$)', desc)
                course_info["antirequisites"] = antireq_info[0][0].strip() if antireq_info else ""

                course_info["description"] = sub(r'Prerequisite\(s\):(.+?)(Antirequisite\(s\):|Not open to|$)', '', desc).strip()

            term_elem = course.find("term")
            term_found = term_elem.get("v") if term_elem else ""
            course_info["term_found"] = term_found

            block = course.find("block")
            course_info["type"] = block.get("type", "") if block else ""
            course_info["teacher"] = block.get("teacher", "") if block else ""
            course_info["location"] = block.get("location", "") if block else ""
            course_info["campus"] = block.get("campus", "") if block else ""
            course_info["notes"] = block.get("n", "") if block else ""

            course_data.append(course_info)

            for course in course_data:
                for key, value in course.items():
                    if isinstance(value, str):
                        course[key] = value.replace("<br/>", "\n").replace("_", " ")

        return course_data