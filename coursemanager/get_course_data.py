import re
import math
import time
import aiohttp
from typing import Tuple, List, Optional, Dict
from bs4 import BeautifulSoup, Tag
from datetime import datetime, timedelta
from redbot.core import Config, commands

class CourseCacheHandler(commands.Cog):
    """Handles course cache and online course verification."""
    CACHE_STALE_DAYS = 120
    CACHE_EXPIRY_DAYS = 240
    TERM_NAMES = ["winter", "spring", "fall"]
    URL_BASE = "https://mytimetable.mcmaster.ca/getclassdata.jsp?term={term}&course_0_0={course_str}&t={t}&e={e}"

    def __init__(self, bot):
        """Initialize the CourseCacheHandler class."""
        self.bot = bot
        self.config = Config.get_conf(self.bot, identifier=3720194665, force_registration=True)
        self.config.register_global(courses={}, term_codes={})
        self.session = aiohttp.ClientSession()

    async def close(self):
        """Close the aiohttp session."""
        await self.session.close()

#    async def check_course_cache(self, course_str: str) -> tuple:
#        """Check if the course data is in the cache and if it is not stale."""
#        courses = await self.config.courses()
#        course_key = course_str

#        if course_key in courses:
#            expiry = datetime.fromisoformat(courses[course_key]["expiry"])
#            stale_time = expiry - timedelta(days=self.CACHE_STALE_DAYS)
#            now = datetime.utcnow()
#            if now < expiry:
#                return courses[course_key]["data"], now >= stale_time
#            del courses[course_key]
#            await self.config.courses.set(courses)

#        return None, False
    
#    async def fetch_course_cache(self, course_str: str, term_name: str = None, ctx=None) -> list:
#        """Fetch course data from the cache or online source."""
#        cached_data, is_stale = await self.check_course_cache(course_str)
#        if cached_data is not None:
#            if not is_stale:
#                return cached_data
#            stale_data = cached_data

#        term_codes = await self.config.term_codes()
#        terms_order = self.TERM_NAMES if term_name is None else [term_name]

#        for term_name in terms_order:
#            term = term_codes.get(term_name)
#            if term is None:
#                await ctx.send("Term code not found. Please set the term codes using the `setterm` subcommand.")
#                return

#            soup, error_message = await self.fetch_course_online(course_str, term)

#            if error_message:
#                await ctx.send(f"Error: {error_message}")
#                return []

#            if soup is None:
#                continue

#            course_data = self.process_soup_content(soup)
#            if course_data:
#                course_key = course_str
#                now = datetime.utcnow()
#                expiry = (now + timedelta(days=self.CACHE_EXPIRY_DAYS)).isoformat()
#                async with self.config.courses() as courses:
#                    courses[course_key] = {"expiry": expiry, "data": course_data}
#                await self.config.courses.set(courses)
#                return course_data

#        if is_stale:
#            return stale_data + [{"note": "The returned data may be out of date as it is older than 4 months and could not be updated from the online source."}]
#        else:
#            return []

    async def term_codes(self, ctx, term_name: str, term_id: int):
        """Set the term code for the specified term."""
        async with self.config.term_codes() as term_codes:
            term_codes[term_name] = term_id
        await ctx.send(f"Term code for {term_name.capitalize()} has been set to: {term_id}")
        print(f"Debug: {term_codes}") # Debug

    def current_term(self) -> str:
        """Determine the current term based on the current month."""
        now = datetime.utcnow()
        if 1 <= now.month <= 4:
            term = self.TERM_NAMES[0]
        elif 5 <= now.month <= 8:
            term = self.TERM_NAMES[1]
        else:
            term = self.TERM_NAMES[2]
        print(f"Debug: {term}") # Debug
        return term

    def generate_time_code(self) -> Tuple[int, int]:
        """Generate a time code for use in the query."""
        t = math.floor(time.time() / 60) % 1000
        e = t % 3 + t % 39 + t % 42
        print(f"Debug: {t}, {e}") # Debug
        return t, e

    async def get_term_id(self, term_name: str) -> int:
        term_codes = await self.config.term_codes()
        term_id = term_codes.get(term_name, None)
        if term_id is None:
            print(f"Debug: {term_id}") # Debug
        return term_id

    async def fetch_course_online(self, course_str: str) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """
        Fetch course data from the online source.

        :param course_str: The formatted course string.
        :return: A tuple containing the BeautifulSoup object and an error message, if any.
        """
        current_term = self.current_term()
        term_order = self.TERM_NAMES[self.TERM_NAMES.index(current_term):] + self.TERM_NAMES[:self.TERM_NAMES.index(current_term)]

        for term_name in term_order:
            term_id = await self.get_term_id(term_name)
            if term_id is None:
                continue

            t, e = self.generate_time_code()
            url = self.URL_BASE.format(term=term_id, course_str=course_str, t=t, e=e)
            print(f"Debug: {url}") # Debug
            
            try:
                async with self.session.get(url) as response:
                    if response.status != 200:
                        continue
                    content = await response.text()
                    soup = BeautifulSoup(content, "xml")
                    error_tag = soup.find("error")
                    error_message = error_tag.text if error_tag else None
                    if error_message is None:
                        return soup, error_message
                    else:
                        print(f"Error fetching course data: {error_message}")
            except aiohttp.ClientError as error:
                print(f"Error fetching course data: {error}")

        return None, "Error: course data not found for any of the terms."

    def create_course_info(self, term: str) -> Dict:
        return {
            "course": "",
            "section": "",
            "teacher": "",
            "location": "",
            "campus": "",
            "courseKey": "",
            "prerequisites": "",
            "antirequisites": "",
            "requirements": "",
            "notes": "",
            "term": "",
            "description": "",
            "title": "",
            "classes": [],
        }

    def create_class_info(self) -> Dict:
        return {
            "class": "",
            "type": "",
        }

    def process_soup_content(self, soup: BeautifulSoup) -> List[Dict]:
        course_data = []
        term_elem = soup.find("term")
        term = term_elem.get("v", "") if isinstance(term_elem, Tag) else ""

        for course in soup.find_all("course"):
            course_info = self.create_course_info(term) # type: ignore
            offering = course.find("offering")
            if offering:
                course_info["title"] = offering["title"]
                course_info["courseKey"] = offering["key"]
                desc = offering.get("desc", "").replace("<br>", "\n").replace("<br/>", "\n").replace("_", " ")
                course_info["description"] = desc
                course_info["prerequisites"] = offering.get("desc", "").split("Prerequisite(s):")[-1].split("Antirequisite(s):")[0].strip()
                course_info["antirequisites"] = offering.get("desc", "").split("Antirequisite(s):")[-1].split("Not open to")[0].strip()

            selection = course.find("selection")
            if selection:
                course_info["credits"] = selection["credits"]

            requirements = course.find("reqgroupline", {"desc": "Refer to course Description."})
            if requirements:
                course_info["requirements"] = requirements["desc"]

            course_info["classes"] = [
                {
                    **self.create_class_info(),
                    "class": block.get("disp", ""),
                    "type": block.get("type", ""),
                } for block in course.find_all("block")
            ]

            for block in course.find_all("block"):
                course_info["teacher"] = block.get("teacher", "")
                course_info["location"] = block.get("location", "")
                course_info["campus"] = block.get("campus", "")
                course_info["notes"] = block.get("n", "")

            course_data.append(course_info)

        return course_data


# // create an updated version of process_soup_content() that pretifies the data before returning it
#    def (self, soup: BeautifulSoup) -> List[Dict]:
#        """Process the BeautifulSoup content and return a list of course data."""
#        course_data = []
#        for course in soup.find_all("course"):
#            course_info = {
#                "course": course["code"],
#                "section": "",
#                "teacher": "",
#                "location": "",
#                "campus": "",
#                "courseKey": "",
#                "cmkey": "",
#                "prerequisites": "",
#                "antirequisites": "",
#                "requirements": "",
#            }
#            offering = course.find("offering")
#            if offering:
#                course_info["prerequisites"] = offering.get("desc", "").split("Prerequisite(s):")[-1].split("Antirequisite(s):")[0].strip()
#                course_info["antirequisites"] = offering.get("desc", "").split("Antirequisite(s):")[-1].strip()

#            course_info["classes"] = []

#            for block in course.find_all("block"):
#                class_info = {
#                    "class": block.get("disp", ""),
#                    "type": block.get("type", ""),
#                    "enrollment": block.get("nres", ""),
#                    "enrollmentLimit": block.get("os", ""),
#                    "waitlist": block.get("wc", ""),
#                    "waitlistLimit": block.get("ws", ""),
#                }
#                course_info["teacher"] = block.get("teacher", "")
#                course_info["location"] = block.get("location", "")
#                course_info["campus"] = block.get("campus", "")

#                course_info["classes"].append(class_info)

#            course_data.append(course_info)
#            print(f"Debug: {course_data}")  # Debug

#        return course_data
