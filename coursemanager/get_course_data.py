import re
import math
import time
import aiohttp
from typing import Tuple, List
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from redbot.core import Config, commands, checks

class CourseCacheHandler(commands.Cog):
    """Handles course cache and online course verification."""
    CACHE_STALE_DAYS = 120
    CACHE_EXPIRY_DAYS = 240
    TERM_NAMES = ["winter", "spring/spring", "fall"]

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self.bot, identifier=3720194665, force_registration=True)
        self.config.register_global(courses={}, term_codes={})
        self.session = aiohttp.ClientSession()

    async def close(self):
        await self.session.close()

    @commands.command()
    @checks.is_owner()
    async def set_term_codes(self, ctx, fall: str, winter: str, spring: str):
        """Set the term codes for Fall, Winter, and Spring/Summer semesters."""
        async with self.config.term_codes() as term_codes:
            term_codes["fall"] = fall
            term_codes["winter"] = winter
            term_codes["spring"] = spring
        await ctx.send(f"Term codes have been set to:\nFall: {fall}\nWinter: {winter}\nSpring/Summer: {spring}")

    def generate_time_code(self) -> Tuple[int, int]:
        """Generate a time code for use in the query."""
        t = math.floor(time.time() / 60) % 1000
        e = t % 3 + t % 39 + t % 42
        return t, e

    def current_term(self) -> str:
        """Determine the current term based on the current month."""
        now = datetime.now()
        if 1 <= now.month <= 4:
            return self.TERM_NAMES[0]
        elif 5 <= now.month <= 8:
            return self.TERM_NAMES[1]
        else:
            return self.TERM_NAMES[2]

    async def check_course_cache(self, course_str: str, term_name: str = None) -> tuple:
        """Check the cache for a course and return the data if not expired."""
        courses = await self.config.courses()

        if term_name is None:
            term_name = self.current_term()

        course_key = f"{course_str}-{term_name}"

        if course_key in courses:
            expiry = datetime.fromisoformat(courses[course_key]["expiry"])
            stale_time = expiry - timedelta(days=self.CACHE_STALE_DAYS)
            now = datetime.utcnow()
            if now < expiry:
                return courses[course_key]["data"], now >= stale_time
            else:
                del courses[course_key]
                await self.config.courses.set(courses)
        return None, False

    async def fetch_course_data(self, course_str: str, term: str) -> BeautifulSoup:
        """Fetch course data from the online source."""
        t, e = self.generate_time_code()
        url = f"https://mytimetable.mcmaster.ca/getclassdata.jsp?term={term}{course_str}&t={t}&e={e}"
        async with self.session.get(url) as response:
            if response.status != 200:
                return None
            content = await response.text()
            return BeautifulSoup(content, "xml")

    def process_soup_content(self, soup: BeautifulSoup) -> List[dict]:
        """Process the BeautifulSoup content and return the course data."""
        course_data = []

        for course in soup.find_all("course"):
            course_info = {
                "classes": [],
                "course": course["code"],
                "section": course["section"],
                "term": course["term"],
                "teacher": course["teacher"],
                "location": course["location"],
                "campus": course["campus"],
                "courseKey": course["courseKey"],
                "cmkey": course["cmkey"],
                "prerequisites": course["prerequisites"],
                "antirequisites": course["antirequisites"],
                "requirements": course["requirements"],
            }

            for offering in course.find_all("offering"):
                class_info = {
                    "class": offering["class"],
                    "type": offering["type"],
                    "enrollment": offering["enrollment"],
                    "enrollmentLimit": offering["enrollmentLimit"],
                    "waitlist": offering["waitlist"],
                    "waitlistLimit": offering["waitlistLimit"],
                }
                course_info["classes"].append(class_info)

            course_data.append(course_info)

        return course_data

async def get_course_data(self, course_str: str, term_name: str = None, ctx=None) -> list:
    """Retrieve course data from cache or fetch from the online source."""
    cached_data, is_stale = await self.check_course_cache(course_str, term_name)
    if cached_data is not None and not is_stale:
        return cached_data
    elif cached_data is not None and is_stale:
        stale_data = cached_data

    term_codes = await self.config.term_codes()
    terms_order = self.TERM_NAMES if term_name is None else [term_name]

    for term_name in terms_order:
        term = term_codes.get(term_name)
        if term is None:
            await ctx.send("Term code not found. Please set the term codes using the `set_term_codes` command.")
            return

        soup = await self.fetch_course_data(course_str, term)
        if soup is None:
            continue

        course_data = self.process_soup_content(soup)
        if course_data:
            course_key = f"{course_str}-{term_name}"
            now = datetime.utcnow()
            expiry = (now + timedelta(days=self.CACHE_EXPIRY_DAYS)).isoformat()
            async with self.config.courses() as courses:
                courses[course_key] = {"expiry": expiry, "data": course_data}
            await self.config.courses.set(courses)
            return course_data

    if is_stale:
        return stale_data + [{"note": "The returned data may be out of date as it is older than 4 months and could not be updated from the online source."}]
    else:
        return []