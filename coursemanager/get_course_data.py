import re
import math
import time
import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from redbot.core import Config, commands, checks

class CourseCacheHandler(commands.Cog):
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

    def generate_time_code(self):
        t = math.floor(time.time() / 60) % 1000
        e = t % 3 + t % 39 + t % 42
        return t, e

    async def get_course_data(self, course_str, term_name=None):
        term_codes = await self.config.term_codes()

        if term_name is None:
            now = datetime.datetime.now()
            if 1 <= now.month <= 4:
                terms_order = ["winter", "spring", "fall"]
            elif 5 <= now.month <= 8:
                terms_order = ["spring", "fall", "winter"]
            else:
                terms_order = ["fall", "winter", "spring"]
        else:
            terms_order = [term_name]

        course_data = None
        for term_name in terms_order:
            term = term_codes.get(term_name)
            if term is None:
                await ctx.send("Term code not found. Please set the term codes using the `set_term_codes` command.")
                return

            t, e = self.generate_time_code()
            url = f"https://mytimetable.mcmaster.ca/getclassdata.jsp?term={term}{course_str}&t={t}&e={e}"
            
            async with self.session.get(url) as response:
                content = await response.text()
                soup = BeautifulSoup(content, "xml")

        course_data = []

        for course in soup.find_all("course"):
            course_info = {
                "classes": [],
                "course": course["code"],
                "section": course["section"],
                "term": term,
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

            if course_data:
                break

        return course_data