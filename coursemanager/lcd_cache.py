import datetime
from redbot.core import Config
import aiohttp
from bs4 import BeautifulSoup


class CacheHandler:
    """Handles course cache and online course verification."""

    def __init__(self, bot):
        """Initialize the CacheHandler with the bot instance."""
        self.bot = bot
        self.config = Config.get_conf(self.bot, identifier=3720194665, force_registration=True)
        self.config.register_global(courses={})

    async def course_code_exists(self, course_code: str):
        """Check if the course code exists in the cache or online."""
        courses = await self.config.courses()
        course_code = course_code.upper()

        if course_code in courses:
            now = datetime.datetime.utcnow()
            expiry = datetime.datetime.fromisoformat(courses[course_code]["expiry"])
            if now < expiry:
                print(f"Course {course_code} found in cache.")
                print(courses[course_code]["details"])
                return True
            else:
                del courses[course_code]

        print(f"Course {course_code} not found in cache. Searching online...")
        exists_online, course_details = await self.check_course_online(course_code)
        if exists_online:
            now = datetime.datetime.utcnow()
            expiry = (now + datetime.timedelta(days=120)).isoformat()
            courses[course_code] = {"expiry": expiry, "details": course_details}
            await self.config.courses.set(courses)
            print(f"Course {course_code} added to cache.")
            print(course_details)
            return True

        return False

    async def check_course_online(self, course_code: str):
        """Verify if the course exists online and return its details."""
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            url = f"https://mytimetable.mcmaster.ca/add_suggest.jsp?course_add={course_code.replace(' ', '%20')}"
            async with session.get(url) as response:
                content = await response.text()
                soup = BeautifulSoup(content, "xml")
                course = soup.add_suggest.find("rs")

                if course and course_code == f"{course.text.split(' ')[0]} {course.text.split(' ')[1]}":
                    dept, code = course.text.split(" ")
                    offered, title = course["info"].split("<br/>")
                    course_details = {
                        "dept": dept,
                        "code": code,
                        "title": title,
                        "offered": offered
                    }
                    print(f"Course {course_code} found online.")
                    print(course_details)
                    return True, course_details
                else:
                    print(f"Course {course_code} not found online.")
                    return False, None