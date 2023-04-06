import re
import math
import time
import requests
from bs4 import BeautifulSoup


def generate_time_code():
    t = math.floor(time.time() / 60) % 1000
    e = t % 3 + t % 39 + t % 42
    return t, e


def get_term_codes():
    url = "https://mytimetable.mcmaster.ca/api/v2/multiselectdata.js"
    page = requests.get(url)
    content = page.text

    term_codes = {}
    matches = re.findall(r'mssession.push\(newMsi\("(.+?)","(.+?)\|(\d+?)"\)\);', content)

    for match in matches:
        term_name = match[1]
        term_code = match[2]
        term_codes[term_name] = term_code

    return term_codes


def get_course_data(course_str, term_name=None):
    term_codes = get_term_codes()
    term = term_codes.get(term_name, 3202330)

    t, e = generate_time_code()
    url = f"https://mytimetable.mcmaster.ca/getclassdata.jsp?term={term}{course_str}&t={t}&e={e}"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "xml")

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

    return course_data


if __name__ == "__main__":
    course_str = "1AA3"
    term_name = "Regular Academic"
    course_data = get_course_data(course_str, term_name)
    print(course_data)
