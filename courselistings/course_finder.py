import sqlite3
import requests
from bs4 import BeautifulSoup
import os

db_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "courses.db")

class Course:
    @staticmethod
    def find_course(course_dept, course_code):
        conn = sqlite3.connect(db_file)
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(f"SELECT * FROM '{course_dept}' WHERE ID = '{course_code}'")
                course = cur.fetchone()
                if not course:
                    return "Error: Course not found"
        except Error as e:
            print(f"Error connecting to database: {e}")
            return "Error"
        return [course[0], f"{course[1]} unit(s)", course[2], course[3], course[4]]

    @staticmethod
    def search_for_course(query):
        query_str = "+".join(query.split())
        url = f"https://academiccalendars.romcmaster.ca/content.php?&filter[keyword]={query_str}&cur_cat_oid=44&navoid=9045"
        try:
            r = requests.get(url, verify=False)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching course data: {e}")
            return "Error"
        soup = BeautifulSoup(r.text, "html.parser")
        list_of_courses = soup.find_all("a", {"href": True, "target": "_blank", "aria-expanded": "false"})
        course_list = [course.text for course in list_of_courses]
        return course_list
