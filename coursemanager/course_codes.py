""" This program extracts course code, name, and terms offered into a db file """
import os
import sqlite3
from sqlite3 import Error
import requests
from bs4 import BeautifulSoup

# path of db file
DB_FILE = os.path.dirname(os.path.realpath(__file__)) + "/courses.db"


def connect_db():
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        return conn
    except Error as err:
        print(err)


def main():
    """ main entry point """
    conn = connect_db()
    if not conn:
        print("Database connection error, program aborted")
        return

    cur = conn.cursor()

    page_num = 0
    count = 20
    courses = []

    while count >= 20:
        print(f"fetching page {page_num}...")
        url = f"https://mytimetable.mcmaster.ca/add_suggest.jsp?course_add=*&page_num={page_num}"
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "xml")

        courses_page = soup.add_suggest.find_all("rs")
        count = len(courses_page)
        courses += courses_page
        page_num += 1
        # DEBUG uncomment to fetch only 1 page
        # if page_num >= 1:
        #     break

    # create table
    cur.execute("""CREATE TABLE IF NOT EXISTS courses(
        dept text NOT NULL,
        code text NOT NULL,
        title text NOT NULL,
        offered text,
        PRIMARY KEY (dept, code)
    );""")

    # add courses to table
    for course in courses:
        dept, code = course.text.split(" ")
        offered, title = course["info"].split("<br/>")
        cur.execute(
            "INSERT OR REPLACE INTO courses(dept, code, title, offered) VALUES (?, ?, ?, ?);",
            [dept, code, title, offered])

    conn.commit()
    print(f"DB insertion completed. {len(courses)} courses discovered.")
    conn.close()


if __name__ == "__main__":
    main()
