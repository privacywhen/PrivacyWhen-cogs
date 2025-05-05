import asyncio
import logging
import random
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Pattern, Tuple

from aiohttp import (
    ClientConnectionError,
    ClientResponseError,
    ClientSession,
    ClientTimeout,
)
from bs4 import BeautifulSoup, Tag
from redbot.core import Config

from .course_code import CourseCode
from .logger_util import get_logger
from .utils import utcnow

log = get_logger("red.course_data_proxy")

# ────────────────────────────── constants ───────────────────────────── #

CACHE_STALE_DAYS_BASIC = 90
CACHE_PURGE_DAYS = 180

TERM_NAMES: List[str] = ["winter", "spring", "fall"]

URL_BASE = (
    "https://mytimetable.mcmaster.ca/api/class-data"
    "?term={term}&course_0_0={course_key}&t={time_code}&e={entropy}"
)
LISTING_URL = (
    "https://mytimetable.mcmaster.ca/api/courses/suggestions?"
    "cams=MCMSTiMCMST_MCMSTiSNPOL_MCMSTiMHK_MCMSTiCON_MCMSTiOFF"
    "&course_add=*&page_num=-1"
)

MAX_ATTEMPTS = 2
BASE_DELAY_SECONDS = 2
XML_PARSER = "lxml-xml"
BR_TAG_REGEX: Pattern[str] = re.compile(r"<br\s*/?>", flags=re.IGNORECASE)

INVALID_PAIR_TTL = timedelta(hours=1)  # how long to remember invalid pairs

PATTERN_TERM_YEAR = re.compile(r"\b(Winter|Spring|Fall)\s+(\d{4})", re.I)
PATTERN_YEAR_TERM = re.compile(r"(\d{4})\s+(Winter|Spring|Fall)", re.I)
PATTERN_TERM_ONLY = re.compile(r"\b(Winter|Spring|Fall)\s+only", re.I)


def extract_caption_hints(caption: str) -> List[Tuple[str, Optional[int]]]:
    hints: List[Tuple[str, Optional[int]]] = [
        (term.lower(), int(year)) for term, year in PATTERN_TERM_YEAR.findall(caption)
    ]
    hints.extend(
        (term.lower(), int(year)) for year, term in PATTERN_YEAR_TERM.findall(caption)
    )
    hints.extend((term.lower(), None) for term in PATTERN_TERM_ONLY.findall(caption))
    return hints


def guess_year_for_term(term: str, current_year: int) -> int:
    month_today = utcnow().month
    if term == "winter":
        return current_year + (1 if month_today >= 10 else 0)
    if term == "spring":
        return current_year + (1 if month_today >= 5 else 0)
    if term == "fall":
        return current_year if month_today >= 8 else current_year - 1
    return current_year


class CourseDataProxy:
    """
    Scrapes McMaster timetable API, caches results in Config,
    uses in‑memory TTL caches, and prunes invalid course-term pairs briefly.
    """

    def __init__(self, config: Config, logger: logging.Logger) -> None:
        self.config = config
        self.log = logger
        self._session: Optional[ClientSession] = None

        # TTL caches
        self._term_codes_cache: Dict[str, int] = {}
        self._term_codes_last_update: datetime = datetime.min.replace(
            tzinfo=timezone.utc
        )
        self._listings_cache: Dict[str, str] = {}
        self._listings_last_update: datetime = datetime.min.replace(tzinfo=timezone.utc)

        # Invalid pair cache with timestamps
        self._invalid_course_term_cache: Dict[Tuple[str, str], datetime] = {}

        self.log.debug(
            "CourseDataProxy initialized with TTL‑based invalid‑pair pruning."
        )

    # ───────────────────── session management ───────────────────── #

    async def _get_session(self) -> ClientSession:
        if not self._session or self._session.closed:
            self._session = ClientSession(
                timeout=ClientTimeout(connect=10, sock_read=10)
            )
            self.log.debug("Created new HTTP session.")
        return self._session

    async def close(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None
            self.log.debug("HTTP session closed.")

    # ───────────────────────── public API ───────────────────────── #

    async def get_course_data(
        self,
        course_code: str,
        *,
        hints: Optional[List[Tuple[str, Optional[int]]]] = None,
        detailed: bool = False,
    ) -> Dict[str, Any]:
        department, course_number, suffix_str = self._get_course_keys(course_code)
        cache_key = "detailed" if detailed else "basic"
        now_iso = utcnow().isoformat()

        courses_cache = await self.config.courses()
        cached_entry = self._get_cache_entry(
            courses_cache, department, course_number, suffix_str, cache_key
        )
        threshold = CACHE_PURGE_DAYS if detailed else CACHE_STALE_DAYS_BASIC

        if cached_entry and not self._is_stale(
            cached_entry.get("last_updated", ""), threshold
        ):
            self.log.debug("Using cached %s data for %s", cache_key, course_code)
            return cached_entry

        normalized = CourseCode(course_code).canonical()
        if hints is None:
            await self._maybe_refresh_listings()
            if listing_info := self._listings_cache.get(normalized):
                hints = extract_caption_hints(listing_info)

        self.log.debug("Fetching %s data for %s", cache_key, normalized)
        soup, error_msg = await self._fetch_course_online(normalized, hints=hints)
        if not soup:
            self.log.error(
                "Error fetching %s for %s: %s", cache_key, normalized, error_msg
            )
            if detailed:
                if fallback := self._get_cache_entry(
                    courses_cache, department, course_number, suffix_str, "basic"
                ):
                    self.log.debug("Falling back to basic data for %s", normalized)
                    return fallback
            return {}

        processed = self._process_course_data(soup)
        new_entry: Dict[str, Any] = {
            "cached_course_data": processed,
            "last_updated": now_iso,
        }
        if not detailed:
            new_entry["available_terms"] = await self._determine_term_order_refined(
                normalized
            )

        if new_entry != cached_entry:
            await self._update_cache_entry(
                department, course_number, suffix_str, cache_key, new_entry
            )
            self.log.debug("Updated cache for %s data on %s", cache_key, normalized)

        return new_entry

    # ───────────────────── internal – listings TTL ───────────────────── #

    async def _maybe_refresh_listings(self) -> None:
        if (utcnow() - self._listings_last_update) > timedelta(hours=1):
            listings = await self.config.course_listings()
            self._listings_cache = listings.get("courses", {})
            self._listings_last_update = utcnow()

    # ───────────────────── internal – term resolution ──────────────────── #

    def _term_order_from_hints(
        self, hints: List[Tuple[str, Optional[int]]]
    ) -> List[Tuple[str, int]]:
        current_year = utcnow().year
        ordered: List[Tuple[str, int]] = []
        seen: set = set()
        for season, yr in hints:
            resolved = yr or guess_year_for_term(season, current_year)
            pair = (season, resolved)
            if pair not in seen:
                ordered.append(pair)
                seen.add(pair)
        return ordered

    async def _determine_term_order_refined(
        self, normalized_course: Optional[str] = None
    ) -> List[Tuple[str, int]]:
        await self._maybe_refresh_listings()
        candidate = (
            await self._extract_term_from_listing(normalized_course)
            if normalized_course
            else None
        )

        month_today = utcnow().month
        year_today = utcnow().year

        def year_for(season: str) -> int:
            if season == "winter":
                return year_today + (1 if month_today >= 10 else 0)
            if season == "spring":
                return year_today + (1 if month_today >= 5 else 0)
            if season == "fall":
                return year_today if month_today >= 8 else year_today - 1
            return year_today

        refined = [(s, year_for(s)) for s in TERM_NAMES]
        if candidate and candidate in refined:
            refined.remove(candidate)
        if candidate:
            refined.insert(0, candidate)
        self.log.debug("Refined term order: %s", refined)
        return refined

    def _determine_term_order_fallback(self) -> List[Tuple[str, int]]:
        year_today = utcnow().year
        fallback = [(s, year_today) for s in TERM_NAMES]
        self.log.debug("Fallback term order: %s", fallback)
        return fallback

    # ─────────────────────── HTTP + retry logic ─────────────────────── #

    async def _fetch_course_online(
        self,
        normalized_course: str,
        *,
        hints: Optional[List[Tuple[str, Optional[int]]]] = None,
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        self.log.debug("Fetching online data for %s", normalized_course)

        term_order = (
            self._term_order_from_hints(hints)
            if hints
            else await self._determine_term_order_refined(normalized_course)
        )
        self.log.debug("Term candidates: %s", term_order)

        last_error: Optional[str] = None
        now = utcnow()
        for season, year in term_order:
            term_key = f"{season}-{year}"

            # prune known invalid pairs if still within TTL
            pair = (normalized_course, term_key)
            if pair in self._invalid_course_term_cache:
                if (now - self._invalid_course_term_cache[pair]) < INVALID_PAIR_TTL:
                    self.log.debug("Skipping cached invalid %s @ %s", *pair)
                    continue
                else:
                    del self._invalid_course_term_cache[pair]

            term_id = await self._get_term_id(term_key)
            if not term_id:
                self.log.debug("No term ID for %s; skipping", term_key)
                continue

            url = self._build_url(term_id, normalized_course)
            for attempt in range(MAX_ATTEMPTS):
                if attempt:
                    delay = BASE_DELAY_SECONDS * 2 ** (attempt - 1) + random.uniform(
                        0, BASE_DELAY_SECONDS
                    )
                    await asyncio.sleep(delay)

                self.log.debug("Attempt %d for URL %s", attempt + 1, url)
                soup, err = await self._fetch_and_parse(url)
                if soup or (err and "not found" in err.lower()):
                    if err and "not found" in err.lower():
                        self._invalid_course_term_cache[pair] = utcnow()
                    return soup, err
                last_error = err
                if last_error and not last_error.startswith("HTTP 500"):
                    self._invalid_course_term_cache[pair] = utcnow()
                    break

        return None, last_error or "Unknown error fetching course data."

    async def _fetch_and_parse(
        self, url: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        session = await self._get_session()
        try:
            async with session.get(url) as resp:
                if resp.status == 500:
                    return None, "HTTP 500"
                if resp.status != 200:
                    return None, f"HTTP {resp.status}"
                text = await resp.text()
        except (
            ClientResponseError,
            ClientConnectionError,
            asyncio.TimeoutError,
        ) as exc:
            self.log.exception("HTTP error: %s", exc)
            return None, "network error"
        except Exception as exc:
            self.log.exception("Unexpected error: %s", exc)
            return None, "unexpected error"

        soup = BeautifulSoup(text, XML_PARSER)
        if err_tag := soup.find("error"):
            return None, err_tag.text.strip() or "unknown remote error"
        return soup, None

    # ─────────────────────── term‑id caching ─────────────────────── #

    async def _get_term_id(self, term_key: str) -> Optional[int]:
        now = utcnow()
        if (now - self._term_codes_last_update) > timedelta(hours=1):
            self._term_codes_cache = await self.config.term_codes()
            self._term_codes_last_update = now
        return self._term_codes_cache.get(term_key.lower())

    # ───────────────────────── caching utils ───────────────────────── #

    @staticmethod
    def _get_course_keys(course_code: str) -> Tuple[str, str, str]:
        obj = CourseCode(course_code)
        return obj.department, obj.code, obj.suffix or "__nosuffix__"

    @staticmethod
    def _get_cache_entry(
        courses: Dict[str, Any], department: str, code: str, suffix: str, key: str
    ) -> Optional[Dict[str, Any]]:
        return courses.get(department, {}).get(code, {}).get(suffix, {}).get(key)

    async def _update_cache_entry(
        self,
        department: str,
        code: str,
        suffix: str,
        key: str,
        value: Dict[str, Any],
    ) -> None:
        async with self.config.courses() as courses_update:
            dept = courses_update.setdefault(department, {})
            course_dict = dept.setdefault(code, {})
            suffix_dict = course_dict.setdefault(suffix, {})
            suffix_dict[key] = value

    @staticmethod
    def _is_stale(last_updated_str: str, threshold_days: int) -> bool:
        try:
            dt = datetime.fromisoformat(last_updated_str)
            if not dt.tzinfo:
                dt = dt.replace(tzinfo=timezone.utc)
            return utcnow() - dt > timedelta(days=threshold_days)
        except ValueError:
            return True

    # ─────────────────── parsing functions ─────────────────── #

    @staticmethod
    def _get_tag_attr(tag: Optional[Tag], attr: str, default: str = "") -> str:
        return tag.get(attr, default) if tag else default

    def _process_course_data(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        courses = soup.find_all("course")
        self.log.debug("Found %d course entries", len(courses))
        result: List[Dict[str, Any]] = []
        for c in courses:
            offering = c.find("offering")
            title = self._get_tag_attr(offering, "title")
            desc, prereq, antireq = self._parse_offering(offering)
            selection = c.find("selection")
            credits = self._get_tag_attr(selection, "credits")
            teacher = self._get_tag_attr(c.find("block"), "teacher")
            result.append(
                {
                    "title": title,
                    "term_found": self._get_tag_attr(c.find("term"), "v"),
                    "teacher": teacher,
                    "course_code": c.get("code", ""),
                    "course_number": c.get("number", ""),
                    "credits": credits,
                    "description": desc,
                    "prerequisites": prereq,
                    "antirequisites": antireq,
                }
            )
        return result

    def _parse_offering(self, offering: Optional[Tag]) -> Tuple[str, str, str]:
        description = prerequisites = antirequisites = ""
        if not offering:
            return description, prerequisites, antirequisites
        if raw := offering.get("descr", offering.get("desc", "")):
            lines = [ln.strip() for ln in BR_TAG_REGEX.split(raw) if ln.strip()]
            if lines:
                description = lines[0]
            for ln in lines:
                lower = ln.lower()
                if lower.startswith("prerequisite"):
                    prerequisites = ln.split(":", 1)[1].strip() if ":" in ln else ""
                elif lower.startswith("antirequisite"):
                    antirequisites = ln.split(":", 1)[1].strip() if ":" in ln else ""
        return description, prerequisites, antirequisites

    async def update_course_listing(self) -> Optional[str]:
        self.log.debug("Retrieving full course listings")
        soup, error_msg = await self._fetch_and_parse(LISTING_URL)
        if soup:
            listing = self._process_course_listing(soup)
            await self.config.course_listings.set(
                {"courses": listing, "date_updated": utcnow().isoformat()}
            )
            self.log.debug("Cached %d listings", len(listing))
            return str(len(listing))
        if error_msg:
            self.log.error("Error fetching listings: %s", error_msg)
            return "0"
        return None

    def _process_course_listing(self, soup: BeautifulSoup) -> Dict[str, str]:
        entries = soup.find_all("rs")
        self.log.debug("Found %d listing entries", len(entries))
        out: Dict[str, str] = {}
        for rs in entries:
            code_raw = rs.text.strip()
            try:
                code_norm = CourseCode(code_raw).canonical()
            except ValueError:
                self.log.exception("Invalid code %s", code_raw)
                continue
            info = BR_TAG_REGEX.sub(" ", rs.get("info", ""))
            out[code_norm] = info
        return out

    async def force_mark_stale(self, course_code: str, detailed: bool = True) -> bool:
        dept, num, suffix = self._get_course_keys(course_code)
        key = "detailed" if detailed else "basic"
        cache = await self.config.courses()
        if entry := self._get_cache_entry(cache, dept, num, suffix, key):
            entry["last_updated"] = "1970-01-01T00:00:00"
            await self._update_cache_entry(dept, num, suffix, key, entry)
            self.log.debug("Marked %s stale for %s", key, course_code)
            return True
        return False

    async def _extract_term_from_listing(
        self, normalized_course: str
    ) -> Optional[Tuple[str, int]]:
        info = self._listings_cache.get(normalized_course)
        if not info:
            return None

        candidates = {
            (m.group("term").lower(), int(m.group("year")))
            for m in PATTERN_TERM_YEAR.finditer(info)
        } | {
            (m.group("term").lower(), int(m.group("year")))
            for m in PATTERN_YEAR_TERM.finditer(info)
        }
        if not candidates:
            return None

        rank = {"winter": 1, "spring": 2, "fall": 3}
        now_year = utcnow().year
        future = [c for c in candidates if c[1] >= now_year]
        chosen = min(future or candidates, key=lambda x: (x[1], rank.get(x[0], 99)))
        self.log.debug("Extracted term %s → %s", normalized_course, chosen)
        return chosen

    def _build_url(self, term_id: int, normalized_course: str) -> str:
        time_code, entropy = self._generate_time_code()
        return URL_BASE.format(
            term=term_id,
            course_key=normalized_course,
            time_code=time_code,
            entropy=entropy,
        )

    @staticmethod
    def _generate_time_code() -> Tuple[int, int]:
        from math import floor
        from time import time

        time_code = floor(time() / 60) % 1000
        entropy = time_code % 3 + time_code % 39 + time_code % 42
        return time_code, entropy
