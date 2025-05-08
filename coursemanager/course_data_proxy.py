"""Manages course data retrieval, caching, term resolution, and HTTP retries/backoff."""

from __future__ import annotations

import asyncio
import random
import re
from datetime import datetime, timedelta, timezone
from math import floor
from re import Pattern
from time import time
from typing import TYPE_CHECKING, Any

from aiohttp import (
    ClientConnectionError,
    ClientResponseError,
    ClientSession,
    ClientTimeout,
)
from bs4 import BeautifulSoup, Tag
from redbot.core import Config  # noqa: TC002

from .course_code import CourseCode
from .logger_util import get_logger
from .utils import utcnow

if TYPE_CHECKING:
    import logging

log = get_logger(__name__)

# ────────────────────────── Constants / Tunables ───────────────────────── #
CACHE_STALE_DAYS_BASIC = 90
CACHE_PURGE_DAYS = 180
INVALID_PAIR_TTL = timedelta(hours=12)

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

HTTP_STATUS_OK = 200
HTTP_STATUS_INTERNAL_ERROR = 500

TERM_NAMES: list[str] = ["winter", "spring", "fall"]
WINTER_THRESHOLD_MONTH = 10
SPRING_THRESHOLD_MONTH = 5
FALL_THRESHOLD_MONTH = 8

PATTERN_TERM_YEAR = re.compile(r"\b(Winter|Spring|Fall)\s+(\d{4})", re.IGNORECASE)
PATTERN_YEAR_TERM = re.compile(r"(\d{4})\s+(Winter|Spring|Fall)", re.IGNORECASE)
PATTERN_TERM_ONLY = re.compile(r"\b(Winter|Spring|Fall)\s+only", re.IGNORECASE)

TRANSIENT_XML_ERRORS: list[str] = [
    "timezone",
    "device's time",
    "correct your clock",
]

# ───────────────────────────── Type Aliases ────────────────────────────── #

CourseData = dict[str, Any]
TermHint = tuple[str, int | None]
TermOrder = list[tuple[str, int]]


# ──────────────────────── Helper Functions ────────────────────────────── #


def extract_caption_hints(caption: str) -> list[TermHint]:
    """Pull out term/year pairs (and 'term only') from a listing caption."""
    hints: list[TermHint] = [
        (term.lower(), int(year)) for term, year in PATTERN_TERM_YEAR.findall(caption)
    ]
    hints.extend(
        (term.lower(), int(year)) for year, term in PATTERN_YEAR_TERM.findall(caption)
    )
    hints.extend((term.lower(), None) for term in PATTERN_TERM_ONLY.findall(caption))
    return hints


class TermHelper:
    """Resolve and rank academic terms for fetching."""

    @staticmethod
    def resolve_term_year(term: str, now: datetime) -> int:
        """Return the resolved year for the given term based on the current date."""
        if term == "winter":
            return now.year + (1 if now.month >= WINTER_THRESHOLD_MONTH else 0)
        if term == "spring":
            return now.year + (1 if now.month >= SPRING_THRESHOLD_MONTH else 0)
        if term == "fall":
            return now.year if now.month >= FALL_THRESHOLD_MONTH else now.year - 1
        return now.year

    @staticmethod
    def hints_to_order(hints: list[TermHint], now: datetime) -> TermOrder:
        """Convert term hints into an ordered list of (season, year) tuples."""
        ordered: TermOrder = []
        seen: set[tuple[str, int]] = set()
        for season, yr in hints:
            resolved = yr or TermHelper.resolve_term_year(season, now)
            pair = (season, resolved)
            if pair not in seen:
                ordered.append(pair)
                seen.add(pair)
        return ordered

    @staticmethod
    def fallback_order(now: datetime) -> TermOrder:
        """Return a fallback list of terms for the current year."""
        return [(season, now.year) for season in TERM_NAMES]


class CourseDataProxy:
    """Fetch, cache, and parse course data with in-memory and persistent TTL."""

    def __init__(self, config: Config, logger: logging.Logger) -> None:
        """Initialize CourseDataProxy with config, logger, session, and caches."""
        self.config = config
        self.log = logger
        self._session: ClientSession | None = None

        self._term_codes_cache: dict[str, int] = {}
        self._term_codes_last_update = datetime.min.replace(tzinfo=timezone.utc)

        self._listings_cache: dict[str, str] = {}
        self._listings_last_update = datetime.min.replace(tzinfo=timezone.utc)

        self._invalid_course_term_cache: dict[tuple[str, str], datetime] = {}

        self.log.debug("CourseDataProxy initialized with TTL caches")

    def _now(self) -> datetime:
        """Return current UTC time. Overrideable in tests."""
        return utcnow()

    async def _get_session(self) -> ClientSession:
        """Ensure an aiohttp session with reasonable timeouts."""
        if not self._session or self._session.closed:
            self._session = ClientSession(
                timeout=ClientTimeout(connect=10, sock_read=10),
            )
            self.log.debug("Created new HTTP session")
        return self._session

    async def close(self) -> None:
        """Shut down the HTTP session if it's open."""
        if self._session:
            await self._session.close()
            self._session = None
            self.log.debug("HTTP session closed")

    async def _load_from_cache(
        self,
        course_code: str,
        *,
        detailed: bool = False,
    ) -> CourseData | None:
        """Return cached entry if within TTL, otherwise None."""
        dept, num, suffix = self._get_course_keys(course_code)
        cache_key = "detailed" if detailed else "basic"
        all_cache = await self.config.courses()
        entry = self._get_cache_entry(all_cache, dept, num, suffix, cache_key)
        threshold = CACHE_PURGE_DAYS if detailed else CACHE_STALE_DAYS_BASIC
        if entry and not self._is_stale(entry.get("last_updated", ""), threshold):
            self.log.debug("Using cached %s data for %s", cache_key, course_code)
            return entry
        return None

    async def _save_to_cache(
        self,
        course_code: str,
        *,
        detailed: bool = False,
        data: CourseData,
    ) -> None:
        """Store fresh course data into persistent config."""
        dept, num, suffix = self._get_course_keys(course_code)
        cache_key = "detailed" if detailed else "basic"
        await self._update_cache_entry(dept, num, suffix, cache_key, data)
        self.log.debug("Saved %s data to cache for %s", cache_key, course_code)

    async def get_course_data(
        self,
        course_code: str,
        *,
        hints: list[TermHint] | None = None,
        detailed: bool = False,
    ) -> CourseData:
        """Return course data, preferring cache.

        Falls back from detailed → basic on failure,
        and persists fresh results back into cache.
        """
        cache_key = "detailed" if detailed else "basic"

        if cached := await self._load_from_cache(course_code, detailed=detailed):
            return cached

        normalized = CourseCode(course_code).canonical()
        if hints is None:
            await self._maybe_refresh_listings()
            if info := self._listings_cache.get(normalized):
                hints = extract_caption_hints(info)

        self.log.debug("Fetching %s data for %s", cache_key, normalized)
        soup, err = await self._fetch_course_online(normalized, hints=hints)
        if not soup:
            self.log.error("Fetch error for %s %s: %s", cache_key, normalized, err)
            if detailed and (
                fallback := await self._load_from_cache(course_code, detailed=False)
            ):
                self.log.debug("Falling back to basic data for %s", normalized)
                return fallback
            return {}

        processed = self._process_course_data(soup)
        new_entry: CourseData = {
            "cached_course_data": processed,
            "last_updated": self._now().isoformat(),
        }
        if not detailed:
            new_entry["available_terms"] = await self._determine_term_order_refined(
                normalized,
            )

        await self._save_to_cache(course_code, detailed=detailed, data=new_entry)
        return new_entry

    # ───────────────────── Internal - Listings TTL ───────────────────── #
    async def _maybe_refresh_listings(self) -> None:
        """Refresh listings cache once per hour."""
        if (self._now() - self._listings_last_update) > timedelta(hours=1):
            listings = await self.config.course_listings()
            self._listings_cache = listings.get("courses", {})
            self._listings_last_update = self._now()

    # ──────────────────── Internal - Term Resolution ──────────────────── #
    async def _determine_term_order_refined(
        self,
        normalized_course: str,
    ) -> TermOrder:
        """Build prioritized term list: extracted candidate first, then others."""
        await self._maybe_refresh_listings()
        candidate: tuple[str, int] | None = await self._extract_term_from_listing(
            normalized_course,
        )

        now = self._now()
        order = [(s, TermHelper.resolve_term_year(s, now)) for s in TERM_NAMES]
        if candidate and candidate in order:
            order.remove(candidate)
            order.insert(0, candidate)

        self.log.debug("Refined term order for %s → %s", normalized_course, order)
        return order

    def _determine_term_order_fallback(self) -> TermOrder:
        """Return fallback term order: Winter, Spring, Fall of current year."""
        order = TermHelper.fallback_order(self._now())
        self.log.debug("Fallback term order → %s", order)
        return order

    # ────────────────── HTTP & Retry Logic ────────────────── #
    async def _fetch_course_online(
        self,
        normalized_course: str,
        *,
        hints: list[TermHint] | None = None,
    ) -> tuple[BeautifulSoup | None, str | None]:
        """Orchestrate fetch attempts over prioritized term orders."""
        now = self._now()
        primary = (
            TermHelper.hints_to_order(hints, now)
            if hints
            else await self._determine_term_order_refined(normalized_course)
        )
        fallback = self._determine_term_order_fallback()
        seen: set[tuple[str, int]] = set()

        for season, year in (*primary, *fallback):
            if (season, year) in seen:
                continue
            seen.add((season, year))
            term_key = f"{season}-{year}"
            pair = (normalized_course, term_key)

            if self._should_skip_invalid(pair):
                self.log.debug("Skipping invalidated %s", term_key)
                continue

            term_id = await self._get_term_id(term_key)
            if not term_id:
                self.log.debug("No term ID for %s", term_key)
                continue

            url = self._build_url(term_id, normalized_course)
            self.log.debug("Attempting fetch for %s (term_id=%s)", term_key, term_id)
            soup, err = await self._fetch_with_backoff(url, term_key, pair)

            if err and any(tok in err.lower() for tok in TRANSIENT_XML_ERRORS):
                self.log.warning("Transient XML error on %s: %s", term_key, err)
                return None, err

            if soup or (err and "not found" in err.lower()):
                if err and "not found" in err.lower():
                    self._record_invalid(pair)
                return soup, err

        return None, "Unknown error fetching course data."

    async def _fetch_with_backoff(
        self,
        url: str,
        term_key: str,
        pair: tuple[str, str],
    ) -> tuple[BeautifulSoup | None, str | None]:
        """Fetch a URL with backoff, handle errors, and invalidate bad pairs."""
        last_err: str | None = None
        for attempt in range(MAX_ATTEMPTS):
            if attempt:
                delay = BASE_DELAY_SECONDS * 2 ** (attempt - 1) + random.uniform(  # noqa: S311
                    0,
                    BASE_DELAY_SECONDS,
                )
                await asyncio.sleep(delay)
            self.log.debug("Fetch attempt %d for URL %s", attempt + 1, url)
            soup, err = await self._fetch_and_parse(url)
            # handle transient XML/parser errors immediately
            if err and any(tok in err.lower() for tok in TRANSIENT_XML_ERRORS):
                self.log.warning("Transient XML error on %s: %s", term_key, err)
                return None, err
            # success or permanent “not found” → return
            if soup or (err and "not found" in err.lower()):
                if err and "not found" in err.lower():
                    self._record_invalid(pair)
                return soup, err
            last_err = err
            # non-500 errors mark this pair invalid
            if last_err and not last_err.startswith(
                f"HTTP {HTTP_STATUS_INTERNAL_ERROR}",
            ):
                self._record_invalid(pair)
                break
        return None, last_err

    def _should_skip_invalid(self, pair: tuple[str, str]) -> bool:
        """Return True if this course-term pair is still in its invalidation TTL."""
        ts = self._invalid_course_term_cache.get(pair)
        if ts and (self._now() - ts) < INVALID_PAIR_TTL:
            return True
        self._invalid_course_term_cache.pop(pair, None)
        return False

    def _record_invalid(self, pair: tuple[str, str]) -> None:
        """Mark a course-term pair as invalid, starting now."""
        self._invalid_course_term_cache[pair] = self._now()

    async def _fetch_and_parse(
        self,
        url: str,
    ) -> tuple[BeautifulSoup | None, str | None]:
        """Perform HTTP GET and parse XML or return an error string."""
        session = await self._get_session()
        try:
            async with session.get(url) as resp:
                if resp.status == HTTP_STATUS_INTERNAL_ERROR:
                    return None, f"HTTP {HTTP_STATUS_INTERNAL_ERROR}"
                if resp.status != HTTP_STATUS_OK:
                    return None, f"HTTP {resp.status}"
                text = await resp.text()
        except (ClientResponseError, ClientConnectionError, asyncio.TimeoutError):
            self.log.exception("HTTP error during GET %s", url)
            return None, "network error"
        except Exception:
            self.log.exception("Unexpected error during GET %s", url)
            return None, "unexpected error"

        soup = BeautifulSoup(text, XML_PARSER)
        if err_tag := soup.find("error"):
            return None, err_tag.text.strip() or "unknown remote error"
        return soup, None

    def _build_url(self, term_id: int, normalized_course: str) -> str:
        """Construct API URL with time_code and entropy."""
        time_code = floor(time() / 60) % 1000
        entropy = time_code % 3 + time_code % 39 + time_code % 42
        return URL_BASE.format(
            term=term_id,
            course_key=normalized_course,
            time_code=time_code,
            entropy=entropy,
        )

    # ─────────────────────── term-id caching ─────────────────────── #
    async def _get_term_id(self, term_key: str) -> int | None:
        """Fetch or refresh term-id cache (hourly TTL)."""
        now = self._now()
        if (now - self._term_codes_last_update) > timedelta(hours=1):
            self._term_codes_cache = await self.config.term_codes()
            self._term_codes_last_update = now
        return self._term_codes_cache.get(term_key.lower())

    # ──────────────────── caching utilities ───────────────────────── #
    @staticmethod
    def _get_course_keys(course_code: str) -> tuple[str, str, str]:
        obj = CourseCode(course_code)
        return obj.department, obj.code, obj.suffix or "__nosuffix__"

    @staticmethod
    def _get_cache_entry(
        courses: dict[str, Any],
        department: str,
        code: str,
        suffix: str,
        key: str,
    ) -> dict[str, Any] | None:
        return courses.get(department, {}).get(code, {}).get(suffix, {}).get(key)

    async def _update_cache_entry(
        self,
        department: str,
        code: str,
        suffix: str,
        key: str,
        value: dict[str, Any],
    ) -> None:
        """Atomically update Config cache for a course entry."""
        async with self.config.courses() as cache:
            dept_dict = cache.setdefault(department, {})
            course_dict = dept_dict.setdefault(code, {})
            suffix_dict = course_dict.setdefault(suffix, {})
            suffix_dict[key] = value

    @staticmethod
    def _is_stale(last_updated: str, threshold: int) -> bool:
        """Return True if an ISO timestamp is older than threshold days."""
        try:
            dt = datetime.fromisoformat(last_updated)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return utcnow() - dt > timedelta(days=threshold)
        except ValueError:
            return True

    # ─────────────────── parsing utilities ────────────────────────── #
    @staticmethod
    def _get_tag_attr(tag: Tag | None, attr: str, default: str = "") -> str:
        """Safely fetch an attribute from a BeautifulSoup Tag."""
        return tag.get(attr, default) if tag else default

    def _process_course_data(self, soup: BeautifulSoup) -> list[dict[str, Any]]:
        """Extract structured course info from fetched XML soup."""
        courses = soup.find_all("course")
        self.log.debug("Found %d <course> entries", len(courses))
        result: list[dict[str, Any]] = []
        for c in courses:
            offering = c.find("offering")
            desc, prereq, antireq = self._parse_offering(offering)
            block = c.find("block")
            result.append(
                {
                    "title": self._get_tag_attr(offering, "title"),
                    "term_found": self._get_tag_attr(c.find("term"), "v"),
                    "teacher": self._get_tag_attr(block, "teacher"),
                    "course_code": c.get("code", ""),
                    "course_number": c.get("number", ""),
                    "credits": self._get_tag_attr(c.find("selection"), "credits"),
                    "description": desc,
                    "prerequisites": prereq,
                    "antirequisites": antireq,
                },
            )
        return result

    def _parse_offering(self, offering: Tag | None) -> tuple[str, str, str]:
        """Parse offering tag for description, prerequisites, antirequisites."""
        desc = prereq = antireq = ""
        if not offering:
            return desc, prereq, antireq
        raw = offering.get("desc", "")
        lines = [ln.strip() for ln in BR_TAG_REGEX.split(raw) if ln.strip()]
        if lines:
            desc = lines[0]
        for ln in lines:
            lower = ln.lower()
            if lower.startswith("prerequisite"):
                prereq = ln.split(":", 1)[1].strip() if ":" in ln else ""
            elif lower.startswith("antirequisite"):
                antireq = ln.split(":", 1)[1].strip() if ":" in ln else ""
        return desc, prereq, antireq

    # ─────────────────── course listings ────────────────────────── #
    async def update_course_listing(self) -> str | None:
        """Fetch and cache full course listings."""
        self.log.debug("Retrieving full course listings")
        soup, err = await self._fetch_and_parse(LISTING_URL)
        if soup:
            listing = self._process_course_listing(soup)
            await self.config.course_listings.set(
                {
                    "courses": listing,
                    "date_updated": self._now().isoformat(),
                },
            )
            self.log.debug("Cached %d listings", len(listing))
            return str(len(listing))
        if err:
            self.log.error("Error fetching listings: %s", err)
            return "0"
        return None

    def _process_course_listing(self, soup: BeautifulSoup) -> dict[str, str]:
        """Parse listing XML into a dict of {course_code: info}."""
        entries = soup.find_all("rs")
        self.log.debug("Found %d listing entries", len(entries))
        out: dict[str, str] = {}
        for rs in entries:
            raw = rs.text.strip()
            try:
                norm = CourseCode(raw).canonical()
            except ValueError:
                self.log.exception("Invalid code %s", raw)
                continue
            info = BR_TAG_REGEX.sub(" ", rs.get("info", ""))
            out[norm] = info
        return out

    async def force_mark_stale(
        self,
        course_code: str,
        *,
        detailed: bool = True,
    ) -> bool:
        """Force a cache entry to be considered stale."""
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
        self,
        normalized_course: str,
    ) -> tuple[str, int] | None:
        """Derive preferred term from cached listing info, if any."""
        info = self._listings_cache.get(normalized_course)
        if not info:
            return None
        candidates = {
            (m.group(1).lower(), int(m.group(2)))
            for m in PATTERN_TERM_YEAR.finditer(info)
        } | {
            (m.group(2).lower(), int(m.group(1)))
            for m in PATTERN_YEAR_TERM.finditer(info)
        }
        if not candidates:
            return None
        rank = {"winter": 1, "spring": 2, "fall": 3}
        now_year = self._now().year
        future = [c for c in candidates if c[1] >= now_year]
        chosen = min(future or candidates, key=lambda x: (x[1], rank.get(x[0], 99)))
        self.log.debug("Extracted term %s → %s", normalized_course, chosen)
        return chosen
