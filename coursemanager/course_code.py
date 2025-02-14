"""
This module provides a unified approach to parsing, normalizing, and converting course codes.
The CourseCode class extracts and standardizes course code information, ensuring consistency
across the codebase.

Example:
    cc = CourseCode("socwork-2a06a")
    print(cc.canonical())    # Output: "SOCWORK-2A06A"
    print(cc.channel_name()) # Output: "socwork-2a06"
    print(cc.department)     # Output: "SOCWORK"
    print(cc.code)           # Output: "2A06" (the core code)
    print(cc.suffix)         # Output: "A" (if present)
"""

import re
from .logger_util import get_logger, log_entry_exit

log = get_logger("red.course_code")


class CourseCode:
    # Precompiled regex pattern to capture the three components:
    #   - Department: one or more letters
    #   - Code: a numeric/alphanumeric sequence (starting with a digit, then two alphanumerics, ending with a digit)
    #   - Optional Suffix: a single trailing letter (if present)
    _pattern = re.compile(r"^\s*([A-Za-z]+)[\s\-_]*(\d[A-Za-z0-9]{2}\d)([A-Za-z])?\s*$")

    def __init__(self, raw: str) -> None:
        """
        Initialize a CourseCode object by parsing the raw course code input.

        Args:
            raw (str): The raw course code string (e.g., "socwork-2a06a").

        Raises:
            ValueError: If the provided input does not match the expected course code pattern.
        """
        self._raw = raw
        self._parse()

    def _parse(self) -> None:
        """Parse the raw course code string using the defined regex pattern."""
        match = self._pattern.match(self._raw)
        if match is None:
            log.error(f"Failed to parse course code: '{self._raw}'")
            raise ValueError(f"Invalid course code format: '{self._raw}'")
        self._department, self._code, suffix = match.group(1, 2, 3)
        self._department = self._department.upper()
        self._code = self._code.upper()
        self._suffix = suffix.upper() if suffix else ""

    @property
    def raw(self) -> str:
        return self._raw

    @property
    def department(self) -> str:
        return self._department

    @property
    def code(self) -> str:
        return self._code

    @property
    def suffix(self) -> str:
        return self._suffix

    def canonical(self) -> str:
        """Return the canonical representation of the course code."""
        return f"{self.department}-{self.code}{self._suffix}"

    def formatted_channel_name(self) -> str:
        """Return a formatted channel name based on the course code."""
        return f"{self.department.lower()}-{self.code.lower()}"

    def __str__(self) -> str:
        return self.canonical()
