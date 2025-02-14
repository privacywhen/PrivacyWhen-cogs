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
    MAX_LENGTH = 50  # SECURITY: Maximum allowed length for course code input

    def __init__(self, raw: str) -> None:

        raw_stripped = raw.strip()
        if len(raw_stripped) > self.MAX_LENGTH:
            raise ValueError("Course code input is too long.")
        self._raw = raw_stripped
        self._parse()

    # @log_entry_exit(log)
    def _parse(self) -> None:
        """
        Parse the raw course code into its components:
          - department (alphabetic characters)
          - code (numeric/alphanumeric segment)
          - optional suffix (a trailing letter)

        The components are normalized to uppercase for consistency.
        """
        match = self._pattern.match(self._raw)
        if not match:
            raise ValueError(f"Invalid course code format: '{self._raw}'")
        self._department: str = match.group(1).upper()
        self._code: str = match.group(2).upper()
        self._suffix: str = match.group(3).upper() if match.group(3) else ""

    @property
    # @log_entry_exit(log)
    def raw(self) -> str:
        """
        The original raw course code input.
        """
        return self._raw

    @property
    # @log_entry_exit(log)
    def department(self) -> str:
        """
        The department portion of the course code, normalized to uppercase.
        """
        return self._department

    @property
    # @log_entry_exit(log)
    def code(self) -> str:
        """
        The core course code (the numeric/alphanumeric segment), normalized to uppercase.
        """
        return self._code

    @property
    # @log_entry_exit(log)
    def suffix(self) -> str:
        """
        The optional suffix of the course code (if present), normalized to uppercase.
        """
        return self._suffix

    # @log_entry_exit(log)
    def canonical(self) -> str:
        """
        Get the canonical representation of the course code.

        - Format: UPPERCASE with a hyphen between the department and code, including the suffix if present.

        Returns:
            str: The canonical course code (e.g., "SOCWORK-2A06A").
        """
        return f"{self.department}-{self.code}{self.suffix}"

    # @log_entry_exit(log)
    def formatted_channel_name(self) -> str:
        """
        Get the version of the course code formatted for Discord channel names.

        - Format: lowercase with a hyphen between the department and code, excluding any suffix.

        Returns:
            str: The course code suitable for channel names (e.g., "socwork-2a06").
        """
        return f"{self.department.lower()}-{self.code.lower()}"

    # @log_entry_exit(log)
    def __str__(self) -> str:
        """
        Return the canonical representation when the CourseCode object is printed.
        """
        return self.canonical()
