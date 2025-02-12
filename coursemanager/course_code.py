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
from .utils import get_logger

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
        self._department = match.group(1).upper()
        self._code = match.group(2).upper()
        self._suffix = match.group(3).upper() if match.group(3) else ""

    @property
    def raw(self) -> str:
        """
        The original raw course code input.
        """
        return self._raw

    @property
    def department(self) -> str:
        """
        The department portion of the course code, normalized to uppercase.
        """
        return self._department

    @property
    def code(self) -> str:
        """
        The core course code (the numeric/alphanumeric segment), normalized to uppercase.
        """
        return self._code

    @property
    def suffix(self) -> str:
        """
        The optional suffix of the course code (if present), normalized to uppercase.
        """
        return self._suffix

    def canonical(self) -> str:
        """
        Get the canonical representation of the course code.

        - Format: UPPERCASE with a hyphen between the department and code, including the suffix if present.

        Returns:
            str: The canonical course code (e.g., "SOCWORK-2A06A").
        """
        return f"{self.department}-{self.code}{self.suffix}"

    def formatted_channel_name(self) -> str:
        """
        Get the version of the course code formatted for Discord channel names.

        - Format: lowercase with a hyphen between the department and code, excluding any suffix.

        Returns:
            str: The course code suitable for channel names (e.g., "socwork-2a06").
        """
        return f"{self.department.lower()}-{self.code.lower()}"

    def __str__(self) -> str:
        """
        Return the canonical representation when the CourseCode object is printed.
        """
        return self.canonical()
