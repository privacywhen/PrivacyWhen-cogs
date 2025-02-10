from typing import Optional
from .constants import COURSE_KEY_PATTERN


def format_course_key(course_key_raw: str) -> Optional[str]:
    """Format a raw course key into the standardized format.

    This function uses a regular expression to capture the subject, numeric
    component, and an optional variant suffix. The output is normalized so that
    the subject and numeric portion are in uppercase and separated by a hyphen.
    """
    match = COURSE_KEY_PATTERN.match(course_key_raw)
    if not match:
        return None
    subject, number, suffix = match.groups()
    return f"{subject.upper()}-{number.upper()}{suffix.upper() if suffix else ''}"


def get_channel_name(course_key: str) -> str:
    """Derive a channel name from a course key.

    This function first attempts to normalize the provided course key using
    `format_course_key`. If successful, it then removes any trailing alphabetical
    character (assumed to be a variant suffix) so that channels for variants of
    the same course group under a common name. The final channel name is returned
    in lowercase.
    """
    formatted = format_course_key(course_key)
    if formatted is None:
        # If the course key cannot be formatted, fall back to lowercasing the input.
        return course_key.lower()

    # Remove a trailing alphabetical character (variant suffix) if present.
    if formatted[-1].isalpha():
        formatted = formatted[:-1]

    return formatted.lower()
