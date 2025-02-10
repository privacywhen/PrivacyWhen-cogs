from typing import Optional
from .constants import COURSE_KEY_PATTERN


def format_course_key(course_key_raw: str) -> Optional[str]:
    """Format a raw course key into the standardized format."""
    match = COURSE_KEY_PATTERN.match(course_key_raw)
    if not match:
        return None
    subject, number, suffix = match.groups()
    return f"{subject.upper()}-{number.upper()}{suffix.upper() if suffix else ''}"


def get_channel_name(course_key: str) -> str:
    """Derive a channel name from a course key."""
    base = course_key[:-1] if course_key and course_key[-1].isalpha() else course_key
    return base.lower()
