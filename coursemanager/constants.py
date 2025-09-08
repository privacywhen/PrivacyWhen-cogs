"""Constants and global defaults for the Course Manager cog."""

from typing import Any

GLOBAL_DEFAULTS: dict[str, Any] = {
    "default_category": "CHANNELS",  # Default category for channels
    "grouping_threshold": 2,  # Minimum overlap threshold for grouping
    "grouping_interval": 1800,  # Grouping interval in seconds
    "course_groups": {},  # Dictionary for course groups mapping
    "course_category": "COURSES",  # Category name for courses
    "term_codes": {},  # Term codes mapping
    "courses": {},  # Cached courses data
    "course_listings": {},  # Cached course listings
    "enabled_guilds": [],  # List of guild IDs where Course Manager is enabled
    "channel_prune_history_limit": 15,  # Messages to scan for pruning activity
    "channel_prune_interval": 86400,  # Interval in seconds for pruning task
    "prune_threshold_days": 120,  # Number of days of inactivity to trigger pruning
}


MAX_CATEGORY_CHANNELS: int = 30
MIN_CATEGORY_CHANNELS: int = 5
MIN_DYNAMIC_THRESHOLD = 1
MIN_SPARSE_OVERLAP = 1
# course_code_resolver
REACTION_OPTIONS: list[str] = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "❌"]
FUZZY_LIMIT: int = 5
FUZZY_SCORE_CUTOFF: int = 70
SCORE_MARGIN: int = 10

GROUPING_INTERVAL: int = GLOBAL_DEFAULTS["grouping_interval"]  # seconds
RATE_LIMIT_DELAY: float = 0.25
