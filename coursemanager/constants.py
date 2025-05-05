from typing import Any, Dict, List

GLOBAL_DEFAULTS: Dict[str, Any] = {
    "default_category": "CHANNELS",  # Default category for channels
    "grouping_threshold": 2,  # Minimum overlap threshold for grouping
    "grouping_interval": 3600,  # Grouping interval in seconds
    "course_groups": {},  # Dictionary for course groups mapping
    "course_category": "COURSES",  # Category name for courses
    "term_codes": {},  # Term codes mapping
    "courses": {},  # Cached courses data
    "course_listings": {},  # Cached course listings
    "enabled_guilds": [],  # List of guild IDs where Course Manager is enabled
    "channel_prune_history_limit": 10,  # Number of messages to check for activity in pruning
    "channel_prune_interval": 2628000,  # Interval in seconds for pruning task
    "prune_threshold_days": 7,  # Number of days of inactivity to trigger pruning
}

REACTION_OPTIONS: List[str] = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "❌"]
MAX_CATEGORY_CHANNELS: int = 50
