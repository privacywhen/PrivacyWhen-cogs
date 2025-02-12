from typing import Dict, Any, List

GLOBAL_DEFAULTS: Dict[str, Any] = {
    "default_category": "CHANNELS",
    "grouping_threshold": 2,
    "grouping_interval": 3600,
    "course_groups": {},
    "course_category": "COURSES",
    "term_codes": {},
    "courses": {},
    "course_listings": {},
    "enabled_guilds": [],
    # Channel pruning configuration:
    "channel_prune_history_limit": 10,  # Number of messages to check for user activity in a channel
    "channel_prune_interval": 2628000,  # Interval (in seconds) between auto-prune cycles (~30 days)
}

REACTION_OPTIONS: List[str] = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "❌"]
