"""
Module containing global constants for the course channel cog.
"""

from typing import Any, Dict, List

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
    "channel_prune_history_limit": 10,
    "channel_prune_interval": 2628000,
    "prune_threshold_days": 7,
}

REACTION_OPTIONS: List[str] = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "❌"]
