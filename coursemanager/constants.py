from typing import Dict, Any, List
import re

GLOBAL_DEFAULTS: Dict[str, Any] = {
    "default_category": "CHANNELS",
    "prune_threshold_days": 30,
    "grouping_threshold": 2,
    "grouping_interval": 3600,
    "course_groups": {},
    "course_category": "COURSES",
    "term_codes": {},
    "courses": {},
    "course_listings": {},
    "enabled_guilds": [],
}

COURSE_KEY_PATTERN: re.Pattern = re.compile(
    r"^\s*([A-Za-z]+)[\s\-_]*(\d+(?:[A-Za-z\d]*\d+)?)([A-Za-z])?\s*$"
)

REACTION_OPTIONS: List[str] = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "❌"]
