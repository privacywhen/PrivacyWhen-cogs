from typing import Optional, List
import discord
from datetime import datetime, timezone, timedelta
import logging
import re

from .constants import COURSE_KEY_PATTERN


def get_logger(name: str, level: int = logging.DEBUG) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        logger.addHandler(logging.StreamHandler())
    return logger


logger = get_logger(__name__)


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


def get_categories_by_prefix(
    guild: discord.Guild, prefix: str
) -> List[discord.CategoryChannel]:
    """Retrieve all category channels in the guild that start with the given prefix (case-insensitive)."""
    return [
        cat for cat in guild.categories if cat.name.upper().startswith(prefix.upper())
    ]


async def prune_channel(
    channel: discord.TextChannel, threshold: timedelta, reason: str
) -> bool:
    """Prune (delete) a channel if its last user activity exceeds the given threshold.

    Returns:
        bool: True if the channel was pruned; otherwise, False.
    """
    try:
        last_user_message: Optional[discord.Message] = None
        async for msg in channel.history(limit=10):
            if not msg.author.bot:
                last_user_message = msg
                break
        last_activity = (
            last_user_message.created_at if last_user_message else channel.created_at
        )
        if datetime.now(timezone.utc) - last_activity > threshold:
            logger.info(
                f"Pruning channel '{channel.name}' in guild '{channel.guild.name}' (last activity: {last_activity})"
            )
            await channel.delete(reason=reason)
            return True
    except Exception:
        logger.exception(
            f"Error pruning channel '{channel.name}' in guild '{channel.guild.name}'"
        )
    return False


async def get_or_create_category(
    guild: discord.Guild, category_name: str
) -> Optional[discord.CategoryChannel]:
    """Retrieve a category by name or create it if it does not exist.

    This helper abstracts the logic used in several parts of the code base.
    """
    category = discord.utils.get(guild.categories, name=category_name)
    if category is None:
        try:
            category = await guild.create_category(category_name)
            logger.debug(f"Created category {category_name} in guild {guild.name}")
        except discord.Forbidden:
            logger.error(
                f"No permission to create category {category_name} in guild {guild.name}"
            )
            return None
    return category
