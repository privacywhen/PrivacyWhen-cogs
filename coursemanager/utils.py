from typing import Optional, List
import discord
from datetime import datetime, timezone, timedelta
import logging
from .constants import COURSE_KEY_PATTERN, REACTION_OPTIONS


def get_logger(name: str, level: int = logging.DEBUG) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("[%(levelname)s] %(name)s: %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


logger = get_logger(__name__)


def format_course_key(course_key_raw: str) -> Optional[str]:
    match = COURSE_KEY_PATTERN.match(course_key_raw)
    if not match:
        return None
    subject, number, suffix = match.groups()
    return f"{subject.upper()}-{number.upper()}{(suffix.upper() if suffix else '')}"


def get_channel_name(course_key: str) -> str:
    formatted = format_course_key(course_key)
    if formatted is None:
        return course_key.lower()
    if formatted[-1].isalpha():
        formatted = formatted[:-1]
    return formatted.lower()


def get_categories_by_prefix(
    guild: discord.Guild, prefix: str
) -> List[discord.CategoryChannel]:
    return [
        cat for cat in guild.categories if cat.name.upper().startswith(prefix.upper())
    ]


async def prune_channel(
    channel: discord.TextChannel, threshold: timedelta, reason: str
) -> bool:
    try:
        last_user_message = None
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
