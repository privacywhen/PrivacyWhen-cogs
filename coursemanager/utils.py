from typing import Optional, List
import discord
from datetime import datetime, timezone, timedelta
import logging


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


def get_categories_by_prefix(
    guild: discord.Guild, prefix: str
) -> List[discord.CategoryChannel]:
    matching = [
        cat for cat in guild.categories if cat.name.upper().startswith(prefix.upper())
    ]
    logger.debug(
        f"get_categories_by_prefix: Found {len(matching)} categories in guild '{guild.name}' with prefix '{prefix}'"
    )
    return matching


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
            logger.debug(
                f"get_or_create_category: Created category '{category_name}' in guild '{guild.name}'"
            )
        except discord.Forbidden:
            logger.error(
                f"get_or_create_category: No permission to create category '{category_name}' in guild '{guild.name}'"
            )
            return None
    else:
        logger.debug(
            f"get_or_create_category: Found existing category '{category_name}' in guild '{guild.name}'"
        )
    return category
