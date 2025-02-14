"""
Module providing logging utilities for the cog.
"""

import logging
import functools
import asyncio
from typing import Callable, TypeVar, Any

T = TypeVar("T")


def get_logger(name: str, level: int = logging.DEBUG) -> logging.Logger:
    """
    Get a configured logger.

    Args:
        name (str): The name of the logger.
        level (int): The logging level.

    Returns:
        logging.Logger: The configured logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "[%(levelname)s] %(module)s.%(funcName)s:%(lineno)d: %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def log_entry_exit(
    logger: logging.Logger,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator to log entry and exit of a function.

    Args:
        logger (logging.Logger): The logger to use.

    Returns:
        Callable: The decorated function.
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> T:
            logger.debug(f"Entering {func.__name__}")
            try:
                result = await func(*args, **kwargs)
                logger.debug(f"Exiting {func.__name__}")
                return result
            except Exception as e:
                logger.exception(f"Exception in {func.__name__}: {e}")
                raise

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> T:
            logger.debug(f"Entering {func.__name__}")
            try:
                result = func(*args, **kwargs)
                logger.debug(f"Exiting {func.__name__}")
                return result
            except Exception as e:
                logger.exception(f"Exception in {func.__name__}: {e}")
                raise

        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper

    return decorator
