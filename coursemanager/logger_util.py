"""Utilities for logging function entry/exit and creating loggers with StreamHandler."""

from __future__ import annotations

import functools
import inspect
import logging
from typing import TYPE_CHECKING, Callable, TypeVar

if TYPE_CHECKING:
    from collections.abc import Awaitable

T = TypeVar("T")


def get_logger(name: str, level: int = logging.DEBUG) -> logging.Logger:
    """Create a logger with a standard StreamHandler if none exists.

    Args:
        name: the logger name (usually `__name__` of the module).
        level: the logging level to set.

    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "[%(levelname)s] %(module)s.%(funcName)s:%(lineno)d: %(message)s",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def log_entry_exit(
    logger: logging.Logger,
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """Decorate functions to log entry and exit, handling both sync and async.

    Args:
        logger: the logger to use for messages.

    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        if inspect.iscoroutinefunction(func):

            async def async_wrapper(*args: object, **kwargs: object) -> T:
                logger.debug("Entering %s", func.__name__)
                try:
                    result = await func(*args, **kwargs)
                except Exception:
                    logger.exception("Exception in %s", func.__name__)
                    raise
                else:
                    logger.debug("Exiting %s", func.__name__)
                    return result

            return functools.wraps(func)(async_wrapper)

        def sync_wrapper(*args: object, **kwargs: object) -> T:
            logger.debug("Entering %s", func.__name__)
            try:
                result = func(*args, **kwargs)
            except Exception:
                logger.exception("Exception in %s", func.__name__)
                raise
            else:
                logger.debug("Exiting %s", func.__name__)
                return result

        return functools.wraps(func)(sync_wrapper)

    return decorator
