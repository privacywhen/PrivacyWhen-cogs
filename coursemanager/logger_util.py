# logger_util.py
import asyncio
import functools
import logging
from typing import Any, Callable, TypeVar

T = TypeVar("T")


def get_logger(name: str, level: int = logging.DEBUG) -> logging.Logger:
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
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any):
            logger.debug(f"Entering {func.__name__}")
            try:
                result = func(*args, **kwargs)
            except Exception as exc:
                logger.exception(f"Exception in {func.__name__}: {exc}")
                raise
            if asyncio.iscoroutine(result):

                async def coro_wrapper():
                    try:
                        res = await result
                        logger.debug(f"Exiting {func.__name__}")
                        return res
                    except Exception as exc:
                        logger.exception(f"Exception in {func.__name__}: {exc}")
                        raise

                return coro_wrapper()
            else:
                logger.debug(f"Exiting {func.__name__}")
                return result

        return wrapper

    return decorator
