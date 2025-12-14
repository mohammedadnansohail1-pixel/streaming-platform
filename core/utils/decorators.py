"""Reusable decorators for the application."""

import functools
import time
from typing import Callable, Type

from core.utils.logging import get_logger

logger = get_logger(__name__)


def log_time(func: Callable) -> Callable:
    """
    Log execution time of a function.

    Usage:
        @log_time
        def slow_function():
            ...
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        logger.info(f"{func.__name__} completed in {elapsed:.3f}s")
        return result

    return wrapper


def log_call(func: Callable) -> Callable:
    """
    Log when function is called and returns.

    Usage:
        @log_call
        def my_function(x, y):
            ...
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.debug(f"Calling {func.__name__}")
        result = func(*args, **kwargs)
        logger.debug(f"{func.__name__} returned")
        return result

    return wrapper


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    exceptions: tuple[Type[Exception], ...] = (Exception,),
) -> Callable:
    """
    Retry function on failure.

    Args:
        max_attempts: Number of attempts before giving up
        delay: Seconds between retries
        exceptions: Tuple of exceptions to catch

    Usage:
        @retry(max_attempts=3, delay=2.0)
        def flaky_api_call():
            ...
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts:
                        logger.warning(
                            f"{func.__name__} failed (attempt {attempt}/{max_attempts}): {e}. "
                            f"Retrying in {delay}s..."
                        )
                        time.sleep(delay)
                    else:
                        logger.error(
                            f"{func.__name__} failed after {max_attempts} attempts"
                        )
            raise last_exception

        return wrapper

    return decorator


def validate_args(**validators) -> Callable:
    """
    Validate function arguments.

    Args:
        **validators: arg_name=validator_func pairs

    Usage:
        @validate_args(count=lambda x: x > 0, name=lambda x: len(x) > 0)
        def create_items(count, name):
            ...
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get function argument names
            import inspect

            sig = inspect.signature(func)
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()

            # Validate each argument
            for arg_name, validator in validators.items():
                if arg_name in bound.arguments:
                    value = bound.arguments[arg_name]
                    if not validator(value):
                        raise ValueError(f"Invalid value for '{arg_name}': {value}")

            return func(*args, **kwargs)

        return wrapper

    return decorator
