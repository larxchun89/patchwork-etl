"""Retry logic for ETL pipeline steps.

Provides configurable retry behavior with exponential backoff,
jitter, and customizable retry conditions.
"""

import time
import logging
import random
from functools import wraps
from typing import Callable, Optional, Tuple, Type, Union

logger = logging.getLogger(__name__)


class RetryExhausted(Exception):
    """Raised when all retry attempts have been exhausted."""

    def __init__(self, attempts: int, last_exception: Exception):
        self.attempts = attempts
        self.last_exception = last_exception
        super().__init__(
            f"Failed after {attempts} attempt(s). "
            f"Last error: {type(last_exception).__name__}: {last_exception}"
        )


class RetryConfig:
    """Configuration for retry behavior.

    Attributes:
        max_attempts: Maximum number of total attempts (including the first).
        base_delay: Initial delay in seconds between retries.
        max_delay: Maximum delay in seconds between retries.
        backoff_factor: Multiplier applied to delay after each retry.
        jitter: If True, adds random jitter to delay to avoid thundering herd.
        retryable_exceptions: Tuple of exception types that should trigger a retry.
                              Defaults to retrying on any Exception.
    """

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        backoff_factor: float = 2.0,
        jitter: bool = True,
        retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
    ):
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if base_delay < 0:
            raise ValueError("base_delay must be non-negative")
        if backoff_factor < 1:
            raise ValueError("backoff_factor must be >= 1")

        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.jitter = jitter
        self.retryable_exceptions = retryable_exceptions

    def get_delay(self, attempt: int) -> float:
        """Calculate the delay before the next retry attempt.

        Args:
            attempt: The current attempt number (0-indexed).

        Returns:
            Delay in seconds.
        """
        delay = min(self.base_delay * (self.backoff_factor ** attempt), self.max_delay)
        if self.jitter:
            delay *= 0.5 + random.random() * 0.5
        return delay

    def is_retryable(self, exc: Exception) -> bool:
        """Determine whether an exception should trigger a retry."""
        return isinstance(exc, self.retryable_exceptions)


def with_retry(
    func: Optional[Callable] = None,
    *,
    config: Optional[RetryConfig] = None,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    backoff_factor: float = 2.0,
    jitter: bool = True,
    retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
) -> Callable:
    """Decorator that adds retry logic to a function.

    Can be used with or without arguments:

        @with_retry
        def my_func(): ...

        @with_retry(max_attempts=5, base_delay=2.0)
        def my_func(): ...

        @with_retry(config=RetryConfig(max_attempts=5))
        def my_func(): ...
    """
    if config is None:
        config = RetryConfig(
            max_attempts=max_attempts,
            base_delay=base_delay,
            max_delay=max_delay,
            backoff_factor=backoff_factor,
            jitter=jitter,
            retryable_exceptions=retryable_exceptions,
        )

    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc: Optional[Exception] = None
            for attempt in range(config.max_attempts):
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:
                    if not config.is_retryable(exc):
                        logger.debug(
                            "Non-retryable exception in '%s': %s",
                            fn.__name__,
                            exc,
                        )
                        raise

                    last_exc = exc
                    remaining = config.max_attempts - attempt - 1

                    if remaining == 0:
                        break

                    delay = config.get_delay(attempt)
                    logger.warning(
                        "Attempt %d/%d failed for '%s': %s. Retrying in %.2fs...",
                        attempt + 1,
                        config.max_attempts,
                        fn.__name__,
                        exc,
                        delay,
                    )
                    time.sleep(delay)

            raise RetryExhausted(config.max_attempts, last_exc)

        return wrapper

    # Support bare @with_retry usage (no parentheses)
    if func is not None:
        return decorator(func)

    return decorator
