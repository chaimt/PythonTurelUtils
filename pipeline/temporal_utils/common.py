from dataclasses import dataclass

from retry_policy import SkipRetry
from temporalio.common import RetryPolicy


def all_subclasses(cls):
    subclasses = set(cls.__subclasses__())
    for sub in cls.__subclasses__():
        subclasses |= all_subclasses(sub)
    return subclasses


all_non_retryable_error_types = list({subclass.__name__ for subclass in all_subclasses(SkipRetry)})


@dataclass
class SkipRetryPolicy(RetryPolicy):
    """Retry policy that skips retries for all non-retryable error types."""

    non_retryable_error_types: list[str] = None  # type: ignore[assignment]

    def __post_init__(self):
        if self.non_retryable_error_types is None:
            object.__setattr__(self, "non_retryable_error_types", all_non_retryable_error_types)
