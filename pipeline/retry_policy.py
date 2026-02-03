import logging
import os

from opentelemetry.metrics import Counter
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class SkipRetry(Exception):
    pass  # Your actual exception definition or import


def should_retry(exception):
    # Return True if we should retry, False otherwise
    return not isinstance(exception, SkipRetry)


def get_retry_policy():
    """
    Returns a retry policy that respects the NO_RETRY environment variable.
    If NO_RETRY is set to any truthy value, returns a no-op decorator.
    """
    no_retry = os.getenv("NO_RETRY", "").lower() in ("true", "1", "yes", "on")

    if no_retry:
        # Return a no-op decorator that just calls the function
        def no_retry_decorator(func):
            return func

        return no_retry_decorator

    # Return the normal retry policy
    return retry(
        reraise=True,
        retry=retry_if_exception(should_retry),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        before_sleep=lambda retry_state: logger.warning(
            f"🔄 Retrying after attempt {retry_state.attempt_number} due to {retry_state.outcome.exception()}"
        ),
    )


# Create the RETRY_POLICY based on environment variable
RETRY_POLICY = get_retry_policy()


class RetryPolicyWithCounter:
    def __init__(self, counter: Counter):
        self.counter = counter
        self.policy = retry(
            reraise=True,
            retry=retry_if_exception(should_retry),
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=4, max=10),
            before_sleep=self._before_sleep_with_counter,
        )

    def _before_sleep_with_counter(self, retry_state):
        self.counter.add(1)
        logger.warning(
            f"🔄 Retrying after attempt {retry_state.attempt_number} due to {retry_state.outcome.exception()} (total retries: {self.counter})"
        )

    def __call__(self, func):
        return self.policy(func)
