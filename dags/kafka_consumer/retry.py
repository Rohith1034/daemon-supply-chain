import random
import time


def retry_call(
    operation,
    *,
    max_attempts,
    initial_backoff,
    max_backoff,
    on_retry=None,
    sleep=time.sleep,
    random_value=random.random,
):
    for attempt in range(1, max_attempts + 1):
        try:
            return operation()
        except Exception as exc:
            if attempt == max_attempts:
                raise
            ceiling = min(max_backoff, initial_backoff * (2 ** (attempt - 1)))
            delay = ceiling * (0.5 + random_value() * 0.5)
            if on_retry:
                on_retry(attempt, delay, exc)
            sleep(delay)
