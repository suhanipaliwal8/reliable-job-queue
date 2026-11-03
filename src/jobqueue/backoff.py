import random


def compute_backoff(
    attempt: int,
    base: float = 2.0,
    cap: float = 300.0,
    jitter: bool = True,
    rng=random,
) -> float:
    """Seconds to wait before retrying after failed attempt number `attempt`.

    attempt 1 -> base, attempt 2 -> base*2, attempt 3 -> base*4 ... up to `cap`.
    With jitter=True the result is random between 50% and 100% of that value.
    """
    if attempt < 1:
        raise ValueError("attempt must be at least 1")

    # Limit the exponent so huge attempt numbers can't overflow.
    exponent = min(attempt - 1, 30)
    delay = min(float(cap), float(base) * (2 ** exponent))

    if jitter:
        half = delay / 2
        delay = half + rng.uniform(0, half)
    return delay