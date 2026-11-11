import random
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import pytest

from recovery_engine.domain.models import AttemptOutcome
from recovery_engine.domain.retry import (
    EqualJitter,
    RetryPolicy,
    backoff_delay,
    decide_retry,
    no_jitter,
    parse_retry_after,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)
POLICY = RetryPolicy(max_attempts=5, base_delay_seconds=1.0, max_delay_seconds=10.0)


@pytest.mark.parametrize(
    ("attempts_made", "expected"), [(1, 1.0), (2, 2.0), (3, 4.0), (4, 8.0), (5, 10.0), (20, 10.0)]
)
def test_backoff_is_exponential_and_capped(attempts_made, expected):
    assert backoff_delay(attempts_made, POLICY) == expected


def test_backoff_applies_injected_jitter_and_keeps_the_cap():
    assert backoff_delay(3, POLICY, jitter=lambda d: d / 2) == 2.0
    assert backoff_delay(3, POLICY, jitter=lambda d: d * 100) == POLICY.max_delay_seconds
    assert backoff_delay(3, POLICY, jitter=lambda d: -5) == 0.0


def test_equal_jitter_is_reproducible_with_seed_and_bounded():
    a = EqualJitter(random.Random(42))
    b = EqualJitter(random.Random(42))
    values = [a(8.0) for _ in range(50)]
    assert values == [b(8.0) for _ in range(50)]
    assert all(4.0 <= v <= 8.0 for v in values)


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("120", 120.0),
        ("  7 ", 7.0),
        ("0", 0.0),
        (None, None),
        ("", None),
        ("   ", None),
        ("-5", None),
        ("1.5", None),
        ("soon", None),
        ("²", None),  # dígito unicode: no es delay-seconds válido
        ("Mon, 99 Foo 2026 00:00:00 GMT", None),
    ],
)
def test_parse_retry_after_seconds_and_invalid_values(header, expected):
    assert parse_retry_after(header, NOW) == expected


def test_parse_retry_after_http_date():
    future = format_datetime(NOW + timedelta(seconds=45), usegmt=True)
    past = format_datetime(NOW - timedelta(hours=1), usegmt=True)
    assert parse_retry_after(future, NOW) == 45.0
    assert parse_retry_after(past, NOW) == 0.0


def test_permanent_and_timeout_outcomes_are_not_retried_by_policy():
    for outcome in (AttemptOutcome.PERMANENT_ERROR, AttemptOutcome.TIMEOUT):
        decision = decide_retry(outcome, attempts_made=1, policy=POLICY, now=NOW)
        assert not decision.retry


def test_retryable_error_uses_backoff():
    decision = decide_retry(AttemptOutcome.RETRYABLE_ERROR, attempts_made=2, policy=POLICY, now=NOW)
    assert (decision.retry, decision.delay_seconds, decision.reason) == (
        True,
        2.0,
        "exponential_backoff",
    )


def test_max_attempts_stops_retries():
    decision = decide_retry(AttemptOutcome.RETRYABLE_ERROR, attempts_made=5, policy=POLICY, now=NOW)
    assert not decision.retry
    assert decision.describe() == "no_retry: max_attempts_exhausted (5)"


def test_rate_limit_respects_retry_after_and_cap():
    policy = RetryPolicy(max_retry_after_seconds=60)
    kwargs = {"attempts_made": 1, "policy": policy, "now": NOW}
    assert decide_retry(AttemptOutcome.RATE_LIMITED, retry_after="30", **kwargs).delay_seconds == 30
    capped = decide_retry(AttemptOutcome.RATE_LIMITED, retry_after="9999", **kwargs)
    assert (capped.delay_seconds, capped.reason) == (60, "retry_after (capped)")
    invalid = decide_retry(AttemptOutcome.RATE_LIMITED, retry_after="x", **kwargs)
    assert invalid.reason == "exponential_backoff; retry-after missing or invalid"


def test_jitter_is_not_applied_to_retry_after():
    decision = decide_retry(
        AttemptOutcome.RATE_LIMITED,
        attempts_made=1,
        policy=POLICY,
        now=NOW,
        retry_after="9",
        jitter=lambda d: d / 2,
    )
    assert decision.delay_seconds == 9.0


@pytest.mark.parametrize(
    "kwargs",
    [{"max_attempts": 0}, {"base_delay_seconds": -1}, {"max_retry_after_seconds": -1}],
)
def test_invalid_policy_is_rejected(kwargs):
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)


def test_no_jitter_is_identity():
    assert no_jitter(3.25) == 3.25
