import pytest

from recovery_engine.domain.errors import InvalidTransitionError
from recovery_engine.domain.models import AttemptOutcome, OperationStatus
from recovery_engine.domain.outcomes import classify_http_status
from recovery_engine.domain.state_machine import (
    ALLOWED_TRANSITIONS,
    RECOVERABLE_STATUSES,
    TERMINAL_STATUSES,
    can_transition,
    ensure_transition,
    status_after,
)

S = OperationStatus


def test_every_status_has_transition_rules():
    assert set(ALLOWED_TRANSITIONS) == set(OperationStatus)


def test_terminal_statuses():
    assert {S.SUCCEEDED, S.FAILED_PERMANENT} == TERMINAL_STATUSES


@pytest.mark.parametrize(
    ("source", "target"),
    [
        (S.PENDING, S.IN_PROGRESS),
        (S.IN_PROGRESS, S.SUCCEEDED),
        (S.IN_PROGRESS, S.FAILED_RETRYABLE),
        (S.IN_PROGRESS, S.OUTCOME_UNKNOWN),
        (S.IN_PROGRESS, S.FAILED_PERMANENT),
        # Preparadas para la Etapa 2:
        (S.FAILED_RETRYABLE, S.IN_PROGRESS),
        (S.OUTCOME_UNKNOWN, S.IN_PROGRESS),
        (S.OUTCOME_UNKNOWN, S.SUCCEEDED),
    ],
)
def test_allowed_transitions(source, target):
    assert can_transition(source, target)
    ensure_transition(source, target)


@pytest.mark.parametrize(
    ("source", "target"),
    [
        (S.PENDING, S.SUCCEEDED),
        (S.PENDING, S.FAILED_PERMANENT),
        (S.SUCCEEDED, S.IN_PROGRESS),
        (S.FAILED_PERMANENT, S.IN_PROGRESS),
        (S.FAILED_RETRYABLE, S.SUCCEEDED),
        # Un timeout no autoriza a dar la operación por fallida.
        (S.OUTCOME_UNKNOWN, S.FAILED_PERMANENT),
        (S.OUTCOME_UNKNOWN, S.FAILED_RETRYABLE),
    ],
)
def test_forbidden_transitions(source, target):
    assert not can_transition(source, target)
    with pytest.raises(InvalidTransitionError):
        ensure_transition(source, target)


def test_recoverable_statuses_are_not_terminal():
    assert not (RECOVERABLE_STATUSES & TERMINAL_STATUSES)


@pytest.mark.parametrize(
    ("outcome", "expected"),
    [
        (AttemptOutcome.SUCCESS, S.SUCCEEDED),
        (AttemptOutcome.RETRYABLE_ERROR, S.FAILED_RETRYABLE),
        (AttemptOutcome.RATE_LIMITED, S.FAILED_RETRYABLE),
        (AttemptOutcome.TIMEOUT, S.OUTCOME_UNKNOWN),
        (AttemptOutcome.PERMANENT_ERROR, S.FAILED_PERMANENT),
    ],
)
def test_status_after_outcome(outcome, expected):
    assert status_after(outcome) is expected


@pytest.mark.parametrize(
    ("http_status", "expected"),
    [
        (200, AttemptOutcome.SUCCESS),
        (201, AttemptOutcome.SUCCESS),
        (429, AttemptOutcome.RATE_LIMITED),
        (500, AttemptOutcome.RETRYABLE_ERROR),
        (503, AttemptOutcome.RETRYABLE_ERROR),
        (400, AttemptOutcome.PERMANENT_ERROR),
        (409, AttemptOutcome.PERMANENT_ERROR),
        (422, AttemptOutcome.PERMANENT_ERROR),
    ],
)
def test_classify_http_status(http_status, expected):
    assert classify_http_status(http_status) is expected
