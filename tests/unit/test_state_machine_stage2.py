import pytest

from recovery_engine.domain.models import OperationStatus
from recovery_engine.domain.state_machine import TERMINAL_STATUSES, can_transition

S = OperationStatus


@pytest.mark.parametrize(
    ("source", "target"),
    [
        (S.IN_PROGRESS, S.DEAD_LETTER),
        (S.IN_PROGRESS, S.OUTCOME_UNKNOWN),  # lease vencido
        (S.DEAD_LETTER, S.OUTCOME_UNKNOWN),  # redrive manual reservado
    ],
)
def test_stage2_allowed_transitions(source, target):
    assert can_transition(source, target)


@pytest.mark.parametrize(
    ("source", "target"),
    [
        # Toda salida de dead_letter que no pase por reconciliación está prohibida.
        (S.DEAD_LETTER, S.IN_PROGRESS),
        (S.DEAD_LETTER, S.SUCCEEDED),
        (S.DEAD_LETTER, S.PENDING),
        # Solo un worker con lease puede decidir dead-letter.
        (S.OUTCOME_UNKNOWN, S.DEAD_LETTER),
        (S.FAILED_RETRYABLE, S.DEAD_LETTER),
        (S.PENDING, S.DEAD_LETTER),
    ],
)
def test_stage2_forbidden_transitions(source, target):
    assert not can_transition(source, target)


def test_dead_letter_requires_intervention_but_is_not_terminal():
    assert S.DEAD_LETTER not in TERMINAL_STATUSES
