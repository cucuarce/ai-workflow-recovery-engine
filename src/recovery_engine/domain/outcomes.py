"""Clasificación determinística de respuestas del servicio externo."""

from __future__ import annotations

from recovery_engine.domain.models import AttemptOutcome


def classify_http_status(status_code: int) -> AttemptOutcome:
    if 200 <= status_code < 300:
        return AttemptOutcome.SUCCESS
    if status_code == 429:
        return AttemptOutcome.RATE_LIMITED
    if status_code >= 500:
        return AttemptOutcome.RETRYABLE_ERROR
    # Resto de 4xx (y códigos inesperados): reintentar no cambiaría el resultado.
    return AttemptOutcome.PERMANENT_ERROR
