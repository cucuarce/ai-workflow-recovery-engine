"""Política de reintentos: backoff exponencial con tope, jitter inyectable y Retry-After.

Todo es puro y determinístico dado el reloj y la función de jitter.
"""

from __future__ import annotations

import random
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

from recovery_engine.domain.models import AttemptOutcome

Jitter = Callable[[float], float]
"""Recibe el delay calculado y devuelve el delay a usar."""

_DELAY_SECONDS = re.compile(r"\d+", re.ASCII)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 5
    """Máximo de envíos (submit) por operación lógica. Las reconciliaciones no cuentan."""
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 300.0
    max_retry_after_seconds: float = 3600.0
    """Tope de seguridad para Retry-After absurdamente grandes."""

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts debe ser >= 1")
        if self.base_delay_seconds < 0 or self.max_delay_seconds < 0:
            raise ValueError("los delays no pueden ser negativos")
        if self.max_retry_after_seconds < 0:
            raise ValueError("max_retry_after_seconds no puede ser negativo")


def no_jitter(delay: float) -> float:
    return delay


class EqualJitter:
    """Jitter "equal": delay/2 + U(0, delay/2). Inyectable con un ``random.Random`` sembrado."""

    def __init__(self, rng: random.Random | None = None) -> None:
        self._rng = rng or random.Random()

    def __call__(self, delay: float) -> float:
        half = delay / 2
        return half + self._rng.random() * half


def backoff_delay(attempts_made: int, policy: RetryPolicy, jitter: Jitter = no_jitter) -> float:
    """Delay tras el intento número ``attempts_made`` (1 => base, 2 => 2*base, ...)."""
    exponent = max(attempts_made - 1, 0)
    raw = min(policy.max_delay_seconds, policy.base_delay_seconds * (2**exponent))
    return min(policy.max_delay_seconds, max(0.0, jitter(raw)))


def parse_retry_after(value: str | None, now: datetime) -> float | None:
    """Interpreta un header Retry-After (RFC 9110 §10.2.3).

    Formatos soportados: ``delay-seconds`` (entero no negativo) y ``HTTP-date``.
    Devuelve ``None`` si falta o es inválido (vacío, negativo, decimal, texto).
    Una fecha pasada equivale a 0 segundos.
    """
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    if _DELAY_SECONDS.fullmatch(text):
        return float(int(text))
    try:
        moment = parsedate_to_datetime(text)
    except (TypeError, ValueError, IndexError):
        return None
    if moment is None:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return max(0.0, (moment - now).total_seconds())


@dataclass(frozen=True, slots=True)
class RetryDecision:
    retry: bool
    delay_seconds: float | None
    reason: str

    def describe(self) -> str:
        if self.retry:
            return f"retry in {self.delay_seconds:.3f}s ({self.reason})"
        return f"no_retry: {self.reason}"


def decide_retry(
    outcome: AttemptOutcome,
    *,
    attempts_made: int,
    policy: RetryPolicy,
    now: datetime,
    retry_after: str | None = None,
    jitter: Jitter = no_jitter,
) -> RetryDecision:
    """Decide si un envío fallido se reintenta y cuándo. Solo para errores con respuesta."""
    if outcome not in (AttemptOutcome.RETRYABLE_ERROR, AttemptOutcome.RATE_LIMITED):
        return RetryDecision(False, None, f"{outcome.value} is not retryable")
    if attempts_made >= policy.max_attempts:
        return RetryDecision(False, None, f"max_attempts_exhausted ({attempts_made})")

    delay = backoff_delay(attempts_made, policy, jitter)
    reason = "exponential_backoff"
    if outcome is AttemptOutcome.RATE_LIMITED:
        requested = parse_retry_after(retry_after, now)
        if requested is None:
            reason = "exponential_backoff; retry-after missing or invalid"
        else:
            capped = min(requested, policy.max_retry_after_seconds)
            if capped >= delay:
                delay = capped
                reason = "retry_after" + (" (capped)" if capped < requested else "")
            else:
                reason = "exponential_backoff >= retry_after"
    return RetryDecision(True, delay, reason)
