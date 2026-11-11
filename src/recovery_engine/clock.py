"""Reloj e identificadores. Se inyectan en servicios para que los tests sean determinísticos."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from uuid import uuid4

Clock = Callable[[], str]
IdFactory = Callable[[], str]


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds")


def new_operation_id() -> str:
    return f"op_{uuid4().hex}"


def new_effect_id() -> str:
    return f"eff_{uuid4().hex}"


def parse_timestamp(value: str) -> datetime:
    """Interpreta un timestamp ISO-8601. Sin zona horaria se asume UTC."""
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def format_timestamp(value: datetime) -> str:
    """Formato canónico (UTC, microsegundos): comparable lexicográficamente en SQL."""
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def normalize_timestamp(value: str) -> str:
    return format_timestamp(parse_timestamp(value))


def add_seconds(value: str, seconds: float) -> str:
    return format_timestamp(parse_timestamp(value) + timedelta(seconds=seconds))
