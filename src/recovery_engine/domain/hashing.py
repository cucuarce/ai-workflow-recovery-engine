"""Huella canónica de un pedido, usada para comparar payloads bajo la misma clave."""

from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def request_fingerprint(operation_type: str, payload: dict[str, Any]) -> str:
    document = canonical_json({"operation_type": operation_type, "payload": payload})
    return hashlib.sha256(document.encode("utf-8")).hexdigest()
