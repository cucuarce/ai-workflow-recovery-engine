"""Acceso a SQLite: conexiones y transacciones explícitas."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from pathlib import Path

BUSY_TIMEOUT_SECONDS = 10


class Database:
    """Fábrica de conexiones a un archivo SQLite.

    Cada unidad de trabajo usa su propia conexión. Las conexiones operan en modo
    autocommit (``isolation_level=None``) y las transacciones se abren de forma
    explícita con :func:`transaction`.
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path,
            timeout=BUSY_TIMEOUT_SECONDS,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def initialize(
        self, schema: str, migrate: Callable[[sqlite3.Connection], None] | None = None
    ) -> None:
        """Aplica ``migrate`` (si hay una base previa que actualizar) y luego el esquema."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self.connect()) as conn:
            conn.execute("PRAGMA journal_mode = WAL")
            if migrate is not None:
                migrate(conn)
            conn.executescript(schema)


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Transacción ``BEGIN IMMEDIATE``: toma el lock de escritura al inicio.

    Esto serializa a los escritores y hace seguro el patrón "leer y luego insertar"
    que usa la idempotencia (no hay ventana entre el SELECT y el INSERT).
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")
