# SPDX-License-Identifier: Apache-2.0
"""Dependency context threaded through every translator operation.

The :class:`TranslatorContext` is a small dataclass holding the
DB connection and the schema cache, plus the handful of access
patterns that *every* operation needs (label resolution, fetch,
EXPLAIN). All schema-/query-/data-mode operations are free
functions in their respective modules taking a
``TranslatorContext`` as the first parameter.

This is the modern-Python alternative to gathering everything on
one large class via mixin-base-inheritance: explicit dependency
injection, real module boundaries, no type-system theater.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any

from jmd import tokenize

from .schema import SchemaInspector, TableInfo


@dataclass
class TranslatorContext:
    """Bag of dependencies for translator operations.

    ``conn`` and ``schema`` are the runtime state every operation
    needs; the methods below are the access patterns that are
    sufficiently universal to live with the data rather than as
    free functions on top of it.
    """

    conn: sqlite3.Connection
    schema: SchemaInspector

    def resolve_or_error(self, label: str) -> TableInfo:
        """Map a JMD label to a :class:`TableInfo` or raise.

        Tries case-insensitive and singular/plural variations
        (handled by :class:`SchemaInspector`); raises
        :class:`ValueError` with the available table list when
        nothing matches.
        """
        table = self.schema.resolve(label)
        if table is None:
            available = ", ".join(self.schema.tables().keys())
            raise ValueError(
                f"Unknown table '{label}'. Available: {available}"
            )
        return table

    def fetchall(
        self, sql: str, params: list[Any]
    ) -> list[dict[str, Any]]:
        """Execute *sql* and return all rows as plain dicts."""
        cur = self.conn.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]

    def explain(self, sql: str, params: list[Any]) -> str:
        """Return ``EXPLAIN QUERY PLAN`` output as a single string."""
        rows = self.conn.execute(
            f"EXPLAIN QUERY PLAN {sql}", params
        ).fetchall()
        return "; ".join(str(row[3]) for row in rows)

    def refresh_schema(self) -> None:
        """Rebuild the schema cache after a DDL operation.

        DDL writers should call this after their CREATE / ALTER /
        DROP succeeds so that subsequent reads see the new shape.
        """
        self.schema = SchemaInspector(self.conn)


def label_from_source(source: str) -> str:
    """Extract the heading label from the first heading of a JMD doc.

    The heading line encodes both the mode prefix and the label::

        ``# Orders``   → ``"Orders"``
        ``#? Orders``  → ``"Orders"``
        ``#! Orders``  → ``"Orders"``
        ``#- Orders``  → ``"Orders"``

    Pure function — doesn't touch the context — but lives here
    because every operation module uses it as the first step.
    """
    for line in tokenize(source):
        if line.heading_depth == 1:
            content = line.content
            # Strip the mode prefix characters (?, !, -) and the space.
            for prefix in ("? ", "! ", "- "):
                if content.startswith(prefix):
                    return content[len(prefix):]
            return content
    return "Result"
