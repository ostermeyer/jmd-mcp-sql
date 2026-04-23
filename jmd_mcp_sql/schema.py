# SPDX-License-Identifier: Apache-2.0
"""SQLite schema introspection — table and column metadata.

This module is intentionally kept separate from the SQL-translation layer
so that schema changes (e.g. tables created at runtime) can be picked up
by constructing a fresh SchemaInspector without touching translation logic.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field


@dataclass
class ColumnInfo:
    """Metadata for a single column in a SQLite table or view."""

    name: str
    type: str          # SQLite declared type, e.g. "TEXT", "INTEGER", "REAL"
    primary_key: bool
    nullable: bool     # True when the column has no NOT NULL constraint


@dataclass
class TableInfo:
    """Metadata for a single table or view."""

    name: str
    is_view: bool = False
    columns: list[ColumnInfo] = field(default_factory=list)

    @property
    def primary_keys(self) -> list[str]:
        """Return the names of all primary-key columns."""
        return [c.name for c in self.columns if c.primary_key]


class SchemaInspector:
    """Lazily loads and caches the database schema.

    The cache is invalidated (replaced) whenever the server modifies the
    schema (CREATE TABLE, ALTER TABLE, DROP TABLE).  Pass a fresh
    SchemaInspector instance to SQLTranslator after any DDL operation.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Store the connection and initialise the lazy cache."""
        self._conn = conn
        # None means "not yet loaded"; an empty dict means "loaded, no tables".
        self._cache: dict[str, TableInfo] | None = None

    def tables(self) -> dict[str, TableInfo]:
        """Return all user tables and views, keyed by exact name."""
        if self._cache is None:
            self._cache = self._load()
        return self._cache

    def resolve(self, label: str) -> TableInfo | None:
        """Map a JMD document label to a table, case-insensitively.

        JMD labels are written by an LLM which may use any capitalisation
        or pluralisation.  This method tries several candidate spellings so
        that ``# Order``, ``# Orders``, and ``# orders`` all resolve to the
        same underlying table.

        Args:
            label: The heading label from the JMD document, e.g. ``"Order"``.

        Returns:
            The matching TableInfo, or None if no table matches.
        """
        tables = self.tables()

        # Build candidate spellings: original, lower-case, with/without a
        # trailing "s".  This covers the most common singular/plural mismatch
        # between LLM-generated labels and actual table names.
        stripped = (
            label[:-1] if label.endswith("s") and len(label) > 1 else None
        )
        stripped_lower = (
            label.lower()[:-1]
            if label.lower().endswith("s") and len(label) > 1
            else None
        )
        candidates = [
            label,
            label.lower(),
            label + "s",
            label.lower() + "s",
            *(([stripped, stripped_lower]) if stripped else []),
        ]
        for name in candidates:
            if name in tables:
                return tables[name]
        return None

    def _load(self) -> dict[str, TableInfo]:
        """Query sqlite_master and PRAGMA table_info for every object."""
        cur = self._conn.cursor()

        # Fetch all user-defined tables and views; skip SQLite internals.
        cur.execute("""
            SELECT name, type FROM sqlite_master
            WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """)
        result = {}
        for (table_name, obj_type) in cur.fetchall():
            # PRAGMA table_info returns one row per column:
            # (cid, name, type, notnull, dflt_value, pk)
            cur.execute(f'PRAGMA table_info("{table_name}")')
            columns = [
                ColumnInfo(
                    name=row[1],
                    type=row[2],
                    primary_key=bool(row[5]),  # pk > 0 means part of PK
                    nullable=not row[3],        # notnull=1 → not nullable
                )
                for row in cur.fetchall()
            ]
            result[table_name] = TableInfo(
                name=table_name,
                is_view=(obj_type == "view"),
                columns=columns,
            )
        return result
