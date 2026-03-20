"""SQLite schema introspection — table/column metadata."""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field


@dataclass
class ColumnInfo:
    name: str
    type: str
    primary_key: bool
    nullable: bool


@dataclass
class TableInfo:
    name: str
    is_view: bool = False
    columns: list[ColumnInfo] = field(default_factory=list)

    @property
    def primary_keys(self) -> list[str]:
        return [c.name for c in self.columns if c.primary_key]


class SchemaInspector:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self._cache: dict[str, TableInfo] | None = None

    def tables(self) -> dict[str, TableInfo]:
        if self._cache is None:
            self._cache = self._load()
        return self._cache

    def resolve(self, label: str) -> TableInfo | None:
        """Map a JMD document label to a table (case-insensitive, singular/plural)."""
        tables = self.tables()
        singular = label[:-1] if label.endswith("s") and len(label) > 1 else None
        singular_lower = label.lower()[:-1] if label.lower().endswith("s") and len(label) > 1 else None
        candidates = [
            label, label.lower(),
            label + "s", label.lower() + "s",
            *(([singular, singular_lower]) if singular else []),
        ]
        for name in candidates:
            if name in tables:
                return tables[name]
        return None

    def _load(self) -> dict[str, TableInfo]:
        cur = self._conn.cursor()
        cur.execute("""
            SELECT name, type FROM sqlite_master
            WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """)
        result = {}
        for (table_name, obj_type) in cur.fetchall():
            cur.execute(f'PRAGMA table_info("{table_name}")')
            columns = [
                ColumnInfo(
                    name=row[1],
                    type=row[2],
                    primary_key=bool(row[5]),
                    nullable=not row[3],
                )
                for row in cur.fetchall()
            ]
            result[table_name] = TableInfo(
                name=table_name,
                is_view=(obj_type == "view"),
                columns=columns,
            )
        return result
