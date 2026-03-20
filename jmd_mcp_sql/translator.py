"""JMD document → SQL translation."""
from __future__ import annotations

import sqlite3
from typing import Any

from jmd import JMDParser, JMDQueryParser, JMDDeleteParser, serialize, tokenize, jmd_mode
from .schema import SchemaInspector, TableInfo


def _quote_identifier(name: str) -> str:
    """Quote a SQL identifier (column or table name) per SQLite rules."""
    return '"' + name.replace('"', '""') + '"'


def _row_to_jmd(row: dict[str, Any], label: str) -> str:
    return serialize(row, label=label)


def _rows_to_jmd(rows: list[dict[str, Any]], label: str) -> str:
    return serialize(rows, label=label + "[]")


class SQLTranslator:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self._conn.row_factory = sqlite3.Row
        self._schema = SchemaInspector(conn)

    # ------------------------------------------------------------------
    # read — # Label\nid: 42        →  SELECT … WHERE (exact match)
    #         #? Label\nfield: val  →  SELECT … WHERE (QBE filters)
    # ------------------------------------------------------------------

    def read(self, jmd_source: str) -> str:
        if jmd_mode(jmd_source) == "query":
            return self._query(jmd_source)

        data = JMDParser().parse(jmd_source)
        label = self._label_from_source(jmd_source)
        table = self._resolve_or_error(label)

        where, params = self._build_where(data)
        sql = f'SELECT * FROM "{table.name}"'
        if where:
            sql += f" WHERE {where}"

        rows = self._fetchall(sql, params)
        if not rows:
            return serialize({"status": 404, "code": "not_found",
                              "message": f"No records found in {table.name}"}, label="Error")
        if len(rows) == 1:
            return _row_to_jmd(rows[0], label)
        return _rows_to_jmd(rows, label)

    def _query(self, jmd_source: str) -> str:
        doc = JMDQueryParser().parse(jmd_source)
        table = self._resolve_or_error(doc.label)

        filters = {}
        for f in doc.fields:
            cond = f.condition
            if cond.op == "=" and cond.values:
                filters[f.key] = cond.values[0]

        where, params = self._build_where(filters)
        sql = f'SELECT * FROM "{table.name}"'
        if where:
            sql += f" WHERE {where}"

        rows = self._fetchall(sql, params)
        return _rows_to_jmd(rows, doc.label)

    # ------------------------------------------------------------------
    # write — # Label\nfield: value  →  INSERT OR REPLACE INTO …
    # ------------------------------------------------------------------

    def write(self, jmd_source: str) -> str:
        data = JMDParser().parse(jmd_source)
        label = self._label_from_source(jmd_source)
        table = self._resolve_or_error(label)

        cols = list(data.keys())
        placeholders = ", ".join("?" * len(cols))
        col_names = ", ".join(_quote_identifier(c) for c in cols)
        values = [data[c] for c in cols]

        sql = f'INSERT OR REPLACE INTO "{table.name}" ({col_names}) VALUES ({placeholders})'
        cur = self._conn.execute(sql, values)
        self._conn.commit()

        rowid = cur.lastrowid
        row = self._conn.execute(
            f'SELECT * FROM "{table.name}" WHERE rowid = ?', (rowid,)
        ).fetchone()
        result = dict(row) if row else data
        return _row_to_jmd(result, label)

    # ------------------------------------------------------------------
    # delete — #- Label\nid: 42  →  DELETE FROM … WHERE …
    # ------------------------------------------------------------------

    def delete(self, jmd_source: str) -> str:
        doc = JMDDeleteParser().parse(jmd_source)
        table = self._resolve_or_error(doc.label)

        identifiers = doc.identifiers if isinstance(doc.identifiers, dict) else {}
        where, params = self._build_where(identifiers)

        if not where:
            return serialize({"status": 400, "code": "bad_request",
                              "message": "Delete requires at least one identifier field"},
                             label="Error")

        sql = f'DELETE FROM "{table.name}" WHERE {where}'
        cur = self._conn.execute(sql, params)
        self._conn.commit()

        return serialize({"deleted": cur.rowcount, "table": table.name}, label="Result")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_or_error(self, label: str) -> TableInfo:
        table = self._schema.resolve(label)
        if table is None:
            available = ", ".join(self._schema.tables().keys())
            raise ValueError(f"Unknown table '{label}'. Available: {available}")
        return table

    def _build_where(self, filters: dict[str, Any]) -> tuple[str, list]:
        if not filters:
            return "", []
        clauses = [f"{_quote_identifier(k)} = ?" for k in filters]
        return " AND ".join(clauses), list(filters.values())

    def _fetchall(self, sql: str, params: list) -> list[dict]:
        cur = self._conn.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]

    def _label_from_source(self, source: str) -> str:
        for line in tokenize(source):
            if line.heading_depth == 1:
                content = line.content
                for prefix in ("? ", "! ", "- "):
                    if content.startswith(prefix):
                        return content[len(prefix):]
                return content
        return "Result"
