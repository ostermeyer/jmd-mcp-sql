"""JMD document → SQL translation."""
from __future__ import annotations

import re
import sqlite3
from typing import Any

from jmd import (
    JMDParser, JMDQueryParser, JMDDeleteParser, JMDSchemaParser,
    SchemaField, serialize, tokenize, jmd_mode,
)
from .schema import SchemaInspector, TableInfo


def _regexp(pattern: str, value: Any) -> bool:
    if value is None:
        return False
    try:
        return bool(re.fullmatch(str(pattern), str(value)))
    except re.error:
        return str(pattern) == str(value)


def _quote_identifier(name: str) -> str:
    """Quote a SQL identifier (column or table name) per SQLite rules."""
    return '"' + name.replace('"', '""') + '"'


def _sqlite_type_to_jmd(sqlite_type: str) -> str:
    """Map a SQLite declared type to the nearest JMD schema type."""
    t = sqlite_type.upper()
    if "INT" in t:
        return "integer"
    if any(s in t for s in ("TEXT", "CHAR", "CLOB")):
        return "string"
    if any(s in t for s in ("REAL", "FLOA", "DOUB", "NUMER", "DECIM")):
        return "float"
    if "BOOL" in t:
        return "boolean"
    return "string"  # SQLite default affinity


_JMD_TO_SQLITE: dict[str, str] = {
    "integer": "INTEGER",
    "int": "INTEGER",
    "string": "TEXT",
    "text": "TEXT",
    "float": "REAL",
    "number": "REAL",
    "boolean": "INTEGER",
    "bool": "INTEGER",
    "any": "TEXT",
}


def _row_to_jmd(row: dict[str, Any], label: str) -> str:
    return serialize(row, label=label)


def _rows_to_jmd(rows: list[dict[str, Any]], label: str) -> str:
    return serialize(rows, label=label + "[]")


class SQLTranslator:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self._conn.row_factory = sqlite3.Row
        self._conn.create_function("REGEXP", 2, _regexp)
        self._schema = SchemaInspector(conn)

    # ------------------------------------------------------------------
    # read — # Label\nid: 42        →  SELECT … WHERE (exact match)
    #         #? Label\nfield: val  →  SELECT … WHERE (QBE filters)
    #         #! Label              →  PRAGMA table_info() as #! document
    # ------------------------------------------------------------------

    def read(self, jmd_source: str) -> str:
        mode = jmd_mode(jmd_source)
        if mode == "query":
            return self._query(jmd_source)
        if mode == "schema":
            return self._read_schema(jmd_source)

        parser = JMDParser()
        data = parser.parse(jmd_source)
        fm = parser.frontmatter
        label = self._label_from_source(jmd_source)
        table = self._resolve_or_error(label)

        where, params = self._build_where(data)
        base_sql = f'SELECT * FROM "{table.name}"'
        count_sql = f'SELECT COUNT(*) FROM "{table.name}"'
        if where:
            base_sql += f" WHERE {where}"
            count_sql += f" WHERE {where}"

        # count-only mode
        if "count" in fm:
            total = self._conn.execute(count_sql, params).fetchone()[0]
            return serialize({"count": total}, label=label)

        # paginated mode
        page_size = int(fm["size"]) if "size" in fm else 0
        if page_size > 0:
            page = max(1, int(fm.get("page", 1)))
            total = self._conn.execute(count_sql, params).fetchone()[0]
            pages = (total + page_size - 1) // page_size
            offset = (page - 1) * page_size
            rows = self._fetchall(base_sql + f" LIMIT {page_size} OFFSET {offset}", params)
            return serialize(
                {"total": total, "page": page, "pages": pages,
                 "page_size": page_size, "data": rows},
                label=label,
            )

        rows = self._fetchall(base_sql, params)
        if not rows:
            return serialize({"status": 404, "code": "not_found",
                              "message": f"No records found in {table.name}"}, label="Error")
        if len(rows) == 1:
            return _row_to_jmd(rows[0], label)
        return _rows_to_jmd(rows, label)

    def _query(self, jmd_source: str) -> str:
        query_parser = JMDQueryParser()
        doc = query_parser.parse(jmd_source)
        fm = query_parser.frontmatter
        table = self._resolve_or_error(doc.label)

        where, params = self._build_where_from_fields(doc.fields)
        base_sql = f'SELECT * FROM "{table.name}"'
        count_sql = f'SELECT COUNT(*) FROM "{table.name}"'
        if where:
            base_sql += f" WHERE {where}"
            count_sql += f" WHERE {where}"

        # count-only mode
        if "count" in fm:
            total = self._conn.execute(count_sql, params).fetchone()[0]
            return serialize({"count": total}, label=doc.label)

        # paginated mode
        page_size = int(fm["size"]) if "size" in fm else 0
        if page_size > 0:
            page = max(1, int(fm.get("page", 1)))
            total = self._conn.execute(count_sql, params).fetchone()[0]
            pages = (total + page_size - 1) // page_size
            offset = (page - 1) * page_size
            rows = self._fetchall(base_sql + f" LIMIT {page_size} OFFSET {offset}", params)
            return serialize(
                {"total": total, "page": page, "pages": pages,
                 "page_size": page_size, "data": rows},
                label=doc.label,
            )

        rows = self._fetchall(base_sql, params)
        return _rows_to_jmd(rows, doc.label)

    # ------------------------------------------------------------------
    # write — # Label\nfield: value  →  INSERT OR REPLACE INTO …
    #          #! Label\nfield: type →  CREATE TABLE or ALTER TABLE
    # ------------------------------------------------------------------

    def write(self, jmd_source: str) -> str:
        if jmd_mode(jmd_source) == "schema":
            return self._write_schema(jmd_source)
        data = JMDParser().parse(jmd_source)
        label = self._label_from_source(jmd_source)
        table = self._resolve_or_error(label)

        if table.is_view:
            return serialize({"status": 400, "code": "read_only",
                              "message": f"'{table.name}' is a view and cannot be written to"},
                             label="Error")

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
    #           #! Label         →  DROP TABLE
    # ------------------------------------------------------------------

    def delete(self, jmd_source: str) -> str:
        if jmd_mode(jmd_source) == "schema":
            return self._delete_schema(jmd_source)
        doc = JMDDeleteParser().parse(jmd_source)
        table = self._resolve_or_error(doc.label)

        if table.is_view:
            return serialize({"status": 400, "code": "read_only",
                              "message": f"'{table.name}' is a view and cannot be deleted from"},
                             label="Error")

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
    # Schema operations (#!)
    # ------------------------------------------------------------------

    def _read_schema(self, jmd_source: str) -> str:
        label = self._label_from_source(jmd_source)
        table = self._resolve_or_error(label)
        lines = [f"#! {label}"]
        for col in table.columns:
            jmd_type = _sqlite_type_to_jmd(col.type)
            modifiers = []
            if col.primary_key:
                modifiers.append("readonly")
            if col.nullable:
                modifiers.append("optional")
            suffix = (" " + " ".join(modifiers)) if modifiers else ""
            lines.append(f"{col.name}: {jmd_type}{suffix}")
        return "\n".join(lines)

    def _write_schema(self, jmd_source: str) -> str:
        schema = JMDSchemaParser().parse(jmd_source)
        table_name = schema.label
        scalar_fields = [f for f in schema.fields if isinstance(f, SchemaField)]

        existing = self._schema.resolve(table_name)
        if existing is not None and existing.is_view:
            return serialize({"status": 400, "code": "read_only",
                              "message": f"'{table_name}' is a view and cannot be altered"},
                             label="Error")
        if existing is None:
            col_defs = []
            for f in scalar_fields:
                sqlite_type = _JMD_TO_SQLITE.get(f.base_type.lower(), "TEXT")
                pk = " PRIMARY KEY" if f.readonly else ""
                not_null = "" if f.optional else " NOT NULL"
                col_defs.append(f"{_quote_identifier(f.key)} {sqlite_type}{pk}{not_null}")
            cols_sql = ", ".join(col_defs)
            self._conn.execute(f"CREATE TABLE {_quote_identifier(table_name)} ({cols_sql})")
            self._conn.commit()
            self._schema = SchemaInspector(self._conn)
            return serialize({"table": table_name, "created": True}, label="Result")
        else:
            existing_cols = {c.name for c in existing.columns}
            added = []
            for f in scalar_fields:
                if f.key not in existing_cols:
                    sqlite_type = _JMD_TO_SQLITE.get(f.base_type.lower(), "TEXT")
                    self._conn.execute(
                        f"ALTER TABLE {_quote_identifier(table_name)} "
                        f"ADD COLUMN {_quote_identifier(f.key)} {sqlite_type}"
                    )
                    added.append(f.key)
            self._conn.commit()
            self._schema = SchemaInspector(self._conn)
            return serialize({"table": table_name, "altered": True, "added": added},
                             label="Result")

    def _delete_schema(self, jmd_source: str) -> str:
        label = self._label_from_source(jmd_source)
        table = self._schema.resolve(label)
        if table is not None and table.is_view:
            self._conn.execute(f"DROP VIEW IF EXISTS {_quote_identifier(table.name)}")
        else:
            self._conn.execute(f"DROP TABLE IF EXISTS {_quote_identifier(label)}")
        self._conn.commit()
        self._schema = SchemaInspector(self._conn)
        return serialize({"dropped": label}, label="Result")

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

    def _build_where_from_fields(self, fields: list[Any]) -> tuple[str, list]:
        """Build a WHERE clause from a list of QueryField nodes."""
        from jmd._query import QueryField
        clauses: list[str] = []
        params: list[Any] = []
        for f in fields:
            if not isinstance(f, QueryField):
                continue  # QueryObject/QueryArray: skip (not translatable to flat SQL)
            if f.condition.op in ("?", "?:"):
                continue  # projection — no filter
            clause, p = self._condition_to_sql(f.key, f.condition)
            if clause:
                clauses.append(clause)
                params.extend(p)
        return (" AND ".join(clauses), params) if clauses else ("", [])

    def _condition_to_sql(self, col: str, cond: Any) -> tuple[str, list]:
        """Translate a single Condition to a SQL fragment and parameter list."""
        from jmd._query import Condition
        qcol = _quote_identifier(col)
        op, values = cond.op, cond.values

        if op == "!":
            inner, p = self._condition_to_sql(col, values[0])
            return (f"NOT ({inner})", p) if inner else ("", [])
        if op == "=":
            return f"{qcol} = ?", [values[0]]
        if op in (">", ">=", "<", "<="):
            return f"{qcol} {op} ?", [values[0]]
        if op == "|":
            placeholders = ", ".join("?" * len(values))
            return f"{qcol} IN ({placeholders})", list(values)
        if op == "~":
            return f"{qcol} LIKE ?", [f"%{values[0]}%"]
        if op == "regex":
            return f"{qcol} REGEXP ?", [values[0]]
        return "", []

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
