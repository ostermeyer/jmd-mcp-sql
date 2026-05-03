# SPDX-License-Identifier: Apache-2.0
"""JMD document → SQL translation.

This module is the heart of the MCP server.  It receives JMD documents
(plain-text, heading-driven) and translates them into SQLite operations,
then serializes the results back to JMD.

JMD document modes
------------------
Every JMD document starts with a heading that encodes both the target
table and the *mode* — what kind of operation the document represents:

    # Product          data mode   — exact lookup or INSERT OR REPLACE
    #? Product         query mode  — Query-by-Example filter / list
    #! Product         schema mode — PRAGMA describe / CREATE / ALTER / DROP
    #- Product         delete mode — DELETE WHERE / DROP TABLE

The ``jmd_mode()`` helper from jmd-format detects the mode from the
heading prefix so the translator can dispatch accordingly.

Query-by-Example (QBE) filters
-------------------------------
In query mode the LLM writes one filter expression per line:

    #? Orders
    ShipCountry: Germany|France|UK
    Freight: > 50
    CustomerID: ~Corp

Each line is parsed by JMDQueryParser into a ``QueryField`` with a
``Condition`` (operator + values).  This module maps those conditions
to SQL WHERE fragments:

    =          →  col = ?
    > >= < <=  →  col OP ?
    |          →  col IN (?, ?, ?)
    ~          →  col LIKE '%val%'
    regex      →  col REGEXP ?   (custom SQLite function, see _regexp)
    !          →  NOT (inner condition)

Pagination
----------
Frontmatter lines *before* the heading pass control parameters:

    page-size: 50
    page: 2

    #? Orders

The translator runs two queries: COUNT(*) for metadata, then SELECT
with LIMIT/OFFSET for the page.  Pagination metadata (``total``,
``page``, ``pages``, ``page-size``) is returned as response frontmatter
— before the root heading — not as body fields.

Aggregation
-----------
Aggregation is also expressed as frontmatter before the ``#?`` heading:

    group: EmployeeID
    sum: revenue
    sort: sum_revenue desc
    page-size: 3

    #? OrderDetails

Supported keys: ``group`` (GROUP BY), ``sum``, ``avg``, ``min``,
``max`` (aggregate functions), ``count`` (COUNT(*)), ``having``
(post-aggregation filter, comma-separated conditions), ``sort``
(ORDER BY, comma-separated columns with optional direction).
Result columns for aggregate functions are named ``<func>_<field>``
(e.g. ``sum_revenue``, ``avg_UnitPrice``).
"""
from __future__ import annotations

import re
import sqlite3
import time
from typing import Any

from jmd import (
    JMDDeleteParser,
    JMDParser,
    JMDQueryParser,
    JMDSchemaParser,
    SchemaField,
    jmd_mode,
    serialize,
    tokenize,
)

# Internal JMD types used for QBE filter translation.
# Imported at module level per Google style — no inline imports.
from jmd._query import Condition, QueryField

# -- DDL helpers (Slices A–F) -------------------------------------
# Build/parse helpers for CREATE TABLE / INDEX / TRIGGER / VIEW /
# VIRTUAL TABLE moved to ._ddl. Re-imported here so the schema-
# operation methods on SQLTranslator find them at the same names.
from ._ddl import (  # noqa: E402, I001
    _as_string_list,
    _build_create_index_sql,
    _build_create_trigger_sql,
    _build_create_virtual_table_sql,
    _coerce_bool,
    _column_enum_values,
    _column_part_name,
    _extract_create_table_body,
    _index_where_clause,
    _is_table_level_constraint,
    _is_unindexed_arg,
    _is_virtual_table_sql,
    _parse_reference,
    _parse_trigger_sql,
    _parse_view_select,
    _parse_virtual_table,
    _quote_default,
    _split_columns,
    _split_top_level,
    _table_check_clauses,
    _table_foreign_keys,
    _table_unique_constraints,
    _unquote_default_for_jmd,
    _user_indexes,
    _user_triggers,
    _user_views,
)
from ._debug import (
    DebugInfo,
    _check_frontmatter,
    _parse_debug,
    _prepend_debug,
    _prepend_ignored_keys,
)

# Query-frontmatter parsing (select / join / aggregate / having)
# and the small expression validator now live in ._query_parser.
from ._query_parser import (  # noqa: E402, I001
    _AGG_FUNCS,
    JoinSpec,
    _parse_agg_expr,
    _parse_comparison,
    _parse_join_specs,
    _parse_select_cols,
    _regexp,
    _validate_and_qualify_expression,
)

# SQL fundamentals (identifier quoting, JMD ⇄ SQLite type vocabulary)
# live in ._sql so DDL helpers can reuse them without an import cycle
# back into translator.py.
# Re-imported here at module level so existing call sites — and any
# downstream tooling that imports them from this module — keep working.
from ._sql import (  # noqa: E402, I001
    _JMD_TO_SQLITE,
    _quote_identifier,
    _sqlite_type_to_jmd,
)
from .schema import SchemaInspector, TableInfo

# -- Known frontmatter keys per operation (for WP2 tolerance) ------

_KNOWN_FM_READ_DATA: frozenset[str] = frozenset({
    "page-size", "page", "count", "select", "debug",
})
_KNOWN_FM_READ_QUERY: frozenset[str] = frozenset({
    "select", "join", "sum", "avg", "min", "max", "count",
    "group", "having", "sort", "page-size", "page", "debug",
})
_KNOWN_FM_WRITE: frozenset[str] = frozenset({"debug", "action"})
_KNOWN_FM_DELETE: frozenset[str] = frozenset({
    "confirm", "debug",
})

def _row_to_jmd(row: dict[str, Any], label: str) -> str:
    """Serialize a single result row as a JMD data document."""
    return serialize(row, label=label)


def _rows_to_jmd(rows: list[dict[str, Any]], label: str) -> str:
    """Serialize a list of result rows as a JMD array document."""
    return serialize(rows, label=label)


class SQLTranslator:
    r"""Translates JMD documents into SQLite operations and back.

    Each public method corresponds to one MCP tool (read, write, delete).
    The constructor receives an open SQLite connection which is reused for
    the lifetime of the server process.

    Example usage::

        conn = sqlite3.connect("mydb.db")
        t = SQLTranslator(conn)
        result = t.read("#? Orders\nShipCountry: Germany")
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Initialise the translator with an open SQLite connection."""
        self._conn = conn
        # sqlite3.Row makes fetchall() return dict-like objects so we can
        # call dict(row) without knowing column names in advance.
        self._conn.row_factory = sqlite3.Row
        # Register our REGEXP function so QBE regex filters work.
        self._conn.create_function("REGEXP", 2, _regexp)
        self._schema = SchemaInspector(conn)

    def close(self) -> None:
        """Close the underlying database connection."""
        self._conn.close()

    # ------------------------------------------------------------------
    # read — dispatches to _query (#?), _read_schema (#!),
    #        or direct SELECT (#, with optional pagination)
    # ------------------------------------------------------------------

    def read(self, jmd_source: str) -> str:
        """Execute a read operation described by a JMD document.

        Dispatches based on document mode:

        - ``#?`` (query): Query-by-Example filter with optional pagination.
        - ``#!`` (schema): Return table structure as a JMD schema document.
        - ``#`` (data): SELECT WHERE with exact field matches.

        Args:
            jmd_source: A complete JMD document string, optionally preceded
                by frontmatter lines (``size:``, ``page:``, ``count:``).

        Returns:
            A JMD document string with the query results, or a
            ``# Error`` document on failure.
        """
        mode = jmd_mode(jmd_source)
        if mode == "query":
            return self._query(jmd_source)
        if mode == "schema":
            return self._read_schema(jmd_source)

        # Data mode: parse key/value pairs and build an exact-match SELECT.
        parser = JMDParser()
        data = parser.parse(jmd_source)
        fm = parser.frontmatter
        dbg = _parse_debug(fm)
        ignored = _check_frontmatter(
            fm, _KNOWN_FM_READ_DATA, "observable"
        )
        label = self._label_from_source(jmd_source)
        table = self._resolve_or_error(label)
        if dbg.wants("table"):
            dbg.table = table.name

        table_cols = {c.name for c in table.columns}
        unknown = [k for k in data if k not in table_cols]
        if unknown:
            return serialize(
                {"status": 400, "code": "bad_request",
                 "message": (
                     f"Unknown column(s) {unknown!r}"
                     f" in table '{table.name}'"
                 )},
                label="Error",
            )
        where, params = self._build_where(data)

        # Apply select: column projection if requested.
        select_clause = "*"
        if "select" in fm:
            sel_cols = _parse_select_cols(str(fm["select"]))
            if sel_cols:
                for col in sel_cols:
                    if col not in table_cols:
                        available = ", ".join(
                            sorted(table_cols)
                        )
                        raise ValueError(
                            f"Unknown column '{col}'"
                            " in 'select'. "
                            f"Available: {available}"
                        )
                select_clause = ", ".join(
                    _quote_identifier(c) for c in sel_cols
                )

        base_sql = (
            f'SELECT {select_clause}'
            f' FROM {_quote_identifier(table.name)}'
        )
        count_sql = (
            f'SELECT COUNT(*)'
            f' FROM {_quote_identifier(table.name)}'
        )
        if where:
            base_sql += f" WHERE {where}"
            count_sql += f" WHERE {where}"

        # count: true — return only the row count, no data.
        if "count" in fm:
            t0 = time.perf_counter()
            total = self._conn.execute(
                count_sql, params
            ).fetchone()[0]
            if dbg.active:
                dbg.timing_ms = (
                    (time.perf_counter() - t0) * 1000
                )
                dbg.sql = count_sql
            resp = (
                f"count: {total}\n\n"
                + serialize({}, label=label)
            )
            return _prepend_debug(
                _prepend_ignored_keys(resp, ignored), dbg
            )

        # Paginated mode.
        page_size = (
            int(fm["page-size"]) if "page-size" in fm else 0
        )
        if page_size > 0:
            page = max(1, int(fm.get("page", 1)))
            total = self._conn.execute(
                count_sql, params
            ).fetchone()[0]
            offset = (page - 1) * page_size
            exec_sql = (
                base_sql
                + f" LIMIT {page_size} OFFSET {offset}"
            )
            t0 = time.perf_counter()
            rows = self._fetchall(exec_sql, params)
            if dbg.active:
                dbg.timing_ms = (
                    (time.perf_counter() - t0) * 1000
                )
                dbg.sql = exec_sql
                if dbg.wants("plan"):
                    dbg.plan = self._explain(
                        exec_sql, params
                    )
            return _prepend_debug(
                _prepend_ignored_keys(
                    self._paginated_jmd(
                        rows, label, total, page, page_size
                    ),
                    ignored,
                ),
                dbg,
            )

        t0 = time.perf_counter()
        rows = self._fetchall(base_sql, params)
        if dbg.active:
            dbg.timing_ms = (
                (time.perf_counter() - t0) * 1000
            )
            dbg.sql = base_sql
            if dbg.wants("plan"):
                dbg.plan = self._explain(base_sql, params)
        if not rows:
            return serialize(
                {"status": 404, "code": "not_found",
                 "message": (
                     f"No records found in {table.name}"
                 )},
                label="Error",
            )
        if len(rows) == 1:
            return _prepend_debug(
                _prepend_ignored_keys(
                    _row_to_jmd(rows[0], label), ignored
                ),
                dbg,
            )
        return _prepend_debug(
            _prepend_ignored_keys(
                _rows_to_jmd(rows, label), ignored
            ),
            dbg,
        )

    def _query(self, jmd_source: str) -> str:
        """Execute a QBE query document (#?) with optional pagination.

        Also handles aggregation mode when frontmatter contains group/agg
        keys, and cross-table joins when ``join:`` is present.

        Frontmatter keys control the execution mode:

        - ``join``: cross-table JOIN — dispatches to
          :meth:`_query_with_joins`.
        - ``count`` (bare): return only the row count, no data.
        - ``size`` / ``page``: paginate the result set.
        - ``group``, ``sum``, ``avg``, ``min``, ``max``: aggregate mode —
          dispatches to :meth:`_aggregate`.
        - ``select``: restrict returned columns.
        """
        query_parser = JMDQueryParser()
        doc = query_parser.parse(jmd_source)
        fm = query_parser.frontmatter
        dbg = _parse_debug(fm)
        ignored = _check_frontmatter(
            fm, _KNOWN_FM_READ_QUERY, "observable"
        )
        table = self._resolve_or_error(doc.label)
        if dbg.wants("table"):
            dbg.table = table.name

        # Translate each QueryField into a SQL WHERE fragment.
        table_cols = {c.name for c in table.columns}
        where, params = self._build_where_from_fields(
            doc.fields, table_cols, dbg=dbg
        )

        # JOIN mode.
        if "join" in fm:
            join_specs = _parse_join_specs(str(fm["join"]))
            return _prepend_debug(
                _prepend_ignored_keys(
                    self._query_with_joins(
                        table, doc.label, doc.fields,
                        fm, join_specs,
                    ),
                    ignored,
                ),
                dbg,
            )

        # Aggregation mode.
        if "group" in fm or any(
            k in fm for k in _AGG_FUNCS
        ):
            return _prepend_debug(
                _prepend_ignored_keys(
                    self._aggregate(
                        table, doc.label, where, params, fm
                    ),
                    ignored,
                ),
                dbg,
            )

        # Apply select: column projection if requested.
        select_clause = "*"
        if "select" in fm:
            sel_cols = _parse_select_cols(
                str(fm["select"])
            )
            if sel_cols:
                for col in sel_cols:
                    if col not in table_cols:
                        available = ", ".join(
                            sorted(table_cols)
                        )
                        raise ValueError(
                            f"Unknown column '{col}'"
                            " in 'select'. "
                            f"Available: {available}"
                        )
                select_clause = ", ".join(
                    _quote_identifier(c) for c in sel_cols
                )

        base_sql = (
            f'SELECT {select_clause}'
            f' FROM {_quote_identifier(table.name)}'
        )
        count_sql = (
            f'SELECT COUNT(*)'
            f' FROM {_quote_identifier(table.name)}'
        )
        if where:
            base_sql += f" WHERE {where}"
            count_sql += f" WHERE {where}"

        # Collect resolved frontmatter→SQL mappings.
        if dbg.wants("resolved"):
            if "select" in fm:
                dbg.resolved.append(
                    ("select", select_clause)
                )
            if where:
                dbg.resolved.append(
                    ("where", f"WHERE {where}")
                )

        # count (bare key without group).
        if "count" in fm:
            t0 = time.perf_counter()
            total = self._conn.execute(
                count_sql, params
            ).fetchone()[0]
            if dbg.active:
                dbg.timing_ms = (
                    (time.perf_counter() - t0) * 1000
                )
                dbg.sql = count_sql
            resp = (
                f"count: {total}\n\n"
                + serialize({}, label=doc.label)
            )
            return _prepend_debug(
                _prepend_ignored_keys(resp, ignored), dbg
            )

        # sort: ORDER BY.
        if "sort" in fm:
            order_parts: list[str] = []
            for item in str(fm["sort"]).split(","):
                parts = item.strip().split()
                if not parts:
                    continue
                col = parts[0]
                if col not in table_cols:
                    available = ", ".join(
                        sorted(table_cols)
                    )
                    raise ValueError(
                        f"Unknown column '{col}'"
                        " in 'sort'. "
                        f"Available: {available}"
                    )
                direction = (
                    parts[1].upper()
                    if len(parts) > 1
                    and parts[1].upper() in ("ASC", "DESC")
                    else "ASC"
                )
                order_parts.append(
                    f"{_quote_identifier(col)} {direction}"
                )
            if order_parts:
                order_sql = ", ".join(order_parts)
                base_sql += " ORDER BY " + order_sql
                if dbg.wants("resolved"):
                    dbg.resolved.append(
                        ("sort", f"ORDER BY {order_sql}")
                    )

        page_size = (
            int(fm["page-size"]) if "page-size" in fm else 0
        )
        if page_size > 0:
            pg = max(1, int(fm.get("page", 1)))
            total = self._conn.execute(
                count_sql, params
            ).fetchone()[0]
            pg_offset = (pg - 1) * page_size
            exec_sql = (
                base_sql
                + f" LIMIT {page_size}"
                + f" OFFSET {pg_offset}"
            )
            if dbg.wants("resolved"):
                dbg.resolved.append(
                    ("page-size",
                     f"LIMIT {page_size}")
                )
                dbg.resolved.append(
                    ("page",
                     f"OFFSET {pg_offset}")
                )
            t0 = time.perf_counter()
            rows = self._fetchall(exec_sql, params)
            if dbg.active:
                dbg.timing_ms = (
                    (time.perf_counter() - t0) * 1000
                )
                dbg.sql = exec_sql
                if dbg.wants("plan"):
                    dbg.plan = self._explain(
                        exec_sql, params
                    )
            return _prepend_debug(
                _prepend_ignored_keys(
                    self._paginated_jmd(
                        rows, doc.label,
                        total, pg, page_size,
                    ),
                    ignored,
                ),
                dbg,
            )

        t0 = time.perf_counter()
        rows = self._fetchall(base_sql, params)
        if dbg.active:
            dbg.timing_ms = (
                (time.perf_counter() - t0) * 1000
            )
            dbg.sql = base_sql
            if dbg.wants("plan"):
                dbg.plan = self._explain(base_sql, params)
        return _prepend_debug(
            _prepend_ignored_keys(
                _rows_to_jmd(rows, doc.label), ignored
            ),
            dbg,
        )

    # ------------------------------------------------------------------
    # write — data document (#) → INSERT OR REPLACE
    #          schema document (#!) → CREATE TABLE or ALTER TABLE
    # ------------------------------------------------------------------

    def write(self, jmd_source: str) -> str:
        """Execute a write operation described by a JMD document.

        Args:
            jmd_source: A JMD data document (``#``) to insert or replace
                a record, or a schema document (``#!``) to create or
                extend a table.

        Returns:
            The written record as a JMD document, or a ``# Result``
            document confirming the DDL operation.
        """
        if jmd_mode(jmd_source) == "schema":
            return self._write_schema(jmd_source)

        parser = JMDParser()
        data = parser.parse(jmd_source)
        fm = parser.frontmatter
        dbg = _parse_debug(fm)
        ignored = _check_frontmatter(
            fm, _KNOWN_FM_WRITE, "observable"
        )
        label = self._label_from_source(jmd_source)

        # Bulk-insert: # Table[] with a list of records.
        if isinstance(data, list):
            if label.endswith("[]"):
                label = label[:-2]
            table = self._resolve_or_error(label)
            return _prepend_debug(
                _prepend_ignored_keys(
                    self._bulk_insert(data, table, label),
                    ignored,
                ),
                dbg,
            )

        table = self._resolve_or_error(label)
        if dbg.wants("table"):
            dbg.table = table.name

        # Prevent writes to views — they are read-only from our perspective.
        if table.is_view:
            return serialize(
                {"status": 400, "code": "read_only",
                 "message": (
                     f"'{table.name}' is a view and cannot"
                     " be written to"
                 )},
                label="Error",
            )

        cols = list(data.keys())
        table_cols = {c.name for c in table.columns}
        unknown = [c for c in cols if c not in table_cols]
        if unknown:
            return serialize(
                {"status": 400, "code": "bad_request",
                 "message": (
                     f"Unknown column(s) {unknown!r}"
                     f" in table '{table.name}'"
                 )},
                label="Error",
            )
        placeholders = ", ".join("?" * len(cols))
        col_names = ", ".join(_quote_identifier(c) for c in cols)
        values = [data[c] for c in cols]

        # INSERT OR REPLACE handles both inserts and updates in one
        # statement.  SQLite replaces a row when a UNIQUE or PRIMARY KEY
        # constraint would otherwise be violated.
        sql = (
            f"INSERT OR REPLACE INTO"
            f" {_quote_identifier(table.name)}"
            f" ({col_names}) VALUES ({placeholders})"
        )
        if dbg.wants("sql"):
            dbg.sql = sql
        t0 = time.perf_counter()
        cur = self._conn.execute(sql, values)
        self._conn.commit()
        if dbg.active:
            dbg.timing_ms = (
                (time.perf_counter() - t0) * 1000
            )

        # Re-read the written row by rowid so we return the
        # definitive state (including DEFAULT values).
        rowid = cur.lastrowid
        qt = _quote_identifier(table.name)
        row = self._conn.execute(
            f"SELECT * FROM {qt} WHERE rowid = ?",
            (rowid,),
        ).fetchone()
        result = dict(row) if row else data
        return _prepend_debug(
            _prepend_ignored_keys(
                _row_to_jmd(result, label), ignored
            ),
            dbg,
        )

    # ------------------------------------------------------------------
    # delete — delete document (#-) → DELETE WHERE
    #           schema document (#!) → DROP TABLE or DROP VIEW
    # ------------------------------------------------------------------

    def delete(self, jmd_source: str) -> str:
        """Execute a delete operation described by a JMD document.

        Args:
            jmd_source: A JMD delete document (``#-``) to delete matching
                records, or a schema document (``#!``) to drop the entire
                table or view.

        Returns:
            The deleted record as a JMD data document, or a ``# Error``
            document if the operation is invalid or the record is not found.
        """
        if jmd_mode(jmd_source) == "schema":
            return self._delete_schema(jmd_source)

        # Extract frontmatter for strict-refusal check and debug.
        # JMDDeleteParser does not expose .frontmatter, so we
        # parse once with JMDParser just for the frontmatter.
        fm_parser = JMDParser()
        fm_parser.parse(jmd_source)
        fm = fm_parser.frontmatter
        dbg = _parse_debug(fm)
        _check_frontmatter(fm, _KNOWN_FM_DELETE, "strict")

        doc = JMDDeleteParser().parse(jmd_source)

        if not doc.label:
            return serialize(
                {"status": 400, "code": "bad_request",
                 "message": (
                     "Bulk delete requires a table label"
                     " (use '#- Table[]', not '#- []')"
                 )},
                label="Error",
            )

        table = self._resolve_or_error(doc.label)

        if table.is_view:
            return serialize(
                {"status": 400, "code": "read_only",
                 "message": (
                     f"'{table.name}' is a view and cannot"
                     " be deleted from"
                 )},
                label="Error",
            )

        if doc.is_bulk:
            return self._bulk_delete(doc, table)

        identifiers = (
            doc.identifiers
            if isinstance(doc.identifiers, dict)
            else {}
        )
        table_cols = {c.name for c in table.columns}
        unknown = [k for k in identifiers if k not in table_cols]
        if unknown:
            return serialize(
                {"status": 400, "code": "bad_request",
                 "message": (
                     f"Unknown column(s) {unknown!r}"
                     f" in table '{table.name}'"
                 )},
                label="Error",
            )
        where, params = self._build_where(identifiers)

        # Require at least one filter to prevent accidental full-table deletes.
        if not where:
            return serialize(
                {"status": 400, "code": "bad_request",
                 "message": "Delete requires at least one identifier field"},
                label="Error",
            )

        qt = _quote_identifier(table.name)
        del_sql = f"DELETE FROM {qt} WHERE {where}"
        if dbg.wants("sql"):
            dbg.sql = del_sql
        if dbg.wants("table"):
            dbg.table = table.name

        # Read the row before deletion so we can return it.
        row = self._conn.execute(
            f"SELECT * FROM {qt} WHERE {where}", params
        ).fetchone()
        if row is None:
            return serialize(
                {"status": 404, "code": "not_found",
                 "message": (
                     f"No matching record in '{table.name}'"
                 )},
                label="Error",
            )

        t0 = time.perf_counter()
        self._conn.execute(del_sql, params)
        self._conn.commit()
        if dbg.active:
            dbg.timing_ms = (
                (time.perf_counter() - t0) * 1000
            )
        return _prepend_debug(
            _row_to_jmd(dict(row), doc.label), dbg
        )

    def _bulk_insert(
        self,
        records: list[Any],
        table: Any,
        label: str,
    ) -> str:
        """Insert multiple records from a ``# Table[]`` document.

        Args:
            records: List of dicts, each representing one record.
            table: Resolved :class:`TableInfo`.
            label: Table label for the response document.

        Returns:
            Inserted records as a JMD array document, or a
            ``# Error`` document on failure.
        """
        if not records:
            return serialize(
                {"status": 400, "code": "bad_request",
                 "message": "Bulk insert list is empty"},
                label="Error",
            )

        if table.is_view:
            return serialize(
                {"status": 400, "code": "read_only",
                 "message": (
                     f"'{table.name}' is a view and cannot"
                     " be written to"
                 )},
                label="Error",
            )

        table_cols = {c.name for c in table.columns}
        qt = _quote_identifier(table.name)
        inserted: list[dict[str, Any]] = []

        for i, record in enumerate(records):
            if not isinstance(record, dict) or not record:
                return serialize(
                    {"status": 400, "code": "bad_request",
                     "message": (
                         f"Item {i} is not a valid record"
                     )},
                    label="Error",
                )
            unknown = [
                k for k in record if k not in table_cols
            ]
            if unknown:
                return serialize(
                    {"status": 400, "code": "bad_request",
                     "message": (
                         f"Unknown column(s) {unknown!r}"
                         f" in table '{table.name}'"
                         f" (item {i})"
                     )},
                    label="Error",
                )

            cols = list(record.keys())
            placeholders = ", ".join("?" for _ in cols)
            col_names = ", ".join(
                _quote_identifier(c) for c in cols
            )
            values = [record[c] for c in cols]

            sql = (
                f"INSERT OR REPLACE INTO {qt}"
                f" ({col_names}) VALUES ({placeholders})"
            )
            cur = self._conn.execute(sql, values)
            rowid = cur.lastrowid
            row = self._conn.execute(
                f"SELECT * FROM {qt} WHERE rowid = ?",
                (rowid,),
            ).fetchone()
            inserted.append(
                dict(row) if row else record
            )

        self._conn.commit()
        return _rows_to_jmd(inserted, label)

    def _bulk_delete(
        self,
        doc: Any,
        table: Any,
    ) -> str:
        """Delete multiple records by primary-key list.

        Implements ``#- Table[]`` bulk-delete per JMD Spec §15
        and §22.2.  Scalar list items are treated as primary-key
        values; object list items provide composite-key fields.

        Args:
            doc: Parsed :class:`JMDDelete` with ``is_bulk=True``.
            table: Resolved :class:`TableInfo`.

        Returns:
            Deleted records as a JMD array document, or a
            ``# Error`` document on failure.
        """
        ids = doc.identifiers
        if not ids:
            return serialize(
                {"status": 400, "code": "bad_request",
                 "message": "Bulk delete list is empty"},
                label="Error",
            )

        pks = table.primary_keys
        qt = _quote_identifier(table.name)

        # Scalar IDs → single PK column.
        if not isinstance(ids[0], dict):
            if len(pks) != 1:
                return serialize(
                    {"status": 400, "code": "bad_request",
                     "message": (
                         "Scalar bulk-delete requires exactly"
                         f" one primary key; '{table.name}'"
                         f" has {len(pks)}: {pks}"
                     )},
                    label="Error",
                )
            pk = _quote_identifier(pks[0])
            placeholders = ", ".join("?" for _ in ids)
            params = list(ids)

            rows = self._fetchall(
                f"SELECT * FROM {qt}"
                f" WHERE {pk} IN ({placeholders})",
                params,
            )
            self._conn.execute(
                f"DELETE FROM {qt}"
                f" WHERE {pk} IN ({placeholders})",
                params,
            )
            self._conn.commit()
            return _rows_to_jmd(rows, doc.label)

        # Object IDs → composite or named keys.
        all_rows: list[dict[str, Any]] = []
        for obj in ids:
            if not isinstance(obj, dict) or not obj:
                continue
            where, params = self._build_where(obj)
            if not where:
                continue
            row = self._conn.execute(
                f"SELECT * FROM {qt} WHERE {where}",
                params,
            ).fetchone()
            if row is not None:
                all_rows.append(dict(row))
                self._conn.execute(
                    f"DELETE FROM {qt} WHERE {where}",
                    params,
                )
        self._conn.commit()
        return _rows_to_jmd(all_rows, doc.label)

    # ------------------------------------------------------------------
    # Schema operations (#!)
    # ------------------------------------------------------------------

    def _read_schema(self, jmd_source: str) -> str:
        """Return the table structure as a JMD #! schema document.

        The output mirrors the input syntax expected by _write_schema,
        so the LLM can read a schema, understand column types and
        constraints, and construct correctly-typed data documents.

        The special label ``Database`` (when no real table of that
        name exists) returns a root-schema document that describes
        the server's full capabilities — tables, supported
        frontmatter keys, QBE operators, and tolerance policies.
        """
        label = self._label_from_source(jmd_source)

        # Root-schema: self-description of the server.
        if (
            label.lower() == "database"
            and self._schema.resolve("Database") is None
        ):
            return self._read_root_schema()

        # Reserved DDL-object labels (Slice B+).
        if label == "Index" and self._schema.resolve("Index") is None:
            return self._read_index_doc(jmd_source)
        if (
            label == "Trigger"
            and self._schema.resolve("Trigger") is None
        ):
            return self._read_trigger_doc(jmd_source)
        if label == "View" and self._schema.resolve("View") is None:
            return self._read_view_doc(jmd_source)

        table = self._resolve_or_error(label)

        # Pull the raw DDL: CHECK constraints (column-level for
        # enum reconstruction, table-level for ## check[]) have no
        # PRAGMA introspection — we have to parse sqlite_master.
        row = self._conn.execute(
            "SELECT sql FROM sqlite_master"
            " WHERE type='table' AND name=?",
            (table.name,),
        ).fetchone()
        create_sql = row[0] if row else ""
        if _is_virtual_table_sql(create_sql):
            return self._read_virtual_table(label, table, create_sql)
        body = _extract_create_table_body(create_sql)
        parts = _split_top_level(body) if body else []
        column_part_by_name: dict[str, str] = {}
        constraint_parts: list[str] = []
        for p in parts:
            if _is_table_level_constraint(p):
                constraint_parts.append(p)
            else:
                cn = _column_part_name(p)
                if cn:
                    column_part_by_name[cn] = p

        pk_columns = [c for c in table.columns if c.primary_key]
        composite_pk = len(pk_columns) >= 2

        lines = [f"#! {label}"]
        for col in table.columns:
            jmd_type = _sqlite_type_to_jmd(col.type)
            modifiers: list[str] = []
            if col.primary_key:
                modifiers.append("readonly")
            if col.nullable:
                modifiers.append("optional")
            # Enum reconstruction: column-level CHECK with IN-list.
            col_part = column_part_by_name.get(col.name, "")
            enum_vals = (
                _column_enum_values(col_part, col.name)
                if col_part else None
            )
            # JMD enum form is `key: a|b|c` — bare pipe-list as
            # the type token (the spec implies base_type=string).
            # Adding `string ` in front breaks the parser's pipe
            # detection, so we emit just the pipe-list when an enum
            # is present.
            type_token = (
                "|".join(enum_vals) if enum_vals else jmd_type
            )
            suffix = (
                " " + " ".join(modifiers) if modifiers else ""
            )
            default_token = (
                f" = {_unquote_default_for_jmd(col.default)}"
                if col.default is not None else ""
            )
            lines.append(
                f"{col.name}: {type_token}{suffix}{default_token}"
            )

        if composite_pk:
            lines.append("")
            lines.append("## primary-key[]")
            for pkc in pk_columns:
                lines.append(f"- {pkc.name}")

        uniques = _table_unique_constraints(self._conn, table.name)
        if uniques:
            lines.append("")
            lines.append("## unique[]")
            for cols in uniques:
                lines.append(f"- {', '.join(cols)}")

        table_checks = _table_check_clauses(constraint_parts)
        if table_checks:
            lines.append("")
            lines.append("## check[]")
            for chk in table_checks:
                lines.append(f"- {chk}")

        fks = _table_foreign_keys(self._conn, table.name)
        if fks:
            lines.append("")
            lines.append("## references[]")
            for local, ftab, fcol in fks:
                lines.append(f"- {local}: {ftab}.{fcol}")

        return "\n".join(lines)

    def _read_root_schema(self) -> str:
        """Build the root-schema document for ``#! Database``.

        The document describes this server's capabilities so that
        an LLM can discover tables, frontmatter keys, filter
        operators, and tolerance policies in a single call.
        """
        tables = sorted(self._schema.tables().keys())

        lines: list[str] = ["#! Database"]

        # Tables — the only entity-level information about a
        # Database.  Server capabilities (frontmatter keys, filter
        # operators, tolerance policies, debug values) belong in
        # the tool descriptions, not in entity schemas.
        lines.append("## tables[]")
        for t in tables:
            lines.append(f"- {t}")

        indexes = _user_indexes(self._conn)
        if indexes:
            lines.append("")
            lines.append("## indexes[]")
            for idx_name, _tbl in indexes:
                lines.append(f"- {idx_name}")

        triggers = _user_triggers(self._conn)
        if triggers:
            lines.append("")
            lines.append("## triggers[]")
            for trg_name, _tbl in triggers:
                lines.append(f"- {trg_name}")

        views = _user_views(self._conn)
        if views:
            lines.append("")
            lines.append("## views[]")
            for v_name in views:
                lines.append(f"- {v_name}")

        return "\n".join(lines)

    def _write_schema(self, jmd_source: str) -> str:
        """Create a new table or add columns to an existing one.

        Column-level modifiers ``readonly`` (single-col PK),
        ``optional`` (NULL), ``= <expr>`` (DEFAULT), and the
        ``a|b|c`` enum form (column-level CHECK) are honoured.

        Table-level constraints come in via sub-sections:
            ``## primary-key[]``  composite PK (column names)
            ``## unique[]``       UNIQUE constraints (one entry =
                                   comma-separated columns)
            ``## check[]``        CHECK expressions (raw SQL)
            ``## references[]``   single-col FKs in the form
                                   ``local: Table.foreign``

        Non-destructive on existing tables: ALTER only adds columns.
        Constraint changes on an existing table will land in
        ``constraint-changes-skipped`` until ``action: rebuild``
        (Slice F) is implemented.
        """
        schema = JMDSchemaParser().parse(jmd_source)
        table_name = schema.label

        # Reserved DDL-object labels: dispatch to the per-kind
        # handler unless an actual table by the same name exists
        # (mirrors the ``#! Database`` root-schema fallback).
        if (
            table_name == "Index"
            and self._schema.resolve("Index") is None
        ):
            return self._write_index_doc(jmd_source)
        if (
            table_name == "Trigger"
            and self._schema.resolve("Trigger") is None
        ):
            return self._write_trigger_doc(jmd_source)
        if (
            table_name == "View"
            and self._schema.resolve("View") is None
        ):
            return self._write_view_doc(jmd_source)

        # Sub-section data is only visible through JMDParser; the
        # schema parser leaves ``## name[]`` as empty SchemaObjects.
        data = JMDParser().parse(jmd_source)
        primary_keys_sec = _as_string_list(data.get("primary-key"))
        uniques_sec = _as_string_list(data.get("unique"))
        checks_sec = _as_string_list(data.get("check"))
        references_sec = _as_string_list(data.get("references"))
        indexes_sec = data.get("Index", []) or []
        triggers_sec = data.get("Trigger", []) or []
        using_module = data.get("using")
        unindexed_sec = _as_string_list(data.get("unindexed"))
        options_sec = _as_string_list(data.get("options"))

        scalar_fields = [
            f for f in schema.fields if isinstance(f, SchemaField)
        ]

        existing = self._schema.resolve(table_name)
        if existing is not None and existing.is_view:
            return serialize(
                {"status": 400, "code": "read_only",
                 "message": (
                     f"'{table_name}' is a view"
                     " and cannot be altered"
                 )},
                label="Error",
            )

        if existing is None and using_module:
            return self._create_virtual_table(
                table_name,
                str(using_module),
                scalar_fields,
                unindexed_sec,
                options_sec,
            )
        if existing is None:
            return self._create_table(
                table_name,
                scalar_fields,
                primary_keys_sec,
                uniques_sec,
                checks_sec,
                references_sec,
                indexes_sec,
                triggers_sec,
            )
        # Frontmatter ``action: rebuild`` → SQLite table-rebuild
        # dance: stage new schema, copy data, swap. Without it we
        # only do additive ALTER (existing behaviour). The data
        # parser above lost its frontmatter view by parsing again;
        # re-extract from source.
        fm_parser = JMDParser()
        fm_parser.parse(jmd_source)
        fm = fm_parser.frontmatter
        if fm.get("action") == "rebuild":
            return self._rebuild_table(
                table_name,
                existing,
                scalar_fields,
                primary_keys_sec,
                uniques_sec,
                checks_sec,
                references_sec,
                indexes_sec,
                triggers_sec,
            )
        return self._alter_table(
            table_name,
            existing,
            scalar_fields,
            bool(
                primary_keys_sec or uniques_sec or checks_sec
                or references_sec
            ),
        )

    def _render_create_table_sql(
        self,
        table_name: str,
        scalar_fields: list[SchemaField],
        primary_keys_sec: list[str],
        uniques_sec: list[str],
        checks_sec: list[str],
        references_sec: list[str],
    ) -> str | dict[str, Any]:
        """Build the CREATE TABLE SQL string (no execute).

        Returns a SQL string on success or an error-payload dict
        (status / code / message) on validation failure (e.g. a
        malformed ``## references[]`` entry).
        """
        composite_pk_cols: list[str] = []
        for entry in primary_keys_sec:
            composite_pk_cols.extend(_split_columns(entry))
        use_table_level_pk = len(composite_pk_cols) >= 2

        col_defs: list[str] = []
        for f in scalar_fields:
            col_defs.append(
                self._render_column_def(f, use_table_level_pk)
            )

        constraints: list[str] = []
        if use_table_level_pk:
            constraints.append(
                "PRIMARY KEY ("
                + ", ".join(
                    _quote_identifier(c) for c in composite_pk_cols
                )
                + ")"
            )
        for u in uniques_sec:
            cols = _split_columns(u)
            if not cols:
                continue
            constraints.append(
                "UNIQUE ("
                + ", ".join(_quote_identifier(c) for c in cols)
                + ")"
            )
        for c in checks_sec:
            constraints.append(f"CHECK ({c})")
        for r in references_sec:
            fk = _parse_reference(r)
            if fk is None:
                return {
                    "status": 400, "code": "bad_request",
                    "message": (
                        f"references entry {r!r} not in form"
                        " 'local_col: Table.foreign_col'"
                    ),
                }
            local, ftab, fcol = fk
            constraints.append(
                f"FOREIGN KEY ({_quote_identifier(local)}) "
                f"REFERENCES {_quote_identifier(ftab)}"
                f"({_quote_identifier(fcol)})"
            )

        body_sql = ", ".join(col_defs + constraints)
        return (
            f"CREATE TABLE {_quote_identifier(table_name)}"
            f" ({body_sql})"
        )

    def _create_table(
        self,
        table_name: str,
        scalar_fields: list[SchemaField],
        primary_keys_sec: list[str],
        uniques_sec: list[str],
        checks_sec: list[str],
        references_sec: list[str],
        indexes_sec: list[Any],
        triggers_sec: list[Any],
    ) -> str:
        """Render and execute a fresh CREATE TABLE."""
        sql_or_err = self._render_create_table_sql(
            table_name, scalar_fields, primary_keys_sec,
            uniques_sec, checks_sec, references_sec,
        )
        if isinstance(sql_or_err, dict):
            return serialize(sql_or_err, label="Error")
        sql = sql_or_err
        try:
            self._conn.execute(sql)
        except sqlite3.Error as e:
            return serialize(
                {"status": 400, "code": "ddl_failed",
                 "message": str(e)},
                label="Error",
            )
        # Inline ``## Index[]`` entries are created in the same
        # logical operation as the table; if any one fails, we
        # drop the table to keep create-table-with-indexes
        # all-or-nothing.
        for entry in indexes_sec:
            if not isinstance(entry, dict):
                continue
            idx_sql_or_err = self._build_inline_index_sql(
                entry, table_name
            )
            if isinstance(idx_sql_or_err, dict):
                # Validation/render error — abort and drop.
                self._conn.execute(
                    f"DROP TABLE {_quote_identifier(table_name)}"
                )
                self._conn.commit()
                return serialize(idx_sql_or_err, label="Error")
            try:
                self._conn.execute(idx_sql_or_err)
            except sqlite3.Error as e:
                self._conn.execute(
                    f"DROP TABLE {_quote_identifier(table_name)}"
                )
                self._conn.commit()
                return serialize(
                    {"status": 400, "code": "ddl_failed",
                     "message": str(e)},
                    label="Error",
                )
        for entry in triggers_sec:
            if not isinstance(entry, dict):
                continue
            trg_sql_or_err = self._build_inline_trigger_sql(
                entry, table_name
            )
            if isinstance(trg_sql_or_err, dict):
                self._conn.execute(
                    f"DROP TABLE {_quote_identifier(table_name)}"
                )
                self._conn.commit()
                return serialize(trg_sql_or_err, label="Error")
            try:
                self._conn.execute(trg_sql_or_err)
            except sqlite3.Error as e:
                self._conn.execute(
                    f"DROP TABLE {_quote_identifier(table_name)}"
                )
                self._conn.commit()
                return serialize(
                    {"status": 400, "code": "ddl_failed",
                     "message": str(e)},
                    label="Error",
                )
        self._conn.commit()
        self._schema = SchemaInspector(self._conn)
        return serialize(
            {"table": table_name, "created": True}, label="Result"
        )

    def _read_virtual_table(
        self,
        label: str,
        table: TableInfo,
        create_sql: str,
    ) -> str:
        """Render the read-back form of a CREATE VIRTUAL TABLE.

        Format mirrors the write input:
            using: <module>
            <col>: string
            ...
            ## unindexed[]
            - <col>
            ## options[]
            - <key = value>
        """
        parsed = _parse_virtual_table(create_sql)
        if parsed is None:
            return f"#! {label}"
        module, args = parsed
        unindexed: list[str] = []
        options: list[str] = []
        for a in args:
            cls = _is_unindexed_arg(a)
            if cls is None:
                # Module option (key = value).
                options.append(a.strip())
            else:
                cname, is_uni = cls
                if is_uni:
                    unindexed.append(cname)
        lines = [f"#! {label}", f"using: {module}"]
        for col in table.columns:
            lines.append(f"{col.name}: string")
        if unindexed:
            lines.append("")
            lines.append("## unindexed[]")
            for u in unindexed:
                lines.append(f"- {u}")
        if options:
            lines.append("")
            lines.append("## options[]")
            for o in options:
                lines.append(f"- \"{o}\"")
        return "\n".join(lines)

    def _create_virtual_table(
        self,
        table_name: str,
        module: str,
        scalar_fields: list[SchemaField],
        unindexed_sec: list[str],
        options_sec: list[str],
    ) -> str:
        """Render and execute a CREATE VIRTUAL TABLE statement."""
        columns = [f.key for f in scalar_fields]
        unindexed = {u.strip() for u in unindexed_sec if u.strip()}
        sql = _build_create_virtual_table_sql(
            table_name, module, columns, unindexed, options_sec,
        )
        try:
            self._conn.execute(sql)
            self._conn.commit()
        except sqlite3.Error as e:
            return serialize(
                {"status": 400, "code": "ddl_failed",
                 "message": str(e)},
                label="Error",
            )
        self._schema = SchemaInspector(self._conn)
        return serialize(
            {"table": table_name, "created": True,
             "virtual": True, "using": module},
            label="Result",
        )

    def _alter_table(
        self,
        table_name: str,
        existing: TableInfo,
        scalar_fields: list[SchemaField],
        any_constraint_section: bool,
    ) -> str:
        """Additive ALTER: add new columns, never modify existing."""
        existing_cols = {c.name for c in existing.columns}
        added: list[str] = []
        with self._conn:
            for f in scalar_fields:
                if f.key in existing_cols:
                    continue
                sqlite_type = _JMD_TO_SQLITE.get(
                    f.base_type.lower(), "TEXT"
                )
                parts = [
                    f"ALTER TABLE"
                    f" {_quote_identifier(table_name)}",
                    "ADD COLUMN",
                    f"{_quote_identifier(f.key)} {sqlite_type}",
                ]
                # SQLite ADD COLUMN restrictions: NOT NULL requires a
                # DEFAULT; UNIQUE / FK with non-NULL default is
                # forbidden. We render DEFAULT and only attach
                # NOT NULL when a default is also given.
                if f.default is not None:
                    parts.append(
                        f"DEFAULT {_quote_default(f.default)}"
                    )
                if not f.optional and f.default is not None:
                    parts.append("NOT NULL")
                self._conn.execute(" ".join(parts))
                added.append(f.key)
        self._schema = SchemaInspector(self._conn)
        skipped = [
            f.key for f in scalar_fields if f.key in existing_cols
        ]
        result: dict[str, Any] = {
            "table": table_name,
            "altered": bool(added),
            "added": added,
        }
        if skipped:
            result["skipped"] = skipped
        if any_constraint_section:
            # Constraint changes on existing tables need a rebuild,
            # which is Slice F. Surface this loud-and-clear.
            result["constraint-changes-skipped"] = True
        return serialize(result, label="Result")

    def _rebuild_table(
        self,
        table_name: str,
        existing: TableInfo,
        scalar_fields: list[SchemaField],
        primary_keys_sec: list[str],
        uniques_sec: list[str],
        checks_sec: list[str],
        references_sec: list[str],
        indexes_sec: list[Any],
        triggers_sec: list[Any],
    ) -> str:
        """SQLite table-rebuild dance for non-additive schema changes.

        1. Build CREATE TABLE for a staging name with the new schema.
        2. INSERT INTO staging SELECT FROM old, copying only columns
           that exist on both sides (added columns get DEFAULT/NULL,
           dropped columns lose their data).
        3. Drop the old table; rename staging to its name.
        4. Recreate inline ## Index[] / ## Trigger[] entries.

        Atomic: the whole sequence runs inside one explicit BEGIN/
        COMMIT (or ROLLBACK on any failure). Pre-existing indexes
        and triggers on the table are dropped along with the table
        and must be redeclared in the rebuild document.
        """
        staging_name = f"{table_name}__rebuild"
        sql_or_err = self._render_create_table_sql(
            staging_name, scalar_fields, primary_keys_sec,
            uniques_sec, checks_sec, references_sec,
        )
        if isinstance(sql_or_err, dict):
            return serialize(sql_or_err, label="Error")
        create_staging_sql = sql_or_err

        # Common columns: copy what survives the schema change.
        old_col_names = {c.name for c in existing.columns}
        new_col_names = {f.key for f in scalar_fields}
        common = [
            f.key for f in scalar_fields if f.key in old_col_names
        ]
        common_q = ", ".join(_quote_identifier(c) for c in common)
        if common:
            insert_sql = (
                f"INSERT INTO {_quote_identifier(staging_name)}"
                f" ({common_q})"
                f" SELECT {common_q}"
                f" FROM {_quote_identifier(table_name)}"
            )
        else:
            insert_sql = None

        # Switch to autocommit so we drive the transaction by hand.
        # SQLite supports DDL inside transactions; the legacy Python
        # sqlite3 isolation modes implicit-commit on DDL otherwise.
        old_iso = self._conn.isolation_level
        self._conn.isolation_level = None
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                # Defensive: drop any leftover staging table from a
                # previous failed rebuild attempt.
                self._conn.execute(
                    f"DROP TABLE IF EXISTS"
                    f" {_quote_identifier(staging_name)}"
                )
                self._conn.execute(create_staging_sql)
                if insert_sql is not None:
                    self._conn.execute(insert_sql)
                self._conn.execute(
                    f"DROP TABLE"
                    f" {_quote_identifier(table_name)}"
                )
                self._conn.execute(
                    f"ALTER TABLE"
                    f" {_quote_identifier(staging_name)}"
                    f" RENAME TO {_quote_identifier(table_name)}"
                )
                # Inline indexes & triggers from the rebuild doc.
                for entry in indexes_sec:
                    if not isinstance(entry, dict):
                        continue
                    idx = self._build_inline_index_sql(
                        entry, table_name
                    )
                    if isinstance(idx, dict):
                        raise sqlite3.Error(
                            str(idx.get("message", ""))
                        )
                    self._conn.execute(idx)
                for entry in triggers_sec:
                    if not isinstance(entry, dict):
                        continue
                    trg = self._build_inline_trigger_sql(
                        entry, table_name
                    )
                    if isinstance(trg, dict):
                        raise sqlite3.Error(
                            str(trg.get("message", ""))
                        )
                    self._conn.execute(trg)
                self._conn.execute("COMMIT")
            except sqlite3.Error as e:
                self._conn.execute("ROLLBACK")
                return serialize(
                    {"status": 400, "code": "rebuild_failed",
                     "message": str(e)},
                    label="Error",
                )
        finally:
            self._conn.isolation_level = old_iso

        self._schema = SchemaInspector(self._conn)
        added = sorted(new_col_names - old_col_names)
        dropped = sorted(old_col_names - new_col_names)
        result: dict[str, Any] = {
            "table": table_name,
            "rebuilt": True,
        }
        if added:
            result["added"] = added
        if dropped:
            result["dropped"] = dropped
        return serialize(result, label="Result")

    def _render_column_def(
        self, f: SchemaField, use_table_level_pk: bool
    ) -> str:
        """Render one column-def fragment for inside CREATE TABLE."""
        sqlite_type = _JMD_TO_SQLITE.get(f.base_type.lower(), "TEXT")
        parts = [_quote_identifier(f.key), sqlite_type]
        # Single-column PK is rendered inline; composite PK takes the
        # table-level path (caller passes use_table_level_pk).
        if f.readonly and not use_table_level_pk:
            parts.append("PRIMARY KEY")
        if not f.optional:
            parts.append("NOT NULL")
        if f.default is not None:
            parts.append(f"DEFAULT {_quote_default(f.default)}")
        if f.enum_values:
            in_list = ", ".join(
                _quote_default(v) for v in f.enum_values
            )
            parts.append(
                f"CHECK ({_quote_identifier(f.key)} IN ({in_list}))"
            )
        return " ".join(parts)

    # ------------------------------------------------------------------
    # Index DDL — top-level ``#! Index`` shape (Slice B)
    # ------------------------------------------------------------------

    def _build_inline_index_sql(
        self, entry: dict[str, Any], table_name: str
    ) -> str | dict[str, Any]:
        """Render one ``## Index[]`` entry into a CREATE INDEX SQL.

        Returns the SQL string on success, or an error dict ready to
        be serialised (``status``, ``code``, ``message``) on failure.
        Inline entries inherit ``table:`` from the enclosing
        ``#! Table`` document; an explicit ``table:`` override is
        accepted for symmetry with the top-level shape.
        """
        name = entry.get("name")
        cols_raw = entry.get("columns", "")
        if not name or not cols_raw:
            return {
                "status": 400, "code": "bad_request",
                "message": (
                    "## Index[] entry requires 'name' and 'columns'"
                ),
            }
        unique = _coerce_bool(entry.get("unique", False))
        where_val = entry.get("where")
        target = str(entry.get("table") or table_name)
        return _build_create_index_sql(
            str(name),
            target,
            _split_columns(str(cols_raw)),
            unique,
            str(where_val) if where_val else None,
        )

    def _write_index_doc(self, jmd_source: str) -> str:
        """Handle ``write('#! Index ...')`` — create one index."""
        data = JMDParser().parse(jmd_source)
        name = data.get("name")
        table = data.get("table")
        cols_raw = data.get("columns", "")
        if not name or not table or not cols_raw:
            return serialize(
                {"status": 400, "code": "bad_request",
                 "message": (
                     "#! Index requires name, table, and columns"
                 )},
                label="Error",
            )
        unique = _coerce_bool(data.get("unique", False))
        where_val = data.get("where")
        sql = _build_create_index_sql(
            str(name),
            str(table),
            _split_columns(str(cols_raw)),
            unique,
            str(where_val) if where_val else None,
        )
        try:
            self._conn.execute(sql)
            self._conn.commit()
        except sqlite3.Error as e:
            return serialize(
                {"status": 400, "code": "ddl_failed",
                 "message": str(e)},
                label="Error",
            )
        self._schema = SchemaInspector(self._conn)
        return serialize(
            {"index": str(name), "table": str(table),
             "created": True},
            label="Result",
        )

    def _read_index_doc(self, jmd_source: str) -> str:
        """Handle ``read('#! Index ...')`` — return one index's schema."""
        data = JMDParser().parse(jmd_source)
        name = data.get("name")
        if not name:
            return serialize(
                {"status": 400, "code": "bad_request",
                 "message": "#! Index read requires 'name' field"},
                label="Error",
            )
        row = self._conn.execute(
            "SELECT name, tbl_name, sql FROM sqlite_master"
            " WHERE type='index' AND name=?",
            (str(name),),
        ).fetchone()
        if not row or not row[2]:
            return serialize(
                {"status": 404, "code": "not_found",
                 "message": f"Index '{name}' does not exist"},
                label="Error",
            )
        idx_name, table, sql = row[0], row[1], row[2]
        info = self._conn.execute(
            f'PRAGMA index_info("{idx_name}")'
        ).fetchall()
        cols = [r[2] for r in info]
        unique = bool(
            re.search(r"\bUNIQUE\b", sql, re.IGNORECASE)
        )
        where = _index_where_clause(sql)
        lines = [
            "#! Index",
            f"name: {idx_name}",
            f"table: {table}",
            f"columns: {', '.join(cols)}",
        ]
        if unique:
            lines.append("unique: true")
        if where:
            lines.append(f"where: {where}")
        return "\n".join(lines)

    def _delete_index_doc(
        self, jmd_source: str, fm: dict[str, Any]
    ) -> str:
        """Handle ``delete('#! Index ...')`` — drop one index."""
        if fm.get("confirm") != "drop-index":
            return serialize(
                {"status": 400, "code": "confirmation_required",
                 "message": (
                     "Dropping an index requires"
                     " 'confirm: drop-index' in the frontmatter"
                 )},
                label="Error",
            )
        data = JMDParser().parse(jmd_source)
        name = data.get("name")
        if not name:
            return serialize(
                {"status": 400, "code": "bad_request",
                 "message": "#! Index delete requires 'name' field"},
                label="Error",
            )
        row = self._conn.execute(
            "SELECT name FROM sqlite_master"
            " WHERE type='index' AND name=?",
            (str(name),),
        ).fetchone()
        if not row:
            return serialize(
                {"status": 404, "code": "not_found",
                 "message": f"Index '{name}' does not exist"},
                label="Error",
            )
        self._conn.execute(
            f"DROP INDEX {_quote_identifier(str(name))}"
        )
        self._conn.commit()
        self._schema = SchemaInspector(self._conn)
        return serialize(
            {"index": str(name), "dropped": True},
            label="Result",
        )

    # ------------------------------------------------------------------
    # Trigger DDL — top-level ``#! Trigger`` shape (Slice C)
    # ------------------------------------------------------------------

    def _build_inline_trigger_sql(
        self, entry: dict[str, Any], table_name: str
    ) -> str | dict[str, Any]:
        """Render one ``## Trigger[]`` entry into a CREATE TRIGGER SQL.

        The enclosing ``#! Table`` supplies a default ``table:`` so
        inline triggers usually omit it; an explicit override is
        accepted.
        """
        name = entry.get("name")
        when = entry.get("when")
        event = entry.get("event")
        body = entry.get("body")
        if not name or not when or not event or not body:
            return {
                "status": 400, "code": "bad_request",
                "message": (
                    "## Trigger[] entry requires"
                    " name, when, event, body"
                ),
            }
        condition = entry.get("condition")
        target = str(entry.get("table") or table_name)
        return _build_create_trigger_sql(
            str(name),
            target,
            str(when),
            str(event),
            str(condition) if condition else None,
            str(body),
        )

    def _write_trigger_doc(self, jmd_source: str) -> str:
        """Handle ``write('#! Trigger ...')`` — create one trigger."""
        data = JMDParser().parse(jmd_source)
        name = data.get("name")
        table = data.get("table")
        when = data.get("when")
        event = data.get("event")
        body = data.get("body")
        if (
            not name or not table or not when
            or not event or not body
        ):
            return serialize(
                {"status": 400, "code": "bad_request",
                 "message": (
                     "#! Trigger requires name, table,"
                     " when, event, body"
                 )},
                label="Error",
            )
        condition = data.get("condition")
        sql = _build_create_trigger_sql(
            str(name),
            str(table),
            str(when),
            str(event),
            str(condition) if condition else None,
            str(body),
        )
        try:
            self._conn.execute(sql)
            self._conn.commit()
        except sqlite3.Error as e:
            return serialize(
                {"status": 400, "code": "ddl_failed",
                 "message": str(e)},
                label="Error",
            )
        self._schema = SchemaInspector(self._conn)
        return serialize(
            {"trigger": str(name), "table": str(table),
             "created": True},
            label="Result",
        )

    def _read_trigger_doc(self, jmd_source: str) -> str:
        """Handle ``read('#! Trigger ...')`` — return one trigger's schema."""
        data = JMDParser().parse(jmd_source)
        name = data.get("name")
        if not name:
            return serialize(
                {"status": 400, "code": "bad_request",
                 "message": (
                     "#! Trigger read requires 'name' field"
                 )},
                label="Error",
            )
        row = self._conn.execute(
            "SELECT sql FROM sqlite_master"
            " WHERE type='trigger' AND name=?",
            (str(name),),
        ).fetchone()
        if not row or not row[0]:
            return serialize(
                {"status": 404, "code": "not_found",
                 "message": f"Trigger '{name}' does not exist"},
                label="Error",
            )
        parsed = _parse_trigger_sql(row[0])
        if parsed is None:
            return serialize(
                {"status": 500, "code": "parse_failed",
                 "message": (
                     "could not parse stored trigger SQL"
                 )},
                label="Error",
            )
        lines = [
            "#! Trigger",
            f"name: {parsed['name']}",
            f"table: {parsed['table']}",
            f"when: {parsed['when']}",
            f"event: {parsed['event']}",
        ]
        if parsed["condition"]:
            lines.append(f"condition: {parsed['condition']}")
        # Body is multi-line; use JMD JSON-escape form.
        body_escaped = (
            parsed["body"]
            .replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
        )
        lines.append(f'body: "{body_escaped}"')
        return "\n".join(lines)

    def _delete_trigger_doc(
        self, jmd_source: str, fm: dict[str, Any]
    ) -> str:
        """Handle ``delete('#! Trigger ...')`` — drop one trigger."""
        if fm.get("confirm") != "drop-trigger":
            return serialize(
                {"status": 400, "code": "confirmation_required",
                 "message": (
                     "Dropping a trigger requires"
                     " 'confirm: drop-trigger' in the frontmatter"
                 )},
                label="Error",
            )
        data = JMDParser().parse(jmd_source)
        name = data.get("name")
        if not name:
            return serialize(
                {"status": 400, "code": "bad_request",
                 "message": (
                     "#! Trigger delete requires 'name' field"
                 )},
                label="Error",
            )
        row = self._conn.execute(
            "SELECT name FROM sqlite_master"
            " WHERE type='trigger' AND name=?",
            (str(name),),
        ).fetchone()
        if not row:
            return serialize(
                {"status": 404, "code": "not_found",
                 "message": f"Trigger '{name}' does not exist"},
                label="Error",
            )
        self._conn.execute(
            f"DROP TRIGGER {_quote_identifier(str(name))}"
        )
        self._conn.commit()
        self._schema = SchemaInspector(self._conn)
        return serialize(
            {"trigger": str(name), "dropped": True},
            label="Result",
        )

    # ------------------------------------------------------------------
    # View DDL — top-level ``#! View`` shape (Slice D)
    # ------------------------------------------------------------------

    def _write_view_doc(self, jmd_source: str) -> str:
        """Handle ``write('#! View ...')`` — create one view."""
        data = JMDParser().parse(jmd_source)
        name = data.get("name")
        select = data.get("select")
        if not name or not select:
            return serialize(
                {"status": 400, "code": "bad_request",
                 "message": "#! View requires name and select"},
                label="Error",
            )
        sql = (
            f"CREATE VIEW {_quote_identifier(str(name))}"
            f" AS {select}"
        )
        try:
            self._conn.execute(sql)
            self._conn.commit()
        except sqlite3.Error as e:
            return serialize(
                {"status": 400, "code": "ddl_failed",
                 "message": str(e)},
                label="Error",
            )
        self._schema = SchemaInspector(self._conn)
        return serialize(
            {"view": str(name), "created": True},
            label="Result",
        )

    def _read_view_doc(self, jmd_source: str) -> str:
        """Handle ``read('#! View ...')`` — return one view's schema."""
        data = JMDParser().parse(jmd_source)
        name = data.get("name")
        if not name:
            return serialize(
                {"status": 400, "code": "bad_request",
                 "message": "#! View read requires 'name' field"},
                label="Error",
            )
        row = self._conn.execute(
            "SELECT sql FROM sqlite_master"
            " WHERE type='view' AND name=?",
            (str(name),),
        ).fetchone()
        if not row or not row[0]:
            return serialize(
                {"status": 404, "code": "not_found",
                 "message": f"View '{name}' does not exist"},
                label="Error",
            )
        select_body = _parse_view_select(row[0])
        if select_body is None:
            return serialize(
                {"status": 500, "code": "parse_failed",
                 "message": (
                     "could not parse stored view SQL"
                 )},
                label="Error",
            )
        # Multi-line bodies survive via JMD JSON-escape.
        select_escaped = (
            select_body
            .replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
        )
        lines = [
            "#! View",
            f"name: {name}",
            f'select: "{select_escaped}"',
        ]
        return "\n".join(lines)

    def _delete_view_doc(
        self, jmd_source: str, fm: dict[str, Any]
    ) -> str:
        """Handle ``delete('#! View ...')`` — drop one view."""
        if fm.get("confirm") != "drop-view":
            return serialize(
                {"status": 400, "code": "confirmation_required",
                 "message": (
                     "Dropping a view requires"
                     " 'confirm: drop-view' in the frontmatter"
                 )},
                label="Error",
            )
        data = JMDParser().parse(jmd_source)
        name = data.get("name")
        if not name:
            return serialize(
                {"status": 400, "code": "bad_request",
                 "message": "#! View delete requires 'name' field"},
                label="Error",
            )
        row = self._conn.execute(
            "SELECT name FROM sqlite_master"
            " WHERE type='view' AND name=?",
            (str(name),),
        ).fetchone()
        if not row:
            return serialize(
                {"status": 404, "code": "not_found",
                 "message": f"View '{name}' does not exist"},
                label="Error",
            )
        self._conn.execute(
            f"DROP VIEW {_quote_identifier(str(name))}"
        )
        self._conn.commit()
        self._schema = SchemaInspector(self._conn)
        return serialize(
            {"view": str(name), "dropped": True},
            label="Result",
        )

    def _delete_schema(self, jmd_source: str) -> str:
        """Drop a table, view, or other DDL object.

        Requires an explicit ``confirm:`` frontmatter key (per kind:
        ``drop-table`` for tables/views, ``drop-index`` for indexes)
        as a safety gate.
        """
        # Extract frontmatter for the confirm check.
        fm_parser = JMDParser()
        fm_parser.parse(jmd_source)
        fm = fm_parser.frontmatter

        # Reserved DDL-object dispatch (Slice B+).
        label = self._label_from_source(jmd_source)
        if label == "Index" and self._schema.resolve("Index") is None:
            return self._delete_index_doc(jmd_source, fm)
        if (
            label == "Trigger"
            and self._schema.resolve("Trigger") is None
        ):
            return self._delete_trigger_doc(jmd_source, fm)
        if label == "View" and self._schema.resolve("View") is None:
            return self._delete_view_doc(jmd_source, fm)

        if fm.get("confirm") != "drop-table":
            return serialize(
                {"status": 400, "code": "confirmation_required",
                 "message": (
                     "Dropping a table requires "
                     "'confirm: drop-table' in the frontmatter"
                 )},
                label="Error",
            )

        table = self._schema.resolve(label)
        if table is None:
            return serialize(
                {"status": 404, "code": "not_found",
                 "message": f"Table '{label}' does not exist"},
                label="Error",
            )
        if table.is_view:
            self._conn.execute(
                f"DROP VIEW IF EXISTS {_quote_identifier(table.name)}"
            )
        else:
            self._conn.execute(
                f"DROP TABLE IF EXISTS {_quote_identifier(table.name)}"
            )
        self._conn.commit()
        # Invalidate the cache after any DDL operation.
        self._schema = SchemaInspector(self._conn)
        return serialize({"dropped": label}, label="Result")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _paginated_jmd(
        self,
        rows: list[dict[str, Any]],
        label: str,
        total: int,
        page: int,
        page_size: int,
    ) -> str:
        """Wrap a page of rows in a JMD document with pagination frontmatter.

        Pagination metadata is emitted *before* the root heading so that
        it is structurally distinct from body fields and immediately
        available to the next agent in the pipeline as document-level
        metadata (see JMD spec §3.5, §16).
        """
        pages = (total + page_size - 1) // page_size
        fm = (
            f"total: {total}\n"
            f"page: {page}\n"
            f"pages: {pages}\n"
            f"page-size: {page_size}\n"
        )
        body = serialize({"data": rows}, label=label)
        return fm + "\n" + body

    def _aggregate(
        self,
        table: TableInfo,
        label: str,
        where: str,
        where_params: list[Any],
        fm: dict[str, Any],
    ) -> str:
        """Build and execute a GROUP BY query from frontmatter aggregation keys.

        Translates frontmatter keys ``group``, ``sum``, ``avg``, ``min``,
        ``max``, ``count``, ``having``, and ``sort`` into a single SQL
        SELECT … GROUP BY … HAVING … ORDER BY statement.

        Result columns for aggregate functions are named ``<func>_<field>``
        (e.g. ``sum_Freight``, ``avg_UnitPrice``).  The ``count`` bare key
        produces a ``count`` column via COUNT(*).

        ``having:`` accepts comma-separated comparison conditions that
        reference result column aliases (e.g. ``having: count > 5,
        sum_Freight > 1000``).  Each condition is parameterized.

        ``sort:`` accepts comma-separated ``<column> [asc|desc]`` pairs
        referencing any result column (grouping key or aggregate alias).

        All field references are validated against the table schema before
        any SQL is generated.  Unknown fields raise a ``ValueError`` which
        the caller converts to a ``# Error`` document.

        Pagination via ``page-size:`` / ``page:`` is applied to the aggregated
        result set using a subquery COUNT.

        If ``select:`` is present it filters the result columns after
        fetching (post-aggregation projection).
        """
        # ----------------------------------------------------------------
        # Validation: every user-supplied field name must exist in the
        # table before we interpolate it into SQL.  This prevents SQLite's
        # silent "unknown quoted identifier → string literal" fallback,
        # which would produce nonsense results without any error.
        # ----------------------------------------------------------------
        table_cols = {c.name for c in table.columns}

        def _require_table_col(field: str, context: str) -> None:
            if field not in table_cols:
                available = ", ".join(sorted(table_cols))
                raise ValueError(
                    f"Unknown column '{field}' in '{context}' for table "
                    f"'{table.name}'. Available columns: {available}"
                )

        select_parts: list[str] = []
        group_cols: list[str] = []

        if "group" in fm:
            for col in str(fm["group"]).split(","):
                col = col.strip()
                if col:
                    _require_table_col(col, "group")
                    group_cols.append(col)
                    select_parts.append(_quote_identifier(col))

        if "count" in fm:
            select_parts.append("COUNT(*) AS count")

        for func in _AGG_FUNCS:
            if func not in fm:
                continue
            for raw_col in str(fm[func]).split(","):
                raw_col = raw_col.strip()
                if not raw_col:
                    continue
                expr, alias = _parse_agg_expr(raw_col)
                # For simple (non-join) aggregation, only plain column names
                # are valid — no arithmetic expressions.
                _require_table_col(expr, func)
                if alias is None:
                    alias = f"{func}_{expr}"
                select_parts.append(
                    f"{func.upper()}({_quote_identifier(expr)})"
                    f" AS {_quote_identifier(alias)}"
                )

        if not select_parts:
            select_parts = ["COUNT(*) AS count"]

        # Result columns: grouping keys + aggregate aliases + count.
        # Used to validate order/having references, which must name a
        # result column, not an underlying table column.
        result_cols: set[str] = set(group_cols)
        if "count" in fm:
            result_cols.add("count")
        for func in _AGG_FUNCS:
            if func not in fm:
                continue
            for raw_col in str(fm[func]).split(","):
                raw_col = raw_col.strip()
                if raw_col:
                    _, alias = _parse_agg_expr(raw_col)
                    col_name = raw_col.split()[0] if alias is None else ""
                    result_cols.add(
                        alias if alias is not None else f"{func}_{col_name}"
                    )

        select_clause = ", ".join(select_parts)
        sql = f'SELECT {select_clause} FROM {_quote_identifier(table.name)}'

        if where:
            sql += f" WHERE {where}"

        if group_cols:
            group_clause = ", ".join(_quote_identifier(c) for c in group_cols)
            sql += f" GROUP BY {group_clause}"

        having_clauses: list[str] = []
        having_params: list[Any] = []
        if "having" in fm:
            for raw in str(fm["having"]).split(","):
                parsed = _parse_comparison(raw.strip())
                if parsed:
                    clause, val = parsed
                    # Validate the column name in the having condition.
                    having_col = clause.split()[0]
                    if having_col not in result_cols:
                        raise ValueError(
                            f"Unknown result column '{having_col}' in "
                            f"'having'. Available: "
                            f"{', '.join(sorted(result_cols))}"
                        )
                    having_clauses.append(clause)
                    having_params.append(val)
        if having_clauses:
            sql += " HAVING " + " AND ".join(having_clauses)

        order_parts: list[str] = []
        if "sort" in fm:
            for item in str(fm["sort"]).split(","):
                parts = item.strip().split()
                if not parts:
                    continue
                col = parts[0]
                if col not in result_cols:
                    raise ValueError(
                        f"Unknown result column '{col}' in 'order'. "
                        f"Available: {', '.join(sorted(result_cols))}"
                    )
                direction = parts[1].upper() if len(parts) > 1 else "ASC"
                if direction not in ("ASC", "DESC"):
                    direction = "ASC"
                order_parts.append(f"{col} {direction}")
        if order_parts:
            sql += " ORDER BY " + ", ".join(order_parts)

        all_params = where_params + having_params

        # select: post-fetch projection — filter rows to named columns.
        sel_cols: list[str] = []
        if "select" in fm:
            sel_cols = _parse_select_cols(str(fm["select"]))
            if sel_cols:
                for col in sel_cols:
                    if col not in result_cols:
                        available = ", ".join(sorted(result_cols))
                        raise ValueError(
                            f"Unknown result column '{col}' in 'select'. "
                            f"Available: {available}"
                        )

        page_size = int(fm["page-size"]) if "page-size" in fm else 0
        if page_size > 0:
            page = max(1, int(fm.get("page", 1)))
            count_sql = f"SELECT COUNT(*) FROM ({sql})"
            total = self._conn.execute(count_sql, all_params).fetchone()[0]
            offset = (page - 1) * page_size
            rows = self._fetchall(
                sql + f" LIMIT {page_size} OFFSET {offset}", all_params
            )
            if sel_cols:
                rows = [{k: r[k] for k in sel_cols} for r in rows]
            return self._paginated_jmd(rows, label, total, page, page_size)

        rows = self._fetchall(sql, all_params)
        if sel_cols:
            rows = [{k: r[k] for k in sel_cols} for r in rows]
        return _rows_to_jmd(rows, label)

    def _build_col_namespace(
        self,
        main_table: TableInfo,
        main_alias: str,
        join_infos: list[tuple[TableInfo, JoinSpec]],
        join_aliases: list[str],
    ) -> dict[str, str | None]:
        """Build a column → qualified-reference mapping for a JOIN query.

        Maps each column name visible in the joined query to a qualified
        SQL reference like ``t0."OrderID"``.  Columns that appear in more
        than one table (excluding equi-join keys, which are resolved to
        the main table) map to ``None`` to signal ambiguity.

        Join keys are always resolved to the main table so that WHERE
        clause fragments generated from QBE filters reference a single
        definitive source.

        Args:
            main_table: TableInfo for the primary (left-hand) table.
            main_alias: SQL alias assigned to the primary table (``"t0"``).
            join_infos: List of ``(TableInfo, JoinSpec)`` pairs, one per
                JOIN clause.
            join_aliases: SQL aliases for joined tables (``"t1"``, …).

        Returns:
            Dict mapping column name → qualified SQL reference, or
            ``None`` for genuinely ambiguous columns.
        """
        join_keys = {spec.on_col for _, spec in join_infos}

        # Map each column name to the list of aliases that own it.
        col_alias_map: dict[str, list[str]] = {}
        for col in main_table.columns:
            col_alias_map.setdefault(col.name, []).append(main_alias)
        for (joined_table, _spec), alias in zip(
            join_infos, join_aliases, strict=True
        ):
            for col in joined_table.columns:
                col_alias_map.setdefault(col.name, []).append(alias)

        namespace: dict[str, str | None] = {}
        for col_name, aliases in col_alias_map.items():
            if col_name in join_keys:
                # Join keys are unambiguously resolved to the main table.
                namespace[col_name] = (
                    f'{main_alias}.{_quote_identifier(col_name)}'
                )
            elif len(aliases) == 1:
                namespace[col_name] = (
                    f'{aliases[0]}.{_quote_identifier(col_name)}'
                )
            else:
                namespace[col_name] = None  # Genuinely ambiguous.
        return namespace

    def _aggregate_join(
        self,
        label: str,
        from_clause: str,
        where: str,
        where_params: list[Any],
        fm: dict[str, Any],
        namespace: dict[str, str | None],
    ) -> str:
        """Build and execute a GROUP BY query over a multi-table FROM clause.

        Mirrors :meth:`_aggregate` but works with an already-built
        ``FROM … JOIN …`` string and a column namespace that maps column
        names to qualified references.  Aggregate expressions may use
        arithmetic (e.g. ``UnitPrice * Quantity * (1 - Discount) as
        revenue``); each expression is validated and qualified via
        :func:`_validate_and_qualify_expression`.

        Args:
            label: Document label for the JMD result.
            from_clause: SQL fragment starting at the table name, e.g.
                ``'"Orders" t0 JOIN "Order Details" t1 ON …'``.
            where: Pre-built WHERE clause string (may be empty).
            where_params: Bind parameters for the WHERE clause.
            fm: Parsed frontmatter dict.
            namespace: Column → qualified-reference mapping from
                :meth:`_build_col_namespace`.

        Returns:
            A JMD document string with the aggregation result.

        Raises:
            ValueError: On unknown columns, ambiguous references, or
                invalid expressions.
        """
        select_parts: list[str] = []
        group_cols: list[str] = []

        if "group" in fm:
            for col in str(fm["group"]).split(","):
                col = col.strip()
                if not col:
                    continue
                if col not in namespace:
                    available = ", ".join(sorted(namespace.keys()))
                    raise ValueError(
                        f"Unknown column '{col}' in 'group'. "
                        f"Available: {available}"
                    )
                if namespace[col] is None:
                    raise ValueError(
                        f"Ambiguous column '{col}' in 'group'. "
                        f"Qualify with a table alias."
                    )
                group_cols.append(col)
                qualified = namespace[col]
                select_parts.append(
                    f'{qualified} AS {_quote_identifier(col)}'
                )

        if "count" in fm:
            select_parts.append("COUNT(*) AS count")

        result_cols: set[str] = set(group_cols)
        if "count" in fm:
            result_cols.add("count")

        for func in _AGG_FUNCS:
            if func not in fm:
                continue
            for raw_expr in str(fm[func]).split(","):
                raw_expr = raw_expr.strip()
                if not raw_expr:
                    continue
                expr, custom_alias = _parse_agg_expr(raw_expr)
                qualified_expr = _validate_and_qualify_expression(
                    expr, namespace
                )
                alias = (
                    custom_alias
                    if custom_alias
                    else f"{func}_{expr.strip()}"
                )
                select_parts.append(
                    f"{func.upper()}({qualified_expr})"
                    f" AS {_quote_identifier(alias)}"
                )
                result_cols.add(alias)

        if not select_parts:
            select_parts = ["COUNT(*) AS count"]
            result_cols.add("count")

        select_clause = ", ".join(select_parts)
        sql = f'SELECT {select_clause} FROM {from_clause}'

        if where:
            sql += f" WHERE {where}"

        if group_cols:
            group_clause = ", ".join(
                namespace[c] for c in group_cols  # type: ignore[misc]
            )
            sql += f" GROUP BY {group_clause}"

        having_clauses: list[str] = []
        having_params: list[Any] = []
        if "having" in fm:
            for raw in str(fm["having"]).split(","):
                parsed = _parse_comparison(raw.strip())
                if parsed:
                    clause, val = parsed
                    having_col = clause.split()[0]
                    if having_col not in result_cols:
                        raise ValueError(
                            f"Unknown result column '{having_col}' in "
                            f"'having'. Available: "
                            f"{', '.join(sorted(result_cols))}"
                        )
                    having_clauses.append(clause)
                    having_params.append(val)
        if having_clauses:
            sql += " HAVING " + " AND ".join(having_clauses)

        order_parts: list[str] = []
        if "sort" in fm:
            for item in str(fm["sort"]).split(","):
                parts = item.strip().split()
                if not parts:
                    continue
                col = parts[0]
                if col not in result_cols:
                    raise ValueError(
                        f"Unknown result column '{col}' in 'order'. "
                        f"Available: {', '.join(sorted(result_cols))}"
                    )
                direction = parts[1].upper() if len(parts) > 1 else "ASC"
                if direction not in ("ASC", "DESC"):
                    direction = "ASC"
                order_parts.append(f"{col} {direction}")
        if order_parts:
            sql += " ORDER BY " + ", ".join(order_parts)

        all_params = where_params + having_params

        # select: post-fetch projection.
        sel_cols: list[str] = []
        if "select" in fm:
            sel_cols = _parse_select_cols(str(fm["select"]))
            if sel_cols:
                for col in sel_cols:
                    if col not in result_cols:
                        available = ", ".join(sorted(result_cols))
                        raise ValueError(
                            f"Unknown result column '{col}' in 'select'. "
                            f"Available: {available}"
                        )

        page_size = int(fm["page-size"]) if "page-size" in fm else 0
        if page_size > 0:
            page = max(1, int(fm.get("page", 1)))
            count_sql = f"SELECT COUNT(*) FROM ({sql})"
            total = self._conn.execute(count_sql, all_params).fetchone()[0]
            offset = (page - 1) * page_size
            rows = self._fetchall(
                sql + f" LIMIT {page_size} OFFSET {offset}", all_params
            )
            if sel_cols:
                rows = [{k: r[k] for k in sel_cols} for r in rows]
            return self._paginated_jmd(rows, label, total, page, page_size)

        rows = self._fetchall(sql, all_params)
        if sel_cols:
            rows = [{k: r[k] for k in sel_cols} for r in rows]
        return _rows_to_jmd(rows, label)

    def _query_with_joins(
        self,
        table: TableInfo,
        label: str,
        fields: list[Any],
        fm: dict[str, Any],
        join_specs: list[JoinSpec],
    ) -> str:
        """Execute a QBE query that spans multiple tables via JOIN clauses.

        Resolves each :class:`JoinSpec`, assigns SQL table aliases, builds
        the column namespace, and delegates to either
        :meth:`_aggregate_join` (when aggregation keys are present) or a
        plain SELECT (otherwise).

        Args:
            table: TableInfo for the primary (left-hand) table.
            label: Document label for the JMD result.
            fields: Parsed query fields from JMDQueryParser.
            fm: Parsed frontmatter dict (must contain ``join``).
            join_specs: Parsed join specifications.

        Returns:
            A JMD document string with the query result.

        Raises:
            ValueError: If a join target is unknown, the join column is
                missing in one of the tables, or a filter column is
                ambiguous.
        """
        # Resolve each join spec to a TableInfo and validate the join col.
        join_infos: list[tuple[TableInfo, JoinSpec]] = []
        for spec in join_specs:
            joined_table = self._schema.resolve(spec.table)
            if joined_table is None:
                available = ", ".join(self._schema.tables().keys())
                raise ValueError(
                    f"Unknown table '{spec.table}' in join. "
                    f"Available: {available}"
                )
            main_cols = {c.name for c in table.columns}
            joined_cols = {c.name for c in joined_table.columns}
            if spec.on_col not in main_cols:
                raise ValueError(
                    f"Join column '{spec.on_col}' not found in "
                    f"table '{table.name}'."
                )
            if spec.on_col not in joined_cols:
                raise ValueError(
                    f"Join column '{spec.on_col}' not found in "
                    f"table '{spec.table}'."
                )
            join_infos.append((joined_table, spec))

        main_alias = "t0"
        join_aliases = [f"t{i + 1}" for i in range(len(join_infos))]

        namespace = self._build_col_namespace(
            table, main_alias, join_infos, join_aliases
        )
        all_cols = set(namespace.keys())

        where, params = self._build_where_from_fields(
            fields, all_cols, col_namespace=namespace
        )

        # Build the FROM … JOIN … clause.
        from_clause = f'{_quote_identifier(table.name)} {main_alias}'
        for (joined_table, spec), alias in zip(
            join_infos, join_aliases, strict=True
        ):
            on_main = f'{main_alias}.{_quote_identifier(spec.on_col)}'
            on_joined = f'{alias}.{_quote_identifier(spec.on_col)}'
            from_clause += (
                f' JOIN {_quote_identifier(joined_table.name)} {alias}'
                f' ON {on_main} = {on_joined}'
            )

        # Aggregation mode.
        if "group" in fm or any(k in fm for k in _AGG_FUNCS):
            return self._aggregate_join(
                label, from_clause, where, params, fm, namespace
            )

        # Plain SELECT mode.
        select_clause: str
        if "select" in fm:
            sel_cols = _parse_select_cols(str(fm["select"]))
            if sel_cols:
                for col in sel_cols:
                    if col not in namespace:
                        available = ", ".join(sorted(namespace.keys()))
                        raise ValueError(
                            f"Unknown column '{col}' in 'select'. "
                            f"Available: {available}"
                        )
                    if namespace[col] is None:
                        raise ValueError(
                            f"Ambiguous column '{col}' in 'select'. "
                            f"Qualify with a table alias."
                        )
                select_clause = ", ".join(
                    f'{namespace[c]} AS {_quote_identifier(c)}'
                    for c in sel_cols
                )
            else:
                select_clause = "*"
        else:
            select_clause = "*"

        base_sql = f'SELECT {select_clause} FROM {from_clause}'
        if where:
            base_sql += f" WHERE {where}"

        # count: true — return only the row count.
        if "count" in fm:
            count_sql = (
                f'SELECT COUNT(*) FROM {from_clause}'
            )
            if where:
                count_sql += f" WHERE {where}"
            total = self._conn.execute(count_sql, params).fetchone()[0]
            return f"count: {total}\n\n" + serialize({}, label=label)

        page_size = int(fm["page-size"]) if "page-size" in fm else 0
        if page_size > 0:
            page = max(1, int(fm.get("page", 1)))
            count_sql = (
                f'SELECT COUNT(*) FROM {from_clause}'
            )
            if where:
                count_sql += f" WHERE {where}"
            total = self._conn.execute(count_sql, params).fetchone()[0]
            offset = (page - 1) * page_size
            rows = self._fetchall(
                base_sql + f" LIMIT {page_size} OFFSET {offset}", params
            )
            return self._paginated_jmd(rows, label, total, page, page_size)

        rows = self._fetchall(base_sql, params)
        return _rows_to_jmd(rows, label)

    def _resolve_or_error(self, label: str) -> TableInfo:
        """Resolve a JMD label to a TableInfo or raise ValueError."""
        table = self._schema.resolve(label)
        if table is None:
            available = ", ".join(self._schema.tables().keys())
            raise ValueError(
                f"Unknown table '{label}'. Available: {available}"
            )
        return table

    def _build_where(self, filters: dict[str, Any]) -> tuple[str, list[Any]]:
        """Build a WHERE clause from a plain key=value dict.

        All conditions are exact equality checks joined with AND.
        Used for data-mode reads (``#``) and delete operations (``#-``).
        """
        if not filters:
            return "", []
        clauses = [f"{_quote_identifier(k)} = ?" for k in filters]
        return " AND ".join(clauses), list(filters.values())

    def _build_where_from_fields(
        self,
        fields: list[Any],
        table_cols: set[str],
        col_namespace: dict[str, str | None] | None = None,
        dbg: DebugInfo | None = None,
    ) -> tuple[str, list[Any]]:
        """Build a WHERE clause from a list of QueryField nodes.

        Args:
            fields: Parsed query fields from JMDQueryParser.
            table_cols: Valid column names for the target table.
            col_namespace: Optional qualified-reference mapping
                for JOIN queries.
            dbg: Optional debug collector for filter mapping.

        Returns:
            Tuple of ``(where_clause, params)``.

        Raises:
            ValueError: On unknown or ambiguous column names.
        """
        clauses: list[str] = []
        params: list[Any] = []
        for f in fields:
            if not isinstance(f, QueryField):
                continue
            if f.condition.op in ("?", "?:"):
                continue
            if f.key not in table_cols:
                available = ", ".join(sorted(table_cols))
                raise ValueError(
                    f"Unknown column '{f.key}' in query"
                    f" filter. Available: {available}"
                )
            qcol: str | None = None
            if col_namespace is not None:
                if col_namespace.get(f.key) is None:
                    raise ValueError(
                        f"Ambiguous column '{f.key}'"
                        " in query filter. Exists in"
                        " multiple joined tables."
                    )
                qcol = col_namespace[f.key]
            clause, p = self._condition_to_sql(
                f.key, f.condition, qcol
            )
            if clause:
                clauses.append(clause)
                params.extend(p)
                if dbg is not None and dbg.wants("filters"):
                    dbg.filters.append((f.key, clause))
                if (
                    dbg is not None
                    and dbg.wants("coercions")
                ):
                    self._collect_coercion(
                        dbg, f.key, f.condition
                    )
        return (
            (" AND ".join(clauses), params)
            if clauses
            else ("", [])
        )

    def _condition_to_sql(
        self,
        col: str,
        cond: Condition,
        qcol: str | None = None,
    ) -> tuple[str, list[Any]]:
        """Translate a single JMD Condition into a SQL fragment.

        JMD supports a rich filter syntax on query documents.  Each filter
        value is parsed into a Condition with an operator and a list of
        values.  This method maps each operator to its SQL equivalent:

            =       →  col = ?                 (exact match)
            >, >=   →  col > ? / col >= ?       (range)
            <, <=   →  col < ? / col <= ?       (range)
            |       →  col IN (?, …)            (alternation / OR)
            ~       →  col LIKE '%val%'         (substring, case-insensitive)
            regex   →  col REGEXP ?             (full-match via UDF)
            !       →  NOT (inner condition)    (negation, composable)

        Args:
            col: The column name (unquoted).
            cond: The parsed Condition object from jmd._query.
            qcol: Optional pre-qualified SQL reference (e.g.
                ``'t0."OrderID"'``).  When ``None``, the column is
                quoted via :func:`_quote_identifier`.

        Returns:
            A tuple of (sql_fragment, parameters).  Returns ("", []) for
            unknown or unsupported operators so callers can skip them.
        """
        effective_qcol = qcol if qcol is not None else _quote_identifier(col)
        op, values = cond.op, cond.values

        if op == "!":
            # Negation wraps any other condition: "!Germany" → NOT (col = ?)
            inner, p = self._condition_to_sql(col, values[0], effective_qcol)
            return (f"NOT ({inner})", p) if inner else ("", [])
        if op == "=":
            return f"{effective_qcol} = ?", [values[0]]
        if op in (">", ">=", "<", "<="):
            return f"{effective_qcol} {op} ?", [values[0]]
        if op == "|":
            # Alternation: Germany|France|UK → col IN (?, ?, ?)
            placeholders = ", ".join("?" * len(values))
            return f"{effective_qcol} IN ({placeholders})", list(values)
        if op == "~":
            # Substring match: ~Corp → col LIKE '%Corp%'
            return f"{effective_qcol} LIKE ?", [f"%{values[0]}%"]
        if op == "regex":
            # Full-match regex via the REGEXP UDF registered in __init__.
            return f"{effective_qcol} REGEXP ?", [values[0]]

        # Unknown operator — skip silently to stay forwards-compatible.
        return "", []

    def _fetchall(self, sql: str, params: list[Any]) -> list[dict[str, Any]]:
        """Execute a SELECT and return all rows as plain dicts."""
        cur = self._conn.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]

    def _explain(self, sql: str, params: list[Any]) -> str:
        """Return EXPLAIN QUERY PLAN output as a single string."""
        rows = self._conn.execute(
            f"EXPLAIN QUERY PLAN {sql}", params
        ).fetchall()
        return "; ".join(str(row[3]) for row in rows)

    def _collect_coercion(
        self,
        dbg: DebugInfo,
        col: str,
        cond: Condition,
    ) -> None:
        """Record type coercion info for a filter column."""
        table_info = None
        for t in self._schema.tables().values():
            for c in t.columns:
                if c.name == col:
                    table_info = c
                    break
            if table_info:
                break
        if table_info is None:
            return
        sqlite_type = (table_info.type or "TEXT").upper()
        jmd_type = _sqlite_type_to_jmd(table_info.type)
        op = cond.op
        if op in (">", ">=", "<", "<=", "="):
            if sqlite_type in ("TEXT", "VARCHAR"):
                dbg.coercions.append(
                    (col, f"string-compare on {jmd_type}")
                )
        elif op == "regex":
            dbg.coercions.append(
                (col, f"regex on {jmd_type}"
                 " (implicit full-match anchoring)")
            )
        elif op == "~":
            dbg.coercions.append(
                (col, f"LIKE on {jmd_type}"
                 " (case-insensitive substring)")
            )

    def _label_from_source(self, source: str) -> str:
        """Extract the table label from the first heading line of a JMD doc.

        The heading line encodes both the mode prefix and the label:

            ``# Orders``   → ``"Orders"``
            ``#? Orders``  → ``"Orders"``
            ``#! Orders``  → ``"Orders"``
            ``#- Orders``  → ``"Orders"``
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
