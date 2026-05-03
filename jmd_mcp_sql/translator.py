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

import sqlite3
import time
from typing import Any

from jmd import (
    JMDDeleteParser,
    JMDParser,
    JMDQueryParser,
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
from ._schema_ops import _SchemaOpsBase

# SQL fundamentals (identifier quoting, JMD ⇄ SQLite type vocabulary)
# live in ._sql so DDL helpers can reuse them without an import cycle
# back into translator.py.
# Re-imported here at module level so existing call sites — and any
# downstream tooling that imports them from this module — keep working.
from ._sql import (  # noqa: E402, I001
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


class SQLTranslator(_SchemaOpsBase):
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
