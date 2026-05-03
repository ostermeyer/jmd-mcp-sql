# SPDX-License-Identifier: Apache-2.0
"""Data-mode (#) and delete-mode (#-) operations as free functions.

Mode dispatch is handled by :class:`SQLTranslator`'s public
``read``/``write``/``delete`` methods; this module implements the
data-mode arms of each. All functions take a
:class:`TranslatorContext` as their first parameter and operate
on :class:`SchemaInspector` and :class:`sqlite3.Connection` via
that context.
"""
from __future__ import annotations

import time
from typing import Any

from jmd import JMDDeleteParser, JMDParser, serialize

from ._context import TranslatorContext, label_from_source
from ._debug import (
    _check_frontmatter,
    _parse_debug,
    _prepend_debug,
    _prepend_ignored_keys,
)
from ._filters import build_where
from ._query_parser import _parse_select_cols
from ._sql import _quote_identifier
from .schema import TableInfo

_KNOWN_FM_READ_DATA: frozenset[str] = frozenset({
    "page-size", "page", "count", "select", "debug",
})
_KNOWN_FM_WRITE: frozenset[str] = frozenset({"debug", "action"})
_KNOWN_FM_DELETE: frozenset[str] = frozenset({
    "confirm", "debug",
})


def _row_to_jmd(row: dict[str, Any], label: str) -> str:
    """Serialize a single row as a JMD data document."""
    return serialize(row, label=label)


def _rows_to_jmd(rows: list[dict[str, Any]], label: str) -> str:
    """Serialize a list of rows as a JMD array document."""
    return serialize(rows, label=label)


def _paginated_jmd(
    rows: list[dict[str, Any]],
    label: str,
    total: int,
    page: int,
    page_size: int,
) -> str:
    """Wrap a page of rows in a JMD doc with pagination frontmatter."""
    pages = (total + page_size - 1) // page_size
    fm = (
        f"total: {total}\n"
        f"page: {page}\n"
        f"pages: {pages}\n"
        f"page-size: {page_size}\n"
    )
    body = serialize({"data": rows}, label=label)
    return fm + "\n" + body


def read_data(ctx: TranslatorContext, jmd_source: str) -> str:
    """Execute a ``# Label`` data-mode read.

    Parses key/value pairs into an exact-match SELECT, optionally
    paginated and projection-filtered via ``select:`` frontmatter.
    """
    parser = JMDParser()
    data = parser.parse(jmd_source)
    fm = parser.frontmatter
    dbg = _parse_debug(fm)
    ignored = _check_frontmatter(
        fm, _KNOWN_FM_READ_DATA, "observable"
    )
    label = label_from_source(jmd_source)
    table = ctx.resolve_or_error(label)
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
    where, params = build_where(data)

    select_clause = "*"
    if "select" in fm:
        sel_cols = _parse_select_cols(str(fm["select"]))
        if sel_cols:
            for col in sel_cols:
                if col not in table_cols:
                    available = ", ".join(sorted(table_cols))
                    raise ValueError(
                        f"Unknown column '{col}' in 'select'."
                        f" Available: {available}"
                    )
            select_clause = ", ".join(
                _quote_identifier(c) for c in sel_cols
            )

    base_sql = (
        f"SELECT {select_clause}"
        f" FROM {_quote_identifier(table.name)}"
    )
    count_sql = (
        f"SELECT COUNT(*)"
        f" FROM {_quote_identifier(table.name)}"
    )
    if where:
        base_sql += f" WHERE {where}"
        count_sql += f" WHERE {where}"

    if "count" in fm:
        t0 = time.perf_counter()
        total = ctx.conn.execute(count_sql, params).fetchone()[0]
        if dbg.active:
            dbg.timing_ms = (time.perf_counter() - t0) * 1000
            dbg.sql = count_sql
        resp = (
            f"count: {total}\n\n" + serialize({}, label=label)
        )
        return _prepend_debug(
            _prepend_ignored_keys(resp, ignored), dbg
        )

    page_size = int(fm["page-size"]) if "page-size" in fm else 0
    if page_size > 0:
        page = max(1, int(fm.get("page", 1)))
        total = ctx.conn.execute(count_sql, params).fetchone()[0]
        offset = (page - 1) * page_size
        exec_sql = (
            base_sql + f" LIMIT {page_size} OFFSET {offset}"
        )
        t0 = time.perf_counter()
        rows = ctx.fetchall(exec_sql, params)
        if dbg.active:
            dbg.timing_ms = (time.perf_counter() - t0) * 1000
            dbg.sql = exec_sql
            if dbg.wants("plan"):
                dbg.plan = ctx.explain(exec_sql, params)
        return _prepend_debug(
            _prepend_ignored_keys(
                _paginated_jmd(
                    rows, label, total, page, page_size
                ),
                ignored,
            ),
            dbg,
        )

    t0 = time.perf_counter()
    rows = ctx.fetchall(base_sql, params)
    if dbg.active:
        dbg.timing_ms = (time.perf_counter() - t0) * 1000
        dbg.sql = base_sql
        if dbg.wants("plan"):
            dbg.plan = ctx.explain(base_sql, params)
    if not rows:
        return serialize(
            {"status": 404, "code": "not_found",
             "message": f"No records found in {table.name}"},
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


def write_data(ctx: TranslatorContext, jmd_source: str) -> str:
    """Execute a ``# Label`` (single) or ``# Label[]`` (bulk) data write."""
    parser = JMDParser()
    data = parser.parse(jmd_source)
    fm = parser.frontmatter
    dbg = _parse_debug(fm)
    ignored = _check_frontmatter(
        fm, _KNOWN_FM_WRITE, "observable"
    )
    label = label_from_source(jmd_source)

    if isinstance(data, list):
        if label.endswith("[]"):
            label = label[:-2]
        table = ctx.resolve_or_error(label)
        return _prepend_debug(
            _prepend_ignored_keys(
                bulk_insert(ctx, data, table, label),
                ignored,
            ),
            dbg,
        )

    table = ctx.resolve_or_error(label)
    if dbg.wants("table"):
        dbg.table = table.name

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

    sql = (
        f"INSERT OR REPLACE INTO"
        f" {_quote_identifier(table.name)}"
        f" ({col_names}) VALUES ({placeholders})"
    )
    if dbg.wants("sql"):
        dbg.sql = sql
    t0 = time.perf_counter()
    cur = ctx.conn.execute(sql, values)
    ctx.conn.commit()
    if dbg.active:
        dbg.timing_ms = (time.perf_counter() - t0) * 1000

    rowid = cur.lastrowid
    qt = _quote_identifier(table.name)
    row = ctx.conn.execute(
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


def delete_data(ctx: TranslatorContext, jmd_source: str) -> str:
    """Execute a ``#- Label`` data-mode delete (single or bulk)."""
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

    table = ctx.resolve_or_error(doc.label)

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
        return bulk_delete(ctx, doc, table)

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
    where, params = build_where(identifiers)

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

    row = ctx.conn.execute(
        f"SELECT * FROM {qt} WHERE {where}", params
    ).fetchone()
    if row is None:
        return serialize(
            {"status": 404, "code": "not_found",
             "message": f"No matching record in '{table.name}'"},
            label="Error",
        )

    t0 = time.perf_counter()
    ctx.conn.execute(del_sql, params)
    ctx.conn.commit()
    if dbg.active:
        dbg.timing_ms = (time.perf_counter() - t0) * 1000
    return _prepend_debug(
        _row_to_jmd(dict(row), doc.label), dbg
    )


def bulk_insert(
    ctx: TranslatorContext,
    records: list[Any],
    table: TableInfo,
    label: str,
) -> str:
    """Insert multiple records from a ``# Table[]`` document."""
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
                 "message": f"Item {i} is not a valid record"},
                label="Error",
            )
        unknown = [k for k in record if k not in table_cols]
        if unknown:
            return serialize(
                {"status": 400, "code": "bad_request",
                 "message": (
                     f"Unknown column(s) {unknown!r}"
                     f" in table '{table.name}' (item {i})"
                 )},
                label="Error",
            )

        cols = list(record.keys())
        placeholders = ", ".join("?" for _ in cols)
        col_names = ", ".join(_quote_identifier(c) for c in cols)
        values = [record[c] for c in cols]

        sql = (
            f"INSERT OR REPLACE INTO {qt}"
            f" ({col_names}) VALUES ({placeholders})"
        )
        cur = ctx.conn.execute(sql, values)
        rowid = cur.lastrowid
        row = ctx.conn.execute(
            f"SELECT * FROM {qt} WHERE rowid = ?",
            (rowid,),
        ).fetchone()
        inserted.append(dict(row) if row else record)

    ctx.conn.commit()
    return _rows_to_jmd(inserted, label)


def bulk_delete(
    ctx: TranslatorContext,
    doc: Any,
    table: TableInfo,
) -> str:
    """Delete multiple records by primary-key list."""
    ids = doc.identifiers
    if not ids:
        return serialize(
            {"status": 400, "code": "bad_request",
             "message": "Bulk delete list is empty"},
            label="Error",
        )

    pks = table.primary_keys
    qt = _quote_identifier(table.name)

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

        rows = ctx.fetchall(
            f"SELECT * FROM {qt}"
            f" WHERE {pk} IN ({placeholders})",
            params,
        )
        ctx.conn.execute(
            f"DELETE FROM {qt}"
            f" WHERE {pk} IN ({placeholders})",
            params,
        )
        ctx.conn.commit()
        return _rows_to_jmd(rows, doc.label)

    all_rows: list[dict[str, Any]] = []
    for obj in ids:
        if not isinstance(obj, dict) or not obj:
            continue
        where, params = build_where(obj)
        if not where:
            continue
        row = ctx.conn.execute(
            f"SELECT * FROM {qt} WHERE {where}",
            params,
        ).fetchone()
        if row is not None:
            all_rows.append(dict(row))
            ctx.conn.execute(
                f"DELETE FROM {qt} WHERE {where}",
                params,
            )
    ctx.conn.commit()
    return _rows_to_jmd(all_rows, doc.label)
