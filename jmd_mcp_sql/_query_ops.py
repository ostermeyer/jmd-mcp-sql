# SPDX-License-Identifier: Apache-2.0
"""Query-mode (#?) operations as free functions over a TranslatorContext.

The ``_query`` dispatcher and its helpers (``_paginated_jmd``,
``_aggregate``, ``_aggregate_join``, ``_query_with_joins``,
``_build_col_namespace``) are module-level free functions taking a
:class:`TranslatorContext` as the first parameter. Same idiom as
:mod:`_schema_ops`. See the workspace memory ``Modernes pythonisches
Design`` for the underlying principle.
"""
from __future__ import annotations

import time
from typing import Any

from jmd import JMDQueryParser, serialize

from ._context import TranslatorContext
from ._debug import (
    _check_frontmatter,
    _parse_debug,
    _prepend_debug,
    _prepend_ignored_keys,
)
from ._filters import build_where_from_fields
from ._query_parser import (
    _AGG_FUNCS,
    JoinSpec,
    _parse_agg_expr,
    _parse_comparison,
    _parse_join_specs,
    _parse_select_cols,
    _validate_and_qualify_expression,
)
from ._sql import _quote_identifier
from .schema import TableInfo

# Frontmatter keys recognised in query-mode read documents.
_KNOWN_FM_READ_QUERY: frozenset[str] = frozenset({
    "select", "join", "sum", "avg", "min", "max", "count",
    "group", "having", "sort", "page-size", "page", "debug",
})


def _row_to_jmd(row: dict[str, Any], label: str) -> str:
    """Serialize a single result row as a JMD data document."""
    return serialize(row, label=label)


def _rows_to_jmd(rows: list[dict[str, Any]], label: str) -> str:
    """Serialize a list of result rows as a JMD array document."""
    return serialize(rows, label=label)


def _query(ctx: TranslatorContext, jmd_source: str) -> str:
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
    table = ctx.resolve_or_error(doc.label)
    if dbg.wants("table"):
        dbg.table = table.name

    # Translate each QueryField into a SQL WHERE fragment.
    table_cols = {c.name for c in table.columns}
    where, params = build_where_from_fields(ctx,
        doc.fields, table_cols, dbg=dbg
    )

    # JOIN mode.
    if "join" in fm:
        join_specs = _parse_join_specs(str(fm["join"]))
        return _prepend_debug(
            _prepend_ignored_keys(
                _query_with_joins(ctx,
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
                _aggregate(ctx,
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
        total = ctx.conn.execute(
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
        total = ctx.conn.execute(
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
        rows = ctx.fetchall(exec_sql, params)
        if dbg.active:
            dbg.timing_ms = (
                (time.perf_counter() - t0) * 1000
            )
            dbg.sql = exec_sql
            if dbg.wants("plan"):
                dbg.plan = ctx.explain(
                    exec_sql, params
                )
        return _prepend_debug(
            _prepend_ignored_keys(
                _paginated_jmd(ctx,
                    rows, doc.label,
                    total, pg, page_size,
                ),
                ignored,
            ),
            dbg,
        )

    t0 = time.perf_counter()
    rows = ctx.fetchall(base_sql, params)
    if dbg.active:
        dbg.timing_ms = (
            (time.perf_counter() - t0) * 1000
        )
        dbg.sql = base_sql
        if dbg.wants("plan"):
            dbg.plan = ctx.explain(base_sql, params)
    return _prepend_debug(
        _prepend_ignored_keys(
            _rows_to_jmd(rows, doc.label), ignored
        ),
        dbg,
    )

def _paginated_jmd(ctx: TranslatorContext, rows: list[dict[str, Any]],
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

def _aggregate(ctx: TranslatorContext, table: TableInfo,
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
        total = ctx.conn.execute(count_sql, all_params).fetchone()[0]
        offset = (page - 1) * page_size
        rows = ctx.fetchall(
            sql + f" LIMIT {page_size} OFFSET {offset}", all_params
        )
        if sel_cols:
            rows = [{k: r[k] for k in sel_cols} for r in rows]
        return _paginated_jmd(ctx, rows, label, total, page, page_size)

    rows = ctx.fetchall(sql, all_params)
    if sel_cols:
        rows = [{k: r[k] for k in sel_cols} for r in rows]
    return _rows_to_jmd(rows, label)

def _build_col_namespace(ctx: TranslatorContext, main_table: TableInfo,
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
        ctx: Translator context.
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

def _aggregate_join(ctx: TranslatorContext, label: str,
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
        ctx: Translator context.
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
        total = ctx.conn.execute(count_sql, all_params).fetchone()[0]
        offset = (page - 1) * page_size
        rows = ctx.fetchall(
            sql + f" LIMIT {page_size} OFFSET {offset}", all_params
        )
        if sel_cols:
            rows = [{k: r[k] for k in sel_cols} for r in rows]
        return _paginated_jmd(ctx, rows, label, total, page, page_size)

    rows = ctx.fetchall(sql, all_params)
    if sel_cols:
        rows = [{k: r[k] for k in sel_cols} for r in rows]
    return _rows_to_jmd(rows, label)

def _query_with_joins(ctx: TranslatorContext, table: TableInfo,
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
        ctx: Translator context.
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
        joined_table = ctx.schema.resolve(spec.table)
        if joined_table is None:
            available = ", ".join(ctx.schema.tables().keys())
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

    namespace = _build_col_namespace(ctx,
        table, main_alias, join_infos, join_aliases
    )
    all_cols = set(namespace.keys())

    where, params = build_where_from_fields(ctx,
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
        return _aggregate_join(ctx,
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
        total = ctx.conn.execute(count_sql, params).fetchone()[0]
        return f"count: {total}\n\n" + serialize({}, label=label)

    page_size = int(fm["page-size"]) if "page-size" in fm else 0
    if page_size > 0:
        page = max(1, int(fm.get("page", 1)))
        count_sql = (
            f'SELECT COUNT(*) FROM {from_clause}'
        )
        if where:
            count_sql += f" WHERE {where}"
        total = ctx.conn.execute(count_sql, params).fetchone()[0]
        offset = (page - 1) * page_size
        rows = ctx.fetchall(
            base_sql + f" LIMIT {page_size} OFFSET {offset}", params
        )
        return _paginated_jmd(ctx, rows, label, total, page, page_size)

    rows = ctx.fetchall(base_sql, params)
    return _rows_to_jmd(rows, label)
