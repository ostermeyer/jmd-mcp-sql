# SPDX-License-Identifier: Apache-2.0
"""QBE filter translation — Conditions and WHERE clauses.

Pure-function module for the JMD Query-by-Example layer. Builds
SQL WHERE fragments from parsed :class:`QueryField` lists, plus
the ``key=value`` shortcut used by data-mode reads and deletes.

Imported by :mod:`_query_ops`, :mod:`_data_ops`, and the
data-mode helpers on :class:`SQLTranslator`.
"""
from __future__ import annotations

from typing import Any

from jmd._query import Condition, QueryField

from ._context import TranslatorContext
from ._debug import DebugInfo
from ._sql import _quote_identifier, _sqlite_type_to_jmd


def build_where(filters: dict[str, Any]) -> tuple[str, list[Any]]:
    """Build a WHERE clause from a plain key=value dict.

    All conditions are exact equality joined with AND. Used for
    data-mode reads (``#``) and delete operations (``#-``).
    """
    if not filters:
        return "", []
    clauses = [f"{_quote_identifier(k)} = ?" for k in filters]
    return " AND ".join(clauses), list(filters.values())


def build_where_from_fields(
    ctx: TranslatorContext,
    fields: list[Any],
    table_cols: set[str],
    *,
    col_namespace: dict[str, str | None] | None = None,
    dbg: DebugInfo | None = None,
) -> tuple[str, list[Any]]:
    """Build a WHERE clause from a list of :class:`QueryField` nodes.

    Args:
        ctx: Translator context (used only for the optional coercion
            debug channel; the schema is read out of it).
        fields: Parsed query fields from JMDQueryParser.
        table_cols: Valid column names for the target table.
        col_namespace: Optional qualified-reference mapping for
            JOIN queries.
        dbg: Optional debug collector for filter mapping and
            coercion notes.

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
        clause, p = _condition_to_sql(f.key, f.condition, qcol)
        if clause:
            clauses.append(clause)
            params.extend(p)
            if dbg is not None and dbg.wants("filters"):
                dbg.filters.append((f.key, clause))
            if dbg is not None and dbg.wants("coercions"):
                _collect_coercion(ctx, dbg, f.key, f.condition)
    return (
        (" AND ".join(clauses), params)
        if clauses
        else ("", [])
    )


def _condition_to_sql(
    col: str,
    cond: Condition,
    qcol: str | None = None,
) -> tuple[str, list[Any]]:
    """Translate a single JMD :class:`Condition` into a SQL fragment.

    JMD supports a rich filter syntax on query documents.  Each
    filter value is parsed into a Condition with an operator and a
    list of values.  This function maps each operator to its SQL
    equivalent::

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
        ``(sql_fragment, parameters)``.  Returns ``("", [])`` for
        unknown or unsupported operators so callers can skip them.
    """
    effective_qcol = qcol if qcol is not None else _quote_identifier(col)
    op, values = cond.op, cond.values

    if op == "!":
        # Negation wraps any other condition: "!Germany" → NOT (col = ?)
        inner, p = _condition_to_sql(col, values[0], effective_qcol)
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


def _collect_coercion(
    ctx: TranslatorContext,
    dbg: DebugInfo,
    col: str,
    cond: Condition,
) -> None:
    """Record type-coercion info for a filter column on the debug bag."""
    table_info = None
    for t in ctx.schema.tables().values():
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
