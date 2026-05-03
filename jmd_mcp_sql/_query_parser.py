# SPDX-License-Identifier: Apache-2.0
"""Query-frontmatter parsers and the small expression-validator.

Pure-function module. Parses the frontmatter values that drive the
``#?`` query mode (``select:``, ``join:``, aggregate / ``having:``
conditions) and validates inline expressions before they are
folded into a SQL SELECT. Also hosts the SQLite ``REGEXP``
function used for the ``^``-prefixed QBE filter operator.

Companion to :mod:`_ddl`. Imported by :mod:`translator` and the
query-ops methods on :class:`SQLTranslator`.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

# Aggregate function names recognised in query frontmatter.
_AGG_FUNCS: tuple[str, ...] = ("sum", "avg", "min", "max")

# SQL function names permitted in aggregate expressions.
_SQL_FUNC_NAMES: frozenset[str] = frozenset({
    "SUM", "AVG", "MIN", "MAX", "COUNT", "COALESCE", "NULLIF",
    "ABS", "ROUND", "LENGTH", "UPPER", "LOWER", "CAST",
})


@dataclass
class JoinSpec:
    """Parsed representation of a single JOIN clause from frontmatter.

    Attributes:
        table: Table name as written (may contain spaces).
        on_col: Equi-join column name (must exist in both tables).
    """

    table: str
    on_col: str


def _parse_select_cols(raw: str) -> list[str]:
    """Parse a comma-separated list of column names from a select: value.

    Args:
        raw: Raw string value from the ``select:`` frontmatter key,
            e.g. ``"OrderID, EmployeeID"``.

    Returns:
        List of stripped, non-empty column name strings.
    """
    return [c.strip() for c in raw.split(",") if c.strip()]


def _parse_join_specs(raw: str) -> list[JoinSpec]:
    """Parse a comma-separated list of join specifications.

    Each segment must be of the form ``<TableName> on <ColumnName>``.
    Multiple joins can be expressed as a single comma-separated value
    for the ``join:`` frontmatter key.

    Args:
        raw: Raw string value from the ``join:`` frontmatter key,
            e.g. ``"Order Details on OrderID, Employees on EmployeeID"``.

    Returns:
        List of :class:`JoinSpec` instances, one per join.

    Raises:
        ValueError: If any segment does not match
            ``<TableName> on <ColumnName>``.
    """
    specs: list[JoinSpec] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        halves = re.split(r'\s+on\s+', part, maxsplit=1, flags=re.IGNORECASE)
        if len(halves) != 2:
            raise ValueError(
                f"Invalid join spec {part!r}. "
                f"Expected '<TableName> on <ColumnName>'"
            )
        specs.append(
            JoinSpec(table=halves[0].strip(), on_col=halves[1].strip())
        )
    return specs


def _parse_agg_expr(raw: str) -> tuple[str, str | None]:
    """Parse an aggregate expression with an optional alias.

    Finds the last `` as `` (case-insensitive) in *raw* to split the
    expression from its alias.

    Args:
        raw: Expression string, e.g. ``"UnitPrice * Quantity as revenue"``.

    Returns:
        A tuple ``(expr, alias)`` where *alias* is ``None`` if no
        ``AS`` clause was found.

    Raises:
        ValueError: If the alias part contains invalid identifier
            characters.
    """
    idx = raw.lower().rfind(' as ')
    if idx != -1:
        expr = raw[:idx].strip()
        alias = raw[idx + 4:].strip()
        if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', alias):
            raise ValueError(
                f"Invalid alias {alias!r} in expression {raw!r}. "
                f"Aliases must be valid SQL identifiers."
            )
        return (expr, alias)
    return (raw.strip(), None)


def _validate_and_qualify_expression(
    expr: str, namespace: dict[str, str | None]
) -> str:
    r"""Validate an arithmetic expression and qualify column references.

    Performs two security checks before substituting qualified column
    references:

    1. Character-level: only word characters, whitespace, arithmetic
       operators, parentheses, and dots are allowed.
    2. Token-level: every alpha-start identifier must be a known SQL
       function name or a column from the namespace.

    Args:
        expr: Raw expression string, e.g.
            ``"UnitPrice * Quantity * (1 - Discount)"``.
        namespace: Mapping of column name to qualified reference
            (e.g. ``'t0."UnitPrice"'``), or ``None`` when the column
            is ambiguous across joined tables.

    Returns:
        The expression with unqualified column names replaced by their
        qualified equivalents.

    Raises:
        ValueError: If the expression contains invalid characters, an
            unknown identifier, or an ambiguous column reference.
    """
    if not re.match(r'^[\w\s\+\-\*\/\(\)\.]+$', expr):
        raise ValueError(
            f"Expression {expr!r} contains invalid characters"
        )
    tokens = re.findall(r'\b([A-Za-z_][A-Za-z0-9_]*)\b', expr)
    for token in tokens:
        if token.upper() in _SQL_FUNC_NAMES:
            continue
        if token in namespace:
            if namespace[token] is None:
                raise ValueError(
                    f"Ambiguous column '{token}' in expression. "
                    f"Exists in multiple joined tables."
                )
        else:
            raise ValueError(
                f"Unknown identifier '{token}' in expression. "
                f"Only column names and standard SQL functions are allowed."
            )

    # Qualify column references: process longest names first so that
    # partial-name matches do not shadow longer names.
    result = expr
    for col_name in sorted(namespace.keys(), key=len, reverse=True):
        qualified = namespace[col_name]
        if qualified is None:
            continue
        result = re.sub(
            r'\b' + re.escape(col_name) + r'\b', qualified, result
        )
    return result


def _parse_comparison(condition: str) -> tuple[str, Any] | None:
    """Parse a bare comparison string into a parameterized SQL fragment.

    Used to translate ``having:`` frontmatter conditions such as
    ``sum_Freight > 1000`` into ``('sum_Freight > ?', 1000)``.
    Only column names matching ``[A-Za-z_][A-Za-z0-9_]*`` are accepted
    to prevent SQL injection through crafted alias names.

    Args:
        condition: A string like ``"count > 5"`` or ``"avg_Price <= 99"``.

    Returns:
        A ``(sql_fragment, value)`` tuple, or ``None`` if the condition
        cannot be parsed.
    """
    for op in (">=", "<=", ">", "<", "="):
        if op in condition:
            left, _, right = condition.partition(op)
            col = left.strip()
            val_str = right.strip()
            if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', col):
                return None
            try:
                val: Any = int(val_str)
            except ValueError:
                try:
                    val = float(val_str)
                except ValueError:
                    val = val_str
            return f"{col} {op} ?", val
    return None


def _regexp(pattern: str, value: Any) -> bool:
    """SQLite user-defined function that implements the REGEXP operator.

    SQLite ships without a REGEXP implementation; the operator exists in
    the grammar but raises an error unless a function named ``regexp`` is
    registered on the connection.  We register this function in
    SQLTranslator.__init__ so that regex QBE filters work out of the box.

    Args:
        pattern: The regular expression pattern.
        value: The column value to test against the pattern.

    Returns:
        True if the full value matches the pattern, False otherwise.
        Returns False for NULL values without raising an error.
    """
    if value is None:
        return False
    try:
        return bool(re.fullmatch(str(pattern), str(value)))
    except re.error:
        # If the pattern is not valid regex, fall back to literal equality
        # so the filter still produces *some* result rather than crashing.
        return str(pattern) == str(value)
