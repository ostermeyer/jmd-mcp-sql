# SPDX-License-Identifier: Apache-2.0
"""DDL build/parse helpers — Slices A–F.

Pure-function module. Builds CREATE TABLE / INDEX / TRIGGER / VIEW
/ VIRTUAL TABLE statements from JMD inputs, parses them back from
the stored ``sqlite_master.sql`` for read-symmetry, and provides
the inspection helpers (``_user_indexes`` / ``_user_triggers`` /
``_user_views`` / ``_table_unique_constraints`` /
``_table_foreign_keys``) the schema-doc methods rely on.

Companion to :mod:`_sql` (identifier quoting + JMD ⇄ SQLite type
vocabulary). Imported by :mod:`translator`.
"""
from __future__ import annotations

import re
import sqlite3
from typing import Any

from ._sql import _quote_identifier


def _quote_default(value: Any) -> str:
    """Format a Python scalar as a SQL literal for DEFAULT/CHECK.

    Bareword keywords like CURRENT_TIMESTAMP pass through unquoted;
    integers and floats render directly; booleans collapse to 0/1
    (SQLite has no native BOOLEAN); strings are single-quoted with
    embedded apostrophes doubled per SQL convention.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    s = str(value)
    if s.upper() in {
        "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME", "NULL",
    }:
        return s.upper()
    return "'" + s.replace("'", "''") + "'"


def _unquote_default_for_jmd(raw: str) -> str:
    """Render a SQLite-stored default value for a JMD `= …` token.

    SQLite returns DEFAULTs as raw expressions: integers as bare
    numbers, strings single-quoted, keywords as bareword. JMD's
    `= value` syntax is unquoted, so we strip the SQL quoting.
    """
    s = raw.strip()
    if s.startswith("'") and s.endswith("'"):
        return s[1:-1].replace("''", "'")
    return s


def _as_string_list(value: Any) -> list[str]:
    """Coerce a JMD sub-section value to a list of non-empty strings.

    JMDParser returns ``## name[]`` sub-sections in three shapes
    depending on bullet content:
      - bare scalars (``- foo``)        → list[str]
      - colon entries (``- a: b``)       → list[dict] (single-key)
      - mixed                            → list[Any]
    For DDL sub-sections we always want a flat list of strings
    that the caller can split / regex; dict entries are flattened
    back to ``key: value`` text.
    """
    if value is None:
        return []
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        if isinstance(item, dict):
            for k, v in item.items():
                out.append(f"{k}: {v}".strip())
        else:
            s = str(item).strip()
            if s:
                out.append(s)
    return out


def _split_columns(entry: str) -> list[str]:
    """Split a comma-separated column-list entry into trimmed names."""
    return [c.strip() for c in entry.split(",") if c.strip()]


def _coerce_bool(value: Any) -> bool:
    """Coerce a JMD-parsed value to bool, tolerating string forms.

    JMDParser may surface ``true``/``false`` as native bools or as
    the string tokens depending on context (e.g. inline values vs
    sub-section dict values). Treat all common shapes uniformly.
    """
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    s = str(value).strip().lower()
    return s in {"true", "yes", "1", "on"}


def _split_top_level(body: str) -> list[str]:
    """Split a string on top-level commas (depth-0 in parens).

    Used to parse CREATE TABLE bodies into column-defs and
    table-level constraint clauses.
    """
    parts: list[str] = []
    depth = 0
    cur: list[str] = []
    in_string = False
    for ch in body:
        if ch == "'" and not in_string:
            in_string = True
            cur.append(ch)
        elif ch == "'" and in_string:
            in_string = False
            cur.append(ch)
        elif in_string:
            cur.append(ch)
        elif ch == "(":
            depth += 1
            cur.append(ch)
        elif ch == ")":
            depth -= 1
            cur.append(ch)
        elif ch == "," and depth == 0:
            piece = "".join(cur).strip()
            if piece:
                parts.append(piece)
            cur = []
        else:
            cur.append(ch)
    piece = "".join(cur).strip()
    if piece:
        parts.append(piece)
    return parts


_TABLE_CONSTRAINT_RE = re.compile(
    r"^\s*(?:CONSTRAINT\s+\S+\s+)?"
    r"(PRIMARY\s+KEY|UNIQUE|CHECK|FOREIGN\s+KEY)\b",
    re.IGNORECASE,
)


def _is_table_level_constraint(part: str) -> bool:
    """Return True if a CREATE-TABLE body part is a constraint clause."""
    return bool(_TABLE_CONSTRAINT_RE.match(part))


def _extract_create_table_body(create_sql: str) -> str:
    """Return the parenthesised column/constraint body of a CREATE TABLE."""
    first = create_sql.find("(")
    last = create_sql.rfind(")")
    if first < 0 or last < 0 or last <= first:
        return ""
    return create_sql[first + 1 : last]


def _column_part_name(part: str) -> str | None:
    """Pull the column name from a column-def part (quoted or bare)."""
    m = re.match(r"^\s*\"([^\"]+)\"", part)
    if m:
        return m.group(1)
    m = re.match(r"^\s*([A-Za-z_][\w]*)", part)
    return m.group(1) if m else None


def _column_enum_values(col_part: str, col_name: str) -> list[str] | None:
    """Pull values from a column-level ``CHECK (col IN (...))`` clause."""
    pattern = (
        r"CHECK\s*\(\s*\"" + re.escape(col_name) + r"\"\s+IN\s*\("
        r"([^)]+)\)\s*\)"
    )
    m = re.search(pattern, col_part, re.IGNORECASE)
    if not m:
        return None
    return [
        v.strip().strip("'").replace("''", "'")
        for v in m.group(1).split(",")
    ]


def _table_check_clauses(parts: list[str]) -> list[str]:
    """Extract inner expressions of every table-level CHECK constraint."""
    out: list[str] = []
    for p in parts:
        m = re.match(
            r"^\s*(?:CONSTRAINT\s+\S+\s+)?CHECK\s*\((.*)\)\s*$",
            p,
            re.IGNORECASE | re.DOTALL,
        )
        if m:
            out.append(m.group(1).strip())
    return out


def _table_unique_constraints(
    conn: sqlite3.Connection, table_name: str
) -> list[list[str]]:
    """Return UNIQUE column-groups (excluding PK) for *table_name*.

    Uses ``PRAGMA index_list`` filtered by ``origin = 'u'`` (explicit
    table-level UNIQUE constraints, not user-created CREATE INDEX).
    """
    qt = _quote_identifier(table_name)
    rows = conn.execute(f"PRAGMA index_list({qt})").fetchall()
    out: list[list[str]] = []
    for row in rows:
        # row: (seq, name, unique, origin, partial)
        if not bool(row[2]) or row[3] != "u":
            continue
        info = conn.execute(
            f'PRAGMA index_info("{row[1]}")'
        ).fetchall()
        # info: (seqno, cid, name)
        out.append([r[2] for r in info])
    return out


def _parse_reference(entry: str) -> tuple[str, str, str] | None:
    """Parse a ``## references[]`` entry into (local, table, column).

    Form: ``local_col: ForeignTable.foreign_col``. Returns None on
    malformed input. Multi-column FKs aren't representable in this
    sub-section vocabulary in v1.
    """
    m = re.match(r"^([^:]+):\s*([^.]+)\.(.+)$", entry.strip())
    if not m:
        return None
    return m.group(1).strip(), m.group(2).strip(), m.group(3).strip()


def _build_create_index_sql(
    name: str,
    table: str,
    columns: list[str],
    unique: bool,
    where: str | None,
) -> str:
    """Render a ``CREATE [UNIQUE] INDEX ... ON ... [WHERE ...]`` statement."""
    cols_q = ", ".join(_quote_identifier(c) for c in columns)
    unique_kw = "UNIQUE " if unique else ""
    where_clause = f" WHERE {where}" if where else ""
    return (
        f"CREATE {unique_kw}INDEX {_quote_identifier(name)}"
        f" ON {_quote_identifier(table)} ({cols_q}){where_clause}"
    )


def _index_where_clause(sql: str) -> str | None:
    """Extract a partial-index WHERE from a CREATE INDEX statement.

    SQLite's PRAGMA index_list reports the partial flag but not the
    expression itself, so we read it out of the stored SQL.
    """
    m = re.search(
        r"\)\s*WHERE\s+(.+?)\s*;?\s*$",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    return m.group(1).strip() if m else None


def _build_create_trigger_sql(
    name: str,
    table: str,
    when: str,
    event: str,
    condition: str | None,
    body: str,
) -> str:
    """Render a ``CREATE TRIGGER ... BEGIN ... END`` statement.

    SQLite supports BEFORE / AFTER / INSTEAD OF for the timing and
    INSERT / UPDATE [OF cols] / DELETE for the event. Trigger
    statements always operate FOR EACH ROW (the only level SQLite
    supports), so the modifier is emitted unconditionally.
    """
    parts = [
        f"CREATE TRIGGER {_quote_identifier(name)}",
        when.strip(),
        event.strip(),
        f"ON {_quote_identifier(table)}",
        "FOR EACH ROW",
    ]
    if condition:
        parts.append(f"WHEN {condition}")
    body_stripped = body.strip()
    if not body_stripped.endswith(";"):
        body_stripped = body_stripped + ";"
    parts.append("BEGIN")
    parts.append(body_stripped)
    parts.append("END")
    return " ".join(parts)


_TRIGGER_RE = re.compile(
    r"^CREATE\s+TRIGGER\s+(?:\"([^\"]+)\"|(\w+))\s+"
    r"(BEFORE|AFTER|INSTEAD\s+OF)\s+"
    r"(INSERT|DELETE|UPDATE(?:\s+OF\s+[\w,\s\"]+)?)\s+"
    r"ON\s+(?:\"([^\"]+)\"|(\w+))\s+"
    r"(?:FOR\s+EACH\s+ROW\s+)?"
    r"(?:WHEN\s+(.+?)\s+)?"
    r"BEGIN\s+(.+?)\s+END\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)


def _parse_trigger_sql(sql: str) -> dict[str, Any] | None:
    """Parse a stored CREATE TRIGGER SQL into a dict of fields."""
    m = _TRIGGER_RE.match(sql.strip())
    if not m:
        return None
    name = m.group(1) or m.group(2) or ""
    when_kw = m.group(3).upper().replace("  ", " ")
    event = re.sub(r"\s+", " ", m.group(4).strip()).upper()
    table = m.group(5) or m.group(6) or ""
    condition = m.group(7).strip() if m.group(7) else None
    body = m.group(8).strip()
    return {
        "name": name,
        "when": when_kw,
        "event": event,
        "table": table,
        "condition": condition,
        "body": body,
    }


def _user_triggers(
    conn: sqlite3.Connection,
) -> list[tuple[str, str]]:
    """Return (name, tbl_name) for user-defined triggers."""
    rows = conn.execute(
        "SELECT name, tbl_name FROM sqlite_master"
        " WHERE type='trigger' AND sql IS NOT NULL"
        " ORDER BY name"
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def _build_create_virtual_table_sql(
    table_name: str,
    module: str,
    columns: list[str],
    unindexed: set[str],
    options: list[str],
) -> str:
    """Render ``CREATE VIRTUAL TABLE name USING module(args)``.

    Args inside the module's parentheses are columns (with optional
    ``UNINDEXED`` flag for FTS5) followed by raw module-option
    strings (``tokenize = 'porter unicode61'`` etc.). Columns are
    rendered without types — FTS5 doesn't carry types, and other
    modules either ignore or accept bare column names.
    """
    args: list[str] = []
    for c in columns:
        if c in unindexed:
            args.append(f"{_quote_identifier(c)} UNINDEXED")
        else:
            args.append(_quote_identifier(c))
    args.extend(options)
    return (
        f"CREATE VIRTUAL TABLE {_quote_identifier(table_name)}"
        f" USING {module}({', '.join(args)})"
    )


_VIRTUAL_TABLE_RE = re.compile(
    r"^\s*CREATE\s+VIRTUAL\s+TABLE\s+(?:\"[^\"]+\"|\w+)"
    r"\s+USING\s+(\w+)\s*\((.*)\)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)


def _is_virtual_table_sql(create_sql: str) -> bool:
    """True if the stored DDL declares a CREATE VIRTUAL TABLE."""
    return bool(
        re.match(
            r"^\s*CREATE\s+VIRTUAL\s+TABLE",
            create_sql or "",
            re.IGNORECASE,
        )
    )


def _parse_virtual_table(
    create_sql: str,
) -> tuple[str, list[str]] | None:
    """Return ``(module_name, comma-split args)`` from a virtual DDL."""
    m = _VIRTUAL_TABLE_RE.match(create_sql.strip())
    if not m:
        return None
    return m.group(1), _split_top_level(m.group(2))


def _is_unindexed_arg(arg: str) -> tuple[str, bool] | None:
    """Classify a virtual-table arg as a column declaration.

    Returns (column_name, is_unindexed) for column args, None if
    the arg looks like a module option (contains ``=``).
    """
    if "=" in arg:
        return None
    s = arg.strip()
    unindexed = False
    if re.search(r"\bUNINDEXED\b", s, re.IGNORECASE):
        s = re.sub(r"\s*UNINDEXED\s*", "", s, flags=re.IGNORECASE)
        unindexed = True
    s = s.strip()
    if s.startswith('"') and s.endswith('"'):
        s = s[1:-1]
    return s, unindexed


def _user_views(conn: sqlite3.Connection) -> list[str]:
    """Return user view names from sqlite_master."""
    rows = conn.execute(
        "SELECT name FROM sqlite_master"
        " WHERE type='view' AND name NOT LIKE 'sqlite_%'"
        " ORDER BY name"
    ).fetchall()
    return [r[0] for r in rows]


_VIEW_RE = re.compile(
    r"^CREATE\s+VIEW\s+(?:\"([^\"]+)\"|(\w+))\s+AS\s+(.+?)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)


def _parse_view_select(sql: str) -> str | None:
    """Pull the SELECT body out of a stored CREATE VIEW statement."""
    m = _VIEW_RE.match(sql.strip())
    return m.group(3).strip() if m else None


def _user_indexes(
    conn: sqlite3.Connection, table_name: str | None = None
) -> list[tuple[str, str]]:
    """Return (name, tbl_name) for user-created indexes.

    Filters to ``sql IS NOT NULL`` to skip auto-indexes generated by
    SQLite for UNIQUE constraints and PRIMARY KEY (those carry the
    ``sqlite_autoindex_…`` prefix and have no associated DDL).
    """
    if table_name is None:
        rows = conn.execute(
            "SELECT name, tbl_name FROM sqlite_master"
            " WHERE type='index' AND sql IS NOT NULL"
            " ORDER BY name"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT name, tbl_name FROM sqlite_master"
            " WHERE type='index' AND tbl_name = ?"
            " AND sql IS NOT NULL"
            " ORDER BY name",
            (table_name,),
        ).fetchall()
    return [(r[0], r[1]) for r in rows]


def _table_foreign_keys(
    conn: sqlite3.Connection, table_name: str
) -> list[tuple[str, str, str]]:
    """Return single-column FK refs as (local_col, ref_table, ref_col).

    Multi-column FKs are dropped from the output for v1 — they would
    need a richer JMD shape than ``local: Table.col`` to round-trip.
    """
    qt = _quote_identifier(table_name)
    rows = conn.execute(f"PRAGMA foreign_key_list({qt})").fetchall()
    by_id: dict[int, list[tuple[str, str, str]]] = {}
    for r in rows:
        # r: (id, seq, table, from, to, on_update, on_delete, match)
        by_id.setdefault(r[0], []).append((r[3], r[2], r[4]))
    out: list[tuple[str, str, str]] = []
    for fk_id in sorted(by_id):
        cols = by_id[fk_id]
        if len(cols) == 1:
            out.append(cols[0])
    return out
