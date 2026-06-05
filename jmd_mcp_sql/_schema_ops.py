# SPDX-License-Identifier: Apache-2.0
"""Schema-mode (#!) operations as free functions over a TranslatorContext.

The full surface from Slices A-F is implemented here as module-level
functions taking a :class:`TranslatorContext` as the first parameter.
This is the modern-Python idiom — module + context + free functions —
in place of the earlier mixin-base-class pattern. See the workspace
memory ``Modernes pythonisches Design`` for the underlying principle.

Public entry points are re-exported by :mod:`translator`'s public
``read``/``write``/``delete`` methods, which dispatch to the right
free function based on the JMD document mode.
"""
from __future__ import annotations

import re
import sqlite3
from typing import Any

from jmd import JMDParser, JMDSchemaParser, SchemaField, serialize

from ._context import TranslatorContext, label_from_source
from ._ddl import (
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
from ._sql import _JMD_TO_SQLITE, _quote_identifier, _sqlite_type_to_jmd
from .schema import TableInfo


def _read_schema(ctx: TranslatorContext, jmd_source: str) -> str:
    """Return the table structure as a JMD #! schema document.

    The output mirrors the input syntax expected by _write_schema,
    so the LLM can read a schema, understand column types and
    constraints, and construct correctly-typed data documents.

    The special label ``Database`` (when no real table of that
    name exists) returns a root-schema document that describes
    the server's full capabilities — tables, supported
    frontmatter keys, QBE operators, and tolerance policies.
    """
    label = label_from_source(jmd_source)

    # Root-schema: self-description of the server.
    if (
        label.lower() == "database"
        and ctx.schema.resolve("Database") is None
    ):
        return _read_root_schema(ctx)

    # Reserved DDL-object labels (Slice B+).
    if label == "Index" and ctx.schema.resolve("Index") is None:
        return _read_index_doc(ctx, jmd_source)
    if (
        label == "Trigger"
        and ctx.schema.resolve("Trigger") is None
    ):
        return _read_trigger_doc(ctx, jmd_source)
    if label == "View" and ctx.schema.resolve("View") is None:
        return _read_view_doc(ctx, jmd_source)

    table = ctx.resolve_or_error(label)

    # Pull the raw DDL: CHECK constraints (column-level for
    # enum reconstruction, table-level for ## check[]) have no
    # PRAGMA introspection — we have to parse sqlite_master.
    row = ctx.conn.execute(
        "SELECT sql FROM sqlite_master"
        " WHERE type='table' AND name=?",
        (table.name,),
    ).fetchone()
    create_sql = row[0] if row else ""
    if _is_virtual_table_sql(create_sql):
        return _read_virtual_table(ctx, label, table, create_sql)
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

    uniques = _table_unique_constraints(ctx.conn, table.name)
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

    fks = _table_foreign_keys(ctx.conn, table.name)
    if fks:
        lines.append("")
        lines.append("## references[]")
        for local, ftab, fcol in fks:
            lines.append(f"- {local}: {ftab}.{fcol}")

    return "\n".join(lines)

def _read_root_schema(ctx: TranslatorContext) -> str:
    """Build the root-schema document for ``#! Database``.

    The document describes this server's capabilities so that
    an LLM can discover tables, frontmatter keys, filter
    operators, and tolerance policies in a single call.
    """
    tables = sorted(ctx.schema.tables().keys())

    lines: list[str] = ["#! Database"]

    # Tables — the only entity-level information about a
    # Database.  Server capabilities (frontmatter keys, filter
    # operators, tolerance policies, debug values) belong in
    # the tool descriptions, not in entity schemas.
    lines.append("## tables[]")
    for t in tables:
        lines.append(f"- {t}")

    indexes = _user_indexes(ctx.conn)
    if indexes:
        lines.append("")
        lines.append("## indexes[]")
        for idx_name, _tbl in indexes:
            lines.append(f"- {idx_name}")

    triggers = _user_triggers(ctx.conn)
    if triggers:
        lines.append("")
        lines.append("## triggers[]")
        for trg_name, _tbl in triggers:
            lines.append(f"- {trg_name}")

    views = _user_views(ctx.conn)
    if views:
        lines.append("")
        lines.append("## views[]")
        for v_name in views:
            lines.append(f"- {v_name}")

    return "\n".join(lines)

def _write_schema(ctx: TranslatorContext, jmd_source: str) -> str:
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
        and ctx.schema.resolve("Index") is None
    ):
        return _write_index_doc(ctx, jmd_source)
    if (
        table_name == "Trigger"
        and ctx.schema.resolve("Trigger") is None
    ):
        return _write_trigger_doc(ctx, jmd_source)
    if (
        table_name == "View"
        and ctx.schema.resolve("View") is None
    ):
        return _write_view_doc(ctx, jmd_source)

    # Sub-section data is only visible through JMDParser; the
    # schema parser leaves ``## name[]`` as empty SchemaObjects.
    data = JMDParser().parse(jmd_source).value
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

    existing = ctx.schema.resolve(table_name)
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
        return _create_virtual_table(ctx,
            table_name,
            str(using_module),
            scalar_fields,
            unindexed_sec,
            options_sec,
        )
    if existing is None:
        return _create_table(ctx,
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
        return _rebuild_table(ctx,
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
    return _alter_table(ctx,
        table_name,
        existing,
        scalar_fields,
        bool(
            primary_keys_sec or uniques_sec or checks_sec
            or references_sec
        ),
    )

def _render_create_table_sql(ctx: TranslatorContext, table_name: str,
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
            _render_column_def(ctx, f, use_table_level_pk)
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

def _create_table(ctx: TranslatorContext, table_name: str,
    scalar_fields: list[SchemaField],
    primary_keys_sec: list[str],
    uniques_sec: list[str],
    checks_sec: list[str],
    references_sec: list[str],
    indexes_sec: list[Any],
    triggers_sec: list[Any],
) -> str:
    """Render and execute a fresh CREATE TABLE."""
    sql_or_err = _render_create_table_sql(ctx,
        table_name, scalar_fields, primary_keys_sec,
        uniques_sec, checks_sec, references_sec,
    )
    if isinstance(sql_or_err, dict):
        return serialize(sql_or_err, label="Error")
    sql = sql_or_err
    try:
        ctx.conn.execute(sql)
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
        idx_sql_or_err = _build_inline_index_sql(ctx,
            entry, table_name
        )
        if isinstance(idx_sql_or_err, dict):
            # Validation/render error — abort and drop.
            ctx.conn.execute(
                f"DROP TABLE {_quote_identifier(table_name)}"
            )
            ctx.conn.commit()
            return serialize(idx_sql_or_err, label="Error")
        try:
            ctx.conn.execute(idx_sql_or_err)
        except sqlite3.Error as e:
            ctx.conn.execute(
                f"DROP TABLE {_quote_identifier(table_name)}"
            )
            ctx.conn.commit()
            return serialize(
                {"status": 400, "code": "ddl_failed",
                 "message": str(e)},
                label="Error",
            )
    for entry in triggers_sec:
        if not isinstance(entry, dict):
            continue
        trg_sql_or_err = _build_inline_trigger_sql(ctx,
            entry, table_name
        )
        if isinstance(trg_sql_or_err, dict):
            ctx.conn.execute(
                f"DROP TABLE {_quote_identifier(table_name)}"
            )
            ctx.conn.commit()
            return serialize(trg_sql_or_err, label="Error")
        try:
            ctx.conn.execute(trg_sql_or_err)
        except sqlite3.Error as e:
            ctx.conn.execute(
                f"DROP TABLE {_quote_identifier(table_name)}"
            )
            ctx.conn.commit()
            return serialize(
                {"status": 400, "code": "ddl_failed",
                 "message": str(e)},
                label="Error",
            )
    ctx.conn.commit()
    ctx.refresh_schema()
    return serialize(
        {"table": table_name, "created": True}, label="Result"
    )

def _read_virtual_table(ctx: TranslatorContext, label: str,
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

def _create_virtual_table(ctx: TranslatorContext, table_name: str,
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
        ctx.conn.execute(sql)
        ctx.conn.commit()
    except sqlite3.Error as e:
        return serialize(
            {"status": 400, "code": "ddl_failed",
             "message": str(e)},
            label="Error",
        )
    ctx.refresh_schema()
    return serialize(
        {"table": table_name, "created": True,
         "virtual": True, "using": module},
        label="Result",
    )

def _alter_table(ctx: TranslatorContext, table_name: str,
    existing: TableInfo,
    scalar_fields: list[SchemaField],
    any_constraint_section: bool,
) -> str:
    """Additive ALTER: add new columns, never modify existing."""
    existing_cols = {c.name for c in existing.columns}
    added: list[str] = []
    with ctx.conn:
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
            ctx.conn.execute(" ".join(parts))
            added.append(f.key)
    ctx.refresh_schema()
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

def _rebuild_table(ctx: TranslatorContext, table_name: str,
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
    sql_or_err = _render_create_table_sql(ctx,
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
    old_iso = ctx.conn.isolation_level
    ctx.conn.isolation_level = None
    try:
        ctx.conn.execute("BEGIN IMMEDIATE")
        try:
            # Defensive: drop any leftover staging table from a
            # previous failed rebuild attempt.
            ctx.conn.execute(
                f"DROP TABLE IF EXISTS"
                f" {_quote_identifier(staging_name)}"
            )
            ctx.conn.execute(create_staging_sql)
            if insert_sql is not None:
                ctx.conn.execute(insert_sql)
            ctx.conn.execute(
                f"DROP TABLE"
                f" {_quote_identifier(table_name)}"
            )
            ctx.conn.execute(
                f"ALTER TABLE"
                f" {_quote_identifier(staging_name)}"
                f" RENAME TO {_quote_identifier(table_name)}"
            )
            # Inline indexes & triggers from the rebuild doc.
            for entry in indexes_sec:
                if not isinstance(entry, dict):
                    continue
                idx = _build_inline_index_sql(ctx,
                    entry, table_name
                )
                if isinstance(idx, dict):
                    raise sqlite3.Error(
                        str(idx.get("message", ""))
                    )
                ctx.conn.execute(idx)
            for entry in triggers_sec:
                if not isinstance(entry, dict):
                    continue
                trg = _build_inline_trigger_sql(ctx,
                    entry, table_name
                )
                if isinstance(trg, dict):
                    raise sqlite3.Error(
                        str(trg.get("message", ""))
                    )
                ctx.conn.execute(trg)
            ctx.conn.execute("COMMIT")
        except sqlite3.Error as e:
            ctx.conn.execute("ROLLBACK")
            return serialize(
                {"status": 400, "code": "rebuild_failed",
                 "message": str(e)},
                label="Error",
            )
    finally:
        ctx.conn.isolation_level = old_iso

    ctx.refresh_schema()
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
    ctx: TranslatorContext, f: SchemaField, use_table_level_pk: bool
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
    ctx: TranslatorContext, entry: dict[str, Any], table_name: str
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

def _write_index_doc(ctx: TranslatorContext, jmd_source: str) -> str:
    """Handle ``write('#! Index ...')`` — create one index."""
    data = JMDParser().parse(jmd_source).value
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
        ctx.conn.execute(sql)
        ctx.conn.commit()
    except sqlite3.Error as e:
        return serialize(
            {"status": 400, "code": "ddl_failed",
             "message": str(e)},
            label="Error",
        )
    ctx.refresh_schema()
    return serialize(
        {"index": str(name), "table": str(table),
         "created": True},
        label="Result",
    )

def _read_index_doc(ctx: TranslatorContext, jmd_source: str) -> str:
    """Handle ``read('#! Index ...')`` — return one index's schema."""
    data = JMDParser().parse(jmd_source).value
    name = data.get("name")
    if not name:
        return serialize(
            {"status": 400, "code": "bad_request",
             "message": "#! Index read requires 'name' field"},
            label="Error",
        )
    row = ctx.conn.execute(
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
    info = ctx.conn.execute(
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
    ctx: TranslatorContext, jmd_source: str, fm: dict[str, Any]
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
    data = JMDParser().parse(jmd_source).value
    name = data.get("name")
    if not name:
        return serialize(
            {"status": 400, "code": "bad_request",
             "message": "#! Index delete requires 'name' field"},
            label="Error",
        )
    row = ctx.conn.execute(
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
    ctx.conn.execute(
        f"DROP INDEX {_quote_identifier(str(name))}"
    )
    ctx.conn.commit()
    ctx.refresh_schema()
    return serialize(
        {"index": str(name), "dropped": True},
        label="Result",
    )

# ------------------------------------------------------------------
# Trigger DDL — top-level ``#! Trigger`` shape (Slice C)
# ------------------------------------------------------------------

def _build_inline_trigger_sql(
    ctx: TranslatorContext, entry: dict[str, Any], table_name: str
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

def _write_trigger_doc(ctx: TranslatorContext, jmd_source: str) -> str:
    """Handle ``write('#! Trigger ...')`` — create one trigger."""
    data = JMDParser().parse(jmd_source).value
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
        ctx.conn.execute(sql)
        ctx.conn.commit()
    except sqlite3.Error as e:
        return serialize(
            {"status": 400, "code": "ddl_failed",
             "message": str(e)},
            label="Error",
        )
    ctx.refresh_schema()
    return serialize(
        {"trigger": str(name), "table": str(table),
         "created": True},
        label="Result",
    )

def _read_trigger_doc(ctx: TranslatorContext, jmd_source: str) -> str:
    """Handle ``read('#! Trigger ...')`` — return one trigger's schema."""
    data = JMDParser().parse(jmd_source).value
    name = data.get("name")
    if not name:
        return serialize(
            {"status": 400, "code": "bad_request",
             "message": (
                 "#! Trigger read requires 'name' field"
             )},
            label="Error",
        )
    row = ctx.conn.execute(
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
    ctx: TranslatorContext, jmd_source: str, fm: dict[str, Any]
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
    data = JMDParser().parse(jmd_source).value
    name = data.get("name")
    if not name:
        return serialize(
            {"status": 400, "code": "bad_request",
             "message": (
                 "#! Trigger delete requires 'name' field"
             )},
            label="Error",
        )
    row = ctx.conn.execute(
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
    ctx.conn.execute(
        f"DROP TRIGGER {_quote_identifier(str(name))}"
    )
    ctx.conn.commit()
    ctx.refresh_schema()
    return serialize(
        {"trigger": str(name), "dropped": True},
        label="Result",
    )

# ------------------------------------------------------------------
# View DDL — top-level ``#! View`` shape (Slice D)
# ------------------------------------------------------------------

def _write_view_doc(ctx: TranslatorContext, jmd_source: str) -> str:
    """Handle ``write('#! View ...')`` — create one view."""
    data = JMDParser().parse(jmd_source).value
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
        ctx.conn.execute(sql)
        ctx.conn.commit()
    except sqlite3.Error as e:
        return serialize(
            {"status": 400, "code": "ddl_failed",
             "message": str(e)},
            label="Error",
        )
    ctx.refresh_schema()
    return serialize(
        {"view": str(name), "created": True},
        label="Result",
    )

def _read_view_doc(ctx: TranslatorContext, jmd_source: str) -> str:
    """Handle ``read('#! View ...')`` — return one view's schema."""
    data = JMDParser().parse(jmd_source).value
    name = data.get("name")
    if not name:
        return serialize(
            {"status": 400, "code": "bad_request",
             "message": "#! View read requires 'name' field"},
            label="Error",
        )
    row = ctx.conn.execute(
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

def _delete_view_doc(ctx: TranslatorContext, jmd_source: str, fm: dict[str, Any]
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
    data = JMDParser().parse(jmd_source).value
    name = data.get("name")
    if not name:
        return serialize(
            {"status": 400, "code": "bad_request",
             "message": "#! View delete requires 'name' field"},
            label="Error",
        )
    row = ctx.conn.execute(
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
    ctx.conn.execute(
        f"DROP VIEW {_quote_identifier(str(name))}"
    )
    ctx.conn.commit()
    ctx.refresh_schema()
    return serialize(
        {"view": str(name), "dropped": True},
        label="Result",
    )

def _delete_schema(ctx: TranslatorContext, jmd_source: str) -> str:
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
    label = label_from_source(jmd_source)
    if label == "Index" and ctx.schema.resolve("Index") is None:
        return _delete_index_doc(ctx, jmd_source, fm)
    if (
        label == "Trigger"
        and ctx.schema.resolve("Trigger") is None
    ):
        return _delete_trigger_doc(ctx, jmd_source, fm)
    if label == "View" and ctx.schema.resolve("View") is None:
        return _delete_view_doc(ctx, jmd_source, fm)

    if fm.get("confirm") != "drop-table":
        return serialize(
            {"status": 400, "code": "confirmation_required",
             "message": (
                 "Dropping a table requires "
                 "'confirm: drop-table' in the frontmatter"
             )},
            label="Error",
        )

    table = ctx.schema.resolve(label)
    if table is None:
        return serialize(
            {"status": 404, "code": "not_found",
             "message": f"Table '{label}' does not exist"},
            label="Error",
        )
    if table.is_view:
        ctx.conn.execute(
            f"DROP VIEW IF EXISTS {_quote_identifier(table.name)}"
        )
    else:
        ctx.conn.execute(
            f"DROP TABLE IF EXISTS {_quote_identifier(table.name)}"
        )
    ctx.conn.commit()
    # Invalidate the cache after any DDL operation.
    ctx.refresh_schema()
    return serialize({"dropped": label}, label="Result")
