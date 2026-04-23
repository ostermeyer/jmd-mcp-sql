# SPDX-License-Identifier: Apache-2.0
"""JMD MCP server for SQLite.

This module wires together the MCP framework (FastMCP), the SQL translator,
and the database connection.  It exposes four tools that an LLM can call:

    open   — open a SQLite database file or show current database info
    read   — query records, filter with QBE, or describe table schemas
    write  — insert/replace records or create/extend tables
    delete — remove records or drop tables

The server can be started against any SQLite database file by passing its
path as a command-line argument.  When no argument is given, it uses the
bundled Northwind demo database, creating it automatically from the SQL
dump (``northwind.sql``) on the first run.

Usage::

    # Against a custom database
    python -m jmd_mcp_sql.server /path/to/mydb.db

    # Against the Northwind demo
    python -m jmd_mcp_sql.server
"""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any

from jmd import JMDParser, serialize
from mcp.server.fastmcp import FastMCP

from .config import load as load_config
from .translator import (
    SQLTranslator,
    StrictRefusalError,
    _check_frontmatter,
    _prepend_ignored_keys,
)

_INSTRUCTIONS = (
    'This is JMD not SQL. Read "#! Database" to learn how.'
)

# Global FastMCP instance.  Tool functions are registered via @mcp.tool()
# decorators below and become available to the MCP host on connection.
mcp = FastMCP("jmd-mcp-sql", instructions=_INSTRUCTIONS)

# Set by main() before mcp.run(); None while the module is imported without
# a running server (e.g. during tests or type-checking).
_translator: SQLTranslator | None = None
_db_path: Path | None = None
_config: dict[str, str | None] = {}


def _t() -> SQLTranslator:
    """Return the active SQLTranslator, raising if the server is not running."""
    if _translator is None:
        raise RuntimeError("Server not initialized — call main() first")
    return _translator


def _resolve_db_path(raw: str) -> Path:
    """Resolve and validate a database path against the configured root.

    Args:
        raw: The path string from the JMD document.

    Returns:
        The resolved absolute ``Path``.

    Raises:
        ValueError: If the path is outside the configured root.
    """
    resolved = Path(raw).expanduser().resolve()
    root = _config.get("root")
    if root is not None:
        root_resolved = Path(root).expanduser().resolve()
        if not resolved.is_relative_to(root_resolved):
            msg = (
                f"Path {resolved} is outside the allowed "
                f"root {root_resolved}"
            )
            raise ValueError(msg)
    return resolved


def _open_db(db_path: Path) -> str:
    """Open a SQLite database and set it as the active connection.

    Closes the previous connection if one exists.  If the file does
    not exist, SQLite creates a new empty database.

    Args:
        db_path: Resolved, validated absolute path.

    Returns:
        JMD document with database metadata in frontmatter and table
        list in the body.
    """
    global _translator, _db_path

    created = not db_path.exists()

    # Close previous connection cleanly.
    if _translator is not None:
        _translator.close()

    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    _translator = SQLTranslator(conn)
    _db_path = db_path

    return _db_status(created=created)


def _db_status(*, created: bool = False) -> str:
    """Build a JMD response describing the active database.

    Args:
        created: Whether the database file was just created.

    Returns:
        JMD document with frontmatter metadata and table list.
    """
    tables = _t()._schema.tables()
    table_names = sorted(tables.keys())

    # --- frontmatter (transport metadata) ---
    parts: list[str] = [f"path: {_db_path}"]
    parts.append(f"table-count: {len(table_names)}")
    if created:
        parts.append("created: true")
    parts.append("")  # blank line separates frontmatter from heading

    # --- body ---
    parts.append("# Database")
    if table_names:
        parts.append("## tables[]")
        for name in table_names:
            parts.append(f"- {name}")

    return "\n".join(parts) + "\n"


@mcp.tool(name="open")
def open_database(document: str) -> str:
    """Open a SQLite database or inspect the current one.

    Open (# Database with path):

        # Database
        path: /path/to/mydb.db

    If the file does not exist, it is CREATED as an empty
    database and the response contains ``created: true``.

    Inspect (# Database with no fields):

        # Database

    Returns path, table count, and a list of all tables.

    Frontmatter policy: observable tolerance — unknown keys are
    echoed in the response as 'ignored-keys: ...'.
    """
    try:
        parser = JMDParser()
        parsed: Any = parser.parse(document)
        ignored = _check_frontmatter(
            parser.frontmatter,
            frozenset({"path"}),
            "observable",
        )
        path_value = parsed.get("path") if isinstance(parsed, dict) else None

        if path_value is None:
            # Status query.
            if _db_path is None:
                return serialize(
                    {
                        "status": 400,
                        "code": "no_database",
                        "message": "No database is currently open",
                    },
                    label="Error",
                )
            return _prepend_ignored_keys(_db_status(), ignored)

        db_path = _resolve_db_path(str(path_value))
        return _prepend_ignored_keys(_open_db(db_path), ignored)
    except ValueError as exc:
        return serialize(
            {"status": 403, "code": "path_denied", "message": str(exc)},
            label="Error",
        )
    except Exception as exc:
        return serialize(
            {"status": 400, "code": "open_failed", "message": str(exc)},
            label="Error",
        )


@mcp.tool()
def read(document: str) -> str:
    """Read records or table schema using a JMD document (https://github.com/ostermeyer/jmd-spec).

    Data document (# Label): look up records by exact field values.
    Returns a single record if exactly one matches, a list otherwise.

        # Order
        id: 42

    Query-by-Example document (#? Label): filter records by field
    values.  Omitted fields match any value.  Always returns a list.

        #? Order
        status: pending

    Schema document (#! Label): describe the table structure.
    Returns a #! document with column names, types, and modifiers.
    Use ``read("#! Database")`` to list all tables.

        #! Order

    QBE filter operators (in #? bodies):
      =   equality (default when no operator)
      >   greater than       >=  greater or equal
      <   less than          <=  less or equal
      |   alternation (OR) e.g. 'Germany|France'
      ~   substring (case-insensitive) e.g. '~Corp'
      ^   regex (implicit full-match anchoring)
      !   negation, composable with any operator

    Frontmatter keys (before the heading):
      page-size, page      pagination
      count                return count only, no rows
      select               projection (comma-separated columns)
      join                 cross-table JOIN, e.g. 'Table on Col'
      sum, avg, min, max   aggregations with optional 'as alias'
      group, having, sort  GROUP BY / HAVING / ORDER BY

    Frontmatter policy: observable tolerance — unknown keys are
    echoed in the response as 'ignored-keys: ...'.
    Debug frontmatter: 'debug: sql, timing, table, plan, filters,
    resolved, coercions' (composable, or 'debug: true' for all).
    """
    try:
        return _t().read(document)
    except Exception as e:
        return serialize(
            {"status": 400, "code": "read_failed", "message": str(e)},
            label="Error",
        )


@mcp.tool()
def write(document: str) -> str:
    """Write a record or define a table schema using a JMD document (https://github.com/ostermeyer/jmd-spec).

    Data document (# Label): insert or replace a record.
    If a record with the same primary key exists, it is replaced.
    Returns the written record as confirmed by the database.

        # Order
        id: 42
        status: shipped
        total: 149.99

    Bulk insert (# Label[]): insert multiple records at once.

        # Order[]
        - id: 42
          status: shipped
        - id: 43
          status: pending

    Schema document (#! Label): create or extend a table.
    If the table does not exist, it is created. If it exists, new
    columns are added. Existing columns are never modified or
    removed (the response carries 'skipped[]' listing ignored
    changes).  Types: integer, float, string, boolean (stored as
    integer in SQLite).  Modifiers: 'readonly' (primary key),
    'optional' (nullable).

        #! Order
        id: integer readonly
        status: string
        total: float optional

    Frontmatter policy: observable tolerance — unknown keys are
    echoed in the response as 'ignored-keys: ...'.
    Debug frontmatter: 'debug: sql, timing, table' (composable,
    or 'debug: true' for all).
    """
    try:
        return _t().write(document)
    except Exception as e:
        return serialize(
            {"status": 400, "code": "write_failed", "message": str(e)},
            label="Error",
        )


@mcp.tool()
def delete(document: str) -> str:
    """Delete records or drop a table using a JMD document (https://github.com/ostermeyer/jmd-spec).

    Delete document (#- Label): delete matching records.
    All fields act as filters. At least one field is required.
    Returns the deleted record as confirmation.

        #- Order
        id: 42

    Bulk delete (#- Label[]): delete by primary-key list.
    Use after a #? read to verify the target set.

        #- Order[]
        - 42
        - 43

    Schema document (#! Label): drop the entire table.
    Requires ``confirm: drop-table`` in frontmatter.

        confirm: drop-table

        #! Order

    Frontmatter policy: strict refusal — unknown keys cause a
    structured error.  Accepted keys: confirm, debug.
    Debug frontmatter: 'debug: sql, timing, table' (composable,
    or 'debug: true' for all).
    """
    try:
        return _t().delete(document)
    except StrictRefusalError as exc:
        return serialize(
            {
                "status": 400,
                "code": "unknown_frontmatter_key",
                "message": str(exc),
            },
            label="Error",
        )
    except Exception as e:  # noqa: BLE001
        return serialize(
            {"status": 400, "code": "delete_failed", "message": str(e)},
            label="Error",
        )


def main() -> None:
    """Entry point: parse arguments, open the database, and start the server."""
    global _translator, _db_path, _config

    _config = load_config()

    parser = argparse.ArgumentParser(description="JMD MCP server for SQLite")
    parser.add_argument(
        "db",
        nargs="?",
        default=None,
        help="Path to SQLite database file (default: Northwind demo)",
    )
    args = parser.parse_args()

    if args.db:
        db_path = Path(args.db)
        if not db_path.exists():
            raise SystemExit(f"Database not found: {db_path}")
    else:
        db_path = Path(__file__).parent / "northwind.db"
        if not db_path.exists():
            # The demo database ships as a plain-text SQL dump.  Create the
            # binary .db from it on first run and reuse it on subsequent runs.
            sql_path = Path(__file__).parent / "northwind.sql"
            if not sql_path.exists():
                raise SystemExit(
                    "Northwind demo database not found. "
                    "northwind.sql is missing from the "
                    "jmd_mcp_sql/ package directory."
                )
            conn = sqlite3.connect(str(db_path))
            conn.executescript(sql_path.read_text(encoding="utf-8"))
            conn.close()

    # check_same_thread=False is safe here because FastMCP processes one
    # request at a time over stdio; there is no concurrent access.
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    _translator = SQLTranslator(conn)
    _db_path = db_path.resolve()

    mcp.run()


if __name__ == "__main__":
    main()
