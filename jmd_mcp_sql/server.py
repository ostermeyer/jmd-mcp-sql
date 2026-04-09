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

from jmd import jmd_to_dict, serialize
from mcp.server.fastmcp import FastMCP

from .config import load as load_config
from .translator import SQLTranslator

_INSTRUCTIONS = """
This server exposes a SQLite database through four tools —
open, read, write, delete — using JMD (JSON Markdown) as the data format.

## JMD document syntax

Every document starts with a heading line that sets the document type
and table name, followed by key: value pairs (one per line):

  # Product          → data document   (exact lookup / insert-or-replace)
  #? Product         → query document  (filter / list)
  #! Product         → schema document (describe / create / drop table)
  #- Product         → delete document (delete matching records)

  key: value         → string, integer, or float — inferred automatically
  key: true/false    → boolean

## Opening a database

Open a SQLite database file at any time:

  open("# Database\npath: /path/to/mydb.db")

If the file does not exist, a new empty database is created.
The previous database is closed automatically.

Check which database is currently active:

  open("# Database")

## Discovering the database

To see which tables exist, read each table's schema:

  read("#! Customers")

This returns a #! document with column names, JMD types, and modifiers
(readonly = primary key, optional = nullable).

## Typical workflows

**List all rows (small tables only):**
  read("#? Orders")

**Filter rows — equality:**
  read("#? Orders\nstatus: shipped")

**Filter rows — comparison:**
  read("#? Orders\nFreight: > 50")

**Filter rows — alternation (OR):**
  read("#? Orders\nShipCountry: Germany|France|UK")

**Filter rows — contains (case-insensitive substring):**
  read("#? Customers\nCompanyName: ~Corp")

**Filter rows — regex pattern:**
  read("#? Products\nProductName: ^Chai.*")

**Filter rows — negation (composes with any operator):**
  read("#? Orders\nShipCountry: !Germany")
  read("#? Products\nProductName: !^LEGACY.*")

**Look up one record:**
  read("# Customers\nid: 42")

**Insert or replace a record:**
  write("# Orders\nid: 1\nstatus: pending\ntotal: 99.90")

**Create a table:**
  write("#! Products\nid: integer readonly\nname: string\n"
        "price: float optional")

**Delete a record:**
  delete("#- Orders\nid: 1")

**Drop a table:**
  delete("#! OldTable")

## Pagination

IMPORTANT: Always use pagination when querying tables that may contain many
rows. Without pagination, large result sets will exceed your context window.

Use frontmatter fields before the #? heading to control pagination:

  read("page-size: 50\npage: 1\n\n#? Orders")

The response carries pagination metadata as frontmatter —
before the root heading:

  total: 830
  page: 1
  pages: 17
  page-size: 50

  # Orders
  ## data[]
  - OrderID: 10248
    ...

Use `total` and `pages` to determine whether to fetch more pages.

**Count only** (no rows returned):
  read("count: true\n\n#? Orders")

Returns: `count: 830\n\n# Orders`

**Rule of thumb:** Use `page-size: 50` for any table you haven't
inspected before.
For tables with fewer than ~20 rows (e.g. Categories, Shippers) pagination is
optional.

## Field projection

Use `select:` frontmatter to return only specific columns — reduces response
size and keeps context windows clean.

  select: OrderID, EmployeeID

  #? Orders

Works with `#` (data) and `#?` (query) documents, including aggregation
(where `select:` filters the result columns after the GROUP BY).

## Joins

Use `join:` frontmatter to query across multiple tables in one call.
The value is `<TableName> on <JoinColumn>` (INNER JOIN, equi-join).

  join: Order Details on OrderID
  sum: UnitPrice * Quantity * (1 - Discount) as revenue
  group: EmployeeID
  sort: revenue desc

  #? Orders

Multiple joins: comma-separated in a single `join:` value.

  join: Order Details on OrderID, Employees on EmployeeID

**Expression syntax in aggregation with joins:**
Use `<expression> as <alias>` to compute derived values:

  sum: UnitPrice * Quantity * (1 - Discount) as revenue

The alias becomes the result column name. Without `as`, the default alias
`<func>_<field>` applies (e.g. `sum_Freight`).

Only column names, numeric literals, and arithmetic operators
(`+`, `-`, `*`, `/`) are allowed in expressions — no subqueries.

## Aggregation

Aggregation is expressed as frontmatter before the #? heading.
QBE filter fields narrow rows *before* aggregation (SQL WHERE).
The `having:` key filters *after* aggregation (SQL HAVING).

| Key               | SQL           | Result column name                  |
| group: f1, f2     | GROUP BY      | (grouping keys pass through)        |
| sum: field        | SUM(field)    | sum_field                           |
| avg: field        | AVG(field)    | avg_field                           |
| min: field        | MIN(field)    | min_field                           |
| max: field        | MAX(field)    | max_field                           |
| count             | COUNT(*)      | count                               |

Multiple fields per function: `sum: Freight, Total`
→ `sum_Freight`, `sum_Total`.

  sort: sum_revenue desc, EmployeeID asc    → ORDER BY (multiple columns, mixed)
  having: count > 5                         → HAVING COUNT(*) > 5
  having: sum_Freight > 1000, count > 2     → HAVING ... AND ... (comma = AND)

`having:` supports: >, >=, <, <=, =
`sort:` references any result column — grouping keys or aggregate aliases.
`page-size:` and `page:` apply to the aggregated result set.

**Example — top 3 employees by revenue:**
  read("group: EmployeeID\nsum: revenue\nsort: sum_revenue desc\n"
       "page-size: 3\n\n#? OrderDetails")

## Error handling

All tools return a `# Error` document on failure:

  # Error
  status: 400
  code: not_found
  message: No records found in Orders

Check the `code` field to decide how to proceed.
"""

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
    """Open a SQLite database file or show current database info.

    Data document (# Database) with a path field opens the database
    and makes it the active connection.  If the file does not exist,
    a new empty database is created.  The previous database is closed
    automatically.

        # Database
        path: /path/to/mydb.db

    Data document (# Database) without fields returns information
    about the currently active database.

        # Database
    """
    try:
        parsed: Any = jmd_to_dict(document)
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
            return _db_status()

        db_path = _resolve_db_path(str(path_value))
        return _open_db(db_path)
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
    """Read records or table schema using a JMD document.

    Data document (# Label): look up records by exact field values.
    Returns a single record if exactly one matches, a list otherwise.

        # Order
        id: 42

    Query-by-Example document (#? Label): filter records by field values.
    Omitted fields match any value. Always returns a list.

        #? Order
        status: pending

    Schema document (#! Label): describe the table structure.
    Returns a #! document with column names, types, and modifiers.

        #! Order
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
    """Write a record or define a table schema using a JMD document.

    Data document (# Label): insert or replace a record.
    If a record with the same primary key exists, it is replaced.
    Returns the written record as confirmed by the database.

        # Order
        id: 42
        status: shipped
        total: 149.99

    Schema document (#! Label): create or extend a table.
    If the table does not exist, it is created. If it exists, new
    columns are added. Existing columns are never modified or removed.

        #! Order
        id: integer readonly
        status: string
        total: float optional
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
    """Delete records or drop a table using a JMD document.

    Delete document (#- Label): delete matching records.
    All fields act as filters. At least one field is required.
    Returns the number of deleted records.

        #- Order
        id: 42

    Schema document (#! Label): drop the entire table.

        #! Order
    """
    try:
        return _t().delete(document)
    except Exception as e:
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
