"""JMD MCP server for SQLite.

This module wires together the MCP framework (FastMCP), the SQL translator,
and the database connection.  It exposes three tools that an LLM can call:

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

from mcp.server.fastmcp import FastMCP

from .translator import SQLTranslator

_INSTRUCTIONS = """
This server exposes a SQLite database through three tools — read, write, delete —
using JMD (JSON Markdown) as the data format.

## JMD document syntax

Every document starts with a heading line that sets the document type and table name,
followed by key: value pairs (one per line):

  # Product          → data document   (exact lookup / insert-or-replace)
  #? Product         → query document  (filter / list)
  #! Product         → schema document (describe / create / drop table)
  #- Product         → delete document (delete matching records)

  key: value         → string, integer, or float — inferred automatically
  key: true/false    → boolean

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
  write("#! Products\nid: integer readonly\nname: string\nprice: float optional")

**Delete a record:**
  delete("#- Orders\nid: 1")

**Drop a table:**
  delete("#! OldTable")

## Pagination

IMPORTANT: Always use pagination when querying tables that may contain many
rows. Without pagination, large result sets will exceed your context window.

Use frontmatter fields before the #? heading to control pagination:

  read("size: 50\npage: 1\n\n#? Orders")

The response wraps results in a root object with metadata:

  # Orders
  total: 830
  page: 1
  pages: 17
  page_size: 50
  ## data[]
  - OrderID: 10248
    ...

Use `total` and `pages` to determine whether to fetch more pages.

**Count only** (no rows returned):
  read("count: true\n\n#? Orders")

Returns: `# Orders\ncount: 830`

**Rule of thumb:** Use `size: 50` for any table you haven't inspected before.
For tables with fewer than ~20 rows (e.g. Categories, Shippers) pagination is
optional.

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


def _t() -> SQLTranslator:
    """Return the active SQLTranslator, raising if the server is not running."""
    if _translator is None:
        raise RuntimeError("Server not initialized — call main() first")
    return _translator


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
        return f"# Error\nstatus: 400\ncode: read_failed\nmessage: {e}"


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
        return f"# Error\nstatus: 400\ncode: write_failed\nmessage: {e}"


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
        return f"# Error\nstatus: 400\ncode: delete_failed\nmessage: {e}"


def main() -> None:
    """Entry point: parse arguments, open the database, and start the server."""
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
                    "northwind.sql is missing from the jmd_mcp_sql/ package directory."
                )
            conn = sqlite3.connect(str(db_path))
            conn.executescript(sql_path.read_text(encoding="utf-8"))
            conn.close()

    global _translator
    # check_same_thread=False is safe here because FastMCP processes one
    # request at a time over stdio; there is no concurrent access.
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    _translator = SQLTranslator(conn)

    mcp.run()


if __name__ == "__main__":
    main()
