"""JMD MCP server for SQLite."""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from .translator import SQLTranslator

mcp = FastMCP("jmd-mcp-sql")
_translator: SQLTranslator | None = None


def _t() -> SQLTranslator:
    if _translator is None:
        raise RuntimeError("Server not initialized — call main() first")
    return _translator


@mcp.tool()
def query(document: str) -> str:
    """Query records using a JMD query document (#? Label).

    The document uses Query-by-Example syntax: field values act as filters.
    Omitted fields match any value. Returns all matching records.

    Example:
        #? Order
        status: pending
    """
    try:
        return _t().query(document)
    except Exception as e:
        return f"# Error\nstatus: 400\ncode: query_failed\nmessage: {e}"


@mcp.tool()
def read(document: str) -> str:
    """Read one or more records using a JMD data document (# Label).

    Provide identifying fields to look up specific records.
    Returns a single record or a list if multiple match.

    Example:
        # Order
        id: 42
    """
    try:
        return _t().read(document)
    except Exception as e:
        return f"# Error\nstatus: 400\ncode: read_failed\nmessage: {e}"


@mcp.tool()
def write(document: str) -> str:
    """Insert or update a record using a JMD data document (# Label).

    If a record with the same primary key exists, it is replaced.
    Returns the written record as confirmed by the database.

    Example:
        # Order
        id: 42
        status: shipped
        total: 149.99
    """
    try:
        return _t().write(document)
    except Exception as e:
        return f"# Error\nstatus: 400\ncode: write_failed\nmessage: {e}"


@mcp.tool()
def delete(document: str) -> str:
    """Delete records using a JMD delete document (#- Label).

    All fields in the document act as filters. At least one field is required.
    Returns the number of deleted records.

    Example:
        #- Order
        id: 42
    """
    try:
        return _t().delete(document)
    except Exception as e:
        return f"# Error\nstatus: 400\ncode: delete_failed\nmessage: {e}"


def main() -> None:
    parser = argparse.ArgumentParser(description="JMD MCP server for SQLite")
    parser.add_argument("db", nargs="?", default=None,
                        help="Path to SQLite database file (default: Northwind demo)")
    args = parser.parse_args()

    if args.db:
        db_path = Path(args.db)
        if not db_path.exists():
            raise SystemExit(f"Database not found: {db_path}")
    else:
        db_path = Path(__file__).parent / "northwind.db"
        if not db_path.exists():
            raise SystemExit(
                "Northwind demo database not found. "
                "Run: python -m jmd_mcp_sql.install_northwind"
            )

    global _translator
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    _translator = SQLTranslator(conn)

    mcp.run()


if __name__ == "__main__":
    main()
