# SPDX-License-Identifier: Apache-2.0
"""Foundational SQL helpers shared across modules.

Tiny module — only the bits that both ``translator.py`` and the
DDL/query helpers need: identifier quoting and the SQLite ⇄ JMD
type vocabulary. Kept separate from larger helper modules to
break what would otherwise be an import cycle.
"""
from __future__ import annotations


def _quote_identifier(name: str) -> str:
    """Wrap a SQL identifier in double-quotes, escaping internal quotes.

    SQLite allows any character in identifiers when double-quoted.
    The Northwind schema uses names like ``"Order Details"`` (with a
    space) that would otherwise be invalid bare identifiers.
    """
    return '"' + name.replace('"', '""') + '"'


def _sqlite_type_to_jmd(sqlite_type: str) -> str:
    """Map a SQLite declared column type to the nearest JMD schema type.

    SQLite uses type affinity rules (§3.1 of the SQLite spec): the
    declared type is a free-form string, not a strict enum.  We match
    substrings to cover common variants such as VARCHAR, NVARCHAR,
    NUMERIC, DECIMAL, etc.

    Args:
        sqlite_type: The declared type string from PRAGMA table_info.

    Returns:
        One of ``"integer"``, ``"float"``, ``"boolean"``, ``"binary"``,
        or ``"string"`` (the JMD fallback for unknown types).
    """
    t = sqlite_type.upper()
    if "INT" in t:
        return "integer"
    if any(s in t for s in ("TEXT", "CHAR", "CLOB")):
        return "string"
    if any(s in t for s in ("REAL", "FLOA", "DOUB", "NUMER", "DECIM")):
        return "float"
    if "BOOL" in t:
        return "boolean"
    if "BLOB" in t:
        return "binary"
    # SQLite's default affinity is NUMERIC, but "string" is the safest
    # JMD representation for unknown or exotic declared types.
    return "string"


# Mapping from JMD schema type names to SQLite column types.
# Used when translating #! schema documents into CREATE TABLE statements.
_JMD_TO_SQLITE: dict[str, str] = {
    "integer": "INTEGER",
    "int": "INTEGER",
    "string": "TEXT",
    "text": "TEXT",
    "float": "REAL",
    "number": "REAL",
    "boolean": "INTEGER",  # SQLite has no native BOOLEAN type
    "bool": "INTEGER",
    "any": "TEXT",
}
