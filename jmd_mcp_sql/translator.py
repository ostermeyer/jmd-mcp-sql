# SPDX-License-Identifier: Apache-2.0
"""Public translator entry point — thin dispatch over the ops modules.

JMD documents are categorised by their root heading prefix:

    # Product          data mode   — exact lookup or INSERT OR REPLACE
    #? Product         query mode  — Query-by-Example filter / list
    #! Product         schema mode — describe / CREATE / ALTER / DROP
    #- Product         delete mode — DELETE WHERE / DROP TABLE

The :class:`SQLTranslator` instance just bundles a SQLite
connection into a :class:`TranslatorContext` and dispatches each
read / write / delete call to the right module:

    * :mod:`_data_ops`    — ``# … / # …[]`` reads, writes, deletes
    * :mod:`_query_ops`   — ``#?`` queries, joins, aggregations
    * :mod:`_schema_ops`  — ``#! …`` schema CRUD across the full
                             SQLite DDL surface (Slices A–F)

That's the whole class. The actual work lives in those modules
as free functions over a TranslatorContext — see the workspace
memory ``Modernes pythonisches Design`` for the principle.
"""
from __future__ import annotations

import sqlite3

from jmd import jmd_mode

from . import _data_ops, _query_ops, _schema_ops
from ._context import TranslatorContext
from ._query_parser import _regexp
from .schema import SchemaInspector


class SQLTranslator:
    r"""Translates JMD documents into SQLite operations and back.

    Each public method corresponds to one MCP tool (read, write,
    delete). The constructor receives an open SQLite connection
    which is reused for the lifetime of the server process; the
    connection is wrapped in a :class:`TranslatorContext` and
    threaded through the per-mode operation modules.

    Example usage::

        conn = sqlite3.connect("mydb.db")
        t = SQLTranslator(conn)
        result = t.read("#? Orders\nShipCountry: Germany")
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Wrap *conn* in a translator with a fresh schema cache.

        Sets ``row_factory`` to :class:`sqlite3.Row` so internal
        ``fetchall`` returns dict-like rows, and registers the
        ``REGEXP`` user-function used by the ``^``-prefixed QBE
        operator.
        """
        conn.row_factory = sqlite3.Row
        conn.create_function("REGEXP", 2, _regexp)
        self._ctx = TranslatorContext(
            conn=conn,
            schema=SchemaInspector(conn),
        )

    @property
    def ctx(self) -> TranslatorContext:
        """Public read-only handle to the underlying context."""
        return self._ctx

    # Back-compat shims: tests and downstream callers from the
    # mixin-base era reach for ``_conn`` / ``_schema`` directly.
    # Forwarding properties keep that working without polluting
    # the dispatch class.
    @property
    def _conn(self) -> sqlite3.Connection:
        return self._ctx.conn

    @property
    def _schema(self) -> SchemaInspector:
        return self._ctx.schema

    def close(self) -> None:
        """Close the underlying database connection."""
        self._ctx.conn.close()

    def read(self, jmd_source: str) -> str:
        """Dispatch to the per-mode read implementation.

        - ``#?`` → :func:`_query_ops._query`
        - ``#!`` → :func:`_schema_ops._read_schema`
        - ``#``  → :func:`_data_ops.read_data`
        """
        mode = jmd_mode(jmd_source)
        if mode == "query":
            return _query_ops._query(self._ctx, jmd_source)
        if mode == "schema":
            return _schema_ops._read_schema(self._ctx, jmd_source)
        return _data_ops.read_data(self._ctx, jmd_source)

    def write(self, jmd_source: str) -> str:
        """Dispatch to the per-mode write implementation.

        - ``#!`` → :func:`_schema_ops._write_schema` (CREATE / ALTER /
          rebuild)
        - ``#``  → :func:`_data_ops.write_data` (INSERT OR REPLACE,
          plus the ``# Label[]`` bulk-insert form)
        """
        if jmd_mode(jmd_source) == "schema":
            return _schema_ops._write_schema(self._ctx, jmd_source)
        return _data_ops.write_data(self._ctx, jmd_source)

    def delete(self, jmd_source: str) -> str:
        """Dispatch to the per-mode delete implementation.

        - ``#!`` → :func:`_schema_ops._delete_schema` (DROP TABLE /
          INDEX / TRIGGER / VIEW)
        - ``#-`` → :func:`_data_ops.delete_data` (DELETE WHERE,
          plus the ``#- Label[]`` bulk-delete form)
        """
        if jmd_mode(jmd_source) == "schema":
            return _schema_ops._delete_schema(self._ctx, jmd_source)
        return _data_ops.delete_data(self._ctx, jmd_source)
