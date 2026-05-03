# SPDX-License-Identifier: Apache-2.0
"""Slice-E tests: virtual-table DDL via ``using:`` modifier.

Covers ``#! Table`` with a ``using: <module>`` field that turns the
declaration into ``CREATE VIRTUAL TABLE`` instead of CREATE TABLE.
The primary target is FTS5; other modules pass through for free.
"""

from __future__ import annotations

import sqlite3

import pytest

from jmd_mcp_sql.translator import SQLTranslator


@pytest.fixture()
def empty() -> SQLTranslator:
    """Return a translator backed by an empty in-memory database."""
    return SQLTranslator(sqlite3.connect(":memory:"))


def _ddl(t: SQLTranslator, name: str) -> str:
    """Return the CREATE statement of a table or virtual table."""
    row = t._conn.execute(
        "SELECT sql FROM sqlite_master"
        " WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row[0] if row else ""


# ---------------------------------------------------------------------------
# Create virtual table
# ---------------------------------------------------------------------------


class TestCreateVirtualTable:
    """`using: <module>` turns CREATE TABLE into CREATE VIRTUAL TABLE."""

    def test_fts5_create(self, empty: SQLTranslator) -> None:
        """A bare FTS5 table is created via ``using: fts5``."""
        result = empty.write(
            "#! Memorys\n"
            "using: fts5\n"
            "title: string\n"
            "body: string\n"
        )
        assert "created" in result
        ddl = _ddl(empty, "Memorys")
        assert "VIRTUAL TABLE" in ddl.upper()
        assert "fts5" in ddl.lower()
        assert "title" in ddl
        assert "body" in ddl

    def test_fts5_insert_and_match(
        self, empty: SQLTranslator
    ) -> None:
        """FTS5 MATCH queries work after insert."""
        empty.write(
            "#! Notes\n"
            "using: fts5\n"
            "title: string\n"
            "body: string\n"
        )
        empty._conn.execute(
            "INSERT INTO Notes(title, body) VALUES"
            " ('hello', 'world wide web')"
        )
        empty._conn.execute(
            "INSERT INTO Notes(title, body) VALUES"
            " ('foo', 'bar')"
        )
        rows = [
            tuple(r) for r in empty._conn.execute(
                "SELECT title FROM Notes WHERE Notes MATCH 'world'"
            ).fetchall()
        ]
        assert rows == [("hello",)]

    def test_unindexed_columns(self, empty: SQLTranslator) -> None:
        """``## unindexed[]`` columns are not searchable via MATCH."""
        empty.write(
            "#! Notes\n"
            "using: fts5\n"
            "title: string\n"
            "body: string\n"
            "memory_id: string\n"
            "\n"
            "## unindexed[]\n"
            "- memory_id\n"
        )
        ddl = _ddl(empty, "Notes")
        assert "UNINDEXED" in ddl.upper()
        assert "memory_id" in ddl

    def test_module_options(self, empty: SQLTranslator) -> None:
        """``## options[]`` entries become module-arg ``key = value``."""
        empty.write(
            "#! Notes\n"
            "using: fts5\n"
            "title: string\n"
            "body: string\n"
            "\n"
            "## options[]\n"
            "- \"tokenize = 'porter unicode61'\"\n"
        )
        ddl = _ddl(empty, "Notes")
        assert "tokenize" in ddl
        assert "porter" in ddl

    def test_missing_using_falls_back_to_normal_table(
        self, empty: SQLTranslator
    ) -> None:
        """Without ``using:`` the table is plain CREATE TABLE."""
        empty.write(
            "#! Plain\n"
            "id: integer readonly\n"
            "name: string\n"
        )
        ddl = _ddl(empty, "Plain")
        assert "VIRTUAL" not in ddl.upper()


# ---------------------------------------------------------------------------
# Read-back of virtual tables
# ---------------------------------------------------------------------------


class TestReadVirtualTable:
    """``read('#! Foo')`` surfaces virtual-table specifics."""

    def test_read_shows_using(self, empty: SQLTranslator) -> None:
        """The ``using:`` field round-trips through read."""
        empty.write(
            "#! Notes\n"
            "using: fts5\n"
            "title: string\n"
            "body: string\n"
        )
        result = empty.read("#! Notes")
        assert "using: fts5" in result

    def test_read_lists_unindexed(self, empty: SQLTranslator) -> None:
        """UNINDEXED columns surface in ``## unindexed[]``."""
        empty.write(
            "#! Notes\n"
            "using: fts5\n"
            "title: string\n"
            "memory_id: string\n"
            "\n"
            "## unindexed[]\n"
            "- memory_id\n"
        )
        result = empty.read("#! Notes")
        assert "## unindexed[]" in result
        assert "- memory_id" in result


# ---------------------------------------------------------------------------
# Drop virtual table
# ---------------------------------------------------------------------------


class TestDropVirtualTable:
    """Virtual tables drop via the regular ``confirm: drop-table`` path."""

    def test_drop(self, empty: SQLTranslator) -> None:
        """A virtual table is dropped like any other table."""
        empty.write(
            "#! Notes\n"
            "using: fts5\n"
            "title: string\n"
            "body: string\n"
        )
        empty.delete(
            "confirm: drop-table\n\n#! Notes"
        )
        rows = empty._conn.execute(
            "SELECT name FROM sqlite_master WHERE name='Notes'"
        ).fetchall()
        assert rows == []
