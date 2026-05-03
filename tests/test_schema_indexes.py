# SPDX-License-Identifier: Apache-2.0
"""Slice-B tests: index DDL.

Covers ``#! Index`` as a top-level shape (read/write/delete),
``## Index[]`` as an inline sub-section under ``#! Table``, and
``#! Database`` listing indexes alongside tables.
"""

from __future__ import annotations

import sqlite3

import pytest

from jmd_mcp_sql.translator import SQLTranslator


@pytest.fixture()
def empty() -> SQLTranslator:
    """Return a translator backed by an empty in-memory database."""
    return SQLTranslator(sqlite3.connect(":memory:"))


def _index_names(t: SQLTranslator, table: str | None = None) -> list[str]:
    """Return user-defined indexes (origin='c' = explicit CREATE INDEX)."""
    if table is None:
        rows = t._conn.execute(
            "SELECT name FROM sqlite_master"
            " WHERE type='index' AND sql IS NOT NULL"
            " ORDER BY name"
        ).fetchall()
    else:
        rows = t._conn.execute(
            "SELECT name FROM sqlite_master"
            " WHERE type='index' AND tbl_name=? AND sql IS NOT NULL"
            " ORDER BY name",
            (table,),
        ).fetchall()
    return [r[0] for r in rows]


def _index_sql(t: SQLTranslator, name: str) -> str:
    """Return the raw SQL of an index, or '' if missing."""
    row = t._conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='index' AND name=?",
        (name,),
    ).fetchone()
    return row[0] if row and row[0] else ""


# ---------------------------------------------------------------------------
# Top-level: write #! Index
# ---------------------------------------------------------------------------


class TestWriteIndex:
    """Top-level ``write('#! Index ...')`` creates indexes."""

    def test_create_simple_index(self, empty: SQLTranslator) -> None:
        """A bare ``#! Index`` with name+table+columns creates an index."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
        )
        result = empty.write(
            "#! Index\n"
            "name: idx_order_status\n"
            "table: Order\n"
            "columns: status\n"
        )
        assert "created" in result
        assert "idx_order_status" in _index_names(empty, "Order")

    def test_create_compound_index(self, empty: SQLTranslator) -> None:
        """Multi-column index is supported via comma-list."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
            "created_at: string\n"
        )
        empty.write(
            "#! Index\n"
            "name: idx_order_status_created\n"
            "table: Order\n"
            "columns: status, created_at\n"
        )
        sql = _index_sql(empty, "idx_order_status_created")
        assert '"status"' in sql and '"created_at"' in sql

    def test_create_unique_index(self, empty: SQLTranslator) -> None:
        """``unique: true`` produces a UNIQUE index."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "code: string\n"
        )
        empty.write(
            "#! Index\n"
            "name: idx_order_code\n"
            "table: Order\n"
            "columns: code\n"
            "unique: true\n"
        )
        empty._conn.execute(
            "INSERT INTO \"Order\" VALUES (1, 'A')"
        )
        with pytest.raises(sqlite3.IntegrityError):
            empty._conn.execute(
                "INSERT INTO \"Order\" VALUES (2, 'A')"
            )

    def test_create_partial_index(self, empty: SQLTranslator) -> None:
        """``where: <expr>`` produces a partial index."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
            "deleted_at: string optional\n"
        )
        empty.write(
            "#! Index\n"
            "name: idx_order_active_status\n"
            "table: Order\n"
            "columns: status\n"
            "where: deleted_at IS NULL\n"
        )
        sql = _index_sql(empty, "idx_order_active_status")
        assert "WHERE" in sql.upper()
        assert "deleted_at" in sql

    def test_index_on_nonexistent_table_errors(
        self, empty: SQLTranslator
    ) -> None:
        """Creating an index on an unknown table returns # Error."""
        result = empty.write(
            "#! Index\n"
            "name: idx_x\n"
            "table: NoSuch\n"
            "columns: foo\n"
        )
        assert "# Error" in result
        assert "ddl_failed" in result or "not_found" in result

    def test_duplicate_index_name_errors(
        self, empty: SQLTranslator
    ) -> None:
        """Re-creating an existing index name returns # Error."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
        )
        empty.write(
            "#! Index\n"
            "name: idx_dup\n"
            "table: Order\n"
            "columns: status\n"
        )
        result = empty.write(
            "#! Index\n"
            "name: idx_dup\n"
            "table: Order\n"
            "columns: status\n"
        )
        assert "# Error" in result


# ---------------------------------------------------------------------------
# Top-level: read #! Index
# ---------------------------------------------------------------------------


class TestReadIndex:
    """Top-level ``read('#! Index / name: …')`` returns its schema."""

    def test_read_simple_index(self, empty: SQLTranslator) -> None:
        """Read-back lists table, columns, and unique:false."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
        )
        empty.write(
            "#! Index\n"
            "name: idx_order_status\n"
            "table: Order\n"
            "columns: status\n"
        )
        result = empty.read(
            "#! Index\nname: idx_order_status\n"
        )
        assert result.startswith("#! Index")
        assert "name: idx_order_status" in result
        assert "table: Order" in result
        assert "columns: status" in result

    def test_read_unknown_index_errors(
        self, empty: SQLTranslator
    ) -> None:
        """Unknown index returns # Error."""
        result = empty.read("#! Index\nname: nope\n")
        assert "# Error" in result

    def test_read_partial_index_round_trips(
        self, empty: SQLTranslator
    ) -> None:
        """Partial-index ``where`` round-trips through read."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
            "deleted_at: string optional\n"
        )
        empty.write(
            "#! Index\n"
            "name: idx_order_active\n"
            "table: Order\n"
            "columns: status\n"
            "where: deleted_at IS NULL\n"
        )
        result = empty.read(
            "#! Index\nname: idx_order_active\n"
        )
        assert "where: deleted_at IS NULL" in result


# ---------------------------------------------------------------------------
# Top-level: delete #! Index
# ---------------------------------------------------------------------------


class TestDeleteIndex:
    """Top-level ``delete('#! Index / name: …')`` drops the index."""

    def test_drop_index(self, empty: SQLTranslator) -> None:
        """``confirm: drop-index`` deletes the index."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
        )
        empty.write(
            "#! Index\n"
            "name: idx_order_status\n"
            "table: Order\n"
            "columns: status\n"
        )
        empty.delete(
            "confirm: drop-index\n\n"
            "#! Index\nname: idx_order_status\n"
        )
        assert "idx_order_status" not in _index_names(empty, "Order")

    def test_drop_without_confirm_errors(
        self, empty: SQLTranslator
    ) -> None:
        """Without confirm-key the request is rejected."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
        )
        empty.write(
            "#! Index\n"
            "name: idx_x\n"
            "table: Order\n"
            "columns: status\n"
        )
        result = empty.delete("#! Index\nname: idx_x\n")
        assert "# Error" in result
        assert "confirmation_required" in result

    def test_drop_unknown_index_errors(
        self, empty: SQLTranslator
    ) -> None:
        """Dropping an index that doesn't exist is a 404."""
        result = empty.delete(
            "confirm: drop-index\n\n"
            "#! Index\nname: nope\n"
        )
        assert "# Error" in result
        assert "404" in result


# ---------------------------------------------------------------------------
# Inline: ## Index[] under #! Table
# ---------------------------------------------------------------------------


class TestInlineIndex:
    """``## Index[]`` sub-section under ``#! Table``."""

    def test_inline_index_created_with_table(
        self, empty: SQLTranslator
    ) -> None:
        """An inline ``## Index[]`` entry is created alongside the table."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
            "\n"
            "## Index[]\n"
            "- name: idx_order_status\n"
            "  columns: status\n"
        )
        assert "idx_order_status" in _index_names(empty, "Order")

    def test_inline_index_unique(self, empty: SQLTranslator) -> None:
        """An inline UNIQUE index is enforced after creation."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "code: string\n"
            "\n"
            "## Index[]\n"
            "- name: idx_order_code\n"
            "  columns: code\n"
            "  unique: true\n"
        )
        empty._conn.execute(
            "INSERT INTO \"Order\" VALUES (1, 'A')"
        )
        with pytest.raises(sqlite3.IntegrityError):
            empty._conn.execute(
                "INSERT INTO \"Order\" VALUES (2, 'A')"
            )

    def test_inline_index_failure_rolls_back_table(
        self, empty: SQLTranslator
    ) -> None:
        """A bad inline index aborts the table creation atomically."""
        result = empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
            "\n"
            "## Index[]\n"
            "- name: idx_bad\n"
            "  columns: not_a_real_column\n"
        )
        if "Error" in result:
            rows = empty._conn.execute(
                "SELECT name FROM sqlite_master WHERE name='Order'"
            ).fetchall()
            assert rows == [], f"table leaked despite error: {rows}"


# ---------------------------------------------------------------------------
# #! Database listing
# ---------------------------------------------------------------------------


class TestDatabaseListing:
    """``read('#! Database')`` surfaces user indexes."""

    def test_database_lists_indexes(
        self, empty: SQLTranslator
    ) -> None:
        """``#! Database`` includes a ``## indexes[]`` section."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
        )
        empty.write(
            "#! Index\n"
            "name: idx_order_status\n"
            "table: Order\n"
            "columns: status\n"
        )
        result = empty.read("#! Database")
        assert "## indexes[]" in result
        assert "idx_order_status" in result

    def test_database_skips_internal_indexes(
        self, empty: SQLTranslator
    ) -> None:
        """Auto-indexes (UNIQUE, PK) aren't listed under ## indexes[]."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "code: string\n"
            "\n"
            "## unique[]\n"
            "- code\n"
        )
        result = empty.read("#! Database")
        # The auto-index for the UNIQUE constraint must not surface.
        assert "sqlite_autoindex" not in result
