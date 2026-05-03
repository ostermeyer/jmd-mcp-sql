# SPDX-License-Identifier: Apache-2.0
"""Slice-F tests: ``action: rebuild`` for non-additive schema changes.

Without ``action: rebuild``, ALTER on an existing table only adds
columns. With it, the translator runs the SQLite table-rebuild
dance: create a staging table with the new schema, copy data,
drop the old table, rename. Atomic; on any failure the original
table survives intact.
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
    """Return the CREATE statement of a table."""
    row = t._conn.execute(
        "SELECT sql FROM sqlite_master"
        " WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row[0] if row else ""


# ---------------------------------------------------------------------------
# Default behaviour: constraint changes still skipped without action: rebuild
# ---------------------------------------------------------------------------


class TestWithoutAction:
    """Constraint changes still skip when ``action: rebuild`` is absent."""

    def test_check_change_skipped(self, empty: SQLTranslator) -> None:
        """A new ## check[] entry on an existing table is ignored."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "total: float\n"
        )
        result = empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "total: float\n"
            "\n"
            "## check[]\n"
            "- total > 0\n"
        )
        assert "constraint-changes-skipped" in result
        # Negative total still allowed.
        empty._conn.execute(
            "INSERT INTO \"Order\" VALUES (1, -5.0)"
        )


# ---------------------------------------------------------------------------
# action: rebuild — additive
# ---------------------------------------------------------------------------


class TestRebuildAdditive:
    """``action: rebuild`` lets us add constraints, preserving data."""

    def test_add_check_constraint(self, empty: SQLTranslator) -> None:
        """A CHECK can be added to an existing table via rebuild."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "total: float\n"
        )
        empty._conn.execute(
            "INSERT INTO \"Order\" VALUES (1, 10.0)"
        )
        empty.write(
            "action: rebuild\n"
            "\n"
            "#! Order\n"
            "id: integer readonly\n"
            "total: float\n"
            "\n"
            "## check[]\n"
            "- total > 0\n"
        )
        # Existing data preserved.
        rows = [
            tuple(r) for r in empty._conn.execute(
                "SELECT id, total FROM \"Order\""
            ).fetchall()
        ]
        assert rows == [(1, 10.0)]
        # New CHECK now enforced.
        with pytest.raises(sqlite3.IntegrityError):
            empty._conn.execute(
                "INSERT INTO \"Order\" VALUES (2, -1.0)"
            )

    def test_add_composite_pk(self, empty: SQLTranslator) -> None:
        """A composite PK can replace a single-column PK."""
        empty.write(
            "#! Membership\n"
            "id: integer readonly\n"
            "user_id: integer\n"
            "group_id: integer\n"
        )
        empty._conn.execute(
            "INSERT INTO Membership VALUES (1, 7, 8)"
        )
        empty.write(
            "action: rebuild\n"
            "\n"
            "#! Membership\n"
            "user_id: integer\n"
            "group_id: integer\n"
            "\n"
            "## primary-key[]\n"
            "- user_id\n"
            "- group_id\n"
        )
        rows = [
            tuple(r) for r in empty._conn.execute(
                "SELECT user_id, group_id FROM Membership"
            ).fetchall()
        ]
        assert rows == [(7, 8)]
        with pytest.raises(sqlite3.IntegrityError):
            empty._conn.execute(
                "INSERT INTO Membership VALUES (7, 8)"
            )


# ---------------------------------------------------------------------------
# action: rebuild — column changes
# ---------------------------------------------------------------------------


class TestRebuildColumns:
    """Column add / drop via rebuild."""

    def test_drop_column(self, empty: SQLTranslator) -> None:
        """A column missing from the new schema is dropped."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
            "note: string optional\n"
        )
        empty._conn.execute(
            "INSERT INTO \"Order\"(id, status, note)"
            " VALUES (1, 'pending', 'gone')"
        )
        empty.write(
            "action: rebuild\n"
            "\n"
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
        )
        ddl = _ddl(empty, "Order")
        assert "note" not in ddl
        rows = [
            tuple(r) for r in empty._conn.execute(
                "SELECT id, status FROM \"Order\""
            ).fetchall()
        ]
        assert rows == [(1, "pending")]

    def test_add_column_with_default(
        self, empty: SQLTranslator
    ) -> None:
        """A new column with a default is filled for existing rows."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
        )
        empty._conn.execute(
            "INSERT INTO \"Order\" VALUES (1, 'pending')"
        )
        empty.write(
            "action: rebuild\n"
            "\n"
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
            "qty: integer = 0\n"
        )
        rows = [
            tuple(r) for r in empty._conn.execute(
                "SELECT id, status, qty FROM \"Order\""
            ).fetchall()
        ]
        assert rows == [(1, "pending", 0)]


# ---------------------------------------------------------------------------
# Atomicity
# ---------------------------------------------------------------------------


class TestRebuildAtomic:
    """A failed rebuild leaves the original table untouched."""

    def test_failed_rebuild_preserves_table(
        self, empty: SQLTranslator
    ) -> None:
        """A bogus CHECK aborts; original data and DDL survive."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "total: float\n"
        )
        empty._conn.execute(
            "INSERT INTO \"Order\" VALUES (1, 10.0)"
        )
        original_ddl = _ddl(empty, "Order")
        result = empty.write(
            "action: rebuild\n"
            "\n"
            "#! Order\n"
            "id: integer readonly\n"
            "total: float\n"
            "\n"
            "## check[]\n"
            "- this @@ is not @@ valid\n"
        )
        assert "Error" in result
        assert _ddl(empty, "Order") == original_ddl
        rows = [
            tuple(r) for r in empty._conn.execute(
                "SELECT id, total FROM \"Order\""
            ).fetchall()
        ]
        assert rows == [(1, 10.0)]

    def test_check_violation_in_existing_data_aborts(
        self, empty: SQLTranslator
    ) -> None:
        """If existing rows would violate the new CHECK, rebuild aborts."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "total: float\n"
        )
        empty._conn.execute(
            "INSERT INTO \"Order\" VALUES (1, -5.0)"
        )
        result = empty.write(
            "action: rebuild\n"
            "\n"
            "#! Order\n"
            "id: integer readonly\n"
            "total: float\n"
            "\n"
            "## check[]\n"
            "- total > 0\n"
        )
        assert "Error" in result
        # Old row preserved.
        rows = [
            tuple(r) for r in empty._conn.execute(
                "SELECT total FROM \"Order\""
            ).fetchall()
        ]
        assert rows == [(-5.0,)]
