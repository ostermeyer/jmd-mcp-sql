# SPDX-License-Identifier: Apache-2.0
"""Slice-A tests: full Table schema DDL.

Covers defaults, enum-as-check, and table-level constraints
(primary-key, unique, check, references).
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
    """Return the raw SQL CREATE statement of a table."""
    row = t._conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    assert row is not None, f"table {name!r} not found"
    return row[0]


# ---------------------------------------------------------------------------
# Column-level: DEFAULT
# ---------------------------------------------------------------------------


class TestDefaults:
    """Column-level default values (= …)."""

    def test_integer_default_renders_in_ddl(
        self, empty: SQLTranslator
    ) -> None:
        """Integer column default appears as DEFAULT N in DDL."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "qty: integer = 0\n"
        )
        ddl = _ddl(empty, "Order")
        assert "DEFAULT 0" in ddl

    def test_string_default_renders_quoted(
        self, empty: SQLTranslator
    ) -> None:
        """String column default appears as DEFAULT 'val' (single-quoted)."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string = pending\n"
        )
        ddl = _ddl(empty, "Order")
        assert "DEFAULT 'pending'" in ddl

    def test_default_round_trips_via_read(
        self, empty: SQLTranslator
    ) -> None:
        """Default value surfaces in #! read-back as ``= value``."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "qty: integer = 0\n"
        )
        schema = empty.read("#! Order")
        assert "qty: integer = 0" in schema

    def test_default_applies_on_insert(
        self, empty: SQLTranslator
    ) -> None:
        """Insert without the column gets the declared default."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "qty: integer = 7\n"
        )
        empty.write("# Order\nid: 1")
        result = empty.read("# Order\nid: 1")
        assert "qty: 7" in result


# ---------------------------------------------------------------------------
# Column-level: ENUM-as-CHECK
# ---------------------------------------------------------------------------


class TestEnumAsCheck:
    """`status: pending|shipped|cancelled` → CHECK (status IN (...))."""

    def test_enum_renders_check_constraint(
        self, empty: SQLTranslator
    ) -> None:
        """Enum form ``a|b|c`` becomes a CHECK (col IN (...))."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: pending|shipped|cancelled\n"
        )
        ddl = _ddl(empty, "Order")
        assert "CHECK" in ddl.upper()
        assert "'pending'" in ddl
        assert "'shipped'" in ddl
        assert "'cancelled'" in ddl

    def test_enum_rejects_invalid_value(
        self, empty: SQLTranslator
    ) -> None:
        """Inserting a value outside the enum trips the CHECK."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: pending|shipped\n"
        )
        with pytest.raises(sqlite3.IntegrityError):
            empty._conn.execute(
                "INSERT INTO \"Order\"(id, status) VALUES (1, 'bogus')"
            )

    def test_enum_round_trips_via_read(
        self, empty: SQLTranslator
    ) -> None:
        """Read-back recovers the ``a|b|c`` enum form."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: pending|shipped|cancelled\n"
        )
        schema = empty.read("#! Order")
        assert "status: pending|shipped|cancelled" in schema


# ---------------------------------------------------------------------------
# Table-level: ## primary-key[]
# ---------------------------------------------------------------------------


class TestCompositePK:
    """Composite PRIMARY KEY via ``## primary-key[]`` sub-section."""

    def test_composite_pk(self, empty: SQLTranslator) -> None:
        """Multi-column ## primary-key[] renders as table-level PK."""
        empty.write(
            "#! Membership\n"
            "user_id: integer\n"
            "group_id: integer\n"
            "joined_at: string\n"
            "## primary-key[]\n"
            "- user_id\n"
            "- group_id\n"
        )
        ddl = _ddl(empty, "Membership")
        # Composite PK is rendered as a table-level constraint.
        up = ddl.upper()
        assert "PRIMARY KEY" in up
        assert "user_id" in ddl and "group_id" in ddl
        # Both columns mark readonly on read-back.
        schema = empty.read("#! Membership")
        assert "user_id: integer readonly" in schema
        assert "group_id: integer readonly" in schema

    def test_composite_pk_unique_enforced(
        self, empty: SQLTranslator
    ) -> None:
        """Composite PK rejects duplicate combinations."""
        empty.write(
            "#! Membership\n"
            "user_id: integer\n"
            "group_id: integer\n"
            "## primary-key[]\n"
            "- user_id\n"
            "- group_id\n"
        )
        empty._conn.execute(
            "INSERT INTO Membership VALUES (1, 1)"
        )
        with pytest.raises(sqlite3.IntegrityError):
            empty._conn.execute(
                "INSERT INTO Membership VALUES (1, 1)"
            )


# ---------------------------------------------------------------------------
# Table-level: ## unique[]
# ---------------------------------------------------------------------------


class TestUnique:
    """UNIQUE constraints via ``## unique[]`` sub-section."""

    def test_single_column_unique(
        self, empty: SQLTranslator
    ) -> None:
        """Single-column UNIQUE rejects duplicates."""
        empty.write(
            "#! Invoice\n"
            "id: integer readonly\n"
            "number: string\n"
            "## unique[]\n"
            "- number\n"
        )
        empty._conn.execute(
            "INSERT INTO Invoice VALUES (1, 'A-1')"
        )
        with pytest.raises(sqlite3.IntegrityError):
            empty._conn.execute(
                "INSERT INTO Invoice VALUES (2, 'A-1')"
            )

    def test_composite_unique(self, empty: SQLTranslator) -> None:
        """Multi-column UNIQUE rejects duplicate combinations."""
        empty.write(
            "#! Membership\n"
            "id: integer readonly\n"
            "user_id: integer\n"
            "group_id: integer\n"
            "## unique[]\n"
            "- user_id, group_id\n"
        )
        empty._conn.execute(
            "INSERT INTO Membership VALUES (1, 7, 8)"
        )
        with pytest.raises(sqlite3.IntegrityError):
            empty._conn.execute(
                "INSERT INTO Membership VALUES (2, 7, 8)"
            )

    def test_unique_round_trips(self, empty: SQLTranslator) -> None:
        """Read-back surfaces the UNIQUE sub-section."""
        empty.write(
            "#! Membership\n"
            "id: integer readonly\n"
            "user_id: integer\n"
            "group_id: integer\n"
            "## unique[]\n"
            "- user_id, group_id\n"
        )
        schema = empty.read("#! Membership")
        assert "## unique[]" in schema
        assert "user_id, group_id" in schema


# ---------------------------------------------------------------------------
# Table-level: ## check[]
# ---------------------------------------------------------------------------


class TestCheck:
    """CHECK constraints via ``## check[]`` sub-section."""

    def test_check_constraint_enforced(
        self, empty: SQLTranslator
    ) -> None:
        """A table-level CHECK is enforced on insert."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "total: float\n"
            "## check[]\n"
            "- total > 0\n"
        )
        with pytest.raises(sqlite3.IntegrityError):
            empty._conn.execute(
                "INSERT INTO \"Order\" VALUES (1, -1.0)"
            )

    def test_multiple_checks(self, empty: SQLTranslator) -> None:
        """Multiple ## check[] entries each become independent CHECKs."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "total: float\n"
            "qty: integer\n"
            "## check[]\n"
            "- total > 0\n"
            "- qty >= 1\n"
        )
        with pytest.raises(sqlite3.IntegrityError):
            empty._conn.execute(
                "INSERT INTO \"Order\" VALUES (1, 5.0, 0)"
            )

    def test_check_round_trips(self, empty: SQLTranslator) -> None:
        """Read-back surfaces the CHECK sub-section."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "total: float\n"
            "## check[]\n"
            "- total > 0\n"
        )
        schema = empty.read("#! Order")
        assert "## check[]" in schema
        assert "total > 0" in schema


# ---------------------------------------------------------------------------
# Table-level: ## references[]
# ---------------------------------------------------------------------------


class TestReferences:
    """Foreign keys via ``## references[]`` sub-section."""

    def test_single_column_fk(self, empty: SQLTranslator) -> None:
        """Single-column FK renders FOREIGN KEY clause."""
        empty.write(
            "#! Customer\n"
            "id: integer readonly\n"
            "name: string\n"
        )
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "customer_id: integer\n"
            "## references[]\n"
            "- customer_id: Customer.id\n"
        )
        ddl = _ddl(empty, "Order")
        up = ddl.upper()
        assert "FOREIGN KEY" in up
        assert "Customer" in ddl

    def test_fk_enforced(self, empty: SQLTranslator) -> None:
        """FK rejects orphan refs once foreign_keys=ON."""
        empty.write(
            "#! Customer\n"
            "id: integer readonly\n"
            "name: string\n"
        )
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "customer_id: integer\n"
            "## references[]\n"
            "- customer_id: Customer.id\n"
        )
        # SQLite needs PRAGMA foreign_keys=ON for FK enforcement.
        empty._conn.execute("PRAGMA foreign_keys = ON")
        with pytest.raises(sqlite3.IntegrityError):
            empty._conn.execute(
                "INSERT INTO \"Order\" VALUES (1, 999)"
            )

    def test_fk_round_trips(self, empty: SQLTranslator) -> None:
        """Read-back surfaces the references sub-section."""
        empty.write(
            "#! Customer\n"
            "id: integer readonly\n"
            "name: string\n"
        )
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "customer_id: integer\n"
            "## references[]\n"
            "- customer_id: Customer.id\n"
        )
        schema = empty.read("#! Order")
        assert "## references[]" in schema
        assert "customer_id: Customer.id" in schema


# ---------------------------------------------------------------------------
# Atomic transaction
# ---------------------------------------------------------------------------


class TestAtomicity:
    """Atomicity of failed CREATE TABLE attempts."""

    def test_failed_create_does_not_leak(
        self, empty: SQLTranslator
    ) -> None:
        """A bad CHECK expression should not leave a half-built table."""
        result = empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "## check[]\n"
            "- this_is_definitely_not_valid_sql_syntax @@@@@\n"
        )
        # Either rejected with error, or table not created at all.
        rows = empty._conn.execute(
            "SELECT name FROM sqlite_master WHERE name='Order'"
        ).fetchall()
        if "Error" in result or "failed" in result:
            assert rows == [], f"table leaked despite error: {rows}"
