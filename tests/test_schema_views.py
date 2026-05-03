# SPDX-License-Identifier: Apache-2.0
"""Slice-D tests: view DDL.

Covers ``#! View`` as a top-level shape (name + select), drop via
``confirm: drop-view``, and ``#! Database`` listing views.
"""

from __future__ import annotations

import sqlite3

import pytest

from jmd_mcp_sql.translator import SQLTranslator


@pytest.fixture()
def empty() -> SQLTranslator:
    """Return a translator backed by an empty in-memory database."""
    return SQLTranslator(sqlite3.connect(":memory:"))


def _view_names(t: SQLTranslator) -> list[str]:
    """Return user view names from sqlite_master."""
    rows = t._conn.execute(
        "SELECT name FROM sqlite_master"
        " WHERE type='view'"
        " ORDER BY name"
    ).fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# Top-level: write #! View
# ---------------------------------------------------------------------------


class TestWriteView:
    """Top-level ``write('#! View ...')`` creates views."""

    def test_create_view(self, empty: SQLTranslator) -> None:
        """A ``#! View`` doc with name + select creates a view."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
        )
        result = empty.write(
            '#! View\n'
            'name: PendingOrders\n'
            'select: "SELECT id FROM \\"Order\\" WHERE status=\'pending\'"\n'
        )
        assert "created" in result
        assert "PendingOrders" in _view_names(empty)

    def test_view_returns_filtered_rows(
        self, empty: SQLTranslator
    ) -> None:
        """Selecting from the view yields the filtered subset."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
        )
        empty.write(
            '#! View\n'
            'name: PendingOrders\n'
            'select: "SELECT id FROM \\"Order\\" WHERE status=\'pending\'"\n'
        )
        empty._conn.execute(
            "INSERT INTO \"Order\" VALUES (1, 'pending')"
        )
        empty._conn.execute(
            "INSERT INTO \"Order\" VALUES (2, 'shipped')"
        )
        rows = [
            tuple(r) for r in
            empty._conn.execute("SELECT id FROM PendingOrders").fetchall()
        ]
        assert rows == [(1,)]

    def test_missing_name_or_select_errors(
        self, empty: SQLTranslator
    ) -> None:
        """Missing name or select returns # Error."""
        result = empty.write(
            "#! View\nname: foo\n"
        )
        assert "# Error" in result

    def test_duplicate_view_name_errors(
        self, empty: SQLTranslator
    ) -> None:
        """Re-creating an existing view name returns # Error."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
        )
        empty.write(
            '#! View\n'
            'name: V1\n'
            'select: "SELECT id FROM \\"Order\\""\n'
        )
        result = empty.write(
            '#! View\n'
            'name: V1\n'
            'select: "SELECT id FROM \\"Order\\""\n'
        )
        assert "# Error" in result


# ---------------------------------------------------------------------------
# Top-level: read #! View
# ---------------------------------------------------------------------------


class TestReadView:
    """Top-level ``read('#! View / name: …')``."""

    def test_read_view(self, empty: SQLTranslator) -> None:
        """Read-back lists name and select."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
        )
        empty.write(
            '#! View\n'
            'name: PendingOrders\n'
            'select: "SELECT id FROM \\"Order\\" WHERE status=\'pending\'"\n'
        )
        result = empty.read(
            "#! View\nname: PendingOrders\n"
        )
        assert result.startswith("#! View")
        assert "name: PendingOrders" in result
        assert "select:" in result
        assert "pending" in result

    def test_read_unknown_view_errors(
        self, empty: SQLTranslator
    ) -> None:
        """Unknown view returns # Error."""
        result = empty.read("#! View\nname: nope\n")
        assert "# Error" in result


# ---------------------------------------------------------------------------
# Top-level: delete #! View
# ---------------------------------------------------------------------------


class TestDeleteView:
    """Top-level ``delete('#! View / name: …')`` drops the view."""

    def test_drop_view(self, empty: SQLTranslator) -> None:
        """``confirm: drop-view`` drops the view."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
        )
        empty.write(
            '#! View\n'
            'name: V\n'
            'select: "SELECT id FROM \\"Order\\""\n'
        )
        empty.delete(
            "confirm: drop-view\n\n"
            "#! View\nname: V\n"
        )
        assert "V" not in _view_names(empty)

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
            '#! View\n'
            'name: V2\n'
            'select: "SELECT id FROM \\"Order\\""\n'
        )
        result = empty.delete("#! View\nname: V2\n")
        assert "# Error" in result
        assert "confirmation_required" in result

    def test_drop_unknown_view_errors(
        self, empty: SQLTranslator
    ) -> None:
        """Dropping a non-existent view is a 404."""
        result = empty.delete(
            "confirm: drop-view\n\n"
            "#! View\nname: nope\n"
        )
        assert "# Error" in result
        assert "404" in result


# ---------------------------------------------------------------------------
# #! Database listing
# ---------------------------------------------------------------------------


class TestDatabaseListing:
    """``read('#! Database')`` surfaces user views."""

    def test_database_lists_views(self, empty: SQLTranslator) -> None:
        """``#! Database`` includes a ``## views[]`` section."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
        )
        empty.write(
            '#! View\n'
            'name: V_ListMe\n'
            'select: "SELECT id FROM \\"Order\\""\n'
        )
        result = empty.read("#! Database")
        assert "## views[]" in result
        assert "V_ListMe" in result
