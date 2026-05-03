# SPDX-License-Identifier: Apache-2.0
"""Slice-C tests: trigger DDL.

Covers ``#! Trigger`` as a top-level shape, ``## Trigger[]`` as an
inline sub-section under ``#! Table``, and ``#! Database`` listing
triggers.
"""

from __future__ import annotations

import sqlite3

import pytest

from jmd_mcp_sql.translator import SQLTranslator


@pytest.fixture()
def empty() -> SQLTranslator:
    """Return a translator backed by an empty in-memory database."""
    return SQLTranslator(sqlite3.connect(":memory:"))


def _trigger_names(t: SQLTranslator) -> list[str]:
    """Return user trigger names from sqlite_master."""
    rows = t._conn.execute(
        "SELECT name FROM sqlite_master"
        " WHERE type='trigger'"
        " ORDER BY name"
    ).fetchall()
    return [r[0] for r in rows]


def _trigger_sql(t: SQLTranslator, name: str) -> str:
    """Return the raw SQL of a trigger or '' if absent."""
    row = t._conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",
        (name,),
    ).fetchone()
    return row[0] if row and row[0] else ""


# ---------------------------------------------------------------------------
# Top-level: write #! Trigger
# ---------------------------------------------------------------------------


class TestWriteTrigger:
    """Top-level ``write('#! Trigger ...')`` creates triggers."""

    def test_create_after_insert_trigger(
        self, empty: SQLTranslator
    ) -> None:
        """A bare AFTER INSERT trigger is created and recorded."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
        )
        empty.write(
            "#! Log\n"
            "id: integer readonly\n"
            "msg: string\n"
        )
        result = empty.write(
            '#! Trigger\n'
            'name: trg_log_inserts\n'
            'table: Order\n'
            'when: AFTER\n'
            'event: INSERT\n'
            'body: "INSERT INTO Log(msg) VALUES (\'inserted\');"\n'
        )
        assert "created" in result
        assert "trg_log_inserts" in _trigger_names(empty)

    def test_trigger_actually_fires(
        self, empty: SQLTranslator
    ) -> None:
        """The trigger executes when its event happens."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
        )
        empty.write(
            "#! Log\n"
            "id: integer readonly\n"
            "msg: string\n"
        )
        empty.write(
            '#! Trigger\n'
            'name: trg_log\n'
            'table: Order\n'
            'when: AFTER\n'
            'event: INSERT\n'
            'body: "INSERT INTO Log(msg) VALUES (\'fired\');"\n'
        )
        empty._conn.execute(
            "INSERT INTO \"Order\" VALUES (1, 'pending')"
        )
        rows = [
            tuple(r) for r in
            empty._conn.execute("SELECT msg FROM Log").fetchall()
        ]
        assert rows == [("fired",)]

    def test_create_with_when_condition(
        self, empty: SQLTranslator
    ) -> None:
        """A trigger with WHEN clause only fires on matching rows."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
        )
        empty.write(
            "#! Log\n"
            "id: integer readonly\n"
            "msg: string\n"
        )
        empty.write(
            '#! Trigger\n'
            'name: trg_pending_only\n'
            'table: Order\n'
            'when: AFTER\n'
            'event: INSERT\n'
            'condition: NEW.status = \'pending\'\n'
            'body: "INSERT INTO Log(msg) VALUES (\'pending\');"\n'
        )
        empty._conn.execute(
            "INSERT INTO \"Order\" VALUES (1, 'shipped')"
        )
        empty._conn.execute(
            "INSERT INTO \"Order\" VALUES (2, 'pending')"
        )
        rows = empty._conn.execute(
            "SELECT COUNT(*) FROM Log"
        ).fetchone()
        assert rows[0] == 1

    def test_update_of_columns(self, empty: SQLTranslator) -> None:
        """``event: UPDATE OF col1, col2`` fires only on those columns."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
            "note: string optional\n"
        )
        empty.write(
            "#! Log\n"
            "id: integer readonly\n"
            "msg: string\n"
        )
        empty.write(
            '#! Trigger\n'
            'name: trg_status_only\n'
            'table: Order\n'
            'when: AFTER\n'
            'event: UPDATE OF status\n'
            'body: "INSERT INTO Log(msg) VALUES (\'status\');"\n'
        )
        empty._conn.execute(
            "INSERT INTO \"Order\"(id, status, note)"
            " VALUES (1, 'pending', 'foo')"
        )
        empty._conn.execute(
            "UPDATE \"Order\" SET note='bar' WHERE id=1"
        )
        # No trigger fire expected.
        c1 = empty._conn.execute(
            "SELECT COUNT(*) FROM Log"
        ).fetchone()[0]
        empty._conn.execute(
            "UPDATE \"Order\" SET status='shipped' WHERE id=1"
        )
        c2 = empty._conn.execute(
            "SELECT COUNT(*) FROM Log"
        ).fetchone()[0]
        assert c1 == 0
        assert c2 == 1

    def test_create_before_delete(self, empty: SQLTranslator) -> None:
        """BEFORE DELETE trigger sees OLD row before deletion."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
        )
        empty.write(
            "#! Archive\n"
            "id: integer readonly\n"
            "status: string\n"
        )
        empty.write(
            '#! Trigger\n'
            'name: trg_archive\n'
            'table: Order\n'
            'when: BEFORE\n'
            'event: DELETE\n'
            'body: "INSERT INTO Archive(id, status)'
            ' VALUES (OLD.id, OLD.status);"\n'
        )
        empty._conn.execute(
            "INSERT INTO \"Order\" VALUES (1, 'cancelled')"
        )
        empty._conn.execute(
            "DELETE FROM \"Order\" WHERE id=1"
        )
        rows = [
            tuple(r) for r in empty._conn.execute(
                "SELECT id, status FROM Archive"
            ).fetchall()
        ]
        assert rows == [(1, "cancelled")]

    def test_missing_required_field_errors(
        self, empty: SQLTranslator
    ) -> None:
        """Missing name/table/when/event/body returns # Error."""
        result = empty.write(
            "#! Trigger\n"
            "name: trg_x\n"
            "table: NoSuch\n"
        )
        assert "# Error" in result


# ---------------------------------------------------------------------------
# Top-level: read #! Trigger
# ---------------------------------------------------------------------------


class TestReadTrigger:
    """Top-level ``read('#! Trigger / name: …')`` returns its schema."""

    def test_read_simple_trigger(self, empty: SQLTranslator) -> None:
        """Read-back lists name, table, when, event, body."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
        )
        empty.write(
            "#! Log\n"
            "id: integer readonly\n"
            "msg: string\n"
        )
        empty.write(
            '#! Trigger\n'
            'name: trg_log\n'
            'table: Order\n'
            'when: AFTER\n'
            'event: INSERT\n'
            'body: "INSERT INTO Log(msg) VALUES (\'x\');"\n'
        )
        result = empty.read(
            "#! Trigger\nname: trg_log\n"
        )
        assert result.startswith("#! Trigger")
        assert "name: trg_log" in result
        assert "table: Order" in result
        assert "when: AFTER" in result
        assert "event: INSERT" in result
        assert "body:" in result

    def test_read_unknown_trigger_errors(
        self, empty: SQLTranslator
    ) -> None:
        """Unknown trigger returns # Error."""
        result = empty.read("#! Trigger\nname: nope\n")
        assert "# Error" in result

    def test_read_includes_when_condition(
        self, empty: SQLTranslator
    ) -> None:
        """``condition:`` round-trips through read."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
        )
        empty.write(
            "#! Log\n"
            "id: integer readonly\n"
            "msg: string\n"
        )
        empty.write(
            '#! Trigger\n'
            'name: trg_cond\n'
            'table: Order\n'
            'when: AFTER\n'
            'event: INSERT\n'
            'condition: NEW.status = \'x\'\n'
            'body: "INSERT INTO Log(msg) VALUES (\'y\');"\n'
        )
        result = empty.read(
            "#! Trigger\nname: trg_cond\n"
        )
        assert "condition: NEW.status = 'x'" in result


# ---------------------------------------------------------------------------
# Top-level: delete #! Trigger
# ---------------------------------------------------------------------------


class TestDeleteTrigger:
    """Top-level ``delete('#! Trigger / name: …')`` drops the trigger."""

    def test_drop_trigger(self, empty: SQLTranslator) -> None:
        """``confirm: drop-trigger`` drops the trigger."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
        )
        empty.write(
            "#! Log\n"
            "id: integer readonly\n"
            "msg: string\n"
        )
        empty.write(
            '#! Trigger\n'
            'name: trg_x\n'
            'table: Order\n'
            'when: AFTER\n'
            'event: INSERT\n'
            'body: "INSERT INTO Log(msg) VALUES (\'x\');"\n'
        )
        empty.delete(
            "confirm: drop-trigger\n\n"
            "#! Trigger\nname: trg_x\n"
        )
        assert "trg_x" not in _trigger_names(empty)

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
            "#! Log\n"
            "id: integer readonly\n"
            "msg: string\n"
        )
        empty.write(
            '#! Trigger\n'
            'name: trg_y\n'
            'table: Order\n'
            'when: AFTER\n'
            'event: INSERT\n'
            'body: "INSERT INTO Log(msg) VALUES (\'y\');"\n'
        )
        result = empty.delete("#! Trigger\nname: trg_y\n")
        assert "# Error" in result
        assert "confirmation_required" in result

    def test_drop_unknown_trigger_errors(
        self, empty: SQLTranslator
    ) -> None:
        """Dropping a non-existent trigger is a 404."""
        result = empty.delete(
            "confirm: drop-trigger\n\n"
            "#! Trigger\nname: nope\n"
        )
        assert "# Error" in result
        assert "404" in result


# ---------------------------------------------------------------------------
# Inline: ## Trigger[] under #! Table
# ---------------------------------------------------------------------------


class TestInlineTrigger:
    """``## Trigger[]`` sub-section under ``#! Table``."""

    def test_inline_trigger_created(
        self, empty: SQLTranslator
    ) -> None:
        """An inline ``## Trigger[]`` entry is created with the table."""
        empty.write(
            "#! Log\n"
            "id: integer readonly\n"
            "msg: string\n"
        )
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
            "\n"
            "## Trigger[]\n"
            "- name: trg_inline\n"
            "  when: AFTER\n"
            "  event: INSERT\n"
            "  body: \"INSERT INTO Log(msg) VALUES ('inline');\"\n"
        )
        assert "trg_inline" in _trigger_names(empty)


# ---------------------------------------------------------------------------
# #! Database listing
# ---------------------------------------------------------------------------


class TestDatabaseListing:
    """``read('#! Database')`` surfaces user triggers."""

    def test_database_lists_triggers(
        self, empty: SQLTranslator
    ) -> None:
        """``#! Database`` includes a ``## triggers[]`` section."""
        empty.write(
            "#! Order\n"
            "id: integer readonly\n"
            "status: string\n"
        )
        empty.write(
            "#! Log\n"
            "id: integer readonly\n"
            "msg: string\n"
        )
        empty.write(
            '#! Trigger\n'
            'name: trg_lst\n'
            'table: Order\n'
            'when: AFTER\n'
            'event: INSERT\n'
            'body: "INSERT INTO Log(msg) VALUES (\'lst\');"\n'
        )
        result = empty.read("#! Database")
        assert "## triggers[]" in result
        assert "trg_lst" in result
