"""Tests for the ``open`` tool, path validation, and config loading."""
from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

from jmd_mcp_sql import config as config_mod
from jmd_mcp_sql import server

# --------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------

def _seed_db(path: Path, ddl: str = "CREATE TABLE t(id INTEGER)") -> None:
    """Create a SQLite database with a single table."""
    conn = sqlite3.connect(str(path))
    conn.execute(ddl)
    conn.close()


@pytest.fixture(autouse=True)
def _reset_server_state() -> None:  # noqa: PT004
    """Reset module-level server state between tests."""
    server._translator = None
    server._db_path = None
    server._config = {}


# ====================================================================
# TestConfig
# ====================================================================


class TestConfig:
    """Config loading from ~/.config/jmd/sql.jmd."""

    def test_defaults_when_no_file(self, tmp_path: Path) -> None:
        """Missing config file returns defaults."""
        fake = tmp_path / "nonexistent.jmd"
        with patch.object(config_mod, "_CONFIG_PATH", fake):
            cfg = config_mod.load()
        assert cfg["root"] is None

    def test_reads_root(self, tmp_path: Path) -> None:
        """Config file with root field is parsed correctly."""
        cfg_file = tmp_path / "sql.jmd"
        cfg_file.write_text("# Config\nroot: /data/dbs\n", encoding="utf-8")
        with patch.object(config_mod, "_CONFIG_PATH", cfg_file):
            cfg = config_mod.load()
        assert cfg["root"] == "/data/dbs"


# ====================================================================
# TestResolvePath
# ====================================================================


class TestResolvePath:
    """Path validation against root restriction."""

    def test_no_root_allows_any_path(self, tmp_path: Path) -> None:
        """Without root restriction, any absolute path is accepted."""
        server._config = {"root": None}
        result = server._resolve_db_path(str(tmp_path / "any.db"))
        assert result == (tmp_path / "any.db").resolve()

    def test_root_allows_subpath(self, tmp_path: Path) -> None:
        """A path under the configured root is accepted."""
        server._config = {"root": str(tmp_path)}
        result = server._resolve_db_path(str(tmp_path / "sub" / "my.db"))
        assert result == (tmp_path / "sub" / "my.db").resolve()

    def test_root_rejects_outside_path(self, tmp_path: Path) -> None:
        """A path outside the configured root raises ValueError."""
        server._config = {"root": str(tmp_path / "allowed")}
        with pytest.raises(ValueError, match="outside the allowed root"):
            server._resolve_db_path("/etc/other.db")

    def test_root_rejects_traversal(self, tmp_path: Path) -> None:
        """Path traversal via .. is resolved before checking."""
        allowed = tmp_path / "allowed"
        allowed.mkdir()
        server._config = {"root": str(allowed)}
        sneaky = str(allowed / ".." / "escaped.db")
        with pytest.raises(ValueError, match="outside the allowed root"):
            server._resolve_db_path(sneaky)

    def test_expanduser(self, tmp_path: Path) -> None:
        """Tilde in path is expanded."""
        server._config = {"root": None}
        with patch.dict("os.environ", {"HOME": str(tmp_path)}):
            result = server._resolve_db_path("~/my.db")
        assert result == (tmp_path / "my.db").resolve()


# ====================================================================
# TestOpenDb
# ====================================================================


class TestOpenDb:
    """Database opening lifecycle via _open_db."""

    def test_open_existing_db(self, tmp_path: Path) -> None:
        """Opening an existing DB returns its table list."""
        db = tmp_path / "test.db"
        _seed_db(db)
        result = server._open_db(db)

        assert "path:" in result
        assert "table-count: 1" in result
        assert "# Database" in result
        assert "- t" in result
        assert server._db_path == db
        assert server._translator is not None

    def test_open_creates_new_db(self, tmp_path: Path) -> None:
        """Opening a non-existent path creates an empty database."""
        db = tmp_path / "brand_new.db"
        assert not db.exists()
        result = server._open_db(db)

        assert db.exists()
        assert "created: true" in result
        assert "table-count: 0" in result

    def test_open_switches_db(self, tmp_path: Path) -> None:
        """Opening a second DB closes the first connection."""
        db1 = tmp_path / "first.db"
        db2 = tmp_path / "second.db"
        _seed_db(db1, "CREATE TABLE alpha(id INTEGER)")
        _seed_db(db2, "CREATE TABLE beta(id INTEGER)")

        server._open_db(db1)
        first_translator = server._translator

        server._open_db(db2)
        assert server._db_path == db2
        assert "- beta" in server._db_status()

        # First connection should be closed.
        assert first_translator is not None
        with pytest.raises(Exception):  # noqa: B017
            first_translator._conn.execute("SELECT 1")

    def test_open_multiple_tables_sorted(self, tmp_path: Path) -> None:
        """Tables are returned in alphabetical order."""
        db = tmp_path / "multi.db"
        conn = sqlite3.connect(str(db))
        conn.execute("CREATE TABLE zebra(id INTEGER)")
        conn.execute("CREATE TABLE alpha(id INTEGER)")
        conn.execute("CREATE TABLE middle(id INTEGER)")
        conn.close()

        result = server._open_db(db)
        assert "table-count: 3" in result
        lines = result.splitlines()
        table_lines = [ln[2:] for ln in lines if ln.startswith("- ")]
        assert table_lines == ["alpha", "middle", "zebra"]


# ====================================================================
# TestOpenTool
# ====================================================================


class TestOpenTool:
    """Integration tests for the open MCP tool."""

    def test_open_via_tool(self, tmp_path: Path) -> None:
        """The open tool accepts a JMD document and opens the DB."""
        db = tmp_path / "via_tool.db"
        _seed_db(db)
        server._config = {"root": None}

        result = server.open_database(f"# Database\npath: {db}\n")
        assert "table-count: 1" in result
        assert "- t" in result

    def test_status_query(self, tmp_path: Path) -> None:
        """Open tool without path returns current DB info."""
        db = tmp_path / "status.db"
        _seed_db(db)
        server._config = {"root": None}
        server._open_db(db)

        result = server.open_database("# Database\n")
        assert str(db) in result
        assert "table-count: 1" in result

    def test_status_no_db_returns_error(self) -> None:
        """Status query without active DB returns error."""
        result = server.open_database("# Database\n")
        assert "# Error" in result
        assert "no_database" in result

    def test_path_denied(self, tmp_path: Path) -> None:
        """Path outside root returns 403 error."""
        server._config = {"root": str(tmp_path / "allowed")}
        result = server.open_database("# Database\npath: /etc/nope.db\n")
        assert "# Error" in result
        assert "path_denied" in result
