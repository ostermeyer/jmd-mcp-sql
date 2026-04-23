# SPDX-License-Identifier: Apache-2.0
"""Configuration for jmd-mcp-sql.

Reads optional settings from ``~/.config/jmd/sql.jmd``.  All settings
have sensible defaults — the config file is not required.

Supported fields:

    ``root``
        Restricts database file access to the given directory tree.
        Paths passed to the ``open`` tool must reside under this root.
        If omitted, no restriction is applied.
"""
from __future__ import annotations

from pathlib import Path

from jmd import jmd_to_dict

_CONFIG_PATH = Path.home() / ".config" / "jmd" / "sql.jmd"

_DEFAULTS: dict[str, str | None] = {
    "root": None,
}


def load() -> dict[str, str | None]:
    """Load SQL server configuration.

    Returns:
        A dict with optional key ``root``.  Missing keys fall back to
        built-in defaults.
    """
    cfg = dict(_DEFAULTS)
    if _CONFIG_PATH.exists():
        parsed = jmd_to_dict(_CONFIG_PATH.read_text(encoding="utf-8"))
        if isinstance(parsed, dict):
            for key in _DEFAULTS:
                if key in parsed:
                    cfg[key] = str(parsed[key])
    return cfg


def config_path() -> Path:
    """Return the path to the JMD config file."""
    return _CONFIG_PATH
