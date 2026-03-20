"""Download and install the Northwind demo database."""
from __future__ import annotations

import urllib.request
import sqlite3
from pathlib import Path

NORTHWIND_SQL_URL = (
    "https://raw.githubusercontent.com/jpwhite3/northwind-SQLite3/main/dist/northwind.db"
)

DEST = Path(__file__).parent / "northwind.db"


def install() -> None:
    if DEST.exists():
        print(f"Northwind database already installed at {DEST}")
        return

    print("Downloading Northwind database…")
    urllib.request.urlretrieve(NORTHWIND_SQL_URL, DEST)

    # Verify it's a valid SQLite file
    conn = sqlite3.connect(str(DEST))
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cur.fetchall()]
    conn.close()

    print(f"Installed: {DEST}")
    print(f"Tables: {', '.join(tables)}")


if __name__ == "__main__":
    install()
