# jmd-mcp-sql

MCP server that exposes a SQLite database through three JMD tools — a natural language database interface for LLM-driven workflows.

## Tools

| Tool | `#` Data | `#?` Query | `#!` Schema | `#-` Delete |
| ------ | ---------- | ------------ | ------------- | ------------- |
| `read` | SELECT by fields | SELECT with filters | PRAGMA (describe table) | — |
| `write` | INSERT OR REPLACE | — | CREATE / ALTER TABLE | — |
| `delete` | — | — | DROP TABLE | DELETE WHERE |

All inputs and outputs are JMD documents. The LLM speaks JMD — no SQL required.

The document mode determines what each tool does: data (`#`), query (`#?`), schema (`#!`), or delete (`#-`).

## Installation

```bash
pip install jmd-mcp-sql
```

Install the Northwind demo database:

```bash
python -m jmd_mcp_sql.install_northwind
```

## Usage

### With Claude Code

Add to your MCP configuration (`~/.claude/settings.json`):

```json
{
  "mcpServers": {
    "sql": {
      "command": "jmd-mcp-sql",
      "args": ["/path/to/your.db"]
    }
  }
}
```

Or use the Northwind demo (no argument needed):

```json
{
  "mcpServers": {
    "sql": {
      "command": "jmd-mcp-sql"
    }
  }
}
```

### Tool examples

**Read by ID:**

```text
# Customer
CustomerID: ALFKI
```

**Read with filters (Query-by-Example):**

```text
#? Order
status: pending
```

**Write (insert or update):**

```text
# Order
OrderID: 99999
CustomerID: ALFKI
status: shipped
```

**Delete:**

```text
#- Order
OrderID: 99999
```

## Specification

The JMD format is documented at [jmd-spec](https://github.com/ostermeyer/jmd-spec).

## License

MIT License. See [LICENSE](LICENSE).
