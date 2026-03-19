# jmd-mcp-sql

MCP server that exposes a SQLite database through four JMD tools — a natural language database interface for LLM-driven workflows.

## Tools

| Tool | JMD mode | SQL |
|------|----------|-----|
| `query` | `#?` Query | SELECT with filters |
| `read` | `#` Data | SELECT by identifier |
| `write` | `#` Data | INSERT OR REPLACE |
| `delete` | `#-` Delete | DELETE WHERE |

All inputs and outputs are JMD documents. The LLM speaks JMD — no SQL required.

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

**Query with filters:**
```
#? Order
status: pending
```

**Read by ID:**
```
# Customer
CustomerID: ALFKI
```

**Write (insert or update):**
```
# Order
OrderID: 99999
CustomerID: ALFKI
status: shipped
```

**Delete:**
```
#- Order
OrderID: 99999
```

## Specification

The JMD format is documented at [jmd-spec](https://github.com/ostermeyer/jmd-spec).

## License

MIT License. See [LICENSE](LICENSE).
