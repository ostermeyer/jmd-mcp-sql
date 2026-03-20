# jmd-mcp-sql

MCP server that exposes a SQLite database through three JMD tools — a natural language database interface for LLM-driven workflows.

## Tools

| Tool | JMD mode | SQL |
|------|----------|-----|
| `read` | `#` Data or `#?` Query | SELECT by identifier or with filters |
| `write` | `#` Data | INSERT OR REPLACE |
| `delete` | `#-` Delete | DELETE WHERE |

All inputs and outputs are JMD documents. The LLM speaks JMD — no SQL required.

`read` accepts both a data document (`# Label`) for exact lookups and a Query-by-Example document (`#? Label`) for filtered queries. The document mode determines the behaviour.

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
```
# Customer
CustomerID: ALFKI
```

**Read with filters (Query-by-Example):**
```
#? Order
status: pending
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
