# jmd-mcp-sql

MCP server that exposes a SQLite database through three JMD tools — a natural language database interface for LLM-driven workflows.

## Tools

| Tool | `#` Data | `#?` Query | `#!` Schema | `#-` Delete |
| --- | --- | --- | --- | --- |
| `read` | SELECT by fields | SELECT with filters + aggregation | PRAGMA (describe table) | — |
| `write` | INSERT OR REPLACE | — | CREATE / ALTER TABLE | — |
| `delete` | — | — | DROP TABLE | DELETE WHERE |

All inputs and outputs are JMD documents. The LLM speaks JMD — no SQL required.

## Installation

Install the latest version directly from GitHub:

```bash
pip install git+https://github.com/ostermeyer/jmd-mcp-sql.git
```

Or pin a specific release:

```bash
pip install git+https://github.com/ostermeyer/jmd-mcp-sql.git@v0.1
```

Pre-built packages are attached to each
[GitHub Release](https://github.com/ostermeyer/jmd-mcp-sql/releases).

## Configuration

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

Or use the bundled Northwind demo database (no argument needed):

```json
{
  "mcpServers": {
    "sql": {
      "command": "jmd-mcp-sql"
    }
  }
}
```

The demo database ships as `northwind.sql` (plain text, version-controlled). On the
first run without an explicit path, the server creates `northwind.db` from that file
automatically. The `.db` file is not tracked by git.

## JMD Document Syntax

Every document starts with a heading line that sets the document type and table name,
followed by `key: value` pairs (one per line):

```text
# Product          → data document   (exact lookup / insert-or-replace)
#? Product         → query document  (filter / list / aggregate)
#! Product         → schema document (describe / create / drop table)
#- Product         → delete document (delete matching records)

key: value         → string, integer, or float — inferred automatically
key: true/false    → boolean
```

## Discovering the Database

To see which tables exist, read each table's schema:

```text
read("#! Customers")
```

This returns a `#!` document with column names, JMD types, and modifiers
(`readonly` = primary key, `optional` = nullable).

## Typical Workflows

**List all rows** (small tables only):

```text
read("#? Orders")
```

**Filter rows — equality:**

```text
read("#? Orders\nstatus: shipped")
```

**Filter rows — comparison:**

```text
read("#? Orders\nFreight: > 50")
```

**Filter rows — alternation (OR):**

```text
read("#? Orders\nShipCountry: Germany|France|UK")
```

**Filter rows — contains (case-insensitive substring):**

```text
read("#? Customers\nCompanyName: ~Corp")
```

**Filter rows — regex pattern:**

```text
read("#? Products\nProductName: ^Chai.*")
```

**Filter rows — negation** (composes with any operator):

```text
read("#? Orders\nShipCountry: !Germany")
read("#? Products\nProductName: !^LEGACY.*")
```

**Look up one record:**

```text
read("# Customers\nid: 42")
```

**Insert or replace a record:**

```text
write("# Orders\nid: 1\nstatus: pending\ntotal: 99.90")
```

**Create a table:**

```text
write("#! Products\nid: integer readonly\nname: string\nprice: float optional")
```

**Delete a record:**

```text
delete("#- Orders\nid: 1")
```

**Drop a table:**

```text
delete("#! OldTable")
```

## Pagination

Always use pagination when querying tables that may contain many rows.

Use frontmatter fields before the `#?` heading to control pagination:

```text
read("page-size: 50\npage: 1\n\n#? Orders")
```

The response carries pagination metadata as **frontmatter** — before the root heading:

```text
total: 830
page: 1
pages: 17
page-size: 50

# Orders
## data[]
- OrderID: 10248
  ...
```

**Count only** (no rows returned):

```text
read("count: true\n\n#? Orders")
```

Returns:

```text
count: 830

# Orders
```

Use `total` and `pages` to determine whether to fetch more pages.
For tables with fewer than ~20 rows pagination is optional.

## Field Projection

Use `select:` frontmatter to return only specific columns. This keeps
responses small and context windows focused.

```text
read("select: OrderID, EmployeeID\npage-size: 50\n\n#? Orders")
```

Works with both `#` (data) and `#?` (query) documents. When combined with
aggregation, `select:` filters the result columns after the GROUP BY.

## Joins

Use `join:` frontmatter to query across multiple tables in one call.
The value is `<TableName> on <JoinColumn>` (INNER JOIN, equi-join on a
column that exists in both tables).

```text
read("join: Order Details on OrderID\nsum: UnitPrice * Quantity * (1 - Discount) as revenue\ngroup: EmployeeID\nsort: revenue desc\n\n#? Orders")
```

**Multiple joins** — comma-separated in a single `join:` value:

```text
join: Order Details on OrderID, Employees on EmployeeID
```

**Expression syntax** — use `<expression> as <alias>` in aggregate functions
to compute derived values across joined columns:

```text
sum: UnitPrice * Quantity * (1 - Discount) as revenue
```

The alias becomes the result column name. Without `as`, the default alias
`<func>_<field>` applies (e.g. `sum_Freight`).

Allowed in expressions: column names, numeric literals, arithmetic operators
(`+`, `-`, `*`, `/`), and standard SQL functions (`SUM`, `AVG`, `ROUND`, …).
Subqueries and SQL keywords are not permitted.

**Projection rules for join queries:**

- Unambiguous columns (appear in exactly one table) resolve automatically.
- Join key columns always resolve to the main table.
- Columns present in multiple tables (other than join keys) require explicit
  qualification — specify them via `select:` or filter on the unambiguous side.

## Aggregation

Aggregation is expressed as **frontmatter** before the `#?` heading.
QBE filter fields narrow rows *before* aggregation (SQL WHERE).
The `having:` key filters *after* aggregation (SQL HAVING).

| Key | SQL | Result column name |
| --- | --- | --- |
| `group: f1, f2` | GROUP BY | grouping keys pass through unchanged |
| `sum: field` | SUM(field) | `sum_field` |
| `avg: field` | AVG(field) | `avg_field` |
| `min: field` | MIN(field) | `min_field` |
| `max: field` | MAX(field) | `max_field` |
| `count` | COUNT(*) | `count` |

Multiple fields per function: `sum: Freight, Total` → `sum_Freight` and `sum_Total`.

| Frontmatter | Meaning |
| --- | --- |
| `sort: sum_revenue desc, EmployeeID asc` | ORDER BY (multiple columns, mixed) |
| `having: count > 5` | HAVING COUNT(*) > 5 |
| `having: sum_Freight > 1000, count > 2` | HAVING … AND … (comma = AND) |

`having:` supports: `>`, `>=`, `<`, `<=`, `=`.
`sort:` references any result column — grouping keys or aggregate aliases.
`page-size:` and `page:` apply to the aggregated result set.

**Example — top 3 employees by revenue:**

```text
read("group: EmployeeID\nsum: revenue\nsort: sum_revenue desc\npage-size: 3\n\n#? OrderDetails")
```

## Error Handling

All tools return a `# Error` document on failure:

```text
# Error
status: 400
code: not_found
message: No records found in Orders
```

Check the `code` field to decide how to proceed.

## Specification

The JMD format is documented at [jmd-spec](https://github.com/ostermeyer/jmd-spec).

## License

MIT License. See [LICENSE](LICENSE).
