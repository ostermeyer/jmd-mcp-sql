# Changelog

All notable changes to `jmd-mcp-sql` are documented here.

## 0.11.0 — 2026-05-03

### Full SQLite DDL surface via JMD `#!` documents

The schema-mode (`#!`) tool was previously additive-only — it could create a
table from a column-list document and add new columns, but constraints,
indexes, triggers, and views were out of reach. 0.11.0 closes that gap.

**Tables — column modifiers and table-level constraints**

* Column-level `DEFAULT` values via `key: type = value`.
* Enum-as-CHECK via the bare-pipe form `key: a|b|c` — generates a column-
  level `CHECK (key IN ('a', 'b', 'c'))`.
* Sub-section constraints under `#! Table`:
  * `## primary-key[]` — composite primary keys.
  * `## unique[]` — UNIQUE constraints, single- or multi-column.
  * `## check[]` — raw CHECK expressions.
  * `## references[]` — single-column foreign keys, form
    `local_col: ForeignTable.foreign_col`.
* `read("#! Table")` round-trips all of the above.

**Indexes — top-level shape and inline form**

* `#! Index` document with `name`, `table`, `columns`, optional `unique`
  and `where` (partial index). Drop via `delete` with
  `confirm: drop-index`.
* Inline `## Index[]` sub-section under `#! Table` creates indexes alongside
  the table; on inline-index failure the whole operation rolls back so
  table-with-indexes stays all-or-nothing.
* `read("#! Database")` lists user indexes alongside tables.

**Triggers — top-level shape and inline form**

* `#! Trigger` document with `name`, `table`, `when` (BEFORE / AFTER /
  INSTEAD OF), `event` (INSERT / UPDATE [OF cols] / DELETE), optional
  `condition` (WHEN clause), and multi-line `body` (JMD JSON-escape form).
  Drop via `delete` with `confirm: drop-trigger`.
* Inline `## Trigger[]` sub-section, same atomic create-or-rollback as
  inline indexes.
* `read("#! Database")` lists user triggers.

**Views — top-level shape**

* `#! View` document with `name` and `select` (multi-line bodies via JMD
  JSON-escape). Drop via `delete` with `confirm: drop-view`. Legacy
  `confirm: drop-table` path on a view-named label still works.
* `read("#! Database")` lists user views.

**Virtual tables — `using:` modifier on `#! Table`**

* `using: fts5` (or any other module) turns CREATE TABLE into
  CREATE VIRTUAL TABLE. Columns render as bare names; `## unindexed[]`
  flags FTS5 UNINDEXED columns; `## options[]` carries module-arg strings
  like `"tokenize = 'porter unicode61'"`.
* Drop via the regular table path.

**`action: rebuild` for non-additive schema changes**

* Frontmatter `action: rebuild` on a write to an existing `#! Table` runs
  the SQLite table-rebuild dance: stage new schema, copy data over the
  column intersection, drop old, rename. Inline `## Index[]` and
  `## Trigger[]` from the rebuild document are recreated post-rename.
  Without `action: rebuild` the existing additive-only ALTER stands.
* Atomic via explicit BEGIN IMMEDIATE / COMMIT (with autocommit-mode
  override of Python sqlite3's legacy isolation handling). On any failure
  — bad CHECK syntax, existing data violating a new CHECK, malformed FK
  reference — the original table is left untouched.

**Reserved DDL-object labels**

`Index`, `Trigger`, and `View` now dispatch to the per-kind shape unless
a real table by that name exists (mirroring the long-standing
`#! Database` root-schema fallback). If you have a user table called
`Index`, `Trigger`, or `View`, schema operations on it keep working
unchanged — those names just take precedence over the DDL-object
shapes.

### Internal: modular structure under `jmd_mcp_sql/`

0.11.0 also restructures the internal layout. The 3,900-line
`translator.py` is split into a 124-line public dispatch hull plus
focused modules:

* `_context.py` — `TranslatorContext` dataclass holding the connection
  and schema cache, with the universal access patterns
  (`resolve_or_error`, `fetchall`, `explain`, `refresh_schema`).
* `_schema_ops.py` — schema-mode operations as free functions over a
  context.
* `_query_ops.py` — query-mode operations as free functions.
* `_data_ops.py` — data-mode reads, writes, and deletes.
* `_filters.py` — QBE WHERE-clause builders.
* `_ddl.py`, `_query_parser.py`, `_debug.py`, `_sql.py` — pure-function
  helper modules.

The public API (`SQLTranslator.read` / `.write` / `.delete` plus the
`main()` entry point) is unchanged. Anyone reaching into private names
(`_build_where`, the internal mixin classes that briefly existed during
the restructure) will need to update; everyone else is unaffected.

## 0.10.0 — 2026-04-23

### License change: AGPL-3.0 → Apache 2.0

Aligned with the rest of the JMD ecosystem. The JMD specification is published
under CC BY 4.0; the reference implementations `jmd-impl` and `jmd-js` are
Apache 2.0. For consistency — and because the goal of this project is adoption,
not product protection — this server now matches: **Apache 2.0, no copyleft,
no dual licensing, no CLA**. Use it, fork it, extend it, ship it. Attribution
is preserved per Apache 2.0 § 4.

**Why the reversal.** The AGPL-3.0 decision in 0.8.0 reflected a brief period
in which a commercial deployment path was under consideration. That path has
since been abandoned in favour of open-standard adoption. Under the new
framing, AGPL protected against a risk that no longer exists and imposed
real adoption friction — many corporate environments pre-emptively block
AGPL code. Apache 2.0 removes that friction while preserving attribution.

**Prior versions.** Releases 0.8.0 – 0.9.x remain available and legally
usable under AGPL-3.0 for anyone who installed them during that window.
License changes are not retroactive.

### No functional changes in 0.10.0

0.10.0 is a license-only release — no API changes, no behavior changes, no
schema changes relative to 0.9.0. All changes are in license metadata,
SPDX identifiers, and documentation.

## 0.8.0 — 2026-04-17

### License change: MIT → AGPL-3.0

Starting with this version, `jmd-mcp-sql` is licensed under the
**[GNU Affero General Public License v3.0 (AGPL-3.0)](LICENSE)**.

**Why the change.** This server has grown from a Northwind demo into a substantive
tool used in real workflows (Claude Cowork and others). To support continued
development while keeping the project sustainable, the license has moved from a
permissive (MIT) to a reciprocal-copyleft (AGPL-3.0) model. Lokale Nutzung bleibt
frei; SaaS-Redistribution fällt unter die AGPL-Reciprocity-Klausel.

**What stays open and unchanged.**
- **JMD itself** is open under [CC BY 4.0](https://github.com/ostermeyer/jmd-spec/blob/main/LICENSE)
  for the specification and [Apache 2.0](https://github.com/ostermeyer/jmd-impl/blob/main/LICENSE)
  for the reference implementations. JMD is and remains a freely usable
  standard — you can build JMD-speaking servers, clients, and tools in any
  language under any license, with no obligation to this project.
- **The AGPL-3.0 obligation applies only to this server's code**, not to
  anything upstream or sideways.

**Prior versions remain under MIT.** Releases 0.4, 0.4.1, 0.5.0, 0.6.0, 0.7.0,
and 0.7.1 were published under the MIT License. Users who installed those
versions retain the rights MIT grants for those specific artifacts. Those
versions are yanked from PyPI as "no longer recommended", but remain
installable by explicit version pin and legally usable under MIT.

**Commercial licensing without AGPL obligations** is available on request:
andreas@ostermeyer.de

### No functional changes in 0.8.0

0.8.0 is a license-only release — no API changes, no behavior changes, no
schema changes relative to 0.7.1. An `upgrade` under AGPL obligations is
functionally identical to staying on 0.7.1 under MIT.

---

Earlier entries (0.4 through 0.7.1) were not captured in a changelog file.
Commit history on the repository is the source of truth for those versions.
