# Changelog

All notable changes to `jmd-mcp-sql` are documented here.

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
