# SPDX-License-Identifier: Apache-2.0
"""Debug-mode infrastructure and frontmatter validation.

Extracted from translator.py during the G1 modularisation slice.
Contains the :class:`DebugInfo` dataclass that collects per-call
debug output, helpers to parse and render it as JMD frontmatter,
the :class:`StrictRefusalError` raised for unknown keys on
destructive operations, and the ``_check_frontmatter`` /
``_prepend_ignored_keys`` helpers used by every tool to enforce
the workspace-level frontmatter policies (strict refusal vs
observable tolerance).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Debug channels recognised in ``debug:`` frontmatter. Anything
# else lands in ``debug-unknown`` so misspellings are visible
# rather than silently dropped.
_KNOWN_DEBUG_VALUES: frozenset[str] = frozenset({
    "sql", "timing", "table", "filters", "plan",
    "resolved", "coercions",
})


@dataclass
class DebugInfo:
    """Collects debug output during a single operation."""

    requested: frozenset[str]
    unknown: list[str]
    sql: str = ""
    timing_ms: float = 0.0
    table: str = ""
    filters: list[tuple[str, str]] = field(
        default_factory=list,
    )
    plan: str = ""
    resolved: list[tuple[str, str]] = field(
        default_factory=list,
    )
    coercions: list[tuple[str, str]] = field(
        default_factory=list,
    )

    @property
    def active(self) -> bool:
        """Whether any debug output was requested."""
        return bool(self.requested)

    def wants(self, key: str) -> bool:
        """Check if a specific debug value was requested."""
        return key in self.requested

    def to_frontmatter(self) -> str:
        """Render debug output as JMD frontmatter lines."""
        parts: list[str] = []
        if self.unknown:
            parts.append(
                "debug-unknown: " + ", ".join(self.unknown)
            )
        if self.wants("sql") and self.sql:
            parts.append(f"debug-sql: {self.sql}")
        if self.wants("timing"):
            parts.append(
                f"debug-timing: {self.timing_ms:.1f}ms"
            )
        if self.wants("table") and self.table:
            parts.append(f"debug-table: {self.table}")
        if self.wants("plan") and self.plan:
            parts.append(f"debug-plan: {self.plan}")
        if self.wants("filters") and self.filters:
            for fld, translation in self.filters:
                parts.append(
                    f"debug-filter-{fld}: {translation}"
                )
        if self.wants("resolved") and self.resolved:
            for key, translation in self.resolved:
                parts.append(
                    f"debug-resolved-{key}: {translation}"
                )
        if self.wants("coercions") and self.coercions:
            for fld, info in self.coercions:
                parts.append(
                    f"debug-coercion-{fld}: {info}"
                )
        return "\n".join(parts)


def _parse_debug(fm: dict[str, Any]) -> DebugInfo:
    """Parse the ``debug:`` frontmatter key.

    Returns a :class:`DebugInfo` with the requested values
    separated into known and unknown.  If ``debug`` is not
    present in *fm*, returns an inactive DebugInfo.

    Special values:
      * ``true`` (or the boolean ``True``) — alias for "all
        known debug channels".  This matches the natural LLM
        intuition of using ``debug: true`` as a boolean flag.
    """
    raw = fm.get("debug")
    if raw is None:
        return DebugInfo(
            requested=frozenset(), unknown=[]
        )
    if raw is True or str(raw).strip().lower() == "true":
        return DebugInfo(
            requested=_KNOWN_DEBUG_VALUES, unknown=[]
        )
    values = {
        v.strip()
        for v in str(raw).split(",")
        if v.strip()
    }
    known = frozenset(values & _KNOWN_DEBUG_VALUES)
    unknown = sorted(values - _KNOWN_DEBUG_VALUES)
    return DebugInfo(requested=known, unknown=unknown)


def _prepend_debug(
    response: str, dbg: DebugInfo,
) -> str:
    """Prepend debug frontmatter to *response* if active."""
    fm = dbg.to_frontmatter()
    if not fm:
        return response
    return f"{fm}\n\n{response}"


class StrictRefusalError(ValueError):
    """Raised when strict refusal rejects unknown frontmatter keys.

    Inherits from :class:`ValueError` so existing ``except
    ValueError`` paths continue to work.  The structured
    attributes ``unknown`` and ``accepted`` let callers build
    detailed error responses.
    """

    def __init__(
        self, unknown: list[str], accepted: list[str],
    ) -> None:
        """Initialise the error with unknown and accepted keys."""
        self.unknown = unknown
        self.accepted = accepted
        accepted_str = (
            ", ".join(accepted) if accepted else "(none)"
        )
        super().__init__(
            f"Unknown frontmatter key(s) {unknown!r} on a"
            " destructive operation. Accepted keys:"
            f" {accepted_str}."
        )


def _check_frontmatter(
    fm: dict[str, Any],
    known: frozenset[str],
    policy: str,
) -> list[str]:
    """Validate frontmatter keys against a known set.

    Args:
        fm: Parsed frontmatter dict from the JMD parser.
        known: Set of keys this operation recognises.
        policy: ``"observable"`` to return unknown keys silently,
            or ``"strict"`` to raise on unknown keys.

    Returns:
        List of unknown key names (may be empty).

    Raises:
        StrictRefusalError: When *policy* is ``"strict"`` and
            unknown keys are present.
    """
    unknown = [k for k in fm if k not in known]
    if unknown and policy == "strict":
        raise StrictRefusalError(
            unknown=unknown, accepted=sorted(known),
        )
    return unknown


def _prepend_ignored_keys(
    response: str,
    ignored: list[str],
) -> str:
    """Prepend an ``ignored-keys`` header to *response*.

    Uses the short form from JMD Spec §23.7:
    ``ignored-keys: key1, key2``.  Returns *response* unchanged
    when *ignored* is empty.
    """
    if not ignored:
        return response
    header = "ignored-keys: " + ", ".join(ignored)
    return f"{header}\n\n{response}"
