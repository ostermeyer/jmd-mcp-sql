"""Comprehensive unit tests for SQLTranslator.

Tests cover all three public methods (read, write, delete) across all four
JMD document modes (#, #?, #!, #-), all frontmatter conventions from §23
(page-size, page, count, select, sort), and all error paths.

Fixtures
--------
nw : session-scoped SQLTranslator over the Northwind demo database.
     Used for all read-only tests — fast because it is created once.
nw_rw : function-scoped SQLTranslator over a fresh Northwind copy.
        Used for tests that mutate data (write, delete).
empty : function-scoped SQLTranslator over an empty in-memory database.
        Used for schema lifecycle tests (create → alter → insert → drop).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from jmd_mcp_sql.translator import SQLTranslator

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NORTHWIND_SQL = (
    Path(__file__).parent.parent / "jmd_mcp_sql" / "northwind.sql"
)


def _make_northwind() -> SQLTranslator:
    """Return a SQLTranslator backed by a fresh in-memory Northwind DB."""
    conn = sqlite3.connect(":memory:")
    conn.executescript(_NORTHWIND_SQL.read_text(encoding="utf-8"))
    return SQLTranslator(conn)


def _make_empty() -> SQLTranslator:
    """Return a SQLTranslator backed by a pristine empty in-memory DB."""
    return SQLTranslator(sqlite3.connect(":memory:"))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def nw() -> SQLTranslator:
    """Session-scoped read-only Northwind translator."""
    return _make_northwind()


@pytest.fixture()
def nw_rw() -> SQLTranslator:
    """Function-scoped mutable Northwind translator."""
    return _make_northwind()


@pytest.fixture()
def empty() -> SQLTranslator:
    """Function-scoped empty-database translator."""
    return _make_empty()


# ---------------------------------------------------------------------------
# 1. TestReadData — # Label (exact-match SELECT)
# ---------------------------------------------------------------------------


class TestReadData:
    """Tests for data-mode read (# Label)."""

    def test_single_match_returns_record(self, nw: SQLTranslator) -> None:
        """Exactly one match returns a single-record document."""
        result = nw.read("# Orders\nOrderID: 10248")
        assert result.startswith("# Orders")
        assert "OrderID: 10248" in result
        assert "ShipCountry: France" in result

    def test_multiple_matches_returns_array(self, nw: SQLTranslator) -> None:
        """Multiple matches return an array document."""
        result = nw.read("# Orders\nShipCountry: France")
        assert "## data[]" in result or result.startswith("# [")

    def test_no_match_returns_404(self, nw: SQLTranslator) -> None:
        """No matching rows returns a 404 Error document."""
        result = nw.read("# Orders\nOrderID: 99999")
        assert "# Error" in result
        assert "404" in result
        assert "not_found" in result

    def test_unknown_column_returns_400(self, nw: SQLTranslator) -> None:
        """An unknown column name returns a 400 bad_request Error."""
        result = nw.read("# Orders\nNoSuchCol: x")
        assert "# Error" in result
        assert "400" in result
        assert "bad_request" in result
        assert "NoSuchCol" in result


# ---------------------------------------------------------------------------
# 2. TestReadQuery — #? Label (Query-by-Example)
# ---------------------------------------------------------------------------


class TestReadQuery:
    """Tests for query-mode read (#? Label)."""

    def test_no_filter_returns_all_rows(self, nw: SQLTranslator) -> None:
        """A bare #? document returns all rows."""
        result = nw.read("count: true\n\n#? Orders")
        assert result.startswith("count: 830")

    def test_equality_filter(self, nw: SQLTranslator) -> None:
        """Equality filter restricts results to matching rows."""
        result = nw.read(
            "count: true\n\n#? Orders\nShipCountry: France"
        )
        assert result.startswith("count: 77")

    def test_comparison_filter_gt(self, nw: SQLTranslator) -> None:
        """Greater-than filter returns only rows above threshold."""
        result = nw.read(
            "count: true\n\n#? Orders\nFreight: > 50"
        )
        # Verify we get fewer rows than total 830
        count = int(result.split("count:")[1].split()[0])
        assert 0 < count < 830

    def test_alternation_filter(self, nw: SQLTranslator) -> None:
        """Pipe-separated values act as OR (IN clause)."""
        result_combined = nw.read(
            "count: true\n\n#? Orders\nShipCountry: Germany|France"
        )
        result_de = nw.read(
            "count: true\n\n#? Orders\nShipCountry: Germany"
        )
        result_fr = nw.read(
            "count: true\n\n#? Orders\nShipCountry: France"
        )
        count_combined = int(
            result_combined.split("count:")[1].split()[0]
        )
        count_de = int(result_de.split("count:")[1].split()[0])
        count_fr = int(result_fr.split("count:")[1].split()[0])
        assert count_combined == count_de + count_fr

    def test_substring_filter(self, nw: SQLTranslator) -> None:
        """Tilde prefix matches case-insensitive substring."""
        result = nw.read(
            "count: true\n\n#? Customers\nCompanyName: ~market"
        )
        count = int(result.split("count:")[1].split()[0])
        assert count > 0

    def test_regex_filter(self, nw: SQLTranslator) -> None:
        """Regex filter matches on pattern."""
        result = nw.read(
            "count: true\n\n#? Products\nProductName: ^Chai"
        )
        count = int(result.split("count:")[1].split()[0])
        assert count > 0

    def test_negation_filter(self, nw: SQLTranslator) -> None:
        """Exclamation prefix excludes matching rows."""
        result_all = nw.read("count: true\n\n#? Orders")
        result_neg = nw.read(
            "count: true\n\n#? Orders\nShipCountry: !Germany"
        )
        result_de = nw.read(
            "count: true\n\n#? Orders\nShipCountry: Germany"
        )
        total = int(result_all.split("count:")[1].split()[0])
        neg = int(result_neg.split("count:")[1].split()[0])
        de = int(result_de.split("count:")[1].split()[0])
        assert neg == total - de

    def test_combined_filters(self, nw: SQLTranslator) -> None:
        """Multiple filter lines are ANDed together."""
        result = nw.read(
            "count: true\n\n"
            "#? Orders\n"
            "ShipCountry: France\n"
            "Freight: > 50"
        )
        count = int(result.split("count:")[1].split()[0])
        result_fr_only = nw.read(
            "count: true\n\n#? Orders\nShipCountry: France"
        )
        count_fr = int(result_fr_only.split("count:")[1].split()[0])
        assert count < count_fr


# ---------------------------------------------------------------------------
# 3. TestPagination — page-size / page frontmatter
# ---------------------------------------------------------------------------


class TestPagination:
    """Tests for §23.1 pagination frontmatter conventions."""

    def test_page_size_produces_frontmatter(
        self, nw: SQLTranslator
    ) -> None:
        """page-size triggers pagination response frontmatter."""
        result = nw.read("page-size: 5\npage: 1\n\n#? Orders")
        assert "total: 830" in result
        assert "page: 1" in result
        assert "pages: 166" in result
        assert "page-size: 5" in result

    def test_page_size_limits_rows(self, nw: SQLTranslator) -> None:
        """page-size limits the number of data rows returned."""
        result = nw.read("page-size: 3\npage: 1\n\n#? Orders")
        # Three items in the array
        assert result.count("\n- ") == 3

    def test_page_offset(self, nw: SQLTranslator) -> None:
        """page: 2 returns the second page of results."""
        r1 = nw.read("page-size: 5\npage: 1\n\n#? Orders")
        r2 = nw.read("page-size: 5\npage: 2\n\n#? Orders")
        # First OrderID on page 1 must not appear as first on page 2
        first_id_p1 = [
            ln for ln in r1.splitlines() if "OrderID:" in ln
        ][0]
        first_id_p2 = [
            ln for ln in r2.splitlines() if "OrderID:" in ln
        ][0]
        assert first_id_p1 != first_id_p2

    def test_no_page_size_no_pagination_frontmatter(
        self, nw: SQLTranslator
    ) -> None:
        """Without page-size there is no pagination frontmatter."""
        result = nw.read(
            "page-size: 5\npage: 1\n\n#? Orders\nShipCountry: France"
        )
        # Remove pagination prefix and verify it was there
        assert "total:" in result
        # Without page-size, no pagination metadata
        result_no_paging = nw.read(
            "#? Shippers"
        )
        assert "total:" not in result_no_paging
        assert "page-size:" not in result_no_paging

    def test_page_beyond_last_returns_empty(
        self, nw: SQLTranslator
    ) -> None:
        """Requesting a page beyond the last returns empty data."""
        result = nw.read("page-size: 100\npage: 999\n\n#? Orders")
        assert "## data[]" in result
        # No list items
        assert "\n- " not in result


# ---------------------------------------------------------------------------
# 4. TestCountMode — count: true frontmatter
# ---------------------------------------------------------------------------


class TestCountMode:
    """Tests for §23.2 count-mode frontmatter convention."""

    def test_count_is_frontmatter(self, nw: SQLTranslator) -> None:
        """count: response value appears as frontmatter before # heading."""
        result = nw.read("count: true\n\n#? Orders")
        assert result.startswith("count: 830")

    def test_count_followed_by_heading(self, nw: SQLTranslator) -> None:
        """Root heading follows the count frontmatter."""
        result = nw.read("count: true\n\n#? Orders")
        assert "# Orders" in result
        count_pos = result.index("count:")
        heading_pos = result.index("# Orders")
        assert count_pos < heading_pos

    def test_count_with_filter(self, nw: SQLTranslator) -> None:
        """count: true respects QBE filters."""
        result = nw.read(
            "count: true\n\n#? Orders\nShipCountry: France"
        )
        assert result.startswith("count: 77")

    def test_count_data_mode(self, nw: SQLTranslator) -> None:
        """count: true works on data-mode documents too."""
        result = nw.read(
            "count: true\n\n# Orders\nShipCountry: France"
        )
        assert result.startswith("count: 77")


# ---------------------------------------------------------------------------
# 5. TestFieldProjection — select: frontmatter
# ---------------------------------------------------------------------------


class TestFieldProjection:
    """Tests for §23.3 field-projection frontmatter convention."""

    def test_select_restricts_columns(self, nw: SQLTranslator) -> None:
        """select: returns only the specified columns."""
        result = nw.read(
            "select: OrderID, ShipCountry\n"
            "page-size: 5\n\n#? Orders"
        )
        assert "OrderID:" in result
        assert "ShipCountry:" in result
        assert "Freight:" not in result
        assert "CustomerID:" not in result

    def test_select_unknown_column_raises(
        self, nw: SQLTranslator
    ) -> None:
        """Selecting an unknown column raises ValueError."""
        with pytest.raises(ValueError, match="NoSuchCol"):
            nw.read("select: NoSuchCol\n\n#? Orders")

    def test_select_with_pagination(self, nw: SQLTranslator) -> None:
        """select: and page-size compose correctly."""
        result = nw.read(
            "select: OrderID\npage-size: 3\n\n#? Orders"
        )
        assert "total: 830" in result
        assert "page-size: 3" in result
        assert "OrderID:" in result
        assert "ShipCountry:" not in result


# ---------------------------------------------------------------------------
# 6. TestSort — sort: frontmatter
# ---------------------------------------------------------------------------


class TestSort:
    """Tests for §23.4 sort frontmatter convention."""

    def test_sort_desc_changes_order(self, nw: SQLTranslator) -> None:
        """sort: desc returns highest values first."""
        result_asc = nw.read(
            "sort: Freight asc\npage-size: 1\n\n#? Orders"
        )
        result_desc = nw.read(
            "sort: Freight desc\npage-size: 1\n\n#? Orders"
        )
        freight_asc = [
            ln for ln in result_asc.splitlines() if "Freight:" in ln
        ][0]
        freight_desc = [
            ln for ln in result_desc.splitlines() if "Freight:" in ln
        ][0]
        val_asc = float(freight_asc.split(":")[1])
        val_desc = float(freight_desc.split(":")[1])
        assert val_desc > val_asc

    def test_sort_asc_is_default(self, nw: SQLTranslator) -> None:
        """sort: without direction defaults to ascending."""
        result_explicit = nw.read(
            "sort: Freight asc\npage-size: 3\n\n#? Orders"
        )
        result_default = nw.read(
            "sort: Freight\npage-size: 3\n\n#? Orders"
        )
        # Extract OrderIDs from both to confirm same sequence
        ids_explicit = [
            ln for ln in result_explicit.splitlines() if "OrderID:" in ln
        ]
        ids_default = [
            ln for ln in result_default.splitlines() if "OrderID:" in ln
        ]
        assert ids_explicit == ids_default

    def test_multi_column_sort(self, nw: SQLTranslator) -> None:
        """Comma-separated sort columns are all applied."""
        result = nw.read(
            "sort: ShipCountry asc, Freight desc\n"
            "page-size: 5\n\n#? Orders"
        )
        assert "total:" in result  # pagination ran without error

    def test_sort_with_filter(self, nw: SQLTranslator) -> None:
        """sort: composes with QBE filters."""
        result = nw.read(
            "sort: Freight desc\npage-size: 3\n\n"
            "#? Orders\nShipCountry: France"
        )
        freights = [
            float(ln.split(":")[1])
            for ln in result.splitlines()
            if ln.strip().startswith("Freight:")
        ]
        assert freights == sorted(freights, reverse=True)


# ---------------------------------------------------------------------------
# 7. TestAggregation — group / sum / avg / min / max / having / sort
# ---------------------------------------------------------------------------


class TestAggregation:
    """Tests for aggregation frontmatter keys."""

    def test_group_count(self, nw: SQLTranslator) -> None:
        """group + count produces count column per group."""
        result = nw.read(
            "group: ShipCountry\ncount\n\n#? Orders"
        )
        assert "ShipCountry:" in result
        assert "count:" in result

    def test_group_sum(self, nw: SQLTranslator) -> None:
        """group + sum produces sum_<field> column."""
        result = nw.read(
            "group: ShipCountry\nsum: Freight\npage-size: 5\n\n#? Orders"
        )
        assert "sum_Freight:" in result

    def test_having_filters_groups(self, nw: SQLTranslator) -> None:
        """having: filters out groups not satisfying the condition."""
        # Count groups without having filter via total: in pagination
        result_all = nw.read(
            "group: ShipCountry\ncount\npage-size: 100\n\n#? Orders"
        )
        result_filtered = nw.read(
            "group: ShipCountry\ncount\nhaving: count > 100\n"
            "page-size: 100\n\n#? Orders"
        )
        total_all = int(
            [ln for ln in result_all.splitlines()
             if ln.startswith("total:")][0].split(":")[1]
        )
        total_filtered = int(
            [ln for ln in result_filtered.splitlines()
             if ln.startswith("total:")][0].split(":")[1]
        )
        assert total_filtered < total_all

    def test_sort_on_aggregate_alias(self, nw: SQLTranslator) -> None:
        """sort: can reference aggregate result column aliases."""
        result = nw.read(
            "group: ShipCountry\n"
            "count\n"
            "sort: count desc\n"
            "page-size: 3\n\n#? Orders"
        )
        counts = [
            int(ln.split(":")[1])
            for ln in result.splitlines()
            if ln.strip().startswith("count:")
            and ln.split(":")[1].strip().isdigit()
        ]
        assert counts == sorted(counts, reverse=True)

    def test_expression_alias(self, nw: SQLTranslator) -> None:
        """Aggregate sum with as-alias uses the alias as result column name."""
        result = nw.read(
            "join: Order Details on OrderID\n"
            "group: EmployeeID\n"
            "sum: UnitPrice * Quantity * (1 - Discount) as revenue\n"
            "page-size: 5\n\n#? Orders"
        )
        assert "revenue:" in result

    def test_simple_alias(self, nw: SQLTranslator) -> None:
        """sum: col as alias in non-join aggregation uses the alias."""
        result = nw.read(
            "group: EmployeeID\n"
            "sum: Freight as total_freight\n"
            "page-size: 5\n\n#? Orders"
        )
        assert "total_freight:" in result

    def test_aggregation_with_pagination(
        self, nw: SQLTranslator
    ) -> None:
        """page-size applies to aggregated result set."""
        result = nw.read(
            "group: ShipCountry\ncount\npage-size: 3\n\n#? Orders"
        )
        assert "page-size: 3" in result
        assert result.count("\n- ") == 3


# ---------------------------------------------------------------------------
# 8. TestJoins — join: frontmatter
# ---------------------------------------------------------------------------


class TestJoins:
    """Tests for join: frontmatter key."""

    def test_simple_join(self, nw: SQLTranslator) -> None:
        """join: fetches columns from both tables."""
        result = nw.read(
            "select: OrderID, CompanyName\n"
            "join: Customers on CustomerID\n"
            "page-size: 3\n\n#? Orders"
        )
        assert "OrderID:" in result
        assert "CompanyName:" in result

    def test_join_with_aggregation(self, nw: SQLTranslator) -> None:
        """join: composes with aggregation frontmatter."""
        result = nw.read(
            "join: Order Details on OrderID\n"
            "sum: UnitPrice * Quantity * (1 - Discount) as revenue\n"
            "group: EmployeeID\n"
            "sort: revenue desc\n"
            "page-size: 3\n\n#? Orders"
        )
        assert "revenue:" in result
        assert "EmployeeID:" in result
        assert "page-size: 3" in result


# ---------------------------------------------------------------------------
# 9. TestWriteData — # Label insert/replace
# ---------------------------------------------------------------------------


class TestWriteData:
    """Tests for data-mode write (# Label)."""

    def test_insert_new_row(self, nw_rw: SQLTranslator) -> None:
        """Inserting a new row returns the written record."""
        result = nw_rw.write(
            "# Orders\nOrderID: 99999\nShipCountry: Testland\nFreight: 1.5"
        )
        assert "# Orders" in result
        assert "OrderID: 99999" in result
        assert "ShipCountry: Testland" in result

    def test_inserted_row_is_readable(self, nw_rw: SQLTranslator) -> None:
        """After insert the row can be read back."""
        nw_rw.write(
            "# Orders\nOrderID: 99999\nShipCountry: Testland\nFreight: 1.5"
        )
        result = nw_rw.read("# Orders\nOrderID: 99999")
        assert "ShipCountry: Testland" in result

    def test_replace_existing_row(self, nw_rw: SQLTranslator) -> None:
        """Writing an existing PK replaces the row."""
        nw_rw.write("# Orders\nOrderID: 10248\nShipCountry: Replaced")
        result = nw_rw.read("# Orders\nOrderID: 10248")
        assert "ShipCountry: Replaced" in result

    def test_write_to_view_returns_error(
        self, nw_rw: SQLTranslator
    ) -> None:
        """Writing to a view returns a read_only Error document."""
        result = nw_rw.write(
            "# Current Product List\nProductID: 1"
        )
        assert "# Error" in result
        assert "read_only" in result


# ---------------------------------------------------------------------------
# 10 + 12. TestSchemaLifecycle — CREATE → ALTER → INSERT → READ → DROP
# ---------------------------------------------------------------------------


class TestSchemaLifecycle:
    """Tests for schema write (#! create/alter) and delete (#! drop)."""

    def test_create_table(self, empty: SQLTranslator) -> None:
        """#! write creates a new table."""
        result = empty.write(
            "#! Widgets\nid: integer readonly\nname: string\nprice: float optional"
        )
        assert "created" in result or "Widgets" in result

    def test_created_table_schema_readable(
        self, empty: SQLTranslator
    ) -> None:
        """After creation #! read returns the column schema."""
        empty.write(
            "#! Widgets\nid: integer readonly\nname: string\nprice: float optional"
        )
        schema = empty.read("#! Widgets")
        assert "id: integer readonly" in schema
        assert "name: string" in schema
        assert "price: float optional" in schema

    def test_alter_adds_column(self, empty: SQLTranslator) -> None:
        """Writing #! on existing table adds new columns."""
        empty.write(
            "#! Widgets\nid: integer readonly\nname: string"
        )
        empty.write(
            "#! Widgets\nid: integer readonly\nname: string\ncolor: string optional"
        )
        schema = empty.read("#! Widgets")
        assert "color: string optional" in schema

    def test_alter_does_not_remove_existing_column(
        self, empty: SQLTranslator
    ) -> None:
        """Altering a table never drops existing columns."""
        empty.write(
            "#! Widgets\nid: integer readonly\nname: string"
        )
        empty.write(
            "#! Widgets\nid: integer readonly\ncolor: string optional"
        )
        schema = empty.read("#! Widgets")
        assert "name: string" in schema

    def test_insert_into_created_table(
        self, empty: SQLTranslator
    ) -> None:
        """Rows can be inserted into a table created via #!."""
        empty.write(
            "#! Widgets\nid: integer readonly\nname: string"
        )
        result = empty.write("# Widgets\nid: 1\nname: Sprocket")
        assert "Widgets" in result
        assert "Sprocket" in result

    def test_drop_table(self, empty: SQLTranslator) -> None:
        """#! delete drops the table."""
        empty.write(
            "#! Widgets\nid: integer readonly\nname: string"
        )
        result = empty.delete("#! Widgets")
        assert "dropped" in result or "Widgets" in result

    def test_dropped_table_is_gone(self, empty: SQLTranslator) -> None:
        """After DROP, reading the table schema raises ValueError."""
        empty.write(
            "#! Widgets\nid: integer readonly\nname: string"
        )
        empty.delete("#! Widgets")
        with pytest.raises(ValueError, match="Widgets"):
            empty.read("#! Widgets")

    def test_drop_nonexistent_table_returns_error(
        self, empty: SQLTranslator
    ) -> None:
        """Dropping a table that does not exist returns a 404 Error."""
        result = empty.delete("#! NoSuchTable")
        assert "# Error" in result
        assert "404" in result


# ---------------------------------------------------------------------------
# 11. TestDeleteData — #- Label
# ---------------------------------------------------------------------------


class TestDeleteData:
    """Tests for delete-mode documents (#- Label)."""

    def test_delete_matching_row(self, nw_rw: SQLTranslator) -> None:
        """Deleting a row by PK returns the deleted record as a data document."""
        nw_rw.write(
            "# Orders\nOrderID: 88888\nShipCountry: Deleteme"
        )
        result = nw_rw.delete("#- Orders\nOrderID: 88888")
        assert "# Orders" in result
        assert "88888" in result
        assert "Deleteme" in result

    def test_delete_removes_row(self, nw_rw: SQLTranslator) -> None:
        """After deletion the row is no longer readable."""
        nw_rw.write(
            "# Orders\nOrderID: 88888\nShipCountry: Deleteme"
        )
        nw_rw.delete("#- Orders\nOrderID: 88888")
        result = nw_rw.read("# Orders\nOrderID: 88888")
        assert "not_found" in result

    def test_delete_no_match_returns_404(
        self, nw_rw: SQLTranslator
    ) -> None:
        """Deleting a non-existent row returns a 404 Error."""
        result = nw_rw.delete("#- Orders\nOrderID: 99999")
        assert "# Error" in result
        assert "404" in result

    def test_delete_without_filter_returns_error(
        self, nw_rw: SQLTranslator
    ) -> None:
        """Delete without any filter fields returns a bad_request Error."""
        result = nw_rw.delete("#- Orders")
        assert "# Error" in result
        assert "bad_request" in result

    def test_delete_from_view_returns_error(
        self, nw_rw: SQLTranslator
    ) -> None:
        """Deleting from a view returns a read_only Error."""
        result = nw_rw.delete("#- Current Product List\nProductID: 1")
        assert "# Error" in result
        assert "read_only" in result


# ---------------------------------------------------------------------------
# 13. TestReadSchema — #! Label (PRAGMA describe)
# ---------------------------------------------------------------------------


class TestReadSchema:
    """Tests for schema-mode read (#! Label)."""

    def test_returns_schema_document(self, nw: SQLTranslator) -> None:
        """#! read returns a schema document."""
        result = nw.read("#! Shippers")
        assert result.startswith("#! Shippers")

    def test_primary_key_has_readonly(self, nw: SQLTranslator) -> None:
        """Primary key columns are marked readonly."""
        result = nw.read("#! Shippers")
        assert "ShipperID: integer readonly" in result

    def test_nullable_column_has_optional(
        self, nw: SQLTranslator
    ) -> None:
        """Nullable columns are marked optional."""
        result = nw.read("#! Shippers")
        assert "Phone: string optional" in result

    def test_not_null_column_has_no_optional(
        self, nw: SQLTranslator
    ) -> None:
        """NOT NULL columns do not have the optional modifier."""
        result = nw.read("#! Shippers")
        # CompanyName is NOT NULL
        assert "CompanyName: string\n" in result or \
               result.endswith("CompanyName: string")

    def test_column_types_mapped_correctly(
        self, nw: SQLTranslator
    ) -> None:
        """SQLite types are mapped to JMD types."""
        result = nw.read("#! Orders")
        assert "OrderID: integer" in result
        assert "Freight: float" in result
        assert "ShipCountry: string" in result
