from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from ade_engine.application.run_completion_report import RunCompletionReportBuilder
from ade_engine.extensions.registry import Registry
from ade_engine.infrastructure.settings import Settings
from ade_engine.models.extension_contexts import FieldDef
from ade_engine.models.run import RunStatus
from ade_engine.models.table import DerivedMapping, MappedColumn, SourceColumn, TableRegion, TableResult


def test_run_completion_report_allows_hooks_to_filter_rows() -> None:
    """Hooks can legally filter rows post-validation; reporting must not crash."""

    builder = RunCompletionReportBuilder(input_file=Path("input.xlsx"), settings=Settings())

    source_columns = [
        SourceColumn(index=0, header="First Name", values=["Alice", "Bob", None, "Zoe"]),
        SourceColumn(index=1, header="Last Name", values=["Smith", "Jones", None, "Zeta"]),
        SourceColumn(index=2, header="Email", values=["alice@example.com", "bob.example.com", "no@example.com", "z@example.com"]),
    ]

    # Simulate a hook filtering the table down to 1 row, but keep the original
    # detected source columns (values for all 4 source rows).
    filtered_table = pl.DataFrame({"first_name": ["Alice"], "last_name": ["Smith"], "email": ["alice@example.com"]})

    builder.record_table(
        TableResult(
            sheet_name="Sheet1",
            sheet_index=0,
            table_index=0,
            source_region=TableRegion(min_row=1, min_col=1, max_row=5, max_col=len(source_columns)),
            source_columns=source_columns,
            table=filtered_table,
            row_count=filtered_table.height,
        )
    )

    payload = builder.build(
        run_status=RunStatus.SUCCEEDED,
        started_at=datetime.now(timezone.utc),
        completed_at=datetime.now(timezone.utc),
        error=None,
        output_path=None,
        output_written=False,
    )

    table_summary = payload.workbooks[0].sheets[0].tables[0]
    assert table_summary.counts.cells is not None
    assert table_summary.counts.cells.total == 12
    assert table_summary.counts.cells.non_empty == 10


def test_run_completion_report_counts_derived_mappings_without_synthetic_columns() -> None:
    builder = RunCompletionReportBuilder(input_file=Path("input.xlsx"), settings=Settings())
    registry = Registry()
    registry.register_field(FieldDef(name="address_line1", label="Address Line 1"))
    registry.register_field(FieldDef(name="postal_code", label="Postal Code"))
    builder.set_registry(registry)

    source_columns = [
        SourceColumn(index=0, header="Address Line 1", values=["123 Main St V1A 2B3"]),
    ]

    table = pl.DataFrame(
        {
            "address_line1": ["123 Main St"],
            "postal_code": ["V1A 2B3"],
        }
    )

    builder.record_table(
        TableResult(
            sheet_name="Sheet1",
            sheet_index=0,
            table_index=0,
            source_region=TableRegion(min_row=1, min_col=1, max_row=2, max_col=1),
            source_columns=source_columns,
            table=table,
            row_count=table.height,
            mapped_columns=[
                MappedColumn(
                    field_name="address_line1",
                    source_index=0,
                    header="Address Line 1",
                    values=["123 Main St V1A 2B3"],
                    score=0.92,
                )
            ],
            derived_mappings=[
                DerivedMapping(
                    field_name="postal_code",
                    source_header="Address Line 1",
                    source_index=0,
                    score=1.0,
                )
            ],
        )
    )

    payload = builder.build(
        run_status=RunStatus.SUCCEEDED,
        started_at=datetime.now(timezone.utc),
        completed_at=datetime.now(timezone.utc),
        error=None,
        output_path=None,
        output_written=False,
    )

    table_summary = payload.workbooks[0].sheets[0].tables[0]
    assert table_summary.counts.columns.total == 1
    assert table_summary.counts.columns.mapped == 1
    assert table_summary.counts.fields.detected == 2
    assert len(table_summary.structure.columns) == 1

    fields = {field.field: field for field in table_summary.fields}
    assert fields["postal_code"].detected is True
    assert fields["postal_code"].derived is True
    assert fields["postal_code"].source_headers == ["Address Line 1"]
    assert fields["postal_code"].occurrences.columns == 1
    assert fields["postal_code"].valid_cells == 1

    run_fields = {field.field: field for field in payload.fields}
    assert payload.counts.fields.detected == 2
    assert run_fields["postal_code"].detected is True
    assert run_fields["postal_code"].derived is True
    assert run_fields["postal_code"].source_headers == ["Address Line 1"]
    assert run_fields["postal_code"].valid_cells == 1


def test_run_completion_report_maps_against_non_empty_source_columns() -> None:
    builder = RunCompletionReportBuilder(input_file=Path("input.xlsx"), settings=Settings())

    source_columns = [
        SourceColumn(index=0, header="Email", values=["alice@example.com", "bob@example.com"]),
        SourceColumn(index=1, header="Notes", values=["keep", "review"]),
        SourceColumn(index=2, header=None, values=[None, ""]),
    ]

    table = pl.DataFrame(
        {
            "email": ["alice@example.com", "bob@example.com"],
            "Notes": ["keep", "review"],
            "col_3": [None, ""],
        }
    )

    builder.record_table(
        TableResult(
            sheet_name="Sheet1",
            sheet_index=0,
            table_index=0,
            source_region=TableRegion(min_row=1, min_col=1, max_row=3, max_col=3),
            source_columns=source_columns,
            table=table,
            row_count=table.height,
            mapped_columns=[
                MappedColumn(
                    field_name="email",
                    source_index=0,
                    header="Email",
                    values=["alice@example.com", "bob@example.com"],
                    score=1.0,
                )
            ],
        )
    )

    payload = builder.build(
        run_status=RunStatus.SUCCEEDED,
        started_at=datetime.now(timezone.utc),
        completed_at=datetime.now(timezone.utc),
        error=None,
        output_path=None,
        output_written=False,
    )

    columns = payload.workbooks[0].sheets[0].tables[0].counts.columns
    assert columns.total == 3
    assert columns.empty == 1
    assert columns.mapped == 1
    assert columns.unmapped == 1


def test_run_completion_report_counts_valid_cells_on_final_fields() -> None:
    builder = RunCompletionReportBuilder(input_file=Path("input.xlsx"), settings=Settings())
    registry = Registry()
    registry.register_field(FieldDef(name="full_name", label="Full Name"))
    registry.register_field(FieldDef(name="first_name", label="First Name"))
    registry.register_field(FieldDef(name="middle_name", label="Middle Name"))
    registry.register_field(FieldDef(name="last_name", label="Last Name"))
    builder.set_registry(registry)

    source_columns = [
        SourceColumn(
            index=0,
            header="Full Name",
            values=[
                "Alice Beth Smith",
                "Brian Carl Jones",
                "Cara Dana Brown",
                "Devin Evan Green",
                "Ella Faye White",
                "Finn Gray Black",
                "Gina Hope Stone",
            ],
        ),
    ]

    table = pl.DataFrame(
        {
            "full_name": source_columns[0].values,
            "first_name": ["Alice", "Brian", "Cara", "Devin", "Ella", "Finn", "Gina"],
            "middle_name": ["Beth", "Carl", "Dana", "Evan", "Faye", "Gray", "Hope"],
            "last_name": ["Smith", "Jones", "Brown", "Green", "White", "Black", "Stone"],
        }
    )

    builder.record_table(
        TableResult(
            sheet_name="Sheet1",
            sheet_index=0,
            table_index=0,
            source_region=TableRegion(min_row=1, min_col=1, max_row=8, max_col=1),
            source_columns=source_columns,
            table=table,
            row_count=table.height,
            mapped_columns=[
                MappedColumn(
                    field_name="full_name",
                    source_index=0,
                    header="Full Name",
                    values=source_columns[0].values,
                    score=1.0,
                )
            ],
            derived_mappings=[
                DerivedMapping(field_name="first_name", source_header="Full Name", source_index=0, score=1.0),
                DerivedMapping(field_name="middle_name", source_header="Full Name", source_index=0, score=1.0),
                DerivedMapping(field_name="last_name", source_header="Full Name", source_index=0, score=1.0),
            ],
        )
    )

    payload = builder.build(
        run_status=RunStatus.SUCCEEDED,
        started_at=datetime.now(timezone.utc),
        completed_at=datetime.now(timezone.utc),
        error=None,
        output_path=None,
        output_written=False,
    )

    fields = {field.field: field for field in payload.fields}
    assert fields["full_name"].valid_cells == 7
    assert fields["first_name"].valid_cells == 7
    assert fields["middle_name"].valid_cells == 7
    assert fields["last_name"].valid_cells == 7


def test_run_completion_report_counts_column_valid_cells_from_final_field_values() -> None:
    builder = RunCompletionReportBuilder(input_file=Path("input.xlsx"), settings=Settings())

    source_columns = [
        SourceColumn(index=0, header="Email", values=["alice@example.com", "not an email"]),
    ]

    table = pl.DataFrame({"email": ["alice@example.com", None]})

    builder.record_table(
        TableResult(
            sheet_name="Sheet1",
            sheet_index=0,
            table_index=0,
            source_region=TableRegion(min_row=1, min_col=1, max_row=3, max_col=1),
            source_columns=source_columns,
            table=table,
            row_count=table.height,
            mapped_columns=[
                MappedColumn(
                    field_name="email",
                    source_index=0,
                    header="Email",
                    values=source_columns[0].values,
                    score=1.0,
                )
            ],
        )
    )

    payload = builder.build(
        run_status=RunStatus.SUCCEEDED,
        started_at=datetime.now(timezone.utc),
        completed_at=datetime.now(timezone.utc),
        error=None,
        output_path=None,
        output_written=False,
    )

    column = payload.workbooks[0].sheets[0].tables[0].structure.columns[0]
    assert column.non_empty_cells == 2
    assert column.valid_cells == 1


def test_run_completion_report_counts_mapped_column_validation_on_final_field_only() -> None:
    builder = RunCompletionReportBuilder(input_file=Path("input.xlsx"), settings=Settings())
    registry = Registry()
    registry.register_field(FieldDef(name="full_name", label="Full Name"))
    registry.register_field(FieldDef(name="first_name", label="First Name"))
    registry.register_field(FieldDef(name="last_name", label="Last Name"))
    builder.set_registry(registry)

    source_columns = [
        SourceColumn(index=0, header="Full Name", values=["Alice Smith", "Bad Name"]),
    ]

    table = pl.DataFrame(
        {
            "full_name": ["Alice Smith", "Bad Name"],
            "first_name": ["Alice", None],
            "last_name": ["Smith", None],
            "__ade_issue__first_name": [None, "missing first name"],
            "__ade_issue__last_name": [None, "missing last name"],
        }
    )

    builder.record_table(
        TableResult(
            sheet_name="Sheet1",
            sheet_index=0,
            table_index=0,
            source_region=TableRegion(min_row=1, min_col=1, max_row=3, max_col=1),
            source_columns=source_columns,
            table=table,
            row_count=table.height,
            mapped_columns=[
                MappedColumn(
                    field_name="full_name",
                    source_index=0,
                    header="Full Name",
                    values=["Alice Smith", "Bad Name"],
                    score=0.98,
                )
            ],
            derived_mappings=[
                DerivedMapping(field_name="first_name", source_header="Full Name", source_index=0, score=1.0),
                DerivedMapping(field_name="last_name", source_header="Full Name", source_index=0, score=1.0),
            ],
        )
    )

    payload = builder.build(
        run_status=RunStatus.SUCCEEDED,
        started_at=datetime.now(timezone.utc),
        completed_at=datetime.now(timezone.utc),
        error=None,
        output_path=None,
        output_written=False,
    )

    table_summary = payload.workbooks[0].sheets[0].tables[0]
    assert len(table_summary.structure.columns) == 1
    assert table_summary.structure.columns[0].header.raw == "Full Name"
    assert table_summary.structure.columns[0].valid_cells == 2
