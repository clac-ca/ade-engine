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
    assert table_summary.counts.columns.total == 2
    assert table_summary.counts.columns.mapped == 2
    assert table_summary.counts.fields.detected == 2
    assert len(table_summary.structure.columns) == 1

    fields = {field.field: field for field in table_summary.fields}
    assert fields["postal_code"].detected is True
    assert fields["postal_code"].derived is True
    assert fields["postal_code"].source_headers == ["Address Line 1"]
    assert fields["postal_code"].occurrences.columns == 1

    run_fields = {field.field: field for field in payload.fields}
    assert payload.counts.fields.detected == 2
    assert run_fields["postal_code"].detected is True
    assert run_fields["postal_code"].derived is True
    assert run_fields["postal_code"].source_headers == ["Address Line 1"]


def test_run_completion_report_counts_derived_validation_against_source_column() -> None:
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
    # Both derived validators fail on the second row, but the physical source
    # cell should only be counted invalid once.
    assert table_summary.structure.columns[0].valid_cells == 1
