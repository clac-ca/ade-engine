"""Workbook IO helpers for :class:`~ade_engine.application.engine.Engine`."""

from __future__ import annotations

import csv
import math
from contextlib import contextmanager, suppress
from datetime import datetime
from pathlib import Path
from typing import Any

import openpyxl
from openpyxl import Workbook

from ade_engine.models.errors import InputError


def load_source_workbook(path: Path) -> Workbook:
    """Load source data from CSV/XLS/XLSX into a workbook."""

    suffix = path.suffix.lower()
    if suffix == ".csv":
        return _load_csv_workbook(path)
    if suffix == ".xls":
        return _load_xls_workbook(path)
    return _load_openpyxl_workbook(path)


def _load_csv_workbook(path: Path) -> Workbook:
    wb = Workbook()
    ws = wb.active
    if ws is None:
        raise InputError("Failed to initialize worksheet for CSV input")
    ws.title = path.stem
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.reader(handle):
            ws.append(row)
    return wb


def _load_openpyxl_workbook(path: Path) -> Workbook:
    try:
        return openpyxl.load_workbook(filename=path, read_only=True, data_only=True)
    except Exception as exc:  # pragma: no cover - openpyxl owns error types
        raise InputError(f"Failed to open workbook '{path}': {exc}") from exc


def _load_xls_workbook(path: Path) -> Workbook:
    try:
        import xlrd
    except ImportError as exc:  # pragma: no cover - dependency should exist in normal installs
        raise InputError("Failed to open .xls workbook: xlrd is not installed") from exc

    try:
        book = xlrd.open_workbook(
            filename=str(path),
            formatting_info=True,
            on_demand=False,
            ragged_rows=True,
        )
    except Exception as exc:  # pragma: no cover - xlrd owns error types
        raise InputError(f"Failed to open .xls workbook '{path}': {exc}") from exc

    try:
        return _convert_xlrd_book_to_openpyxl(book)
    except InputError:
        raise
    except Exception as exc:  # pragma: no cover - defensive
        raise InputError(f"Failed to convert .xls workbook '{path}': {exc}") from exc


def _convert_xlrd_book_to_openpyxl(book: Any) -> Workbook:
    workbook = Workbook()
    if workbook.worksheets:
        workbook.remove(workbook.worksheets[0])

    visible_sheet_index: int | None = None
    visibility_by_sheet = list(getattr(book, "sheet_visibility", []))

    for sheet_index in range(book.nsheets):
        sheet = book.sheet_by_index(sheet_index)
        ws = workbook.create_sheet(title=sheet.name)

        visibility = visibility_by_sheet[sheet_index] if sheet_index < len(visibility_by_sheet) else 0
        ws.sheet_state = _map_sheet_visibility(visibility)
        if visible_sheet_index is None and ws.sheet_state == "visible":
            visible_sheet_index = sheet_index

        for row_index in range(sheet.nrows):
            row_values = [_convert_xls_cell_value(book, sheet.cell(row_index, col_index)) for col_index in range(sheet.ncols)]
            ws.append(row_values)

        for merged_range in getattr(sheet, "merged_cells", ()):
            rlo, rhi, clo, chi = merged_range
            if rhi - rlo <= 1 and chi - clo <= 1:
                continue
            ws.merge_cells(
                start_row=rlo + 1,
                end_row=rhi,
                start_column=clo + 1,
                end_column=chi,
            )

    if not workbook.worksheets:
        raise InputError("Workbook contains no worksheets")

    workbook.active = visible_sheet_index if visible_sheet_index is not None else 0
    return workbook


def _map_sheet_visibility(visibility: int) -> str:
    if visibility == 1:
        return "hidden"
    if visibility == 2:
        return "veryHidden"
    return "visible"


def _convert_xls_cell_value(book: Any, cell: Any) -> Any:
    try:
        import xlrd
        from xlrd.biffh import error_text_from_code
    except ImportError as exc:  # pragma: no cover - dependency should exist in normal installs
        raise InputError("Failed to convert .xls workbook: xlrd is not installed") from exc

    cell_type = cell.ctype
    value = cell.value

    if cell_type in (xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK):
        return None
    if cell_type == xlrd.XL_CELL_TEXT:
        return str(value)
    if cell_type == xlrd.XL_CELL_BOOLEAN:
        return bool(value)
    if cell_type == xlrd.XL_CELL_ERROR:
        return error_text_from_code.get(value)
    if cell_type == xlrd.XL_CELL_DATE:
        converted = xlrd.xldate_as_datetime(value, book.datemode)
        if isinstance(converted, datetime):
            return converted
        return converted
    if cell_type == xlrd.XL_CELL_NUMBER:
        numeric = float(value)
        if math.isfinite(numeric) and numeric.is_integer():
            return int(numeric)
        return numeric
    return value


@contextmanager
def open_source_workbook(path: Path):
    """Context manager for safely opening source workbooks."""

    workbook = load_source_workbook(path)
    try:
        yield workbook
    finally:
        with suppress(Exception):
            workbook.close()


def create_output_workbook() -> Workbook:
    """Create a clean output workbook with no default sheet."""

    workbook = Workbook()
    if workbook.worksheets:
        workbook.remove(workbook.worksheets[0])
    return workbook


def resolve_sheet_names(
    workbook: Workbook,
    requested: list[str] | None,
    *,
    active_only: bool = False,
) -> list[str]:
    """Determine which sheets to process, preserving source order."""

    visible = [ws.title for ws in workbook.worksheets if getattr(ws, "sheet_state", "visible") == "visible"]
    if active_only:
        if not visible:
            raise InputError("No visible worksheets available")
        active = workbook.active
        active_name = getattr(active, "title", None)
        if not active_name:
            raise InputError("Active worksheet is not available")
        if active_name not in visible:
            raise InputError(f"Active worksheet is hidden: {active_name}")
        return [active_name]
    if not requested:
        return visible

    cleaned = [name.strip() for name in requested if isinstance(name, str) and name.strip()]
    unique_requested = list(dict.fromkeys(cleaned))  # preserve order, drop duplicates

    missing = [name for name in unique_requested if name not in visible]
    if missing:
        raise InputError(f"Worksheet(s) not found: {', '.join(missing)}")

    order_index = {name: idx for idx, name in enumerate(visible)}
    return sorted(unique_requested, key=lambda n: order_index[n])


__all__ = [
    "create_output_workbook",
    "load_source_workbook",
    "open_source_workbook",
    "resolve_sheet_names",
]
