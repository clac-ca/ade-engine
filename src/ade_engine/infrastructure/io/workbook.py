"""Workbook IO helpers for :class:`~ade_engine.application.engine.Engine`."""

from __future__ import annotations

import csv
import importlib
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any

import openpyxl
from openpyxl import Workbook

from ade_engine.infrastructure.settings import Settings
from ade_engine.models.errors import InputError, InputNormalizationError
from ade_engine.models.input_normalization import InputNormalizationMetadata


def _import_optional_module(name: str) -> Any:
    """Import an optional dependency and raise an InputError when unavailable."""

    try:
        return importlib.import_module(name)
    except ImportError as exc:
        raise InputNormalizationError(
            f"Input format requires optional dependency '{name}'. "
            f"Install engine dependencies (for example: pip install -e .)"
        ) from exc


def _create_workbook_without_default_sheet() -> Workbook:
    workbook = Workbook()
    if workbook.worksheets:
        workbook.remove(workbook.worksheets[0])
    return workbook


def _create_unique_sheet(workbook: Workbook, preferred_title: str) -> Any:
    base = (preferred_title or "Sheet").strip() or "Sheet"
    base = base[:31]
    title = base
    idx = 2
    while title in workbook.sheetnames:
        suffix = f"_{idx}"
        title = f"{base[: max(1, 31 - len(suffix))]}{suffix}"
        idx += 1
    return workbook.create_sheet(title=title)


def _coerce_xls_cell(*, xlrd: Any, cell: Any, datemode: int, warnings: list[str]) -> Any:
    if cell.ctype in {xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK}:
        return None
    if cell.ctype == xlrd.XL_CELL_TEXT:
        return cell.value
    if cell.ctype == xlrd.XL_CELL_NUMBER:
        value = cell.value
        if isinstance(value, float) and value.is_integer():
            return int(value)
        return value
    if cell.ctype == xlrd.XL_CELL_DATE:
        try:
            return xlrd.xldate_as_datetime(cell.value, datemode)
        except Exception:
            warnings.append("Encountered an .xls date cell that could not be converted; keeping raw value.")
            return cell.value
    if cell.ctype == xlrd.XL_CELL_BOOLEAN:
        return bool(cell.value)
    if cell.ctype == xlrd.XL_CELL_ERROR:
        return None
    return cell.value


def _load_xls_workbook(path: Path) -> tuple[Workbook, InputNormalizationMetadata]:
    xlrd = _import_optional_module("xlrd")
    warnings: list[str] = []
    try:
        source = xlrd.open_workbook(filename=str(path), formatting_info=False)
    except Exception as exc:
        raise InputNormalizationError(f"Failed to read .xls input: {path}") from exc

    workbook = _create_workbook_without_default_sheet()
    for index, sheet in enumerate(source.sheets(), start=1):
        title = sheet.name if str(sheet.name).strip() else f"Sheet{index}"
        ws = _create_unique_sheet(workbook, title)
        for row_index in range(sheet.nrows):
            values = [
                _coerce_xls_cell(
                    xlrd=xlrd,
                    cell=sheet.cell(row_index, col_index),
                    datemode=source.datemode,
                    warnings=warnings,
                )
                for col_index in range(sheet.ncols)
            ]
            ws.append(values)

    if not workbook.worksheets:
        _create_unique_sheet(workbook, "Sheet1")

    return workbook, InputNormalizationMetadata(
        source_format=".xls",
        normalized_format="workbook",
        adapter="xlrd",
        warnings=tuple(dict.fromkeys(warnings)),
    )


def _normalize_pdf_rows(rows: list[list[Any]]) -> list[list[Any]]:
    normalized: list[list[Any]] = []
    max_cols = 0
    for row in rows:
        cleaned = [None if cell is None else str(cell).strip() for cell in row]
        if any((cell is not None and str(cell).strip()) for cell in cleaned):
            normalized.append(cleaned)
            max_cols = max(max_cols, len(cleaned))
    if max_cols == 0:
        return []
    return [row + [None] * (max_cols - len(row)) for row in normalized]


def _load_pdf_workbook(path: Path, *, settings: Settings) -> tuple[Workbook, InputNormalizationMetadata]:
    extractor = settings.pdf_table_extractor
    if extractor not in {"auto", "pdfplumber"}:
        raise InputNormalizationError(f"Unsupported pdf_table_extractor setting: {extractor}")
    pdfplumber = _import_optional_module("pdfplumber")
    workbook = _create_workbook_without_default_sheet()
    warnings: list[str] = []
    table_count = 0
    page_count = 0

    try:
        with pdfplumber.open(str(path)) as pdf:
            pages = list(pdf.pages)
            page_count = len(pages)
            for page_index, page in enumerate(pages, start=1):
                extracted = page.extract_tables() or []
                cleaned_tables = []
                for table in extracted:
                    if not table:
                        continue
                    normalized_rows = _normalize_pdf_rows(table)
                    if normalized_rows:
                        cleaned_tables.append(normalized_rows)

                if not cleaned_tables:
                    continue

                for table_index, rows in enumerate(cleaned_tables, start=1):
                    title = f"Page_{page_index}" if len(cleaned_tables) == 1 else f"Page_{page_index}_Table_{table_index}"
                    ws = _create_unique_sheet(workbook, title)
                    for row in rows:
                        ws.append(row)
                    table_count += 1
    except InputNormalizationError:
        raise
    except Exception as exc:
        raise InputNormalizationError(f"Failed to extract tables from PDF input: {path}") from exc

    if table_count == 0:
        if settings.pdf_fail_if_no_tables:
            raise InputNormalizationError(f"No tabular data detected in PDF input: {path}")
        warnings.append("No tables detected in PDF; created an empty worksheet.")
        _create_unique_sheet(workbook, "Page_1")

    return workbook, InputNormalizationMetadata(
        source_format=".pdf",
        normalized_format="workbook",
        adapter="pdfplumber",
        warnings=tuple(dict.fromkeys(warnings)),
        page_count=page_count,
        table_count=table_count,
    )


def load_source_workbook(path: Path, *, settings: Settings | None = None) -> tuple[Workbook, InputNormalizationMetadata]:
    """Load source data and normalize it into a workbook plus metadata."""

    cfg = settings or Settings()
    suffix = path.suffix.lower()

    if suffix == ".csv":
        wb = Workbook()
        ws = wb.active
        if ws is None:
            raise InputError("Failed to initialize worksheet for CSV input")
        ws.title = path.stem
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.reader(handle):
                ws.append(row)
        return wb, InputNormalizationMetadata(
            source_format=".csv",
            normalized_format="workbook",
            adapter="csv.reader",
            warnings=(),
        )
    if suffix == ".xls":
        return _load_xls_workbook(path)
    if suffix == ".pdf":
        return _load_pdf_workbook(path, settings=cfg)

    if suffix in {".xlsx", ".xlsm"}:
        try:
            workbook = openpyxl.load_workbook(filename=path, read_only=True, data_only=True)
        except Exception as exc:
            raise InputNormalizationError(f"Failed to read workbook input: {path}") from exc
        return workbook, InputNormalizationMetadata(
            source_format=suffix,
            normalized_format="workbook",
            adapter="openpyxl",
            warnings=(),
        )

    raise InputNormalizationError(f"Unsupported input format: {path.suffix or '<none>'}")


@contextmanager
def open_source_workbook(path: Path, *, settings: Settings | None = None):
    """Context manager for safely opening source workbooks."""

    workbook, normalization = load_source_workbook(path, settings=settings)
    try:
        yield workbook, normalization
    finally:
        with suppress(Exception):
            workbook.close()


def create_output_workbook() -> Workbook:
    """Create a clean output workbook with no default sheet."""

    return _create_workbook_without_default_sheet()


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
            return []
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
