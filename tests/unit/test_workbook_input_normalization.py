from __future__ import annotations

from typing import Any

import pytest

from ade_engine.infrastructure.io import workbook as workbook_io
from ade_engine.infrastructure.settings import Settings
from ade_engine.models.errors import InputError


class _FakeXlsCell:
    def __init__(self, ctype: int, value: Any):
        self.ctype = ctype
        self.value = value


class _FakeXlsSheet:
    def __init__(self, name: str, rows: list[list[_FakeXlsCell]]):
        self.name = name
        self._rows = rows
        self.nrows = len(rows)
        self.ncols = len(rows[0]) if rows else 0

    def cell(self, row_index: int, col_index: int) -> _FakeXlsCell:
        return self._rows[row_index][col_index]


class _FakeXlsBook:
    def __init__(self, sheets: list[_FakeXlsSheet]):
        self._sheets = sheets
        self.datemode = 0

    def sheets(self) -> list[_FakeXlsSheet]:
        return self._sheets


class _FakePdfPage:
    def __init__(self, tables: list[list[list[str | None]]]):
        self._tables = tables

    def extract_tables(self) -> list[list[list[str | None]]]:
        return self._tables


class _FakePdf:
    def __init__(self, pages: list[_FakePdfPage]):
        self.pages = pages

    def __enter__(self) -> "_FakePdf":
        return self

    def __exit__(self, *_: Any) -> bool:
        return False


def test_load_source_workbook_xls_converts_to_openpyxl_workbook(tmp_path, monkeypatch):
    path = tmp_path / "input.xls"
    path.write_bytes(b"fake-xls")

    fake_xlrd = type("FakeXlrd", (), {})()
    fake_xlrd.XL_CELL_EMPTY = 0
    fake_xlrd.XL_CELL_TEXT = 1
    fake_xlrd.XL_CELL_NUMBER = 2
    fake_xlrd.XL_CELL_DATE = 3
    fake_xlrd.XL_CELL_BOOLEAN = 4
    fake_xlrd.XL_CELL_ERROR = 5
    fake_xlrd.XL_CELL_BLANK = 6
    fake_xlrd.xldate_as_datetime = lambda *_: None
    fake_xlrd.open_workbook = lambda **_: _FakeXlsBook(
        sheets=[
            _FakeXlsSheet(
                "SheetA",
                [
                    [_FakeXlsCell(fake_xlrd.XL_CELL_TEXT, "Name"), _FakeXlsCell(fake_xlrd.XL_CELL_TEXT, "Email")],
                    [_FakeXlsCell(fake_xlrd.XL_CELL_TEXT, "Alice"), _FakeXlsCell(fake_xlrd.XL_CELL_TEXT, "a@example.com")],
                ],
            ),
            _FakeXlsSheet(
                "SheetB",
                [[_FakeXlsCell(fake_xlrd.XL_CELL_NUMBER, 3.0)]],
            ),
        ]
    )

    monkeypatch.setattr(workbook_io, "_import_optional_module", lambda name: fake_xlrd if name == "xlrd" else None)

    workbook, meta = workbook_io.load_source_workbook(path)
    assert workbook.sheetnames == ["SheetA", "SheetB"]
    assert workbook["SheetA"]["A2"].value == "Alice"
    assert workbook["SheetB"]["A1"].value == 3
    assert meta.source_format == ".xls"
    assert meta.adapter == "xlrd"
    workbook.close()


def test_load_source_workbook_pdf_extracts_tables(tmp_path, monkeypatch):
    path = tmp_path / "input.pdf"
    path.write_bytes(b"%PDF-1.4 fake")

    fake_pdfplumber = type("FakePdfPlumber", (), {})()
    fake_pdfplumber.open = lambda *_: _FakePdf(
        pages=[
            _FakePdfPage(
                tables=[
                    [["Name", "Email"], ["Alice", "a@example.com"]],
                    [["Code"], ["X1"]],
                ]
            ),
            _FakePdfPage(tables=[]),
        ]
    )
    monkeypatch.setattr(
        workbook_io,
        "_import_optional_module",
        lambda name: fake_pdfplumber if name == "pdfplumber" else None,
    )

    workbook, meta = workbook_io.load_source_workbook(path, settings=Settings())
    assert workbook.sheetnames == ["Page_1_Table_1", "Page_1_Table_2"]
    assert workbook["Page_1_Table_1"]["A2"].value == "Alice"
    assert meta.source_format == ".pdf"
    assert meta.table_count == 2
    assert meta.page_count == 2
    workbook.close()


def test_load_source_workbook_pdf_no_tables_raises_when_enabled(tmp_path, monkeypatch):
    path = tmp_path / "input.pdf"
    path.write_bytes(b"%PDF-1.4 fake")

    fake_pdfplumber = type("FakePdfPlumber", (), {})()
    fake_pdfplumber.open = lambda *_: _FakePdf(pages=[_FakePdfPage(tables=[]), _FakePdfPage(tables=[])])
    monkeypatch.setattr(
        workbook_io,
        "_import_optional_module",
        lambda name: fake_pdfplumber if name == "pdfplumber" else None,
    )

    with pytest.raises(InputError, match="No tabular data detected in PDF input"):
        workbook_io.load_source_workbook(path, settings=Settings(pdf_fail_if_no_tables=True))


def test_load_source_workbook_xls_missing_dependency_raises_input_error(tmp_path, monkeypatch):
    path = tmp_path / "input.xls"
    path.write_bytes(b"fake-xls")

    def _raise_import_error(_: str) -> Any:
        raise InputError(
            "Input format requires optional dependency 'xlrd'. Install engine dependencies (for example: pip install -e .)"
        )

    monkeypatch.setattr(workbook_io, "_import_optional_module", _raise_import_error)

    with pytest.raises(InputError, match="optional dependency 'xlrd'"):
        workbook_io.load_source_workbook(path)
