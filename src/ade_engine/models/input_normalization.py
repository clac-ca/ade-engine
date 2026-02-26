"""Contracts for input normalization metadata."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class InputNormalizationMetadata:
    """Facts about source input normalization before pipeline execution."""

    source_format: str
    normalized_format: str
    adapter: str
    warnings: tuple[str, ...] = ()
    page_count: int | None = None
    table_count: int | None = None


__all__ = ["InputNormalizationMetadata"]
