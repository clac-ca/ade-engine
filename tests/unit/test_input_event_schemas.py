from __future__ import annotations

import pytest
from pydantic import ValidationError

from ade_engine.models.events import (
    ENGINE_EVENT_SCHEMAS,
    InputNormalizationFailedPayloadV1,
    InputNormalizedPayloadV1,
    InputPdfTablesDetectedPayloadV1,
)


def test_engine_event_schema_registry_includes_input_normalization_events():
    assert ENGINE_EVENT_SCHEMAS["engine.input.normalized"] is InputNormalizedPayloadV1
    assert ENGINE_EVENT_SCHEMAS["engine.input.pdf.tables_detected"] is InputPdfTablesDetectedPayloadV1
    assert ENGINE_EVENT_SCHEMAS["engine.input.normalization_failed"] is InputNormalizationFailedPayloadV1


def test_input_normalized_payload_requires_expected_fields():
    payload = InputNormalizedPayloadV1.model_validate(
        {
            "schema_version": 1,
            "source_format": ".xls",
            "normalized_format": "workbook",
            "adapter": "xlrd",
            "warnings": [],
        },
        strict=True,
    )
    assert payload.source_format == ".xls"

    with pytest.raises(ValidationError):
        InputNormalizedPayloadV1.model_validate(
            {
                "schema_version": 1,
                "source_format": ".xls",
                "adapter": "xlrd",
                "warnings": [],
            },
            strict=True,
        )
