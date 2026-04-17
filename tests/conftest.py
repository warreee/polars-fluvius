"""Pytest configuration for polars-fluvius tests."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_mandate() -> MagicMock:
    """Create a mock mandate object."""
    mandate = MagicMock()
    mandate.reference_number = "test-ref-001"
    mandate.status = "Approved"
    mandate.ean = "541234567890123456"
    mandate.energy_type = "E"
    mandate.data_period_from = datetime(2024, 1, 1, tzinfo=timezone.utc)
    mandate.data_period_to = datetime(2024, 12, 31, tzinfo=timezone.utc)
    mandate.data_service_type = "VH_dag"
    mandate.mandate_expiration_date = datetime(2025, 1, 1, tzinfo=timezone.utc)
    mandate.renewal_status = "ToBeRenewed"
    return mandate


@pytest.fixture
def mock_mandates_response(mock_mandate: MagicMock) -> MagicMock:
    """Create a mock mandates API response."""
    response = MagicMock()
    response.data.mandates = [mock_mandate]
    return response


@pytest.fixture
def mock_energy_response():
    """Create a real Pydantic energy API response for testing.

    Uses model_validate() so that isinstance() checks in
    parse_energy_response() work correctly.
    """
    from fluvius_energy_api import GetEnergyResponseApiDataResponse

    return GetEnergyResponseApiDataResponse.model_validate(
        {
            "data": {
                "headpoint": {
                    "$type": "metering-on-headpoint",
                    "ean": "541234567890123456",
                    "energyType": "E",
                    "dailyEnergy": [
                        {
                            "start": "2024-01-01T00:00:00Z",
                            "end": "2024-01-02T00:00:00Z",
                            "measurements": [
                                {
                                    "offtake": {
                                        "total": {
                                            "value": 123.45,
                                            "unit": "kWh",
                                            "validationState": "VAL",
                                        }
                                    }
                                }
                            ],
                        }
                    ],
                }
            }
        }
    )
