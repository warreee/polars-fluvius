"""Convert mandate models to row tuples for Polars."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fluvius_energy_api.models.mandate import Mandate

MandateTuple = tuple[
    str | None,  # reference_number
    str | None,  # status
    str | None,  # ean
    str | None,  # energy_type
    datetime | None,  # data_period_from
    datetime | None,  # data_period_to
    str | None,  # data_service_type
    datetime | None,  # mandate_expiration_date
    str | None,  # renewal_status
]


def _get_enum_value(value: object) -> str | None:
    """Extract string value from an enum or return string as-is."""
    if value is None:
        return None
    if hasattr(value, "value"):
        return value.value
    return str(value)


def convert_mandate(mandate: Mandate) -> MandateTuple:
    """Convert a Mandate Pydantic model to a tuple.

    Args:
        mandate: The Mandate model from fluvius-energy-api.

    Returns:
        A tuple matching the MANDATES_SCHEMA field order.
    """
    return (
        mandate.reference_number,
        _get_enum_value(mandate.status),
        mandate.ean,
        _get_enum_value(mandate.energy_type),
        mandate.data_period_from,
        mandate.data_period_to,
        _get_enum_value(mandate.data_service_type),
        mandate.mandate_expiration_date,
        _get_enum_value(mandate.renewal_status),
    )