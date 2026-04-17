"""Convert energy API responses to row tuples for Polars."""

from __future__ import annotations

from datetime import datetime

from fluvius_energy_api import GetEnergyResponseApiDataResponse

from ..models.energy_measurement import EnergyMeasurement, parse_energy_response

# Tuple structure matches ENERGY_SCHEMA field order (20 fields).
EnergyTuple = tuple[
    str,  # ean
    str | None,  # energy_type
    str,  # metering_type
    str,  # source
    str | None,  # meter_id
    str | None,  # seq_number
    str | None,  # sub_headpoint_ean
    str | None,  # sub_headpoint_type
    str | None,  # sub_headpoint_seq_number
    str | None,  # vreg_id
    str | None,  # production_installation_type
    str,  # granularity
    datetime,  # start
    datetime,  # end
    str,  # direction
    str,  # register_type
    float | None,  # value
    str | None,  # unit
    str | None,  # validation_state
    str | None,  # gas_conversion_factor
]


def _to_tuple(m: EnergyMeasurement) -> EnergyTuple:
    """Convert an EnergyMeasurement to a tuple matching ENERGY_SCHEMA."""
    return (
        m.ean,
        m.energy_type,
        m.metering_type,
        m.source,
        m.meter_id,
        m.seq_number,
        m.sub_headpoint_ean,
        m.sub_headpoint_type,
        m.sub_headpoint_seq_number,
        m.vreg_id,
        m.production_installation_type,
        m.granularity,
        m.start,
        m.end,
        m.direction,
        m.register_type,
        m.value,
        m.unit,
        m.validation_state,
        m.gas_conversion_factor,
    )


def convert_energy_response(response: GetEnergyResponseApiDataResponse) -> list[EnergyTuple]:
    """Convert an energy API response to a list of tuples.

    Args:
        response: The energy API response from fluvius-energy-api.

    Returns:
        A list of 20-element tuples matching the ENERGY_SCHEMA field order.
    """
    return [_to_tuple(m) for m in parse_energy_response(response)]