"""Normalized energy measurement model for Unity Catalog / Delta Lake storage."""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel

from fluvius_energy_api import (
    GetEnergyResponseApiDataResponse,
    MeasurementTimeSlice,
    MeasurementValue,
    MeteringOnHeadpoint,
    MeteringOnHeadpointAndMeter,
    MeteringOnMeter,
    PhysicalMeter,
    SubHeadpoint,
)


class Granularity(str, Enum):
    DAILY = "daily"
    HOURLY = "hourly"
    QUARTER_HOURLY = "quarter_hourly"


class MeasurementSource(str, Enum):
    HEADPOINT = "headpoint"
    METER = "meter"
    SUB_HEADPOINT = "sub_headpoint"


class Direction(str, Enum):
    OFFTAKE = "offtake"
    INJECTION = "injection"
    PRODUCTION = "production"
    AUXILIARY = "auxiliary"


class RegisterType(str, Enum):
    TOTAL = "total"
    DAY = "day"
    NIGHT = "night"
    REACTIVE = "reactive"
    INDUCTIVE = "inductive"
    CAPACITIVE = "capacitive"


class EnergyMeasurement(BaseModel):
    """A single normalized measurement row suitable for Delta Lake storage.

    Composite unique key:
        (ean, source, meter_id, sub_headpoint_ean, granularity,
         start, direction, register_type, unit)
    """

    ean: str
    energy_type: str | None = None
    metering_type: str
    source: str
    meter_id: str | None = None
    seq_number: str | None = None
    sub_headpoint_ean: str | None = None
    sub_headpoint_type: str | None = None
    sub_headpoint_seq_number: str | None = None
    vreg_id: str | None = None
    production_installation_type: str | None = None
    granularity: str
    start: datetime
    end: datetime
    direction: str
    register_type: str
    value: float | None = None
    unit: str | None = None
    validation_state: str | None = None
    gas_conversion_factor: str | None = None


def parse_energy_response(
    response: GetEnergyResponseApiDataResponse,
) -> list[EnergyMeasurement]:
    """Convert a nested energy API response into normalized measurement rows.

    Args:
        response: The parsed API response.

    Returns:
        A list of normalized measurement rows. One row per atomic measurement value.
    """
    if response.data is None or response.data.headpoint is None:
        return []

    headpoint = response.data.headpoint
    ean = headpoint.ean
    if ean is None:
        return []

    energy_type = headpoint.energy_type
    metering_type = headpoint.type_discriminator

    rows: list[EnergyMeasurement] = []

    if isinstance(headpoint, MeteringOnMeter):
        _collect_meters(rows, headpoint.physical_meters, ean, energy_type, metering_type)

    elif isinstance(headpoint, MeteringOnHeadpoint):
        _collect_headpoint_energy(rows, headpoint, ean, energy_type, metering_type)
        _collect_sub_headpoints(
            rows, headpoint.sub_headpoints, ean, energy_type, metering_type
        )

    elif isinstance(headpoint, MeteringOnHeadpointAndMeter):
        _collect_meters(rows, headpoint.physical_meters, ean, energy_type, metering_type)
        _collect_headpoint_energy(rows, headpoint, ean, energy_type, metering_type)
        _collect_sub_headpoints(
            rows, headpoint.sub_headpoints, ean, energy_type, metering_type
        )

    return rows


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

_DIRECTIONS: list[tuple[str, str]] = [
    ("offtake", Direction.OFFTAKE),
    ("injection", Direction.INJECTION),
    ("production", Direction.PRODUCTION),
    ("auxiliary", Direction.AUXILIARY),
]

_REGISTERS: list[tuple[str, str]] = [
    ("total", RegisterType.TOTAL),
    ("day", RegisterType.DAY),
    ("night", RegisterType.NIGHT),
    ("reactive", RegisterType.REACTIVE),
    ("inductive", RegisterType.INDUCTIVE),
    ("capacitive", RegisterType.CAPACITIVE),
]


def _energy_arrays(
    obj: PhysicalMeter | MeteringOnHeadpoint | MeteringOnHeadpointAndMeter | SubHeadpoint,
) -> list[tuple[str, list[MeasurementTimeSlice]]]:
    """Return (granularity, timeslices) pairs for an object with energy arrays."""
    result: list[tuple[str, list[MeasurementTimeSlice]]] = []
    for attr, granularity in [
        ("daily_energy", Granularity.DAILY),
        ("hourly_energy", Granularity.HOURLY),
        ("quarter_hourly_energy", Granularity.QUARTER_HOURLY),
    ]:
        slices = getattr(obj, attr, None)
        if slices:
            result.append((granularity, slices))
    return result


def _emit_rows_from_timeslice(
    rows: list[EnergyMeasurement],
    time_slice: MeasurementTimeSlice,
    *,
    ean: str,
    energy_type: str | None,
    metering_type: str,
    source: str,
    granularity: str,
    meter_id: str | None = None,
    seq_number: str | None = None,
    sub_headpoint_ean: str | None = None,
    sub_headpoint_type: str | None = None,
    sub_headpoint_seq_number: str | None = None,
    vreg_id: str | None = None,
    production_installation_type: str | None = None,
) -> None:
    """Emit normalized rows from a single MeasurementTimeSlice."""
    if time_slice.start is None or time_slice.end is None:
        return
    if not time_slice.measurements:
        return

    for measurement_direction in time_slice.measurements:
        for dir_attr, direction_val in _DIRECTIONS:
            value_set = getattr(measurement_direction, dir_attr, None)
            if value_set is None:
                continue
            for reg_attr, register_val in _REGISTERS:
                mv: MeasurementValue | None = getattr(value_set, reg_attr, None)
                if mv is None:
                    continue
                rows.append(
                    EnergyMeasurement(
                        ean=ean,
                        energy_type=energy_type,
                        metering_type=metering_type,
                        source=source,
                        meter_id=meter_id,
                        seq_number=seq_number,
                        sub_headpoint_ean=sub_headpoint_ean,
                        sub_headpoint_type=sub_headpoint_type,
                        sub_headpoint_seq_number=sub_headpoint_seq_number,
                        vreg_id=vreg_id,
                        production_installation_type=production_installation_type,
                        granularity=granularity,
                        start=time_slice.start,
                        end=time_slice.end,
                        direction=direction_val,
                        register_type=register_val,
                        value=mv.value,
                        unit=mv.unit,
                        validation_state=mv.validation_state,
                        gas_conversion_factor=mv.gas_conversion_factor,
                    )
                )


def _collect_meters(
    rows: list[EnergyMeasurement],
    physical_meters: list[PhysicalMeter] | None,
    ean: str,
    energy_type: str | None,
    metering_type: str,
) -> None:
    if not physical_meters:
        return
    for meter in physical_meters:
        for granularity, slices in _energy_arrays(meter):
            for ts in slices:
                _emit_rows_from_timeslice(
                    rows,
                    ts,
                    ean=ean,
                    energy_type=energy_type,
                    metering_type=metering_type,
                    source=MeasurementSource.METER,
                    granularity=granularity,
                    meter_id=meter.meter_id,
                    seq_number=meter.seq_number,
                )


def _collect_headpoint_energy(
    rows: list[EnergyMeasurement],
    headpoint: MeteringOnHeadpoint | MeteringOnHeadpointAndMeter,
    ean: str,
    energy_type: str | None,
    metering_type: str,
) -> None:
    for granularity, slices in _energy_arrays(headpoint):
        for ts in slices:
            _emit_rows_from_timeslice(
                rows,
                ts,
                ean=ean,
                energy_type=energy_type,
                metering_type=metering_type,
                source=MeasurementSource.HEADPOINT,
                granularity=granularity,
            )


def _collect_sub_headpoints(
    rows: list[EnergyMeasurement],
    sub_headpoints: list[SubHeadpoint] | None,
    ean: str,
    energy_type: str | None,
    metering_type: str,
) -> None:
    if not sub_headpoints:
        return
    for shp in sub_headpoints:
        for granularity, slices in _energy_arrays(shp):
            for ts in slices:
                _emit_rows_from_timeslice(
                    rows,
                    ts,
                    ean=ean,
                    energy_type=energy_type,
                    metering_type=metering_type,
                    source=MeasurementSource.SUB_HEADPOINT,
                    granularity=granularity,
                    sub_headpoint_ean=shp.ean,
                    sub_headpoint_type=shp.type_discriminator,
                    sub_headpoint_seq_number=shp.seq_number,
                    vreg_id=shp.vreg_id,
                    production_installation_type=shp.type,
                )
