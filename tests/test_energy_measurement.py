"""Tests for the normalized energy measurement model and parse_energy_response function."""

from fluvius_energy_api import GetEnergyResponseApiDataResponse

from polars_fluvius.models.energy_measurement import EnergyMeasurement, parse_energy_response


class TestFlattenEmpty:
    def test_none_response_data(self):
        response = GetEnergyResponseApiDataResponse(data=None)
        assert parse_energy_response(response) == []

    def test_none_headpoint(self):
        response = GetEnergyResponseApiDataResponse.model_validate(
            {"data": {"headpoint": None}}
        )
        assert parse_energy_response(response) == []

    def test_none_ean(self):
        response = GetEnergyResponseApiDataResponse.model_validate(
            {
                "data": {
                    "headpoint": {
                        "$type": "metering-on-meter",
                        "ean": None,
                        "physicalMeters": [],
                    }
                }
            }
        )
        assert parse_energy_response(response) == []

    def test_empty_physical_meters(self):
        response = GetEnergyResponseApiDataResponse.model_validate(
            {
                "data": {
                    "headpoint": {
                        "$type": "metering-on-meter",
                        "ean": "541448800000004312",
                        "energyType": "E",
                        "physicalMeters": [],
                    }
                }
            }
        )
        assert parse_energy_response(response) == []


class TestFlattenMeteringOnMeter:
    def test_electricity_daily(self):
        response = GetEnergyResponseApiDataResponse.model_validate(
            {
                "data": {
                    "headpoint": {
                        "$type": "metering-on-meter",
                        "ean": "541448800000004312",
                        "energyType": "E",
                        "physicalMeters": [
                            {
                                "seqNumber": "1",
                                "meterID": "1SAG2120063032",
                                "dailyEnergy": [
                                    {
                                        "start": "2024-07-02T22:00:00Z",
                                        "end": "2024-07-03T22:00:00Z",
                                        "measurements": [
                                            {
                                                "offtake": {
                                                    "day": {
                                                        "value": 12.580,
                                                        "unit": "kWh",
                                                        "validationState": "READ",
                                                    },
                                                    "night": {
                                                        "value": 20.140,
                                                        "unit": "kWh",
                                                        "validationState": "READ",
                                                    },
                                                },
                                                "injection": {
                                                    "day": {
                                                        "value": 4.800,
                                                        "unit": "kWh",
                                                        "validationState": "READ",
                                                    },
                                                },
                                            }
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                }
            }
        )
        rows = parse_energy_response(response)
        assert len(rows) == 3

        for row in rows:
            assert row.ean == "541448800000004312"
            assert row.energy_type == "E"
            assert row.metering_type == "metering-on-meter"
            assert row.source == "meter"
            assert row.meter_id == "1SAG2120063032"
            assert row.seq_number == "1"
            assert row.granularity == "daily"
            assert row.unit == "kWh"
            assert row.sub_headpoint_ean is None

        offtake_day = [r for r in rows if r.direction == "offtake" and r.register_type == "day"]
        assert len(offtake_day) == 1
        assert offtake_day[0].value == 12.580
        assert offtake_day[0].validation_state == "READ"

        offtake_night = [r for r in rows if r.direction == "offtake" and r.register_type == "night"]
        assert len(offtake_night) == 1
        assert offtake_night[0].value == 20.140

        injection_day = [r for r in rows if r.direction == "injection" and r.register_type == "day"]
        assert len(injection_day) == 1
        assert injection_day[0].value == 4.800

    def test_gas_dual_entries_m3_and_kwh(self):
        """Gas metering-on-meter returns two entries per time slice: m3 and kWh."""
        response = GetEnergyResponseApiDataResponse.model_validate(
            {
                "data": {
                    "headpoint": {
                        "$type": "metering-on-meter",
                        "ean": "541448860000000001",
                        "energyType": "G",
                        "physicalMeters": [
                            {
                                "seqNumber": "1",
                                "meterID": "GAS001",
                                "dailyEnergy": [
                                    {
                                        "start": "2024-07-02T22:00:00Z",
                                        "end": "2024-07-03T22:00:00Z",
                                        "measurements": [
                                            {
                                                "offtake": {
                                                    "total": {
                                                        "value": 5.0,
                                                        "unit": "m3",
                                                        "validationState": "VAL",
                                                    }
                                                }
                                            },
                                            {
                                                "offtake": {
                                                    "total": {
                                                        "value": 52.75,
                                                        "unit": "kWh",
                                                        "validationState": "VAL",
                                                        "gasConversionFactor": "P",
                                                    }
                                                }
                                            },
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                }
            }
        )
        rows = parse_energy_response(response)
        assert len(rows) == 2

        m3_row = [r for r in rows if r.unit == "m3"]
        kwh_row = [r for r in rows if r.unit == "kWh"]
        assert len(m3_row) == 1
        assert len(kwh_row) == 1

        assert m3_row[0].value == 5.0
        assert m3_row[0].gas_conversion_factor is None

        assert kwh_row[0].value == 52.75
        assert kwh_row[0].gas_conversion_factor == "P"
        assert kwh_row[0].energy_type == "G"

    def test_multiple_meters(self):
        response = GetEnergyResponseApiDataResponse.model_validate(
            {
                "data": {
                    "headpoint": {
                        "$type": "metering-on-meter",
                        "ean": "541448800000004312",
                        "energyType": "E",
                        "physicalMeters": [
                            {
                                "seqNumber": "1",
                                "meterID": "METER_A",
                                "dailyEnergy": [
                                    {
                                        "start": "2024-07-02T22:00:00Z",
                                        "end": "2024-07-03T22:00:00Z",
                                        "measurements": [
                                            {
                                                "offtake": {
                                                    "total": {
                                                        "value": 10.0,
                                                        "unit": "kWh",
                                                    }
                                                }
                                            }
                                        ],
                                    }
                                ],
                            },
                            {
                                "seqNumber": "2",
                                "meterID": "METER_B",
                                "dailyEnergy": [
                                    {
                                        "start": "2024-07-02T22:00:00Z",
                                        "end": "2024-07-03T22:00:00Z",
                                        "measurements": [
                                            {
                                                "offtake": {
                                                    "total": {
                                                        "value": 20.0,
                                                        "unit": "kWh",
                                                    }
                                                }
                                            }
                                        ],
                                    }
                                ],
                            },
                        ],
                    }
                }
            }
        )
        rows = parse_energy_response(response)
        assert len(rows) == 2
        meter_ids = {r.meter_id for r in rows}
        assert meter_ids == {"METER_A", "METER_B"}

    def test_hourly_and_quarter_hourly(self):
        response = GetEnergyResponseApiDataResponse.model_validate(
            {
                "data": {
                    "headpoint": {
                        "$type": "metering-on-meter",
                        "ean": "541448800000004312",
                        "energyType": "E",
                        "physicalMeters": [
                            {
                                "seqNumber": "1",
                                "meterID": "M1",
                                "hourlyEnergy": [
                                    {
                                        "start": "2024-07-02T22:00:00Z",
                                        "end": "2024-07-02T23:00:00Z",
                                        "measurements": [
                                            {
                                                "offtake": {
                                                    "total": {
                                                        "value": 1.5,
                                                        "unit": "kWh",
                                                    }
                                                }
                                            }
                                        ],
                                    }
                                ],
                                "quarterHourlyEnergy": [
                                    {
                                        "start": "2024-07-02T22:00:00Z",
                                        "end": "2024-07-02T22:15:00Z",
                                        "measurements": [
                                            {
                                                "offtake": {
                                                    "total": {
                                                        "value": 0.4,
                                                        "unit": "kWh",
                                                    }
                                                }
                                            }
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                }
            }
        )
        rows = parse_energy_response(response)
        assert len(rows) == 2
        granularities = {r.granularity for r in rows}
        assert granularities == {"hourly", "quarter_hourly"}


class TestFlattenMeteringOnHeadpoint:
    def test_headpoint_energy(self):
        response = GetEnergyResponseApiDataResponse.model_validate(
            {
                "data": {
                    "headpoint": {
                        "$type": "metering-on-headpoint",
                        "ean": "541448800000004312",
                        "energyType": "E",
                        "dailyEnergy": [
                            {
                                "start": "2024-07-02T22:00:00Z",
                                "end": "2024-07-03T22:00:00Z",
                                "measurements": [
                                    {
                                        "offtake": {
                                            "day": {
                                                "value": 15.0,
                                                "unit": "kWh",
                                                "validationState": "EST",
                                            },
                                            "night": {
                                                "value": 8.0,
                                                "unit": "kWh",
                                                "validationState": "EST",
                                            },
                                        },
                                        "injection": {
                                            "total": {
                                                "value": 3.2,
                                                "unit": "kWh",
                                                "validationState": "EST",
                                            }
                                        },
                                    }
                                ],
                            }
                        ],
                    }
                }
            }
        )
        rows = parse_energy_response(response)
        assert len(rows) == 3
        for row in rows:
            assert row.source == "headpoint"
            assert row.meter_id is None
            assert row.sub_headpoint_ean is None

    def test_with_sub_headpoints_production(self):
        response = GetEnergyResponseApiDataResponse.model_validate(
            {
                "data": {
                    "headpoint": {
                        "$type": "metering-on-headpoint",
                        "ean": "541448800000004312",
                        "energyType": "E",
                        "dailyEnergy": [],
                        "subHeadpoints": [
                            {
                                "$type": "submetering-production",
                                "ean": "541448800000009999",
                                "seqNumber": "1",
                                "vregId": "G10345678",
                                "type": "PV",
                                "dailyEnergy": [
                                    {
                                        "start": "2024-07-02T22:00:00Z",
                                        "end": "2024-07-03T22:00:00Z",
                                        "measurements": [
                                            {
                                                "production": {
                                                    "total": {
                                                        "value": 25.0,
                                                        "unit": "kWh",
                                                        "validationState": "READ",
                                                    }
                                                }
                                            }
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                }
            }
        )
        rows = parse_energy_response(response)
        assert len(rows) == 1
        row = rows[0]
        assert row.source == "sub_headpoint"
        assert row.sub_headpoint_ean == "541448800000009999"
        assert row.sub_headpoint_type == "submetering-production"
        assert row.sub_headpoint_seq_number == "1"
        assert row.vreg_id == "G10345678"
        assert row.production_installation_type == "PV"
        assert row.direction == "production"
        assert row.value == 25.0

    def test_with_sub_headpoints_auxiliary(self):
        response = GetEnergyResponseApiDataResponse.model_validate(
            {
                "data": {
                    "headpoint": {
                        "$type": "metering-on-headpoint",
                        "ean": "541448800000004312",
                        "energyType": "E",
                        "dailyEnergy": [],
                        "subHeadpoints": [
                            {
                                "$type": "submetering-auxiliary",
                                "ean": "541448800000008888",
                                "seqNumber": "2",
                                "dailyEnergy": [
                                    {
                                        "start": "2024-07-02T22:00:00Z",
                                        "end": "2024-07-03T22:00:00Z",
                                        "measurements": [
                                            {
                                                "auxiliary": {
                                                    "total": {
                                                        "value": 1.5,
                                                        "unit": "kWh",
                                                    }
                                                }
                                            }
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                }
            }
        )
        rows = parse_energy_response(response)
        assert len(rows) == 1
        row = rows[0]
        assert row.sub_headpoint_type == "submetering-auxiliary"
        assert row.direction == "auxiliary"
        assert row.vreg_id is None
        assert row.production_installation_type is None

    def test_with_sub_headpoints_offtake(self):
        response = GetEnergyResponseApiDataResponse.model_validate(
            {
                "data": {
                    "headpoint": {
                        "$type": "metering-on-headpoint",
                        "ean": "541448800000004312",
                        "energyType": "E",
                        "dailyEnergy": [],
                        "subHeadpoints": [
                            {
                                "$type": "submetering-offtake",
                                "ean": "541448800000007777",
                                "seqNumber": "3",
                                "dailyEnergy": [
                                    {
                                        "start": "2024-07-02T22:00:00Z",
                                        "end": "2024-07-03T22:00:00Z",
                                        "measurements": [
                                            {
                                                "offtake": {
                                                    "total": {
                                                        "value": 7.0,
                                                        "unit": "kWh",
                                                    }
                                                }
                                            }
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                }
            }
        )
        rows = parse_energy_response(response)
        assert len(rows) == 1
        row = rows[0]
        assert row.sub_headpoint_type == "submetering-offtake"
        assert row.direction == "offtake"


class TestFlattenMeteringOnHeadpointAndMeter:
    def test_combined(self):
        """metering-on-headpoint-and-meter has meters + headpoint energy + sub-headpoints."""
        response = GetEnergyResponseApiDataResponse.model_validate(
            {
                "data": {
                    "headpoint": {
                        "$type": "metering-on-headpoint-and-meter",
                        "ean": "541448800000004312",
                        "energyType": "E",
                        "physicalMeters": [
                            {
                                "seqNumber": "1",
                                "meterID": "METER_X",
                                "dailyEnergy": [
                                    {
                                        "start": "2024-07-02T22:00:00Z",
                                        "end": "2024-07-03T22:00:00Z",
                                        "measurements": [
                                            {
                                                "offtake": {
                                                    "total": {
                                                        "value": 100.0,
                                                        "unit": "kWh",
                                                    }
                                                }
                                            }
                                        ],
                                    }
                                ],
                            }
                        ],
                        "dailyEnergy": [
                            {
                                "start": "2024-07-02T22:00:00Z",
                                "end": "2024-07-03T22:00:00Z",
                                "measurements": [
                                    {
                                        "offtake": {
                                            "total": {
                                                "value": 100.0,
                                                "unit": "kWh",
                                            }
                                        }
                                    }
                                ],
                            }
                        ],
                        "subHeadpoints": [
                            {
                                "$type": "submetering-production",
                                "ean": "541448800000009999",
                                "seqNumber": "1",
                                "vregId": "G10345678",
                                "type": "PV",
                                "dailyEnergy": [
                                    {
                                        "start": "2024-07-02T22:00:00Z",
                                        "end": "2024-07-03T22:00:00Z",
                                        "measurements": [
                                            {
                                                "production": {
                                                    "total": {
                                                        "value": 50.0,
                                                        "unit": "kWh",
                                                    }
                                                }
                                            }
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                }
            }
        )
        rows = parse_energy_response(response)
        assert len(rows) == 3

        sources = {r.source for r in rows}
        assert sources == {"meter", "headpoint", "sub_headpoint"}

        meter_rows = [r for r in rows if r.source == "meter"]
        assert len(meter_rows) == 1
        assert meter_rows[0].meter_id == "METER_X"

        hp_rows = [r for r in rows if r.source == "headpoint"]
        assert len(hp_rows) == 1
        assert hp_rows[0].meter_id is None

        shp_rows = [r for r in rows if r.source == "sub_headpoint"]
        assert len(shp_rows) == 1
        assert shp_rows[0].sub_headpoint_ean == "541448800000009999"


class TestFlattenNoDuplicateKeys:
    def test_no_duplicate_composite_keys(self):
        """Verify that the composite key is unique across all rows."""
        response = GetEnergyResponseApiDataResponse.model_validate(
            {
                "data": {
                    "headpoint": {
                        "$type": "metering-on-headpoint-and-meter",
                        "ean": "541448800000004312",
                        "energyType": "E",
                        "physicalMeters": [
                            {
                                "seqNumber": "1",
                                "meterID": "METER_X",
                                "dailyEnergy": [
                                    {
                                        "start": "2024-07-02T22:00:00Z",
                                        "end": "2024-07-03T22:00:00Z",
                                        "measurements": [
                                            {
                                                "offtake": {
                                                    "day": {
                                                        "value": 10.0,
                                                        "unit": "kWh",
                                                    },
                                                    "night": {
                                                        "value": 5.0,
                                                        "unit": "kWh",
                                                    },
                                                },
                                                "injection": {
                                                    "total": {
                                                        "value": 3.0,
                                                        "unit": "kWh",
                                                    }
                                                },
                                            }
                                        ],
                                    }
                                ],
                            }
                        ],
                        "dailyEnergy": [
                            {
                                "start": "2024-07-02T22:00:00Z",
                                "end": "2024-07-03T22:00:00Z",
                                "measurements": [
                                    {
                                        "offtake": {
                                            "day": {
                                                "value": 10.0,
                                                "unit": "kWh",
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
        rows = parse_energy_response(response)

        keys = [
            (
                r.ean,
                r.source,
                r.meter_id,
                r.sub_headpoint_ean,
                r.granularity,
                r.start,
                r.direction,
                r.register_type,
                r.unit,
            )
            for r in rows
        ]
        assert len(keys) == len(set(keys)), "Duplicate composite keys found"

    def test_gas_dual_entries_unique_by_unit(self):
        """Gas m3 and kWh rows for same time slice are distinguished by unit."""
        response = GetEnergyResponseApiDataResponse.model_validate(
            {
                "data": {
                    "headpoint": {
                        "$type": "metering-on-meter",
                        "ean": "541448860000000001",
                        "energyType": "G",
                        "physicalMeters": [
                            {
                                "seqNumber": "1",
                                "meterID": "GAS001",
                                "dailyEnergy": [
                                    {
                                        "start": "2024-07-02T22:00:00Z",
                                        "end": "2024-07-03T22:00:00Z",
                                        "measurements": [
                                            {
                                                "offtake": {
                                                    "total": {
                                                        "value": 5.0,
                                                        "unit": "m3",
                                                    }
                                                }
                                            },
                                            {
                                                "offtake": {
                                                    "total": {
                                                        "value": 52.75,
                                                        "unit": "kWh",
                                                        "gasConversionFactor": "D",
                                                    }
                                                }
                                            },
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                }
            }
        )
        rows = parse_energy_response(response)
        keys = [
            (
                r.ean,
                r.source,
                r.meter_id,
                r.sub_headpoint_ean,
                r.granularity,
                r.start,
                r.direction,
                r.register_type,
                r.unit,
            )
            for r in rows
        ]
        assert len(keys) == len(set(keys)), "Duplicate composite keys found"


class TestFlattenEdgeCases:
    def test_timeslice_missing_start(self):
        response = GetEnergyResponseApiDataResponse.model_validate(
            {
                "data": {
                    "headpoint": {
                        "$type": "metering-on-meter",
                        "ean": "541448800000004312",
                        "energyType": "E",
                        "physicalMeters": [
                            {
                                "seqNumber": "1",
                                "meterID": "M1",
                                "dailyEnergy": [
                                    {
                                        "start": None,
                                        "end": "2024-07-03T22:00:00Z",
                                        "measurements": [
                                            {
                                                "offtake": {
                                                    "total": {
                                                        "value": 10.0,
                                                        "unit": "kWh",
                                                    }
                                                }
                                            }
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                }
            }
        )
        assert parse_energy_response(response) == []

    def test_timeslice_empty_measurements(self):
        response = GetEnergyResponseApiDataResponse.model_validate(
            {
                "data": {
                    "headpoint": {
                        "$type": "metering-on-meter",
                        "ean": "541448800000004312",
                        "energyType": "E",
                        "physicalMeters": [
                            {
                                "seqNumber": "1",
                                "meterID": "M1",
                                "dailyEnergy": [
                                    {
                                        "start": "2024-07-02T22:00:00Z",
                                        "end": "2024-07-03T22:00:00Z",
                                        "measurements": [],
                                    }
                                ],
                            }
                        ],
                    }
                }
            }
        )
        assert parse_energy_response(response) == []

    def test_measurement_value_none_value(self):
        """MeasurementValue with value=None should still produce a row."""
        response = GetEnergyResponseApiDataResponse.model_validate(
            {
                "data": {
                    "headpoint": {
                        "$type": "metering-on-meter",
                        "ean": "541448800000004312",
                        "energyType": "E",
                        "physicalMeters": [
                            {
                                "seqNumber": "1",
                                "meterID": "M1",
                                "dailyEnergy": [
                                    {
                                        "start": "2024-07-02T22:00:00Z",
                                        "end": "2024-07-03T22:00:00Z",
                                        "measurements": [
                                            {
                                                "offtake": {
                                                    "total": {
                                                        "value": None,
                                                        "unit": "kWh",
                                                        "validationState": "NVAL",
                                                    }
                                                }
                                            }
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                }
            }
        )
        rows = parse_energy_response(response)
        assert len(rows) == 1
        assert rows[0].value is None
        assert rows[0].validation_state == "NVAL"

    def test_reactive_registers(self):
        response = GetEnergyResponseApiDataResponse.model_validate(
            {
                "data": {
                    "headpoint": {
                        "$type": "metering-on-meter",
                        "ean": "541448800000004312",
                        "energyType": "E",
                        "physicalMeters": [
                            {
                                "seqNumber": "1",
                                "meterID": "M1",
                                "dailyEnergy": [
                                    {
                                        "start": "2024-07-02T22:00:00Z",
                                        "end": "2024-07-03T22:00:00Z",
                                        "measurements": [
                                            {
                                                "offtake": {
                                                    "reactive": {
                                                        "value": 2.0,
                                                        "unit": "kVArh",
                                                    },
                                                    "inductive": {
                                                        "value": 1.0,
                                                        "unit": "kVArh",
                                                    },
                                                    "capacitive": {
                                                        "value": 0.5,
                                                        "unit": "kVArh",
                                                    },
                                                }
                                            }
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                }
            }
        )
        rows = parse_energy_response(response)
        assert len(rows) == 3
        registers = {r.register_type for r in rows}
        assert registers == {"reactive", "inductive", "capacitive"}
        for r in rows:
            assert r.unit == "kVArh"
