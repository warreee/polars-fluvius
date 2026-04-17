# polars-fluvius

Polars IO source for the [Fluvius Energy API](https://github.com/warreee/fluvius-energy-api).

Read energy measurements and mandates directly into Polars DataFrames / LazyFrames.

## Installation

```bash
pip install polars-fluvius
```

## Quick Start

```python
import polars_fluvius as pf

# Read mandates (LazyFrame)
mandates_lf = pf.scan_mandates(status="Approved")
mandates_df = mandates_lf.collect()

# Read energy data
energy_lf = pf.scan_energy(
    ean="541234567890123456",
    period_type="readTime",
    granularity="daily",
    from_date="2024-01-01",
    to_date="2024-01-31",
)
energy_df = energy_lf.collect()
```

`scan_energy` and `scan_mandates` return `polars.LazyFrame`s backed by a Polars
IO source plugin, so you can chain `.filter(...)`, `.select(...)`, `.head(...)`
etc. before calling `.collect()`.

## Authentication

Credentials can be provided via environment variables or as keyword arguments
to `scan_energy` / `scan_mandates`.

### Environment Variables

```bash
# Required
export FLUVIUS_SUBSCRIPTION_KEY="your-subscription-key"
export FLUVIUS_CLIENT_ID="your-client-id"
export FLUVIUS_TENANT_ID="your-tenant-id"
export FLUVIUS_SCOPE="your-scope"
export FLUVIUS_DATA_ACCESS_CONTRACT_NUMBER="your-contract-number"

# For sandbox (client secret auth)
export FLUVIUS_CLIENT_SECRET="your-client-secret"

# For production (certificate auth)
export FLUVIUS_CERTIFICATE_THUMBPRINT="your-thumbprint"
export FLUVIUS_PRIVATE_KEY="-----BEGIN RSA PRIVATE KEY-----..."
# Or use a file path:
export FLUVIUS_PRIVATE_KEY_PATH="/path/to/private_key.pem"
```

### Inline options

```python
df = pf.scan_mandates(
    subscription_key="...",
    client_id="...",
    tenant_id="...",
    scope="...",
    data_access_contract_number="...",
    client_secret="...",
).collect()
```

## Data Sources

### fluvius.mandates

Read mandate data from the Fluvius API.

**Options:**
| Option | Description |
|--------|-------------|
| `reference_number` | Filter by custom reference number |
| `ean` | Filter by GSRN EAN-code |
| `data_service_types` | Comma-separated list (e.g., "VH_dag,VH_kwartier_uur") |
| `energy_type` | "E" (electricity) or "G" (gas) |
| `status` | Requested, Approved, Rejected, or Finished |
| `mandate_expiration_date` | ISO format date filter |
| `renewal_status` | ToBeRenewed, RenewalRequested, or Expired |
| `last_updated_from` | ISO format datetime |
| `last_updated_to` | ISO format datetime |
| `environment` | "sandbox" (default) or "production" |

**Schema:**
| Column | Type |
|--------|------|
| reference_number | string |
| status | string |
| ean | string |
| energy_type | string |
| data_period_from | datetime |
| data_period_to | datetime |
| data_service_type | string |
| mandate_expiration_date | datetime |
| renewal_status | string |

### fluvius.energy

Read energy measurement data from the Fluvius API.

**Required Options:**
| Option | Description |
|--------|-------------|
| `ean` | GSRN EAN-code (required) |
| `period_type` | "readTime" or "insertTime" (required) |

**Optional Options:**
| Option | Description |
|--------|-------------|
| `reference_number` | Custom reference number |
| `granularity` | e.g., "daily", "hourly_quarterhourly" |
| `complex_energy_types` | e.g., "active,reactive" |
| `from_date` | ISO format date (e.g., "2024-01-01") |
| `to_date` | ISO format date (e.g., "2024-01-31") |
| `environment` | "sandbox" (default) or "production" |

**Schema (normalized, 20 columns — one row per measurement value):**
| Column | Type | Description |
|--------|------|-------------|
| ean | string | EAN code of the installation |
| energy_type | string | "E" or "G" |
| metering_type | string | Type of metering installation |
| source | string | "headpoint", "meter", or "sub_headpoint" |
| meter_id | string | Physical meter ID (if source is "meter") |
| seq_number | string | Meter or sub-headpoint sequence number |
| sub_headpoint_ean | string | Sub-headpoint EAN (if source is "sub_headpoint") |
| sub_headpoint_type | string | Type of sub-headpoint |
| sub_headpoint_seq_number | string | Sub-headpoint sequence number |
| vreg_id | string | VREG ID for production installations |
| production_installation_type | string | e.g., "PV" |
| granularity | string | daily, hourly, or quarter_hourly |
| start | datetime | Start of measurement period |
| end | datetime | End of measurement period |
| direction | string | offtake, injection, production, or auxiliary |
| register_type | string | total, day, night, reactive, inductive, or capacitive |
| value | float64 | The measurement value |
| unit | string | e.g., kWh, m3, kVArh |
| validation_state | string | READ, EST, VAL, or NVAL |
| gas_conversion_factor | string | P, D, or C (gas only) |

## Requirements

- Python 3.12+
- Polars 1.10+
- fluvius-energy-api 0.2.0+

## License

AGPL-3.0-or-later