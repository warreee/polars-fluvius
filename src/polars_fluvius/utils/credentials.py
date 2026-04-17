"""Credential handling for Fluvius data sources."""

from __future__ import annotations

from fluvius_energy_api import FluviusCredentials
from fluvius_energy_api.client import Environment


def get_credentials(options: dict[str, str]) -> FluviusCredentials:
    """Get Fluvius credentials from options or environment variables.

    Priority:
    1. Options dict (if all required fields are present)
    2. Environment variables (via FluviusCredentials.from_env())

    Args:
        options: Dictionary of options from the data source.

    Returns:
        FluviusCredentials instance.

    Raises:
        ConfigurationError: If credentials cannot be loaded.
    """
    subscription_key = options.get("subscription_key")
    client_id = options.get("client_id")
    tenant_id = options.get("tenant_id")
    scope = options.get("scope")
    data_access_contract_number = options.get("data_access_contract_number")

    if all([subscription_key, client_id, tenant_id, scope, data_access_contract_number]):
        return FluviusCredentials(
            subscription_key=subscription_key,  # type: ignore[arg-type]
            client_id=client_id,  # type: ignore[arg-type]
            tenant_id=tenant_id,  # type: ignore[arg-type]
            scope=scope,  # type: ignore[arg-type]
            data_access_contract_number=data_access_contract_number,  # type: ignore[arg-type]
            certificate_thumbprint=options.get("certificate_thumbprint"),
            private_key=options.get("private_key"),
            client_secret=options.get("client_secret"),
        )

    prefix = options.get("credentials_prefix", "FLUVIUS")
    return FluviusCredentials.from_env(prefix=prefix)


def get_environment(options: dict[str, str]) -> Environment:
    """Get the Fluvius API environment from options.

    Args:
        options: Dictionary of options from the data source.

    Returns:
        Environment enum value.
    """
    env_str = options.get("environment", "sandbox").lower()
    if env_str == "production":
        return Environment.PRODUCTION
    return Environment.SANDBOX