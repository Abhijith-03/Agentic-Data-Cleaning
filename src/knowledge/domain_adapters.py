"""Domain-specific heuristics and constraint libraries."""

from __future__ import annotations

import re
from typing import Any

DOMAIN_CONSTRAINTS: dict[str, dict[str, Any]] = {
    "healthcare": {
        "icd_code_pattern": r"^[A-Z]\d{2}(\.\d{1,4})?$",
        "npi_pattern": r"^\d{10}$",
        "required_columns": ["patient_id", "date_of_service"],
    },
    "finance": {
        "cusip_pattern": r"^[A-Z0-9]{9}$",
        "currency_pattern": r"^-?\$?\d{1,3}(,\d{3})*(\.\d{2})?$",
        "required_columns": ["transaction_id", "amount"],
    },
    "retail": {
        "sku_pattern": r"^[A-Z0-9\-]{4,20}$",
        "upc_pattern": r"^\d{12}$",
        "required_columns": ["product_id", "price"],
    },
    "generic": {
        "required_columns": [],
    },
}


def get_domain_constraints(domain: str) -> dict[str, Any]:
    return DOMAIN_CONSTRAINTS.get(domain, DOMAIN_CONSTRAINTS["generic"])


def validate_domain_value(
    value: str, domain: str, constraint_key: str
) -> bool:
    """Check if a value matches a domain-specific constraint pattern."""
    constraints = get_domain_constraints(domain)
    pattern = constraints.get(constraint_key)
    if not pattern or not isinstance(pattern, str):
        return True
    return bool(re.match(pattern, value.strip()))
