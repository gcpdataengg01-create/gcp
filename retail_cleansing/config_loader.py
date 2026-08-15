from importlib import resources
from typing import Any, Dict, List

import yaml


CONFIG_PACKAGE = "retail_cleansing.config"


def load_yaml(filename: str) -> Dict[str, Any]:
    resource = (
        resources.files(CONFIG_PACKAGE)
        .joinpath(filename)
    )

    with resource.open(
        "r",
        encoding="utf-8",
    ) as file:
        return yaml.safe_load(file)


def load_sentinels() -> List[str]:
    return load_yaml(
        "sentinels.yaml"
    )["null_values"]


def load_date_formats() -> List[str]:
    return load_yaml(
        "date_formats.yaml"
    )["invoice_date_formats"]


def load_country_map() -> Dict[str, str]:
    return load_yaml(
        "country_map.yaml"
    )["country_map"]


def load_stock_code_types() -> Dict[str, dict]:
    return load_yaml(
        "stock_code_types.yaml"
    )["stock_code_types"]


def load_operational_descriptions() -> List[str]:
    config = load_yaml(
        "operational_descriptions.yaml"
    )

    return config[
        "operational_descriptions"
    ].get(
        "exact_values",
        [],
    )