from pathlib import Path
from typing import Any, Dict, List

import yaml


CONFIG_DIR = Path(__file__).parent / "config"


def load_yaml(filename: str) -> Dict[str, Any]:
    path = CONFIG_DIR / filename

    with path.open(
        "r",
        encoding="utf-8",
    ) as file:
        return yaml.safe_load(file)


def load_sentinels() -> List[str]:
    config = load_yaml(
        "sentinels.yaml"
    )

    return config["null_values"]


def load_date_formats() -> List[str]:
    config = load_yaml(
        "date_formats.yaml"
    )

    return config["invoice_date_formats"]


def load_country_map() -> Dict[str, str]:
    config = load_yaml(
        "country_map.yaml"
    )

    return config["country_map"]


def load_stock_code_types() -> Dict[str, dict]:
    config = load_yaml(
        "stock_code_types.yaml"
    )

    return config["stock_code_types"]