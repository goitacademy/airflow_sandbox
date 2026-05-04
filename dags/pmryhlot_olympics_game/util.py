from datetime import datetime
from enum import StrEnum
from typing import Any


class Medal(StrEnum):
    BRONZE = "Bronze"
    SILVER = "Silver"
    GOLD = "Gold"


DEFAULT_ARGS: dict[str, Any] = {
    "owner": "airflow",
    "start_date": datetime(2024, 8, 4, 0, 0),
}
