from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter

from app.services.garmin import GarminService
from app.config import config

router = APIRouter(prefix="/api/v1/tools", tags=["tools"])


@router.get("/sleep")
def get_sleep(target_date: Optional[date] = None):
    """Get sleep data from Garmin Connect.

    Args:
        target_date: Date to fetch sleep for (YYYY-MM-DD). Defaults to last night.

    Returns:
        Raw Garmin sleep data for the given date.
    """
    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    garmin = GarminService()
    garmin.login()

    important_sleep_fields = config['important_sleep_fields']

    data = (garmin.get_sleep(target_date.isoformat()))
    sleep_data = {}
    for key, value in data.items():
        if key in important_sleep_fields:
            sleep_data[key] = value

    return sleep_data
