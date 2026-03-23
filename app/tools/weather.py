from fastapi import APIRouter
from datetime import date
from typing import Optional
import httpx

from app.config import config

router = APIRouter(prefix="/api/v1/tools", tags=["tools"])


@router.get("/weather")
async def get_weather(
        lat: float,
        lon: float,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
):
    """Get weather for a location - current or historical.

    Args:
        lat: Latitude of the location
        lon: Longitude of the location
        start_date: Start date for historical data (optional, format: YYYY-MM-DD)
        end_date: End date for historical data (optional, format: YYYY-MM-DD)

    Returns:
        Current weather or historical daily weather data
    """
    async with httpx.AsyncClient() as client:
        # Historical request
        if start_date and end_date:
            response = await client.get(
                config["weather"]["archive_url"],
                params={
                    "latitude": lat,
                    "longitude": lon,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max",
                    "temperature_unit": "fahrenheit",
                    "wind_speed_unit": "mph",
                }
            )
            data = response.json()
            daily = data.get("daily", {})

            days = []
            dates = daily.get("time", [])
            for i, d in enumerate(dates):
                days.append({
                    "date": d,
                    "temp_max_f": daily.get("temperature_2m_max", [])[i],
                    "temp_min_f": daily.get("temperature_2m_min", [])[i],
                    "precipitation_mm": daily.get("precipitation_sum", [])[i],
                    "wind_max_mph": daily.get("wind_speed_10m_max", [])[i],
                })

            return {
                "type": "historical",
                "location": {"lat": lat, "lon": lon},
                "days": days
            }

        # Current weather
        else:
            response = await client.get(
                config["weather"]["forecast_url"],
                params={
                    "latitude": lat,
                    "longitude": lon,
                    "current": "temperature_2m,wind_speed_10m,precipitation,relative_humidity_2m",
                    "temperature_unit": "fahrenheit",
                    "wind_speed_unit": "mph",
                }
            )
            data = response.json()
            current = data.get("current", {})

            return {
                "type": "current",
                "location": {"lat": lat, "lon": lon},
                "temperature_f": current.get("temperature_2m"),
                "wind_speed_mph": current.get("wind_speed_10m"),
                "precipitation_mm": current.get("precipitation"),
                "humidity_percent": current.get("relative_humidity_2m"),
            }