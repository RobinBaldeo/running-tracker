import os

from garminconnect import Garmin


class GarminService:
    def __init__(self):
        self.client = Garmin(
            os.environ["GARMIN_EMAIL"],
            os.environ["GARMIN_PASSWORD"],
        )

    def login(self):
        self.client.login()

    def get_sleep(self, date: str) -> dict:
        """Fetch sleep data for a given date (YYYY-MM-DD)."""
        return self.client.get_sleep_data(date)
