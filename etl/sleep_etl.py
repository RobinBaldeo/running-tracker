"""Standalone ETL script to download 2 years of sleep data from Garmin Connect."""

import json
import time
from datetime import date, timedelta
from pathlib import Path

import yaml
from dotenv import load_dotenv
from garminconnect import Garmin
import os

ROOT_DIR = Path(__file__).resolve().parent.parent
ENV_FILE = ROOT_DIR / ".env"
CONFIG_FILE = ROOT_DIR / "config.yaml"
OUTPUT_FILE = Path(__file__).resolve().parent / "sleep_data.json"
DAYS_BACK = 730  # ~2 years


def load_config():
    with open(CONFIG_FILE) as f:
        return yaml.safe_load(f)


def main():
    load_dotenv(ENV_FILE)

    email = os.environ["GARMIN_EMAIL"]
    password = os.environ["GARMIN_PASSWORD"]

    garmin = Garmin(email, password)
    garmin.login()

    config = load_config()
    important_sleep_fields = config["important_sleep_fields"]

    end_date = date.today()
    start_date = end_date - timedelta(days=DAYS_BACK)
    all_sleep_data = {}
    current = start_date

    print(f"Downloading sleep data from {start_date} to {end_date} ...")

    while current <= end_date:
        date_str = current.isoformat()
        try:
            raw = garmin.get_sleep_data(date_str)
            filtered = {k: v for k, v in raw.items() if k in important_sleep_fields}
            all_sleep_data[date_str] = filtered
            print(f"  {date_str} OK")
        except Exception as e:
            print(f"  {date_str} FAILED: {e}")
            all_sleep_data[date_str] = {"error": str(e)}

        current += timedelta(days=1)
        time.sleep(0.5)  # avoid rate-limiting

    OUTPUT_FILE.write_text(json.dumps(all_sleep_data, indent=2, default=str))
    print(f"\nDone. {len(all_sleep_data)} days written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()