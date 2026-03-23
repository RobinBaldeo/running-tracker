"""Standalone script to upload sleep_data.json into the Railway Postgres sleep_sessions table."""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parent.parent
ENV_FILE = ROOT_DIR / ".env"
INPUT_FILE = Path(__file__).resolve().parent / "sleep_data.json"


def epoch_ms_to_timestamptz(ms):
    """Convert Garmin epoch milliseconds to a datetime."""
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def get_score(sleep_scores, category, field="value"):
    """Safely extract a nested sleep score field."""
    if not sleep_scores:
        return None
    cat = sleep_scores.get(category)
    if not cat:
        return None
    return cat.get(field)


def get_qualifier(sleep_scores, category):
    """Extract qualifier string, stripping the 'SLEEP_SCORE_' prefix Garmin uses."""
    raw = get_score(sleep_scores, category, "qualifierKey")
    if raw and raw.startswith("SLEEP_SCORE_QUALIFIER_"):
        return raw.replace("SLEEP_SCORE_QUALIFIER_", "")
    if raw and raw.startswith("SLEEP_SCORE_"):
        return raw.replace("SLEEP_SCORE_", "")
    return raw


def parse_record(date_str, data):
    """Parse a single day's sleep data into a row tuple matching the sleep_sessions schema."""
    dto = data.get("dailySleepDTO")
    if not dto:
        return None

    sleep_id = dto.get("id")
    if not sleep_id:
        return None

    sleep_scores = dto.get("sleepScores") or {}

    return (
        sleep_id,
        dto.get("calendarDate", date_str),
        # Timestamps
        epoch_ms_to_timestamptz(dto.get("sleepStartTimestampGMT")),
        epoch_ms_to_timestamptz(dto.get("sleepEndTimestampGMT")),
        epoch_ms_to_timestamptz(dto.get("sleepStartTimestampLocal")),
        epoch_ms_to_timestamptz(dto.get("sleepEndTimestampLocal")),
        # Durations
        dto.get("sleepTimeSeconds", 0),
        dto.get("napTimeSeconds", 0),
        # Sleep stages
        dto.get("deepSleepSeconds"),
        dto.get("lightSleepSeconds"),
        dto.get("remSleepSeconds"),
        dto.get("awakeSleepSeconds"),
        # Scores
        get_score(sleep_scores, "overall"),
        get_qualifier(sleep_scores, "overall"),
        get_score(sleep_scores, "deep"),
        get_qualifier(sleep_scores, "deep"),
        get_score(sleep_scores, "light"),
        get_qualifier(sleep_scores, "light"),
        get_score(sleep_scores, "rem"),
        get_qualifier(sleep_scores, "rem"),
        get_qualifier(sleep_scores, "duration"),
        get_qualifier(sleep_scores, "stress"),
        get_qualifier(sleep_scores, "restlessness"),
        get_qualifier(sleep_scores, "awakeCount"),
        # Vitals
        dto.get("averageSleepStress"),
        dto.get("averageRespirationValue"),
        dto.get("lowestRespirationValue"),
        dto.get("highestRespirationValue"),
        data.get("restingHeartRate"),
        data.get("bodyBatteryChange"),
        dto.get("awakeCount"),
        # Sleep need
        dto.get("sleepNeedBaselineMinutes") or dto.get("sleepNeedBaseline"),
        dto.get("sleepNeedActualMinutes") or dto.get("sleepNeedActual"),
        # Feedback
        dto.get("sleepScoreFeedback"),
        # Raw JSON
        json.dumps(data, default=str),
    )


INSERT_SQL = """
INSERT INTO sleep_sessions (
    id, calendar_date,
    sleep_start_gmt, sleep_end_gmt, sleep_start_local, sleep_end_local,
    sleep_time_seconds, nap_time_seconds,
    deep_sleep_seconds, light_sleep_seconds, rem_sleep_seconds, awake_sleep_seconds,
    overall_score, overall_qualifier,
    deep_pct, deep_qualifier,
    light_pct, light_qualifier,
    rem_pct, rem_qualifier,
    duration_qualifier, stress_qualifier, restlessness_qualifier, awake_count_qualifier,
    avg_sleep_stress, avg_respiration, lowest_respiration, highest_respiration,
    resting_heart_rate, body_battery_change, awake_count,
    sleep_need_baseline, sleep_need_actual,
    sleep_score_feedback,
    raw_json
) VALUES %s
ON CONFLICT (id) DO UPDATE SET
    overall_score = EXCLUDED.overall_score,
    overall_qualifier = EXCLUDED.overall_qualifier,
    raw_json = EXCLUDED.raw_json;
"""


def main():
    load_dotenv(ENV_FILE)

    db_url = os.environ.get("RAILWAY_DATABASE_URL")
    if not db_url:
        print("ERROR: Set RAILWAY_DATABASE_URL in .env")
        print("  Find it in Railway dashboard → Connect → Postgres Connection URL")
        return

    print(f"Reading {INPUT_FILE} ...")
    raw_data = json.loads(INPUT_FILE.read_text())

    rows = []
    skipped = 0
    for date_str, data in raw_data.items():
        if "error" in data:
            skipped += 1
            continue
        row = parse_record(date_str, data)
        if row:
            rows.append(row)
        else:
            skipped += 1

    print(f"Parsed {len(rows)} valid records, skipped {skipped}")

    if not rows:
        print("No data to upload.")
        return

    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            execute_values(cur, INSERT_SQL, rows, page_size=100)
        conn.commit()
        print(f"Uploaded {len(rows)} records to sleep_sessions.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()