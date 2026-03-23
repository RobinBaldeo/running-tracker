"""Daily cron job: sync last 3 days of Garmin sleep data into Railway Postgres."""

import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import execute_values
from garminconnect import Garmin

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

LOOKBACK_DAYS = 3


# ── Garmin field helpers ─────────────────────────────────────────────

def epoch_ms_to_timestamptz(ms):
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def get_score(sleep_scores, category, field="value"):
    if not sleep_scores:
        return None
    cat = sleep_scores.get(category)
    if not cat:
        return None
    return cat.get(field)


def get_qualifier(sleep_scores, category):
    raw = get_score(sleep_scores, category, "qualifierKey")
    if raw and raw.startswith("SLEEP_SCORE_QUALIFIER_"):
        return raw.replace("SLEEP_SCORE_QUALIFIER_", "")
    if raw and raw.startswith("SLEEP_SCORE_"):
        return raw.replace("SLEEP_SCORE_", "")
    return raw


def parse_record(date_str, data):
    dto = data.get("dailySleepDTO")
    if not dto:
        return None

    sleep_id = dto.get("id")
    if not sleep_id:
        return None

    scores = dto.get("sleepScores") or {}

    return (
        sleep_id,
        dto.get("calendarDate", date_str),
        epoch_ms_to_timestamptz(dto.get("sleepStartTimestampGMT")),
        epoch_ms_to_timestamptz(dto.get("sleepEndTimestampGMT")),
        epoch_ms_to_timestamptz(dto.get("sleepStartTimestampLocal")),
        epoch_ms_to_timestamptz(dto.get("sleepEndTimestampLocal")),
        dto.get("sleepTimeSeconds", 0),
        dto.get("napTimeSeconds", 0),
        dto.get("deepSleepSeconds"),
        dto.get("lightSleepSeconds"),
        dto.get("remSleepSeconds"),
        dto.get("awakeSleepSeconds"),
        get_score(scores, "overall"),
        get_qualifier(scores, "overall"),
        get_score(scores, "deep"),
        get_qualifier(scores, "deep"),
        get_score(scores, "light"),
        get_qualifier(scores, "light"),
        get_score(scores, "rem"),
        get_qualifier(scores, "rem"),
        get_qualifier(scores, "duration"),
        get_qualifier(scores, "stress"),
        get_qualifier(scores, "restlessness"),
        get_qualifier(scores, "awakeCount"),
        dto.get("averageSleepStress"),
        dto.get("averageRespirationValue"),
        dto.get("lowestRespirationValue"),
        dto.get("highestRespirationValue"),
        data.get("restingHeartRate"),
        data.get("bodyBatteryChange"),
        dto.get("awakeCount"),
        dto.get("sleepNeedBaselineMinutes") or dto.get("sleepNeedBaseline"),
        dto.get("sleepNeedActualMinutes") or dto.get("sleepNeedActual"),
        dto.get("sleepScoreFeedback"),
        json.dumps(data, default=str),
    )


UPSERT_SQL = """
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
ON CONFLICT (calendar_date) DO UPDATE SET
    id = EXCLUDED.id,
    sleep_start_gmt = EXCLUDED.sleep_start_gmt,
    sleep_end_gmt = EXCLUDED.sleep_end_gmt,
    sleep_start_local = EXCLUDED.sleep_start_local,
    sleep_end_local = EXCLUDED.sleep_end_local,
    sleep_time_seconds = EXCLUDED.sleep_time_seconds,
    nap_time_seconds = EXCLUDED.nap_time_seconds,
    deep_sleep_seconds = EXCLUDED.deep_sleep_seconds,
    light_sleep_seconds = EXCLUDED.light_sleep_seconds,
    rem_sleep_seconds = EXCLUDED.rem_sleep_seconds,
    awake_sleep_seconds = EXCLUDED.awake_sleep_seconds,
    overall_score = EXCLUDED.overall_score,
    overall_qualifier = EXCLUDED.overall_qualifier,
    deep_pct = EXCLUDED.deep_pct,
    deep_qualifier = EXCLUDED.deep_qualifier,
    light_pct = EXCLUDED.light_pct,
    light_qualifier = EXCLUDED.light_qualifier,
    rem_pct = EXCLUDED.rem_pct,
    rem_qualifier = EXCLUDED.rem_qualifier,
    duration_qualifier = EXCLUDED.duration_qualifier,
    stress_qualifier = EXCLUDED.stress_qualifier,
    restlessness_qualifier = EXCLUDED.restlessness_qualifier,
    awake_count_qualifier = EXCLUDED.awake_count_qualifier,
    avg_sleep_stress = EXCLUDED.avg_sleep_stress,
    avg_respiration = EXCLUDED.avg_respiration,
    lowest_respiration = EXCLUDED.lowest_respiration,
    highest_respiration = EXCLUDED.highest_respiration,
    resting_heart_rate = EXCLUDED.resting_heart_rate,
    body_battery_change = EXCLUDED.body_battery_change,
    awake_count = EXCLUDED.awake_count,
    sleep_need_baseline = EXCLUDED.sleep_need_baseline,
    sleep_need_actual = EXCLUDED.sleep_need_actual,
    sleep_score_feedback = EXCLUDED.sleep_score_feedback,
    raw_json = EXCLUDED.raw_json;
"""


# ── Main ─────────────────────────────────────────────────────────────

def main():
    db_url = os.environ.get("DATABASE_URL")
    garmin_email = os.environ.get("GARMIN_EMAIL")
    garmin_password = os.environ.get("GARMIN_PASSWORD")

    if not all([db_url, garmin_email, garmin_password]):
        log.error("Missing env vars. Need DATABASE_URL, GARMIN_EMAIL, GARMIN_PASSWORD")
        sys.exit(1)

    # Authenticate to Garmin
    log.info("Logging in to Garmin Connect...")
    garmin = Garmin(garmin_email, garmin_password)
    try:
        garmin.login()
    except Exception as e:
        log.error("Garmin login failed: %s", e)
        sys.exit(1)

    # Fetch last N days
    today = date.today()
    rows = []
    errors = 0

    for days_ago in range(LOOKBACK_DAYS):
        target = today - timedelta(days=days_ago)
        date_str = target.isoformat()
        try:
            raw = garmin.get_sleep_data(date_str)
            row = parse_record(date_str, raw)
            if row:
                rows.append(row)
                log.info("  %s — OK", date_str)
            else:
                log.warning("  %s — no sleep data in response", date_str)
        except Exception as e:
            error_msg = str(e)
            if "429" in error_msg or "Too Many Requests" in error_msg:
                log.error("Rate limited by Garmin (429). Exiting gracefully.")
                sys.exit(0)
            log.error("  %s — FAILED: %s", date_str, e)
            errors += 1

        time.sleep(1)

    if not rows:
        log.warning("No valid rows to upsert.")
        sys.exit(0)

    # Upsert into Postgres
    log.info("Upserting %d rows into sleep_sessions...", len(rows))
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            execute_values(cur, UPSERT_SQL, rows, page_size=100)
        conn.commit()
        log.info("Done. Upserted %d rows, %d errors.", len(rows), errors)
    except Exception as e:
        log.error("Database error: %s", e)
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()