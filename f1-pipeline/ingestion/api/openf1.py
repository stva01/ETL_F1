import os
import json
import time
import boto3
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from ingestion.utils.logger import get_logger
from datetime import date, datetime, timezone

load_dotenv()

logger = get_logger("openf1.ingestion")

BASE_URL      = "https://api.openf1.org/v1"
S3_BUCKET     = os.getenv("S3_RAW_BUCKET")
S3_PREFIX     = "raw/openf1"
WATERMARK_KEY = "raw/watermark/openf1_watermark.json"
START_YEAR    = 2023

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)


def get_current_season():
    today = date.today()
    if today.month <= 2:
        return today.year - 1
    return today.year

def get_completed_sessions(year, last_session_key):
    """
    Fetch only race sessions that have actually happened.
    OpenF1 pre-registers future sessions — filtering by date_start
    prevents attempting to fetch data that doesn't exist yet.
    """
    all_sessions = fetch(f"{BASE_URL}/sessions", params={"year": year, "session_name": "Race"})

    if not all_sessions:
        logger.info("no race sessions found for year=%s", year)
        return []

    now = datetime.now(timezone.utc)

    completed = [
        s for s in all_sessions
        if s["session_key"] > last_session_key
        and datetime.fromisoformat(s["date_end"].replace("Z", "+00:00")) < now
    ]

    logger.info(
        "sessions this year=%s total=%s new=%s completed=%s",
        year, len(all_sessions),
        len([s for s in all_sessions if s["session_key"] > last_session_key]),
        len(completed)
    )

    return completed

def fetch(url, params=None, retries=3):
    for attempt in range(1, retries + 1):
        try:
            logger.debug("fetching url=%s params=%s attempt=%s", url, params, attempt)
            response = requests.get(url, params=params, timeout=30)

            if response.status_code == 429:
                wait = 10 * attempt
                logger.warning("rate limited — waiting %ss (attempt %s/%s)", wait, attempt, retries)
                time.sleep(wait)
                continue

            if response.status_code == 404:
                logger.debug("404 not found — url=%s", url)
                raise requests.HTTPError(response=response)

            response.raise_for_status()
            time.sleep(1)
            return response.json()

        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                raise
            if attempt == retries:
                logger.error("all %s retries exhausted url=%s error=%s", retries, url, e)
                raise
            logger.warning("http error attempt %s/%s waiting %ss error=%s", attempt, retries, 5 * attempt, e)
            time.sleep(5 * attempt)

        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt == retries:
                logger.error("connection failed after %s retries url=%s error=%s", retries, url, e)
                raise
            logger.warning("connection error attempt %s/%s url=%s", attempt, retries, url)
            time.sleep(5 * attempt)

    raise requests.HTTPError(f"failed after {retries} retries: {url}")


def upload(data, s3_key):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(data),
        ContentType="application/json"
    )
    logger.info("uploaded s3://%s/%s | records=%s", S3_BUCKET, s3_key, len(data) if isinstance(data, list) else 1)


def read_watermark():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=WATERMARK_KEY)
        watermark = json.loads(obj["Body"].read())
        logger.info("watermark read — year=%s last_session_key=%s", watermark["year"], watermark["last_session_key"])
        return watermark
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            logger.info("no watermark found — first run, defaulting to session_key=0")
            return {"year": START_YEAR, "last_session_key": 0}
        logger.error("unexpected s3 error reading watermark — %s", e)
        raise


def write_watermark(year, last_session_key):
    payload = {"year": year, "last_session_key": last_session_key, "updated_at": str(date.today())}
    s3.put_object(Bucket=S3_BUCKET, Key=WATERMARK_KEY, Body=json.dumps(payload), ContentType="application/json")
    logger.info("watermark updated — year=%s last_session_key=%s", year, last_session_key)


def validate_session(session_key, drivers, laps):
    """
    Validate that a session has meaningful data before storing.
    Catches cases where OpenF1 has the session registered but
    data hasn't been published yet.
    """
    if not drivers:
        return False, "no drivers returned — session data not published yet"

    if len(drivers) < 10:
        return False, f"only {len(drivers)} drivers — incomplete session"

    if not laps:
        return False, "no lap data returned — session data not published yet"

    driver_numbers_in_laps = {lap.get("driver_number") for lap in laps}
    if len(driver_numbers_in_laps) < 10:
        return False, f"lap data only covers {len(driver_numbers_in_laps)} drivers — incomplete"

    return True, "ok"


def write_validation_log(session_key, year, meeting_key, is_valid, reason):
    log = {
        "session_key": session_key,
        "year": year,
        "meeting_key": meeting_key,
        "is_valid": is_valid,
        "reason": reason,
        "checked_at": str(date.today())
    }
    key = f"raw/validation/openf1/{year}/meeting_{meeting_key}/session_{session_key}.json"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(log), ContentType="application/json")
    if is_valid:
        logger.info("validation passed — session_key=%s", session_key)
    else:
        logger.warning("validation failed — session_key=%s reason=%s", session_key, reason)


def ingest_session(session):
    session_key  = session["session_key"]
    year         = session["year"]
    meeting_key  = session["meeting_key"]
    session_name = session.get("session_name", "race").lower().replace(" ", "_")

    logger.info("ingesting session_key=%s year=%s meeting=%s", session_key, year, meeting_key)
    prefix = f"{S3_PREFIX}/{year}/meeting_{meeting_key}/{session_name}"

    try:
        drivers = fetch(f"{BASE_URL}/drivers", params={"session_key": session_key})
        laps    = fetch(f"{BASE_URL}/laps",    params={"session_key": session_key})
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            logger.warning("session_key=%s not published yet — watermark will not advance", session_key)
            write_validation_log(session_key, year, meeting_key, False, "404 — data not published yet")
            return False
        raise

    is_valid, reason = validate_session(session_key, drivers, laps)
    write_validation_log(session_key, year, meeting_key, is_valid, reason)

    if not is_valid:
        logger.warning("skipping session_key=%s — watermark will not advance", session_key)
        return False

    upload(session, f"{prefix}/session_metadata.json")
    upload(drivers, f"{prefix}/drivers.json")
    upload(laps,    f"{prefix}/laps.json")

    supplementary = {
        "stints": {"session_key": session_key},
        "pit":    {"session_key": session_key},
    }

    for name, params in supplementary.items():
        try:
            data = fetch(f"{BASE_URL}/{name}", params=params)
            if data:
                upload(data, f"{prefix}/{name}.json")
            else:
                logger.debug("no %s data for session_key=%s — skipping", name, session_key)
        except requests.HTTPError as e:
            logger.error("failed to fetch %s for session_key=%s — error=%s", name, session_key, e)

    return True


def main():
    logger.info("openf1 ingestion started")

    watermark      = read_watermark()
    current_season = get_current_season()

    if watermark["year"] < current_season:
        logger.info("new season detected — advancing from %s to %s", watermark["year"], current_season)
        watermark = {"year": current_season, "last_session_key": 0}
        write_watermark(current_season, 0)

    year             = watermark["year"]
    last_session_key = watermark["last_session_key"]

    new_sessions = get_completed_sessions(year, last_session_key)

    if not new_sessions:
        logger.info("already up to date — nothing to ingest")
        return

    logger.info("found %s completed session(s) to ingest", len(new_sessions))

    for session in new_sessions:
        success = ingest_session(session)
        if success:
            write_watermark(year, session["session_key"])
        else:
            logger.warning("stopping at session_key=%s — will retry on next run", session["session_key"])
            break

    logger.info("openf1 ingestion complete")
# ```

# This filters on `date_end` rather than `date_start` — a session is only considered complete when its end time has passed. Now the log will show something like:
# ```
# sessions this year=2026 total=24 new=22 completed=2
# found 2 completed session(s) to ingest


if __name__ == "__main__":
    main()