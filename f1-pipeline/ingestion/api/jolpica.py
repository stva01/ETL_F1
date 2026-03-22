import os
import json
import time
import boto3
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from datetime import date
from ingestion.utils.logger import get_logger

load_dotenv()

logger = get_logger("jolpica.ingestion")

BASE_URL      = "https://api.jolpi.ca/ergast/f1"
S3_BUCKET     = os.getenv("S3_RAW_BUCKET")
S3_PREFIX     = "raw/jolpica"
WATERMARK_KEY = "raw/watermark/jolpica_watermark.json"
START_SEASON  = 2025

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


def fetch(url, retries=3):
    for attempt in range(1, retries + 1):
        try:
            logger.debug("fetching url=%s attempt=%s", url, attempt)
            response = requests.get(url, timeout=10)

            if response.status_code == 429:
                wait = 10 * attempt
                logger.warning("rate limited — waiting %ss (attempt %s/%s)", wait, attempt, retries)
                time.sleep(wait)
                continue

            response.raise_for_status()
            time.sleep(1)
            return response.json()

        except requests.HTTPError as e:
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
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=json.dumps(data), ContentType="application/json")
    logger.info("uploaded s3://%s/%s", S3_BUCKET, s3_key)


def read_watermark():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=WATERMARK_KEY)
        watermark = json.loads(obj["Body"].read())
        logger.info("watermark read — season=%s last_round=%s", watermark["season"], watermark["last_round"])
        return watermark
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            logger.info("no watermark found — first run, defaulting to round 0")
            return {"season": START_SEASON, "last_round": 0}
        logger.error("unexpected s3 error reading watermark — %s", e)
        raise


def write_watermark(season, last_round):
    payload = {"season": season, "last_round": last_round, "updated_at": str(date.today())}
    s3.put_object(Bucket=S3_BUCKET, Key=WATERMARK_KEY, Body=json.dumps(payload), ContentType="application/json")
    logger.info("watermark updated — season=%s last_round=%s", season, last_round)


def validate_round(data, season, round_num):
    try:
        races = data["MRData"]["RaceTable"]["Races"]
        if not races:
            return False, "empty response — api not updated yet"
        results = races[0].get("Results", [])
        if len(results) < 10:
            return False, f"only {len(results)} results — incomplete data"
        missing = [r for r in results if not r.get("Driver", {}).get("driverId")]
        if missing:
            return False, f"{len(missing)} results missing driverId"
        return True, "ok"
    except (KeyError, IndexError) as e:
        return False, f"unexpected response structure: {e}"


def write_validation_log(season, round_num, endpoint, is_valid, reason):
    log = {"season": season, "round": round_num, "endpoint": endpoint,
           "is_valid": is_valid, "reason": reason, "checked_at": str(date.today())}
    key = f"raw/validation/jolpica/{season}/round_{round_num:02d}/{endpoint}.json"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(log), ContentType="application/json")
    if is_valid:
        logger.info("validation passed — round=%s endpoint=%s", round_num, endpoint)
    else:
        logger.warning("validation failed — round=%s endpoint=%s reason=%s", round_num, endpoint, reason)


def get_total_rounds(season):
    data = fetch(f"{BASE_URL}/{season}.json")
    total = len(data["MRData"]["RaceTable"]["Races"])
    logger.info("jolpica reports %s rounds for season %s", total, season)
    return total


def ingest_round(season, round_num):
    logger.info("ingesting season=%s round=%s", season, round_num)
    prefix = f"{S3_PREFIX}/seasons/{season}/round_{round_num:02d}"

    results_data = fetch(f"{BASE_URL}/{season}/{round_num}/results.json")
    is_valid, reason = validate_round(results_data, season, round_num)
    write_validation_log(season, round_num, "results", is_valid, reason)

    if not is_valid:
        logger.warning("skipping round=%s — watermark will not advance", round_num)
        return False

    upload(results_data, f"{prefix}/results.json")

    other_endpoints = {
        "qualifying":            f"{BASE_URL}/{season}/{round_num}/qualifying.json",
        "pitstops":              f"{BASE_URL}/{season}/{round_num}/pitstops.json?limit=100",
        "driver_standings":      f"{BASE_URL}/{season}/{round_num}/driverStandings.json",
        "constructor_standings": f"{BASE_URL}/{season}/{round_num}/constructorStandings.json",
        "sprint":                f"{BASE_URL}/{season}/{round_num}/sprint.json",
    }

    for name, url in other_endpoints.items():
        try:
            data = fetch(url)
            upload(data, f"{prefix}/{name}.json")
            write_validation_log(season, round_num, name, True, "ok")
        except requests.HTTPError as e:
            status = e.response.status_code
            if status == 404:
                logger.debug("no %s data for round=%s — expected for non-sprint rounds", name, round_num)
            else:
                logger.error("unexpected http %s fetching %s round=%s", status, name, round_num)
            write_validation_log(season, round_num, name, False, f"http {status}")

    return True


def main():
    logger.info("jolpica ingestion started")

    watermark      = read_watermark()
    current_season = get_current_season()

    if watermark["season"] < current_season:
        logger.info("new season detected — advancing from %s to %s",
                    watermark["season"], current_season)
        watermark = {"season": current_season, "last_round": 0}
        write_watermark(current_season, 0)

    season       = watermark["season"]
    last_round   = watermark["last_round"]
    total_rounds = get_total_rounds(season)

    if last_round >= total_rounds:
        logger.info("already up to date — nothing to ingest")
        return

    rounds_to_fetch = list(range(last_round + 1, total_rounds + 1))
    logger.info("rounds to ingest: %s", rounds_to_fetch)

    for round_num in rounds_to_fetch:
        success = ingest_round(season, round_num)
        if success:
            write_watermark(season, round_num)
        else:
            logger.warning("stopping at round=%s — will retry on next run", round_num)
            break

    logger.info("jolpica ingestion complete")


if __name__ == "__main__":
    main()