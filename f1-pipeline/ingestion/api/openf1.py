"""
openf1.py — Incremental ingestion from the OpenF1 API

Purpose:
    Fetch near-real-time and post-race F1 data from the OpenF1 REST API
    (https://openf1.org) and store raw JSON/CSV in the S3 raw bucket.
    Designed to run automatically after each race via Airflow.

Endpoints of interest:
    - /v1/sessions   → session metadata (practice, qualifying, race)
    - /v1/drivers    → driver info per session
    - /v1/laps       → lap-by-lap timing data
    - /v1/stints     → tyre stint data
    - /v1/car_data   → telemetry snapshots
    - /v1/position   → position changes

Steps:
    1. Determine the latest session not yet ingested (track watermark)
    2. Call relevant endpoints for that session
    3. Write raw responses to  s3://<RAW_BUCKET>/openf1/<endpoint>/<session_key>/
    4. Update watermark

Dependencies:
    - requests
    - boto3
    - python-dotenv
"""

import os
import json
import time
import boto3
import requests
from dotenv import load_dotenv
from datetime import date

load_dotenv()

BASE_URL      = "https://api.openf1.org/v1"
S3_BUCKET     = os.getenv("S3_RAW_BUCKET")
S3_PREFIX     = "raw/openf1"
WATERMARK_KEY = "raw/watermark/openf1_watermark.json"
START_YEAR    = 2025

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

# ── Helpers ────────────────────────────────────────────────────

def fetch(endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}"
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    time.sleep(0.3)
    return response.json()

def upload(data, s3_key):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(data),
        ContentType="application/json"
    )
    print(f"  Uploaded → s3://{S3_BUCKET}/{s3_key}")

# ── Watermark 

def read_watermark():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=WATERMARK_KEY)
        return json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        return {"year": START_YEAR, "last_session_key": 0}

def write_watermark(year, last_session_key):
    watermark = {
        "year": year,
        "last_session_key": last_session_key,
        "updated_at": str(date.today())
    }
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=WATERMARK_KEY,
        Body=json.dumps(watermark),
        ContentType="application/json"
    )
    print(f"  Watermark updated → session_key {last_session_key}")

# ── Ingest one session 

def ingest_session(session):
    """Fetch all endpoints for one race session and push to S3."""
    session_key  = session["session_key"]
    year         = session["year"]
    round_num    = session["meeting_key"]  # OpenF1's meeting_key maps to round
    session_type = session["session_name"].lower().replace(" ", "_")

    prefix = f"{S3_PREFIX}/{year}/meeting_{round_num}/{session_type}"
    print(f"  Ingesting session {session_key} ({session_type}, {year})...")

    # session metadata itself
    upload(session, f"{prefix}/session_metadata.json")

    endpoints = {
        "drivers": {"session_key": session_key},
        "laps":    {"session_key": session_key},
        "stints":  {"session_key": session_key},
        "pit":     {"session_key": session_key},
    }

    for name, params in endpoints.items():
        try:
            data = fetch(name, params)
            upload(data, f"{prefix}/{name}.json")
        except requests.HTTPError as e:
            print(f"    Skipped {name} (HTTP {e.response.status_code})")

# ── Main 

def main():
    watermark        = read_watermark()
    year             = watermark["year"]
    last_session_key = watermark["last_session_key"]

    print(f"Watermark: year {year}, last session_key {last_session_key}")

    # fetch all race sessions for the year
    all_sessions = fetch("sessions", {
        "year": year,
        "session_name": "Race"
    })

    # filter to only sessions not yet ingested
    new_sessions = [
        s for s in all_sessions
        if s["session_key"] > last_session_key
    ]

    if not new_sessions:
        print("Already up to date. Nothing to ingest.")
        return

    print(f"Found {len(new_sessions)} new session(s) to ingest.")

    for session in new_sessions:
        ingest_session(session)
        # update watermark after each session
        write_watermark(year, session["session_key"])

    print(f"\nOpenF1 ingestion complete.")

if __name__ == "__main__":
    main()