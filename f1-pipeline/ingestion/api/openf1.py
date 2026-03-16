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

# from dotenv import load_dotenv
# import requests
# import boto3

# load_dotenv()

# OPENF1_BASE_URL = "https://api.openf1.org/v1"
# S3_RAW_BUCKET   = os.getenv("S3_RAW_BUCKET")


def fetch_sessions(year: int):
    """Fetch all sessions for a given year."""
    # TODO: GET {OPENF1_BASE_URL}/sessions?year={year}
    pass


def fetch_laps(session_key: int):
    """Fetch lap data for a specific session."""
    # TODO: GET {OPENF1_BASE_URL}/laps?session_key={session_key}
    pass


def upload_to_s3(data, endpoint: str, session_key: int):
    """Upload raw API response to the appropriate S3 path."""
    # TODO: s3://<RAW_BUCKET>/openf1/{endpoint}/{session_key}.json
    pass


def main():
    """Orchestrate incremental OpenF1 ingestion."""
    # TODO: Implement watermark-based incremental fetch
    pass


if __name__ == "__main__":
    main()
