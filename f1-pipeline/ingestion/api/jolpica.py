"""
jolpica.py — Incremental ingestion from the Jolpica (Ergast successor) API

Purpose:
    Fetch structured F1 results data from the Jolpica REST API
    (https://api.jolpi.ca/ergast/) — the community-maintained successor
    to the deprecated Ergast API — and store raw JSON in S3.
    Runs automatically after each race via Airflow.

Endpoints of interest:
    - /f1/{season}/results          → race results
    - /f1/{season}/qualifying       → qualifying results
    - /f1/{season}/sprint           → sprint results
    - /f1/{season}/driverStandings  → championship standings
    - /f1/{season}/constructorStandings
    - /f1/{season}/circuits         → circuit details
    - /f1/{season}/drivers          → driver details

Steps:
    1. Determine latest round not yet ingested (track watermark)
    2. Call relevant endpoints for new round(s)
    3. Write raw JSON to  s3://<RAW_BUCKET>/jolpica/<endpoint>/<season>/<round>/
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

# JOLPICA_BASE_URL = "https://api.jolpi.ca/ergast/f1"
# S3_RAW_BUCKET    = os.getenv("S3_RAW_BUCKET")


def fetch_race_results(season: int, round_num: int):
    """Fetch race results for a specific round."""
    # TODO: GET {JOLPICA_BASE_URL}/{season}/{round_num}/results.json
    pass


def fetch_qualifying(season: int, round_num: int):
    """Fetch qualifying results for a specific round."""
    # TODO: GET {JOLPICA_BASE_URL}/{season}/{round_num}/qualifying.json
    pass


def fetch_standings(season: int, standing_type: str = "driverStandings"):
    """Fetch current championship standings."""
    # TODO: GET {JOLPICA_BASE_URL}/{season}/{standing_type}.json
    pass


def upload_to_s3(data, endpoint: str, season: int, round_num: int):
    """Upload raw API response to the appropriate S3 path."""
    # TODO: s3://<RAW_BUCKET>/jolpica/{endpoint}/{season}/{round_num}.json
    pass


def main():
    """Orchestrate incremental Jolpica ingestion."""
    # TODO: Implement watermark-based incremental fetch
    pass


if __name__ == "__main__":
    main()
