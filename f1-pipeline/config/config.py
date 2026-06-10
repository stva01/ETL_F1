import os
from pathlib import Path
from dotenv import load_dotenv

# Get the absolute path of config.py
current_file_path = Path(__file__).resolve()

# Go up TWO levels: config.py -> config/ -> f1-pipeline/
pipeline_dir = current_file_path.parent.parent 

# Now it correctly finds C:\Satva\Tech\ETL_F1\f1-pipeline\.env
load_dotenv(dotenv_path=pipeline_dir / ".env")

class Config:
    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    S3_BUCKET = os.getenv("S3_PROCESSED_BUCKET")  # Correctly using your new variable
    GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
    BQ_DATASET_ID = os.getenv("BQ_DATASET_ID")
    S3_BUCKET_RAW = os.getenv("S3_RAW_BUCKET")  # For raw data access in OpenF1 ingestion