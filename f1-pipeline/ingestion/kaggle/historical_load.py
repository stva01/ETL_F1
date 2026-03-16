import os
import boto3
from kaggle.api.kaggle_api_extended import KaggleApi
from dotenv import load_dotenv
from datetime import date
import zipfile
import shutil

load_dotenv()

# Config
DATASET = "rohanrao/formula-1-world-championship-1950-2020"
DOWNLOAD_DIR = "tmp_kaggle"
S3_BUCKET = os.getenv("S3_RAW_BUCKET")
S3_PREFIX = f"raw/kaggle/{date.today()}/"

def download_dataset():
    print("Downloading dataset from Kaggle...")
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(DATASET, path=DOWNLOAD_DIR, unzip=True)
    print("Download complete.")

def upload_to_s3():
    print("Uploading to S3...")
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION")
    )

    files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith(".csv")]

    for filename in files:
        local_path = os.path.join(DOWNLOAD_DIR, filename)
        s3_key = S3_PREFIX + filename
        print(f"  Uploading {filename} -> s3://{S3_BUCKET}/{s3_key}")
        s3.upload_file(local_path, S3_BUCKET, s3_key)

    print(f"Done. {len(files)} files uploaded.")

def cleanup():
    print("Cleaning up local files...")
    shutil.rmtree(DOWNLOAD_DIR)

if __name__ == "__main__":
    download_dataset()
    upload_to_s3()
    cleanup()
    print("Kaggle historical load complete.")