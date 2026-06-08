import io
import boto3
from google.cloud import bigquery
from config.config import Config

def run_s3_to_bigquery_pipeline(s3_file_key: str, target_table_name: str):
    """
    Streams a Parquet file from S3 and loads it straight into a BigQuery table.
    """
    # 1. Initialize Cloud Clients
    s3_client = boto3.client(
        's3',
        aws_access_key_id=Config.AWS_ACCESS_KEY,
        aws_secret_access_key=Config.AWS_SECRET_KEY
    )
    
    # bigquery.Client automatically checks the ADC file we set up in terminal
    bq_client = bigquery.Client(project=Config.GCP_PROJECT_ID)
    
    # 2. Build full BigQuery Table path: project_id.dataset_id.table_name
    table_id = f"{Config.GCP_PROJECT_ID}.{Config.BQ_DATASET_ID}.{target_table_name}"
    
    print(f"📥 Fetching s3://{Config.S3_BUCKET}/{s3_file_key} into memory...")
    
    # 3. Stream Parquet into an in-memory byte buffer
    parquet_buffer = io.BytesIO()
    s3_client.download_fileobj(Config.S3_BUCKET, s3_file_key, parquet_buffer)
    parquet_buffer.seek(0) # Reset buffer pointer to start
    
    # 4. Configure BigQuery Load Job Options
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Overwrites old data if table exists
        autodetect=True # Automatically builds schema from Parquet file metadata
    )
    
    print(f"🚀 Loading directly into BigQuery Table: {table_id}...")
    
    # 5. Run the load job
    load_job = bq_client.load_table_from_file(
        parquet_buffer,
        table_id,
        job_config=job_config
    )
    
    # Wait for completion
    load_job.result()
    
    # Verify rows loaded
    table = bq_client.get_table(table_id)
    print(f"✅ Success! Loaded {table.num_rows} rows into {table_id}.\n")