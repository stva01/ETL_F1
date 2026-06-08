from pipelines.s3_to_bigquery import run_s3_to_bigquery_pipeline

if __name__ == "__main__":
    # Example paths - replace with your actual keys in S3
    s3_parquet_key = "processed_data/f1_race_results.parquet"
    target_bq_table = "race_results"
    
    try:
        run_s3_to_bigquery_pipeline(s3_parquet_key, target_bq_table)
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")