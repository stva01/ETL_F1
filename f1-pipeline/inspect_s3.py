import boto3
from config.config import Config

def list_s3_contents():
    if not Config.S3_BUCKET:
        print("❌ Error: S3_BUCKET_NAME environment variable is not loaded. Check your .env path.")
        return

    print(f"🔍 Scanning bucket: {Config.S3_BUCKET}...\n")
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id=Config.AWS_ACCESS_KEY,
        aws_secret_access_key=Config.AWS_SECRET_KEY
    )
    
    try:
        # List objects in the bucket
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=Config.S3_BUCKET)
        
        file_count = 0
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    # Skip empty folder metadata items
                    if key.endswith('/'):
                        continue
                    print(f"  📄 {key}")
                    file_count += 1
            else:
                print("The bucket appears to be empty.")
                
        print(f"\nTotal files discovered: {file_count}")
        
    except Exception as e:
        print(f"❌ Failed to read S3 bucket: {e}")

if __name__ == "__main__":
    list_s3_contents()