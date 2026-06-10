import argparse
import boto3
from config.config import Config

def list_all_files(bucket_name, prefix=""):
    """List all files in a bucket with optional prefix"""
    s3_client = boto3.client(
        's3',
        aws_access_key_id=Config.AWS_ACCESS_KEY,
        aws_secret_access_key=Config.AWS_SECRET_KEY
    )
    
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    
    file_count = 0
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if not key.endswith('/'):
                    print(f"  📄 {key}")
                    file_count += 1
    
    print(f"\n✅ Total files: {file_count}")
    return file_count

def main():
    parser = argparse.ArgumentParser(description="List S3 bucket contents")
    parser.add_argument(
        'mode',
        choices=['raw', 'processed-with-kaggle', 'processed-without-kaggle', 'all'],
        help="Which mode to run"
    )
    
    args = parser.parse_args()
    
    if args.mode == 'raw':
        print(f"\n🔍 RAW BUCKET: {Config.S3_BUCKET_RAW}\n" + "="*50)
        list_all_files(Config.S3_BUCKET_RAW)
        
    elif args.mode == 'processed-with-kaggle':
        print(f"\n🔍 PROCESSED BUCKET (with Kaggle): {Config.S3_BUCKET}\n" + "="*50)
        list_all_files(Config.S3_BUCKET)
        
    elif args.mode == 'processed-without-kaggle':
        print(f"\n🔍 PROCESSED BUCKET (without Kaggle): {Config.S3_BUCKET}\n" + "="*50)
        # List everything except kaggle folder
        list_all_files(Config.S3_BUCKET, prefix="processed/jolpica/")
        list_all_files(Config.S3_BUCKET, prefix="processed/openf1/")
        # Add other non-kaggle folders here if needed
        
    elif args.mode == 'all':
        print(f"\n🔍 RAW BUCKET: {Config.S3_BUCKET_RAW}\n" + "="*50)
        list_all_files(Config.S3_BUCKET_RAW)
        
        print(f"\n\n🔍 PROCESSED BUCKET (with Kaggle): {Config.S3_BUCKET}\n" + "="*50)
        list_all_files(Config.S3_BUCKET)

if __name__ == "__main__":
    # If no args, show interactive menu
    import sys
    if len(sys.argv) == 1:
        print("\n📋 S3 Bucket File Lister")
        print("="*40)
        print("1. Raw Bucket")
        print("2. Processed Bucket (with Kaggle)")
        print("3. Processed Bucket (without Kaggle)")
        print("4. All Buckets")
        
        choice = input("\nEnter choice (1/2/3/4): ").strip()
        
        mode_map = {
            '1': 'raw',
            '2': 'processed-with-kaggle', 
            '3': 'processed-without-kaggle',
            '4': 'all'
        }
        
        if choice in mode_map:
            args = argparse.Namespace(mode=mode_map[choice])
            main()
        else:
            print("❌ Invalid choice")
    else:
        main()