import boto3
import os
from dotenv import load_dotenv

load_dotenv()

def download_s3_file(local_path='data/spy_minute_data.csv'):
    bucket = os.getenv("S3_BUCKET_NAME")
    s3_key = os.getenv("S3_DATA_PATH")

    s3 = boto3.client("s3")
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    s3.download_file(bucket, s3_key, local_path)
    return local_path
