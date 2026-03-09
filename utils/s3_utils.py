import boto3
from utils.constants import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    AWS_BUCKET_NAME
)


def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )


def upload_file_to_s3(local_path: str, s3_key: str):
    s3 = get_s3_client()
    s3.upload_file(local_path, AWS_BUCKET_NAME, s3_key)
    print(f"[INFO] Uploaded {local_path} to s3://{AWS_BUCKET_NAME}/{s3_key}")