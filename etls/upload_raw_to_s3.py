import os
import time
import sys
import shutil

sys.path.append("/opt/airflow")
from utils.s3_utils import upload_file_to_s3

INPUT_DIR = "/opt/airflow/data/input/"
ARCHIVE_DIR = "/opt/airflow/data/archive/"
S3_BASE_PREFIX = "raw/reddit/incoming/"

os.makedirs(ARCHIVE_DIR, exist_ok=True)

for file_name in os.listdir(INPUT_DIR):
    if not file_name.endswith(".csv"):
        continue

    local_file_path = os.path.join(INPUT_DIR, file_name)
    timestamp = int(time.time())
    s3_key = f"{S3_BASE_PREFIX}latest_{file_name}"

    print(f"📤 Uploading {file_name} → s3://{s3_key}")

    upload_file_to_s3(local_file_path, s3_key)

    shutil.move(
        local_file_path,
        os.path.join(ARCHIVE_DIR, file_name)
    )

    print(f"📦 Archived {file_name}")