
import configparser
import os
import sys

# ----------------------------
# Resolve project root safely
# ----------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CONFIG_PATH = os.path.join(BASE_DIR, "config", "config.conf")

if not os.path.exists(CONFIG_PATH):
    raise FileNotFoundError(f"config.conf not found at {CONFIG_PATH}")

# ----------------------------
# Load config
# ----------------------------
parser = configparser.ConfigParser()
parser.read(CONFIG_PATH)

print("CONFIG_PATH =", CONFIG_PATH)
print("CONFIG EXISTS =", os.path.exists(CONFIG_PATH))
print("SECTIONS FOUND =", parser.sections())

if not parser.sections():
    raise RuntimeError(f"config.conf loaded but no sections found: {CONFIG_PATH}")

# ----------------------------
# Database
# ----------------------------
# --------------------------------------------------
# Database (Postgres / Airflow metadata DB)
# --------------------------------------------------
DATABASE_HOST = parser.get("postgres", "database_host")
DATABASE_NAME = parser.get("postgres", "database_name")
DATABASE_PORT = parser.getint("postgres", "database_port")
DATABASE_USER = parser.get("postgres", "database_username")
DATABASE_PASSWORD = parser.get("postgres", "database_password")

# ----------------------------
# AWS
# ----------------------------
AWS_ACCESS_KEY_ID = parser.get("aws", "aws_access_key_id")
AWS_SECRET_ACCESS_KEY = parser.get("aws", "aws_secret_access_key")
AWS_REGION = parser.get("aws", "aws_region")
AWS_BUCKET_NAME = parser.get("aws", "aws_bucket_name")

# ----------------------------
# File paths
# ----------------------------
INPUT_PATH = parser.get("file_paths", "input_path")
OUTPUT_PATH = parser.get("file_paths", "output_path")