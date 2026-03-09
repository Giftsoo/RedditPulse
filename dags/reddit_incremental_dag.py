from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import sys

# Allow imports from /opt/airflow
sys.path.append("/opt/airflow")

from etls.sentiment_transform import compute_sentiment


# =========================================================
# CHECK S3 INCOMING FILES (CSV ONLY)



# =========================================================
# DATA QUALITY CHECK (PYTHON-BASED)
# =========================================================
def fail_if_rows_exist(sql, check_name):
    hook = PostgresHook(postgres_conn_id="redshift_serverless")
    result = hook.get_first(sql)
    bad_rows = result[0]

    if bad_rows > 0:
        raise ValueError(f"❌ DATA QUALITY FAILED ({check_name}): {bad_rows} bad rows")

    print(f"✅ {check_name} passed")


# =========================================================
# SENTIMENT COMPLETENESS VALIDATION (INCREMENTAL)
# =========================================================
def validate_sentiment_completion():
    hook = PostgresHook(postgres_conn_id="redshift_serverless")

    record = hook.get_first("""
        SELECT COUNT(*)
        FROM reddit_posts_final f
        LEFT JOIN reddit_post_sentiment s
            ON f.post_id = s.post_id
        WHERE f.created_utc >= (
            SELECT last_processed_timestamp
            FROM ingestion_metadata
            WHERE source_name = 'reddit_files'
        )
        AND s.post_id IS NULL;
    """)

    missing = record[0]

    if missing > 0:
        raise ValueError(
            f"❌ SENTIMENT VALIDATION FAILED: {missing} new posts missing sentiment"
        )

    print("✅ Sentiment validation passed for incremental data")


# =========================================================
# DAG DEFAULT ARGS
# =========================================================
default_args = {
    "owner": "airflow",
    "email": ["giftsondhanapal@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
}


# =========================================================
# DAG DEFINITION
# =========================================================
with DAG(
    dag_id="reddit_incremental_pipeline",
    start_date=datetime(2026, 3, 5),
    schedule_interval="*/5 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["reddit", "etl", "redshift", "sentiment", "analytics"],
) as dag:

    # -------------------------
    # INGESTION
    # -------------------------
    extract = BashOperator(
        task_id="extract_incremental",
        bash_command="python /opt/airflow/etls/extract_incremental.py"
    )

    upload = BashOperator(
        task_id="upload_to_s3",
        bash_command="python /opt/airflow/etls/upload_raw_to_s3.py"
    )


    copy_to_staging = PostgresOperator(
        task_id="copy_to_staging",
        postgres_conn_id="redshift_serverless",
        sql="sql/copy_to_staging.sql"
    )

    # -------------------------
    # TRANSFORMATION
    # -------------------------
    transform_to_final = PostgresOperator(
        task_id="transform_to_final",
        postgres_conn_id="redshift_serverless",
        sql="sql/transform_to_final.sql"
    )

    # -------------------------
    # SENTIMENT ANALYSIS
    # -------------------------
    sentiment_transform = PythonOperator(
        task_id="sentiment_transform",
        python_callable=compute_sentiment
    )

    # -------------------------
    # PIPELINE METRICS
    # -------------------------
    insert_run_metrics = PostgresOperator(
        task_id="insert_run_metrics",
        postgres_conn_id="redshift_serverless",
        sql="sql/insert_run_metrics.sql"
    )

    # -------------------------
    # VALIDATIONS
    # -------------------------
    validate_sentiment = PythonOperator(
        task_id="validate_sentiment",
        python_callable=validate_sentiment_completion
    )

    dq_post_id_not_null = PythonOperator(
        task_id="dq_post_id_not_null",
        python_callable=fail_if_rows_exist,
        op_kwargs={
            "sql": "SELECT COUNT(*) FROM reddit_posts_final WHERE post_id IS NULL;",
            "check_name": "post_id_not_null"
        }
    )

    # -------------------------
    # ANALYTICS LAYER
    # -------------------------
    create_analytics_views = PostgresOperator(
        task_id="create_analytics_views",
        postgres_conn_id="redshift_serverless",
        sql="sql/analytics_views.sql"
    )

    # -------------------------
    # STATE + OUTPUT
    # -------------------------
    update_watermark = PostgresOperator(
        task_id="update_watermark",
        postgres_conn_id="redshift_serverless",
        sql="sql/update_watermark.sql"
    )

    unload_curated_to_s3 = PostgresOperator(
        task_id="unload_curated_to_s3",
        postgres_conn_id="redshift_serverless",
        sql="sql/unload_curated_to_s3.sql"
    )

    cleanup_staging = PostgresOperator(
        task_id="cleanup_staging",
        postgres_conn_id="redshift_serverless",
        sql="TRUNCATE TABLE reddit_posts_staging;"
    )
    dq_no_duplicate_post_ids = PythonOperator(
        task_id="dq_no_duplicate_post_ids",
        python_callable=fail_if_rows_exist,
        op_kwargs={
            "sql": """
                   SELECT COUNT(*)
                   FROM (SELECT post_id
                         FROM reddit_posts_final
                         GROUP BY post_id
                         HAVING COUNT(*) > 1) d;
                   """,
            "check_name": "No duplicate post_id in reddit_posts_final"
        }
    )

    # =========================================================
    # DAG FLOW (FINAL)
    # =========================================================
    (
        extract
        >> upload
        >> copy_to_staging
        >> transform_to_final
        >> sentiment_transform
        >> insert_run_metrics
        >> validate_sentiment
        >> dq_post_id_not_null
        >>dq_no_duplicate_post_ids
        >> create_analytics_views
        >> update_watermark
        >> unload_curated_to_s3
        >> cleanup_staging
    )