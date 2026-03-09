-- sql/insert_run_metrics.sql
INSERT INTO pipeline_run_metrics (
    dag_run_id,
    inserted_rows,
    run_ts
)
SELECT
    '{{ dag_run.run_id }}',
    COUNT(*),
    GETDATE()
FROM reddit_posts_staging
WHERE created_utc > (
    SELECT last_processed_timestamp
    FROM ingestion_metadata
    WHERE source_name = 'reddit_files'
);