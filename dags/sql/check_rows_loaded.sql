SELECT
    COUNT(*) AS rows_loaded
FROM reddit_posts_final
WHERE created_utc >= (
    SELECT last_processed_timestamp
    FROM ingestion_metadata
    WHERE source_name = 'reddit_files'
);