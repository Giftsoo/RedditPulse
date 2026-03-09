UPDATE ingestion_metadata
SET last_processed_timestamp = (
    SELECT COALESCE(MAX(created_utc), last_processed_timestamp)
    FROM reddit_posts_final
)
WHERE source_name = 'reddit_files';