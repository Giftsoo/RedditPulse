UNLOAD ('
SELECT
    post_id,
    title,
    created_utc,
    score,
    author,
    num_comments
FROM reddit_posts_final
')
TO 's3://reddit-data-engineering-giftson-us-east-1/curated/reddit/'
IAM_ROLE DEFAULT
FORMAT AS PARQUET
ALLOWOVERWRITE;