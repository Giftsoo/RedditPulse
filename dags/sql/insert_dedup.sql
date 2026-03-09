INSERT INTO reddit_posts_final (
    post_id,
    title,
    created_utc,
    score,
    author,
    num_comments
)
SELECT
    s.post_id,
    s.title,
    NULLIF(s.created_utc, '')::BIGINT,
    NULLIF(s.score, '')::INT,
    s.author,
    NULLIF(s.num_comments, '')::INT
FROM reddit_posts_staging s
WHERE
    s.score ~ '^[0-9]+$'
    AND s.num_comments ~ '^[0-9]+$'
    AND s.created_utc ~ '^[0-9]+$'
    AND NOT EXISTS (
        SELECT 1
        FROM reddit_posts_final f
        WHERE f.post_id = s.post_id
    );