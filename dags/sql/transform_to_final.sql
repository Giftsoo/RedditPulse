BEGIN;

-- 1️⃣ Remove existing rows for incoming post_ids (idempotent)
DELETE FROM reddit_posts_final
USING reddit_posts_staging
WHERE reddit_posts_final.post_id = reddit_posts_staging.post_id;

-- 2️⃣ Insert one row per post_id (latest record)
INSERT INTO reddit_posts_final (
    post_id,
    title,
    created_utc,
    score,
    author,
    num_comments
)
SELECT
    post_id,
    title,
    created_utc,
    score,
    author,
    num_comments
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY post_id
               ORDER BY created_utc DESC
           ) AS rn
    FROM reddit_posts_staging
) t
WHERE rn = 1;

COMMIT;