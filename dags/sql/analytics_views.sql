-- =====================================================
-- ANALYTICS SCHEMA (SAFE)
-- =====================================================
CREATE SCHEMA IF NOT EXISTS analytics;


-- =====================================================
-- DAILY POST VOLUME
-- =====================================================
CREATE OR REPLACE VIEW analytics.daily_post_volume AS
SELECT
    DATE_TRUNC('day', created_ts) AS day,
    COUNT(*) AS posts
FROM reddit_posts_final
GROUP BY 1;


-- =====================================================
-- DAILY SENTIMENT SUMMARY
-- =====================================================
CREATE OR REPLACE VIEW analytics.daily_sentiment_summary AS
SELECT
    DATE_TRUNC('day', f.created_ts) AS day,
    s.sentiment_label,
    COUNT(*) AS post_count,
    AVG(s.sentiment_score) AS avg_sentiment
FROM reddit_posts_final f
JOIN reddit_post_sentiment s
    ON f.post_id = s.post_id
GROUP BY 1, 2;


-- =====================================================
-- SENTIMENT DISTRIBUTION
-- =====================================================
CREATE OR REPLACE VIEW analytics.sentiment_distribution AS
SELECT
    sentiment_label,
    COUNT(*) AS total_posts
FROM reddit_post_sentiment
GROUP BY sentiment_label;