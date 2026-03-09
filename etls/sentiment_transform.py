from airflow.providers.postgres.hooks.postgres import PostgresHook
from textblob import TextBlob

BATCH_LIMIT = 200   # keep small to avoid SSL timeout

def compute_sentiment():
    hook = PostgresHook(postgres_conn_id="redshift_serverless")
    conn = hook.get_conn()
    cur = conn.cursor()

    # 1️⃣ Fetch ONLY posts that do NOT have sentiment yet
    cur.execute("""
        SELECT f.post_id, f.title
        FROM reddit_posts_final f
        LEFT JOIN reddit_post_sentiment s
            ON f.post_id = s.post_id
        WHERE s.post_id IS NULL
        LIMIT %s
    """, (BATCH_LIMIT,))

    rows = cur.fetchall()

    if not rows:
        print("✅ No new posts pending sentiment")
        cur.close()
        conn.close()
        return True

    print(f"🔄 Processing {len(rows)} posts for sentiment")


    for post_id, title in rows:
        polarity = TextBlob(title).sentiment.polarity

        if polarity > 0.1:
            label = "positive"
        elif polarity < -0.1:
            label = "negative"
        else:
            label = "neutral"

        cur.execute("""
            INSERT INTO reddit_post_sentiment (
                post_id,
                sentiment_score,
                sentiment_label
            )
            SELECT %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1
                FROM reddit_post_sentiment
                WHERE post_id = %s
            );
        """, (post_id, polarity, label, post_id))

    conn.commit()
    cur.close()
    conn.close()

    print(f"✅ Sentiment processed for {len(rows)} posts")
    return True