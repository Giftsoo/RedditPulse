from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv
import time
import os

# Consistent with ingestion_metadata table
SOURCE_NAME = "reddit_files"

# Output CSV that COPY command uses
OUTPUT_FILE = "/opt/airflow/data/input/reddit_batch_1.csv"

def get_last_processed_timestamp():
    hook = PostgresHook(postgres_conn_id="redshift_serverless")
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COALESCE(last_processed_timestamp, 0)
        FROM ingestion_metadata
        WHERE source_name = %s
    """, (SOURCE_NAME,))

    result = cursor.fetchone()
    cursor.close()
    conn.close()

    return result[0] if result else 0


if __name__ == "__main__":
    last_ts = get_last_processed_timestamp()
    print(f"Last processed timestamp: {last_ts}")

    # 🔹 Simulated new Reddit posts (acts like API)
    new_posts = [
        {
            "post_id": f"post_{int(time.time())}",
            "title": "Incremental Reddit Post",
            "created_utc": int(time.time()),
            "score": 15,
            "author": "demo_user",
            "num_comments": 3
        }
    ]

    # 🔹 Incremental filter
    incremental_posts = [
        post for post in new_posts
        if post["created_utc"] > last_ts
    ]

    if not incremental_posts:
        print("No new data to extract")
        exit(0)

    # 🔹 Create file with header if it doesn't exist
    file_exists = os.path.exists(OUTPUT_FILE)

    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        if not file_exists:
            writer.writerow([
                "post_id",
                "title",
                "created_utc",
                "score",
                "author",
                "num_comments"
            ])

        for post in incremental_posts:
            writer.writerow([
                post["post_id"],
                post["title"],
                post["created_utc"],
                post["score"],
                post["author"],
                post["num_comments"]
            ])

    print(f"Extracted {len(incremental_posts)} new records")