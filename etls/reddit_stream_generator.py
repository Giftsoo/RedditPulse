import csv
import time
import os
import random

OUTPUT_FILE = "/opt/airflow/data/input/stream_posts.csv"

titles = [
    "I absolutely love this product",
    "This update is terrible and disappointing",
    "The new feature works great",
    "Worst experience ever",
    "Amazing improvement in performance"
]

os.makedirs("/opt/airflow/data/input", exist_ok=True)

# create header if file doesn't exist
if not os.path.exists(OUTPUT_FILE):
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "post_id",
            "title",
            "created_utc",
            "score",
            "author",
            "num_comments"
        ])

counter = 1

print("Starting streaming generator...")

while True:

    created_utc = int(time.time())

    post_id = f"live_post_{counter:03d}"

    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        writer.writerow([
            post_id,
            random.choice(titles),
            created_utc,
            random.randint(1,100),
            "stream_user",
            random.randint(1,20)
        ])


    print(f"Generated {post_id}")

    counter += 1

    time.sleep(20)