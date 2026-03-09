import pandas as pd
import csv
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

analyzer = SentimentIntensityAnalyzer()

INPUT_CSV = "data/input/reddit_batch_1.csv"
OUTPUT_CSV = "data/input/reddit_batch_1_clean.csv"

df = pd.read_csv(INPUT_CSV)

df["created_utc"] = pd.to_numeric(df["created_utc"], errors="coerce")
df = df.dropna(subset=["created_utc"])
df["created_utc"] = df["created_utc"].astype(int)

df["sentiment_score"] = df["title"].astype(str).apply(
    lambda x: analyzer.polarity_scores(x)["compound"]
)

df["flagged"] = df["sentiment_score"] <= -0.5

df = df[
    ["post_id", "title", "created_utc", "sentiment_score", "flagged"]
]

df.to_csv(
    OUTPUT_CSV,
    index=False,
    encoding="utf-8",
    quoting=csv.QUOTE_MINIMAL
)

print(f"[OK] Clean CSV written to {OUTPUT_CSV}")