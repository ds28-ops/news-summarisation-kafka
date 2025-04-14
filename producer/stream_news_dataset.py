import pandas as pd
from kafka import KafkaProducer
import json
import time

# Load CSV
df = pd.read_csv("data/50k_political_repository.csv")

# Setup Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Stream function
def stream_articles(batch_size=5, delay=2):
    total = len(df)
    print(f"Total articles: {total}")

    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        batch = df.iloc[start:end]

        for _, row in batch.iterrows():
            if pd.isnull(row['body']):
                continue

            article = {
                "id": int(row['id']),
                "headline": row['headline'],
                "body": row['body'],
                "date": row['date_publish'],
                "outlet": row['outlet'],
                "url": row['url']
            }

            producer.send('news-raw', value=article)
            print(f"Produced: {article['headline'][:60]}...")

        print(f"Batch {start}-{end} sent. Sleeping {delay}s...\n")
        time.sleep(delay)

    producer.close()

if __name__ == "__main__":
    stream_articles(batch_size=5, delay=3)
