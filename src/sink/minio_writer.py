import json
import os
import time
import io
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import Consumer
from minio import Minio

TRANSACTION_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
KAFKA_BOOTSTRAP_SERVERS = f'{os.getenv("KAFKA_HOST", "localhost")}:{os.getenv("KAFKA_PORT", "9092")}'
GROUP_ID = os.getenv("MINIO_GROUP_ID", "minio-consumer")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9002")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "transactions")

BATCH_SIZE = int(os.getenv("MINIO_BATCH_SIZE", "100"))
BATCH_TIMEOUT = int(os.getenv("MINIO_BATCH_TIMEOUT", "30"))


def get_minio_client():
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        print(f"Bucket '{MINIO_BUCKET}' cree.")
    return client


def flush_to_minio(minio_client, batch, batch_num):
    if not batch:
        return

    df = pd.DataFrame(batch)
    date_str = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    filename = f"raw/{date_str}/batch_{batch_num:06d}.parquet"

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    minio_client.put_object(
        MINIO_BUCKET,
        filename,
        buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/parquet"
    )
    print(f"  [MinIO] Sauvegarde: {filename} ({len(batch)} transactions)")


def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest"
    })
    consumer.subscribe([TRANSACTION_TOPIC])

    minio_client = get_minio_client()

    print(f"Consumer MinIO en ecoute sur '{TRANSACTION_TOPIC}'...")
    print("CTRL+C pour arreter\n")

    batch = []
    batch_num = 0
    last_flush = time.time()

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                if batch and (time.time() - last_flush) >= BATCH_TIMEOUT:
                    flush_to_minio(minio_client, batch, batch_num)
                    batch_num += 1
                    batch = []
                    last_flush = time.time()
                continue

            if msg.error():
                print(f"Erreur: {msg.error()}")
                continue

            transaction = json.loads(msg.value().decode("utf-8"))
            transaction["kafka_offset"] = msg.offset()
            transaction["kafka_partition"] = msg.partition()
            batch.append(transaction)

            if len(batch) >= BATCH_SIZE:
                flush_to_minio(minio_client, batch, batch_num)
                batch_num += 1
                batch = []
                last_flush = time.time()

    except KeyboardInterrupt:
        print("\nArret du consumer.")
        if batch:
            flush_to_minio(minio_client, batch, batch_num)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
