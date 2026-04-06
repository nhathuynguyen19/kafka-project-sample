from kafka import KafkaConsumer
import json
from minio import Minio
import io
import time
import os
import traceback
import pandas as pd
from datetime import datetime

minio_client = Minio(
    endpoint='minio:9000',
    access_key='minioadmin',
    secret_key='minioadmin',
    secure=False
)

# Tạo bucket nếu chưa có
try:
    minio_client.make_bucket("stock-data")
except Exception as e:
    if "BucketAlreadyExists" in str(e):
        pass

print("📥 Starting consumer (Parquet mode)...", flush=True)

try:
    # Wait for Kafka to be ready (retry up to 30 times)
    for i in range(30):
        try:
            consumer = KafkaConsumer(
                'stock-market',
                bootstrap_servers=['kafka:9092'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id='stock-group',
                max_poll_records=10000,
                fetch_max_bytes=52428800,
                consumer_timeout_ms=5000  # 5s timeout để kiểm tra connection
            )
            # Test connection bằng cách lấy metadata
            consumer.subscribe(['stock-market'])
            print("✅ Connected to Kafka, group=stock-group", flush=True)
            break
        except Exception as e:
            print(f"⚠️  Kafka not ready (attempt {i+1}/30): {e}", flush=True)
            time.sleep(2)
    else:
        raise Exception("❌ Failed to connect to Kafka after 30 attempts")

    # Buffer configuration (reduced for quick testing)
    BUFFER_MAX_SIZE = 100  # Từ 10000 → 100 (test nhanh)
    BUFFER_TIMEOUT_SECONDS = 30  # Từ 300 → 30s
    buffer = []
    last_flush_time = time.time()

    for msg in consumer:
        buffer.append(msg.value)

        current_time = time.time()
        should_flush = (
            len(buffer) >= BUFFER_MAX_SIZE or
            (buffer and (current_time - last_flush_time) >= BUFFER_TIMEOUT_SECONDS)
        )

        if should_flush:
            # Convert buffer to DataFrame
            df = pd.DataFrame(buffer)

            # Generate partition path: stock/date=YYYY-MM-DD/hour=HH/part-<uuid>.parquet
            now = datetime.now()
            date_str = now.strftime('%Y-%m-%d')
            hour_str = now.strftime('%H')
            object_name = f"stock/date={date_str}/hour={hour_str}/part-{int(time.time())}.parquet"

            # Write to Parquet in memory
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, engine='pyarrow', compression='snappy')
            parquet_buffer.seek(0)

            # Upload to MinIO
            try:
                minio_client.put_object(
                    bucket_name='stock-data',
                    object_name=object_name,
                    data=parquet_buffer,
                    length=parquet_buffer.getbuffer().nbytes,
                    content_type='application/parquet'
                )
                print(f"✅ Uploaded batch: {object_name} ({len(buffer)} rows)", flush=True)
            except Exception as e:
                print(f"❌ MinIO upload failed: {e}", flush=True)
                traceback.print_exc()

            # Clear buffer
            buffer.clear()
            last_flush_time = current_time

except KeyboardInterrupt:
    print("\n🛑 Consumer stopped by user", flush=True)
except Exception as e:
    print(f"❌ Consumer fatal error: {e}", flush=True)
    traceback.print_exc()
finally:
    # Flush remaining buffer on exit
    if buffer:
        print(f"⚠️ Flushing {len(buffer)} remaining messages...", flush=True)
        df = pd.DataFrame(buffer)
        now = datetime.now()
        date_str = now.strftime('%Y-%m-%d')
        hour_str = now.strftime('%H')
        object_name = f"stock/date={date_str}/hour={hour_str}/part-{int(time.time())}_final.parquet"
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow', compression='snappy')
        parquet_buffer.seek(0)
        try:
            minio_client.put_object(
                bucket_name='stock-data',
                object_name=object_name,
                data=parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type='application/parquet'
            )
            print(f"✅ Uploaded final batch: {object_name}", flush=True)
        except Exception as e:
            print(f"❌ Final upload failed: {e}", flush=True)
    print("🛑 Consumer stopped", flush=True)
