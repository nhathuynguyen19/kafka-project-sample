from kafka import KafkaConsumer
import json
from minio import Minio
import io
import time
import os
import traceback

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

print("📥 Starting consumer...", flush=True)
try:
    consumer = KafkaConsumer(
        'stock-market',
        bootstrap_servers=['kafka:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='stock-group'  # thêm group
    )
    print("✅ Connected to Kafka, group=stock-group", flush=True)

    for idx, msg in enumerate(consumer):
        data = msg.value
        json_bytes = json.dumps(data).encode('utf-8')
        object_name = f"stock_{int(time.time())}_{idx}.json"
        
        try:
            minio_client.put_object(
                bucket_name='stock-data',
                object_name=object_name,
                data=io.BytesIO(json_bytes),
                length=len(json_bytes),
                content_type='application/json'
            )
            print(f"✅ Uploaded: {object_name}", flush=True)
        except Exception as e:
            print(f"❌ MinIO upload failed: {e}", flush=True)
            traceback.print_exc()
except Exception as e:
    print(f"❌ Consumer fatal error: {e}", flush=True)
    traceback.print_exc()
finally:
    print("🛑 Consumer stopped", flush=True)
