import json
import time

import pandas as pd
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=["kafka:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=10,
    max_in_flight_requests_per_connection=5,  # Tăng từ 1 → 5
    batch_size=16384,  # 16KB batch (mặc định 16KB, có thể tăng)
    linger_ms=10,  # Chờ tối đa 10ms để batch (mặc định 0)
    compression_type='gzip',  # Nén message để tăng throughput
)

df = pd.read_csv("stock_data.csv")
print("🚀 Starting producer (FAST MODE)...", flush=True)
try:
    while True:
        row = df.sample(1).to_dict("records")[0]
        future = producer.send("stock-market", value=row)
        meta = future.get(timeout=30)
        # Không flush mỗi lần - để producer tự batch
        print(
            f"📤 Sent: {row['date']} - Close: {row['close']} - offset={meta.offset}",
            flush=True,
        )
        time.sleep(0.1)  # Giảm từ 2s → 0.1s (10x faster)
except KeyboardInterrupt:
    print("\n🛑 Producer stopped", flush=True)
except Exception as e:
    print(f"❌ Error: {e}", flush=True)
finally:
    producer.flush()  # Flush hết buffer trước khi thoát
    producer.close()
