from kafka import KafkaProducer
import json
import pandas as pd
import time

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=10,
    max_in_flight_requests_per_connection=1
)

df = pd.read_csv('stock_data.csv')
print("🚀 Starting producer...", flush=True)
try:
    while True:
        row = df.sample(1).to_dict('records')[0]
        future = producer.send('stock-market', value=row)
        meta = future.get(timeout=30)
        producer.flush()
        print(f"📤 Sent: {row['date']} - Close: {row['close']} - offset={meta.offset}", flush=True)
        time.sleep(2)
except KeyboardInterrupt:
    print("\n🛑 Producer stopped", flush=True)
except Exception as e:
    print(f"❌ Error: {e}", flush=True)
finally:
    producer.close()
