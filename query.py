import duckdb
from minio import Minio
import os
import shutil

minio_client = Minio(
    endpoint='minio:9000',
    access_key='minioadmin',
    secret_key='minioadmin',
    secure=False
)

# Tạo thư mục temp
os.makedirs('temp_stock_data', exist_ok=True)

# Download files từ MinIO
print("📥 Downloading files from MinIO...")
objects = minio_client.list_objects('stock-data')
for obj in objects:
    print(f" Downloading {obj.object_name}...")
    data = minio_client.get_object('stock-data', obj.object_name).read()
    with open(f'temp_stock_data/{obj.object_name}', 'wb') as f:
        f.write(data)

# Query với DuckDB
print("🔍 Querying data...")
con = duckdb.connect()
con.execute("""
    CREATE OR REPLACE TABLE stock_data AS 
    SELECT * FROM read_json_auto('temp_stock_data/*.json')
""")

result = con.execute("""
    SELECT 
        date,
        AVG(close) as avg_close,
        MAX(high) as max_high,
        MIN(low) as min_low,
        SUM(volume) as total_volume
    FROM stock_data
    GROUP BY date
    ORDER BY date
""").df()

print("\n📊 Query Result:")
print(result)

# Cleanup
shutil.rmtree('temp_stock_data', ignore_errors=True)
