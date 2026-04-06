#!/usr/bin/env python3
"""
Query trực tiếp từ MinIO S3 mà không cần download files.
DuckDB đọc Parquet files directly qua S3 API using HTTPFS extension.
"""

import duckdb
import sys
import time

# === Cấu hình MinIO ===
MINIO_ENDPOINT = 'minio:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
MINIO_REGION = 'us-east-1'
MINIO_SECURE = False

# Bucket và path - Parquet files với partition hierarchy
BUCKET_NAME = 'stock-data'
# Query tất cả files trong partition hierarchy: stock/date=*/hour=*/*.parquet
PREFIX = 'stock/date=*/hour=*/*.parquet'

S3_PATH = f's3://{BUCKET_NAME}/{PREFIX}'

print(f"🔍 Querying Parquet data from MinIO: {S3_PATH}")
print(f"📍 Endpoint: {MINIO_ENDPOINT}")

# DuckDB connection
con = duckdb.connect()

# Install and load HTTPFS extension (required for S3)
print("⏳ Loading HTTPFS extension...")
t0 = time.time()
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")
t1 = time.time()
print(f"✅ HTTPFS loaded ({t1-t0:.2f}s)")

# Configure S3 settings
con.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
con.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
con.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
con.execute(f"SET s3_region='{MINIO_REGION}';")
con.execute(f"SET s3_use_ssl={str(MINIO_SECURE).lower()};")
con.execute("SET s3_url_style='path';")

print("✅ S3 configured")
print("⏳ Loading data from S3 (Parquet)...")

try:
    # Load tất cả Parquet files từ MinIO với partition pruning
    t_load_start = time.time()
    con.execute(f"""
        CREATE OR REPLACE TABLE stock_data AS
        SELECT * FROM read_parquet('{S3_PATH}')
    """)
    t_load_end = time.time()

    # Kiểm tra số dòng
    count_result = con.execute("SELECT COUNT(*) as total_rows FROM stock_data").df()
    total_rows = count_result['total_rows'].iloc[0]
    print(f"✅ Loaded {total_rows} rows from Parquet ({t_load_end-t_load_start:.2f}s)")

    if total_rows == 0:
        print("⚠️ No data found. Waiting for producer/consumer to generate data...")
        sys.exit(0)

    # Aggregation
    print("\n📊 Running aggregation query...")
    t_query_start = time.time()
    result = con.execute("""
        SELECT
            date,
            AVG(close) as avg_close,
            MAX(high) as max_high,
            MIN(low) as min_low,
            SUM(volume) as total_volume,
            COUNT(*) as trade_count
        FROM stock_data
        GROUP BY date
        ORDER BY date
    """).df()
    t_query_end = time.time()

    print("\n📈 Query Result:")
    print(result.to_string())

    # Save result to CSV (local in container)
    result.to_csv('query_result.csv', index=False)
    print("\n✅ Saved to query_result.csv")

    # Optional: Show partitions info
    print("\n📁 Partition stats:")
    partitions = con.execute("""
        SELECT date, hour, COUNT(*) as rows
        FROM stock_data
        GROUP BY date, hour
        ORDER BY date, hour
    """).df()
    print(partitions.to_string())

    # === TIMING SUMMARY ===
    total_time = t_query_end - t0
    print("\n" + "="*50)
    print("⏱️  TIMING SUMMARY:")
    print(f"   HTTPFS load:    {t1-t0:.2f}s")
    print(f"   Data load:      {t_load_end-t_load_start:.2f}s")
    print(f"   Query exec:     {t_query_end-t_query_start:.2f}s")
    print(f"   Total time:     {total_time:.2f}s")
    print("="*50)

except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    con.close()
