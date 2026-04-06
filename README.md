# Kafka Project Sample

Real-time stock data pipeline: Producer → Kafka → Consumer → MinIO → DuckDB analytics.

## Architecture
- **Zookeeper** (port 2181)
- **Kafka** (port 9092)
- **MinIO** (ports 9000 API, 9001 Console)
- **Producer**: Random stock data → Kafka topic `stock-market`
- **Consumer**: Kafka → MinIO bucket `stock-data` (Parquet files with partition hierarchy)
- **Query**: DuckDB direct S3 scan (no download) → aggregates

## Quick Start
```bash
docker compose up -d
docker compose logs -f producer   # watch producer
docker compose logs -f consumer   # watch consumer (Parquet batches)
```

## Run Analytics (Direct S3 Scan)

```bash
# Build query service (if not built)
docker compose build query

# Run one-shot query (does NOT download files)
docker compose run --rm query

# Output: aggregated result + query_result.csv in container
```

## Data Format

### Consumer (Parquet with Partitioning)
- Files stored as: `stock/date=YYYY-MM-DD/hour=HH/part-<timestamp>.parquet`
- Batch size: 10,000 messages or 5 minutes
- Compression: Snappy
- Partition pruning: DuckDB can query specific dates/hours efficiently

### Query (Direct S3)
- DuckDB HTTPFS reads Parquet directly from MinIO
- No temporary files, no downloads
- Partition-aware queries
- Result: aggregated by date (avg, max, min, sum, count)

## Cleanup

```bash
docker compose down -v  # stop + remove volumes
```

## Notes

• MinIO console: http://localhost:9001 (minioadmin/minioadmin)
• Kafka advertised listener: kafka:9092 (internal Docker network)
• Query service: one-shot container (runs and exits)
• For large datasets: Parquet + partitioning reduces query time significantly
