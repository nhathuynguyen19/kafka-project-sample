# Kafka Project Sample

Real-time stock data pipeline: Producer → Kafka → Consumer → MinIO → DuckDB analytics.

## Architecture
- **Zookeeper** (port 2181)
- **Kafka** (port 9092)
- **MinIO** (ports 9000 API, 9001 Console)
- **Producer**: Random stock data → Kafka topic `stock-market`
- **Consumer**: Kafka → MinIO bucket `stock-data` (JSON files)
- **Query**: DuckDB aggregates from MinIO files

## Quick Start
```bash
docker compose up -d
docker compose logs -f producer  # watch producer
docker compose logs -f consumer  # watch consumer
```

## Run Analytics

```
docker compose exec consumer python query.py
```

## Cleanup

```
docker compose down -v  # stop + remove volumes
```

## Notes

• MinIO console: http://localhost:9001 (minioadmin/minioadmin)
• Kafka advertised listener: kafka:9092 (internal Docker network)
