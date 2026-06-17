# pipeline_kpi

Streaming data pipeline that reads JSON messages from Apache Kafka, computes KPIs (requests per minute/hour/day), and stores aggregated counts in PostgreSQL.

## Stack

Python 3, Kafka (confluent-kafka), PostgreSQL, Docker Compose, Fluent Bit

## Services

| Service | Description |
|---|---|
| Zookeeper | Kafka coordination |
| Kafka | Message broker |
| PostgreSQL | KPI storage |
| CloudBeaver | Web database client |
| Fluent Bit | Log processor |

## Usage

```bash
docker compose up
```

The consumer app (`app.py`) reads from the `test` Kafka topic and upserts KPI aggregations into PostgreSQL.

## License

MIT
