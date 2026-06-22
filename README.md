# pipeline_kpi

Streaming data pipeline that reads JSON messages from Apache Kafka, computes KPIs (requests per minute/hour/day), and stores aggregated counts in PostgreSQL.

**Security:** Database credentials read from environment variables, not hardcoded.

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

## Configuration

Set these environment variables before running:

| Variable | Default | Description |
|---|---|---|
| `DB_NAME` | `postgresdb` | PostgreSQL database |
| `DB_HOST` | `127.0.0.1` | Database host |
| `DB_USER` | `postgres` | Database user |
| `DB_PASSWORD` | `postgres` | Database password |
| `DB_PORT` | `5432` | Database port |

Create a `.env` file for Docker Compose:

```env
DB_NAME=postgresdb
DB_USER=postgres
DB_PASSWORD=your_secure_password
CB_ADMIN_PASSWORD=your_admin_password
```

## Usage

```bash
docker compose up -d
```

The consumer app (`app.py`) reads from the `test` Kafka topic and upserts KPI aggregations into PostgreSQL.

## Security

- Database passwords are never hardcoded — always use env vars
- CloudBeaver admin password configurable via `CB_ADMIN_PASSWORD`
- Kafka listeners use PLAINTEXT protocol (for development only — use SASL_SSL in production)

## License

MIT
