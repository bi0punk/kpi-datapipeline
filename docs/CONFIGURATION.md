# Configuración

Todo se lee del entorno (`app.load_config`). Si falta una variable obligatoria,
el proceso termina con código 2 y un mensaje que lista las faltantes.

## PostgreSQL

| Variable | Default | Obligatoria | Descripción |
|---|---|---|---|
| `DB_NAME` | — | sí | Base de datos |
| `DB_HOST` | — | sí | Host |
| `DB_USER` | — | sí | Usuario |
| `DB_PASSWORD` | — | sí | Password |
| `DB_PORT` | `5432` | no | Puerto |
| `DB_TABLE` | `test.kpis` | no | Tabla destino (`schema.tabla`) |

`DB_TABLE` debe existir. El `db/init.sql` del compose crea `test.kpis`; si usas
otro esquema/tabla, ajusta ese archivo.

## Kafka

| Variable | Default | Descripción |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `127.0.0.1:9092` | Lista separada por comas |
| `KAFKA_TOPIC` | `test` | Tópico de entrada |
| `KAFKA_GROUP` | `kpi-processor` | Consumer group |

En compose, `kafka-init` crea el tópico al arrancar. Si usas un broker externo,
créalo tú:

```bash
kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic test --partitions 1 --replication-factor 1
```

## Batching y commit

| Variable | Default | Descripción |
|---|---|---|
| `KPI_FLUSH_INTERVAL` | `2.0` | Segundos máximos entre flushes |
| `KPI_FLUSH_MAX_ROWS` | `500` | Filas por flush |

El flush ocurre cuando se cumple cualquiera de las dos condiciones. Después del
flush se confirma el offset del consumer. Si el upsert falla:

1. se hace `rollback` de la transacción,
2. las filas quedan en memoria para reintentar,
3. **no** se commitea el offset (se reprocesará al reiniciar o en el próximo
   ciclo).

## Formato del mensaje

```json
{
  "@timestamp": 1767227045.5,
  "service": "api",
  "status": 200,
  "latency_ms": 42.3
}
```

- `@timestamp` debe ser numérico (epoch en segundos). `true`, `null`, listas o
  strings no numéricos se descartan con un warning.
- Los demás campos se ignoran.

## Ejemplo de `.env`

```env
DB_NAME=postgresdb
DB_HOST=postgres
DB_USER=postgres
DB_PASSWORD=postgres
DB_PORT=5432
DB_TABLE=test.kpis
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=test
KAFKA_GROUP=kpi-processor
KPI_FLUSH_INTERVAL=2.0
KPI_FLUSH_MAX_ROWS=500
```
