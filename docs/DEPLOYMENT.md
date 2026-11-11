# Despliegue y demo

> Las imágenes publicadas usan **Python 3.15**. Para el entorno local se recomienda 3.12.

## 1. Stack local (compose)

```bash
cp .env.example .env
docker compose up -d --build
docker compose ps
```

Arranca en orden:

1. `kafka` (KRaft) y `postgres` (con `db/init.sql`).
2. `kafka-init` crea el tópico y termina.
3. `processor` consume y escribe KPIs.

## 2. Demo

```bash
# 200 eventos con latencia aleatoria
docker compose --profile demo run --rm producer

# KPIs por minuto/hora/día
docker compose exec postgres psql -U postgres -d postgresdb \
  -c "SELECT * FROM test.kpis ORDER BY vcount DESC LIMIT 10;"
```

También puedes generar eventos desde el host:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:29092 python scripts/produce_demo.py --count 50
```

## 3. CloudBeaver (opcional)

```bash
docker compose --profile tools up -d cloudbeaver
# http://localhost:8080  (admin / valor de CB_ADMIN_PASSWORD)
```

Conexión: host `postgres`, puerto `5432`, base y credenciales del `.env`.

## 4. Producción

- Sustituye PLAINTEXT por SASL_SSL y credenciales reales.
- No publiques 9092/29092/5432 fuera de la red interna.
- Usa un esquema/tabla propio y un `init.sql` versionado.
- Escala el consumidor con más particiones y más réplicas del `processor`
  (mismo `KAFKA_GROUP`).
- Ajusta `KPI_FLUSH_MAX_ROWS` según el throughput: lotes grandes reducen I/O,
  lotes chicos reducen la latencia de escritura.
- Monitoriza el lag del consumer group:

```bash
kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --describe --group kpi-processor
```

## 5. Imagen publicada

```bash
docker pull ghcr.io/bi0punk/kpi-datapipeline:latest
```

`main` publica `:latest`; los tags `vX.Y.Z` generan `:X.Y.Z`. La imagen corre
como uid 10001 y su entrypoint es `kpi-processor`.

## 6. Troubleshooting

| Síntoma | Causa | Solución |
|---|---|---|
| `Faltan variables de entorno` | `.env` incompleto | Completa `DB_*` |
| `relation "test.kpis" does not exist` | No corrió `init.sql` | Recrea el volumen o crea la tabla |
| El processor no lee | Tópico/particiones | Revisa `kafka-init` y `KAFKA_TOPIC` |
| Mensajes descartados | `@timestamp` inválido | Revisa el productor |
| KPIs sin crecer | Offset commiteado sin flush | Revisa logs de error de PostgreSQL |
| `kafka` unhealthy | Puerto/CLUSTER_ID | Revisa logs del broker |

Diagnóstico:

```bash
docker compose logs --tail=200 kafka processor postgres
docker compose exec postgres psql -U postgres -d postgresdb -c "\d test.kpis"
```
