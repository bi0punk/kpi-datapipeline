# kpi-datapipeline

Pipeline de datos en streaming que lee mensajes JSON desde Apache Kafka, calcula KPIs (peticiones por minuto/hora/día) y guarda conteos agregados en PostgreSQL.

[![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB?logo=python)](https://python.org)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?logo=apachekafka)](https://kafka.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)](https://www.docker.com/)
[![CI](https://github.com/bi0punk/kpi-datapipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/bi0punk/kpi-datapipeline/actions)
[![Licencia](https://img.shields.io/badge/Licencia-MIT-blue)](LICENSE)

## Contenido

- [Características](#características)
- [Arquitectura](#arquitectura)
- [Stack](#stack)
- [Estructura del repositorio](#estructura-del-repositorio)
- [Servicios](#servicios)
- [Instalación](#instalación)
- [Uso](#uso)
- [Configuración](#configuración)
- [Datos](#datos)
- [Seguridad](#seguridad)
- [Tests / CI](#tests--ci)
- [Roadmap](#roadmap)
- [Licencia](#licencia)

## Características

- Consumo de mensajes JSON desde Kafka (tópico `test`)
- Cálculo de KPIs: requests por minuto, hora y día
- Almacenamiento upsert en PostgreSQL con contadores acumulativos
- Procesamiento en tiempo real con consumer group
- Pipeline containerizado con Docker Compose
- Log processor con Fluent Bit

## Arquitectura

```mermaid
flowchart TD
  NGINX[nginx.log] --> FLUENT[Fluent Bit: tail + parser nginx]
  FLUENT -->|Brokers 127.0.0.1:29092, Topic test| KAFKA[Apache Kafka]
  KAFKA --> CONSUMER[KpiProcessor: KafkaConsumer group kpi-processor]
  CONSUMER --> TS{¿@timestamp?}
  TS -->|sí| MIN[KPI REQUEST_X_MINUTE]
  TS -->|sí| HOUR[KPI REQUEST_X_HOUR]
  TS -->|sí| DAY[KPI REQUEST_X_DAY]
  MIN --> PG[PostgreSQL: INSERT ... ON CONFLICT DO UPDATE]
  HOUR --> PG
  DAY --> PG
  PG --> CB[CloudBeaver: interfaz web de DB]
```

Flujo: Fluent Bit taila logs de nginx y los publica en Kafka; el consumer `KpiProcessor` lee los mensajes, extrae `@timestamp` y upserta 3 KPIs por mensaje en PostgreSQL con contadores acumulativos.

## Stack

| Componente | Tecnología |
|---|---|
| Consumo | Python 3.11+, kafka-python |
| Broker | Apache Kafka (bitnami) |
| DB | PostgreSQL 15 |
| Logs | Fluent Bit (tail + parser nginx) |
| UI DB | CloudBeaver |
| Orquestación | Docker Compose |

## Estructura del repositorio

```
kpi-datapipeline/
├── app.py                    # Consumer KPI processor
├── docker-compose.yml        # Zookeeper, Kafka, PostgreSQL, CloudBeaver
├── dockerfile                # Imagen del consumer
├── fluent-bit.conf           # Configuración Fluent Bit
├── parser.conf               # Parseo de logs
├── tests/
├── requirements.txt
├── pyproject.toml
├── .env.example
├── .github/workflows/ci.yml
└── README.md
```

## Servicios

| Servicio | Puerto | Descripción |
|---|---|---|
| Zookeeper | 2181 | Coordinación Kafka |
| Kafka | 9092/29092 | Broker de mensajes |
| PostgreSQL | 5432 | Almacenamiento de KPIs |
| CloudBeaver | 8080 | Cliente web de base de datos |
| Fluent Bit | — | Procesador de logs |

## Instalación

Requisitos: Docker Engine 24+ y Docker Compose v2.

```bash
git clone https://github.com/bi0punk/kpi-datapipeline.git
cd kpi-datapipeline
cp .env.example .env
# Editar credenciales si es necesario
```

## Uso

```bash
# Iniciar servicios (Zookeeper, Kafka, PostgreSQL, CloudBeaver)
docker compose up -d

# Construir y ejecutar el consumer
docker build -t kpi-consumer .
docker run --network kpi-datapipeline_backend_net kpi-consumer
```

El consumer lee del tópico `test` y upserta agregaciones KPI en PostgreSQL.

## Configuración

Variables de entorno (ver `.env.example`):

| Variable | Default | Descripción |
|---|---|---|
| `DB_NAME` | `postgresdb` | Base de datos PostgreSQL |
| `DB_HOST` | `127.0.0.1` | Host de PostgreSQL |
| `DB_USER` | `postgres` | Usuario PostgreSQL |
| `DB_PASSWORD` | `postgres` | Contraseña PostgreSQL |
| `DB_PORT` | `5432` | Puerto PostgreSQL |
| `KAFKA_BOOTSTRAP_SERVERS` | `127.0.0.1:9092` | Servidores Kafka |
| `KAFKA_TOPIC` | `test` | Tópico a consumir |
| `KAFKA_GROUP` | `kpi-processor` | Grupo de consumidores |
| `DB_TABLE` | `test.kpis` | Tabla destino |
| `CB_ADMIN_PASSWORD` | `admin123` | Password admin de CloudBeaver |

## Datos

Los mensajes JSON deben contener un campo `@timestamp` (timestamp Unix). El consumer genera 3 KPIs por mensaje:
- `REQUEST_X_MINUTE` — agrupado por minuto
- `REQUEST_X_HOUR` — agrupado por hora
- `REQUEST_X_DAY` — agrupado por día

Esquema PostgreSQL:
```sql
CREATE TABLE test.kpis (
  kpi_key TEXT,
  kpi_value TEXT,
  vcount INTEGER,
  PRIMARY KEY (kpi_key, kpi_value)
);
```

## Seguridad

- Las credenciales de base de datos se leen de variables de entorno, no están hardcodeadas
- CloudBeaver admin password configurable via `CB_ADMIN_PASSWORD`
- Kafka listeners usan PLAINTEXT (solo desarrollo — usar SASL_SSL en producción)

## Tests / CI

CI: GitHub Actions ejecuta ruff lint + pytest en cada push y PR.

```bash
pip install pytest ruff
pytest -q
ruff check .
```

## Roadmap

- [x] Pipeline Kafka → PostgreSQL con KPIs por minuto/hora/día
- [ ] Ventanas de tiempo deslizantes (Sliding windows)
- [ ] Exportación de KPIs a Prometheus
- [ ] Dashboard en tiempo real con Grafana
- [ ] Alertas basadas en umbrales de KPI

## Licencia

MIT — ver [LICENSE](LICENSE).