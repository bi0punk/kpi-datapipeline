# pipeline_kpi

Streaming data pipeline that reads JSON messages from Apache Kafka, computes KPIs (requests per minute/hour/day), and stores aggregated counts in PostgreSQL.

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.11+-blue)](https://python.org)
[![CI](https://github.com/bi0punk/pipeline_kpi/actions/workflows/ci.yml/badge.svg)](https://github.com/bi0punk/pipeline_kpi/actions/workflows/ci.yml)

## Tabla de Contenidos

- [Características](#características)
- [Stack](#stack)
- [Arquitectura](#arquitectura)
- [Requisitos](#requisitos)
- [Instalación](#instalación)
- [Uso](#uso)
- [Tests](#tests)
- [Configuración](#configuración)
- [CI](#ci)
- [Datos](#datos)
- [Seguridad](#seguridad)
- [Limitaciones / Roadmap](#limitaciones--roadmap)
- [Licencia](#licencia)

## Características

- Consumo de mensajes JSON desde Kafka (tópico `test`)
- Cálculo de KPIs: requests por minuto, hora y día
- Almacenamiento upsert en PostgreSQL con contadores acumulativos
- Procesamiento en tiempo real con consumer group
- Pipeline containerizado con Docker Compose
- Log processor con Fluent Bit

## Stack

- Python 3.11+, Kafka (confluent-kafka), PostgreSQL, Docker Compose, Fluent Bit

## Arquitectura

```
pipeline_kpi/
├── app.py                    # Consumer KPI processor
├── docker-compose.yml        # Zookeeper, Kafka, PostgreSQL, CloudBeaver
├── dockerfile                # Imagen del consumer
├── fluent-bit.conf           # Configuración Fluent Bit
├── parser.conf               # Parseo de logs
├── tests/
├── requirements.txt
├── pyproject.toml
├── .env.example
└── README.md
```

## Servicios

| Servicio    | Puerto   | Descripción                     |
|-------------|----------|----------------------------------|
| Zookeeper   | 2181     | Coordinación Kafka               |
| Kafka       | 9092/29092 | Broker de mensajes             |
| PostgreSQL  | 5432     | Almacenamiento de KPIs           |
| CloudBeaver | 8080     | Cliente web de base de datos     |
| Fluent Bit  | —        | Procesador de logs               |

## Requisitos

- Docker Engine 24+
- Docker Compose v2

## Instalación

```bash
git clone https://github.com/bi0punk/pipeline_kpi.git
cd pipeline_kpi
cp .env.example .env
# Editar credenciales si es necesario
```

## Uso

```bash
# Iniciar servicios (Zookeeper, Kafka, PostgreSQL, CloudBeaver)
docker compose up -d

# Construir y ejecutar el consumer
docker build -t kpi-consumer .
docker run --network pipeline_kpi_backend_net kpi-consumer
```

El consumer lee del tópico `test` y upserta agregaciones KPI en PostgreSQL.

## Tests

```bash
pip install pytest ruff
pytest -q
ruff check .
```

## Configuración

Variables de entorno (ver `.env.example`):

| Variable        | Default      | Descripción                        |
|-----------------|--------------|------------------------------------|
| `DB_NAME`       | `postgresdb` | Base de datos PostgreSQL           |
| `DB_HOST`       | `127.0.0.1`  | Host de PostgreSQL                 |
| `DB_USER`       | `postgres`   | Usuario PostgreSQL                 |
| `DB_PASSWORD`   | `postgres`   | Contraseña PostgreSQL              |
| `DB_PORT`       | `5432`       | Puerto PostgreSQL                  |
| `CB_ADMIN_PASSWORD` | `admin123` | Password admin de CloudBeaver    |

## CI

GitHub Actions ejecuta ruff lint + pytest en cada push y PR.

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

## Limitaciones / Roadmap

- [ ] Ventanas de tiempo deslizantes (Sliding windows)
- [ ] Exportación de KPIs a Prometheus
- [ ] Dashboard en tiempo real con Grafana
- [ ] Alertas basadas en umbrales de KPI

## Licencia

MIT
