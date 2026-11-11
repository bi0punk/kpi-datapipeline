# Changelog

Formato [Keep a Changelog](https://keepachangelog.com/es/1.1.0/) y
[SemVer](https://semver.org/lang/es/).

## [1.0.0] - 2026-11-11

Primera versión con demo reproducible y garantías de entrega.

### Añadido

- `kpi.py` con parseo de timestamp, ventanas UTC y contador vaciable.
- Batching configurable (`KPI_FLUSH_INTERVAL`, `KPI_FLUSH_MAX_ROWS`) y commit
  de offsets **después** de persistir (at-least-once).
- `db/init.sql` con el esquema `test.kpis`.
- `scripts/produce_demo.py` y perfil `demo` en compose.
- Kafka 3.9 en modo KRaft (sin Zookeeper) y `kafka-init` para el tópico.
- Imagen Docker sin root y publicación en GHCR.
- CI: calidad, tests 3.11/3.12 con cobertura, `pip-audit` y validación de compose.
- Documentación de configuración y despliegue, Dependabot y plantillas.

### Cambiado

- Configuración en `Config`/`load_config` con error claro si falta una variable.
- El procesador ya no hace `sys.exit()` desde el signal handler: el loop termina
  solo y hace flush final.
- CloudBeaver pasa al perfil `tools`; ya no arranca por defecto.

### Corregido

- Las ventanas KPI usaban la hora local del host; ahora son UTC.
- El paquete no incluía `app`/`kpi` en el wheel: `pip install .` dejaba módulos
  no importables. Se declaran `py-modules` y el entry point `kpi-processor`.
- El CI usaba `uv pip install --system` (PEP 668) y `uv run` sin proyecto.

### Seguridad

- Contenedor sin root y `no-new-privileges`.
- `DB_TABLE` solo desde entorno (no desde mensajes).
- `pip-audit` en CI.

## [0.1.0] - 2026-10-12

- Consumo de JSON desde Kafka y upsert de KPIs por minuto/hora/día.
- Compose con Zookeeper, Kafka, PostgreSQL y CloudBeaver.
