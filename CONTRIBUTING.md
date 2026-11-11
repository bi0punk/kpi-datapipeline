# Contribuir

Rama de trabajo: `dev`; `main` recibe merges `--no-ff` con CI verde.

## Entorno

```bash
uv venv .venv --python 3.12
uv pip install -e ".[dev]"
```

## Flujo

1. Rama desde `dev` (`feat/*`, `fix/*`, `docs/*`).
2. Commits atómicos en imperativo.
3. PR contra `dev` con CI verde.

## Calidad

```bash
ruff check .
mypy app.py kpi.py scripts
pytest -q --cov=app --cov=kpi --cov=scripts --cov-fail-under=65
pip-audit
docker compose config -q
```

## Tests

- `tests/test_kpi.py` — timestamps, ventanas UTC y contador.
- `tests/test_processor.py` — config, procesamiento, flush, rollback y shutdown.
- `tests/test_producer.py` — payload de demo y envío con productor falso.
- No se necesita Kafka ni PostgreSQL reales: se usan dobles.

## Estilo

- Python 3.11+, tipado, líneas de 100 (ruff).
- `logging`, sin `print` en el procesador (el productor sí imprime el resumen).
- Las funciones puras van en `kpi.py`; `app.py` solo orquesta.
