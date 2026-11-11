## Qué cambia

## Por qué

## Cómo se probó

- [ ] `ruff check .`
- [ ] `mypy app.py kpi.py scripts`
- [ ] `pytest -q --cov=app --cov=kpi --cov=scripts --cov-fail-under=65`
- [ ] `docker compose config -q`
- [ ] `docker compose up -d --build` + productor de demo (si aplica)

## Checklist

- [ ] Tests nuevos para el cambio
- [ ] Sin credenciales en el diff
- [ ] `docs/` actualizado si cambia configuración o despliegue
