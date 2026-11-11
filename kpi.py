"""Funciones puras de ventanas KPI.

Separadas de app.py para poder testearlas sin Kafka ni PostgreSQL.
"""

from __future__ import annotations

import datetime as dt
from collections import Counter
from dataclasses import dataclass, field

KPI_MINUTE = "REQUEST_X_MINUTE"
KPI_HOUR = "REQUEST_X_HOUR"
KPI_DAY = "REQUEST_X_DAY"


def parse_timestamp(value: object) -> float:
    """Convierte un timestamp UNIX (int, float o str numérico) a float.

    Lanza TypeError/ValueError con mensajes claros para que el llamador decida
    si descarta el mensaje.
    """
    if isinstance(value, bool) or value is None:
        raise TypeError(f"timestamp inválido: {value!r}")
    if isinstance(value, (int, float)):
        ts = float(value)
    elif isinstance(value, str):
        try:
            ts = float(value.strip())
        except ValueError as exc:
            raise ValueError(f"timestamp no numérico: {value!r}") from exc
    else:
        raise TypeError(f"tipo de timestamp no soportado: {type(value).__name__}")

    if ts <= 0:
        raise ValueError(f"timestamp fuera de rango: {ts}")
    return ts


def window_keys(timestamp: float) -> dict[str, str]:
    """Devuelve las claves minuto/hora/día en UTC para un timestamp UNIX."""
    when = dt.datetime.fromtimestamp(timestamp, tz=dt.UTC)
    return {
        KPI_MINUTE: when.strftime("%Y-%m-%d-%H-%M"),
        KPI_HOUR: when.strftime("%Y-%m-%d-%H"),
        KPI_DAY: when.strftime("%Y-%m-%d"),
    }


@dataclass
class KpiCounter:
    """Acumula conteos en memoria y los vacía en lote."""

    counts: Counter[tuple[str, str]] = field(default_factory=Counter)

    def add(self, timestamp: float) -> None:
        for kpi_key, kpi_value in window_keys(timestamp).items():
            self.counts[(kpi_key, kpi_value)] += 1

    def __len__(self) -> int:
        return len(self.counts)

    def drain(self) -> list[tuple[str, str, int]]:
        """Devuelve las filas acumuladas y limpia el contador."""
        rows = [(key, value, count) for (key, value), count in self.counts.items()]
        self.counts.clear()
        return rows
