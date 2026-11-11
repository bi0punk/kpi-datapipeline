"""Consumidor Kafka que calcula KPIs y los persiste en PostgreSQL."""

from __future__ import annotations

import contextlib
import json
import logging
import os
import signal
import sys
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import psycopg2
from dotenv import load_dotenv
from kafka import KafkaConsumer

from kpi import KpiCounter, parse_timestamp

logger = logging.getLogger(__name__)

DEFAULT_TABLE = "test.kpis"


@dataclass(frozen=True)
class Config:
    db_name: str
    db_host: str
    db_user: str
    db_password: str
    db_port: str = "5432"
    db_table: str = DEFAULT_TABLE
    kafka_bootstrap: str = "127.0.0.1:9092"
    kafka_topic: str = "test"
    kafka_group: str = "kpi-processor"
    flush_interval: float = 2.0
    flush_max_rows: int = 500


def load_config(env: Mapping[str, str] | None = None) -> Config:
    """Lee la configuración del entorno y falla con un mensaje claro si falta algo."""
    source: Mapping[str, str] = os.environ if env is None else env
    missing = [key for key in ("DB_NAME", "DB_HOST", "DB_USER", "DB_PASSWORD") if not source.get(key)]
    if missing:
        raise RuntimeError("Faltan variables de entorno: " + ", ".join(missing))

    return Config(
        db_name=source["DB_NAME"],
        db_host=source["DB_HOST"],
        db_user=source["DB_USER"],
        db_password=source["DB_PASSWORD"],
        db_port=source.get("DB_PORT", "5432"),
        db_table=source.get("DB_TABLE", DEFAULT_TABLE),
        kafka_bootstrap=source.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
        kafka_topic=source.get("KAFKA_TOPIC", "test"),
        kafka_group=source.get("KAFKA_GROUP", "kpi-processor"),
        flush_interval=float(source.get("KPI_FLUSH_INTERVAL", "2.0")),
        flush_max_rows=int(source.get("KPI_FLUSH_MAX_ROWS", "500")),
    )


class KpiProcessor:
    def __init__(self, config: Config):
        self.config = config
        self.conn: Any = None
        self.cur: Any = None
        self.consumer: Any = None
        self.running = False
        self.counter = KpiCounter()
        self._pending = 0

    @property
    def insert_sql(self) -> str:
        table = self.config.db_table
        # DB_TABLE viene de configuración de entorno, no de entrada de usuario.
        return (
            f"INSERT INTO {table} (kpi_key, kpi_value, vcount) VALUES (%s, %s, %s) "
            f"ON CONFLICT (kpi_key, kpi_value) "
            f"DO UPDATE SET vcount = {table}.vcount + EXCLUDED.vcount"  # nosec B608
        )

    # ----- conexiones -----

    def connect_db(self) -> None:
        self.conn = psycopg2.connect(
            database=self.config.db_name,
            host=self.config.db_host,
            user=self.config.db_user,
            password=self.config.db_password,
            port=self.config.db_port,
        )
        self.cur = self.conn.cursor()
        logger.info(
            "Conectado a PostgreSQL %s@%s:%s/%s",
            self.config.db_user,
            self.config.db_host,
            self.config.db_port,
            self.config.db_name,
        )

    def connect_kafka(self) -> None:
        self.consumer = KafkaConsumer(
            self.config.kafka_topic,
            group_id=self.config.kafka_group,
            bootstrap_servers=self.config.kafka_bootstrap.split(","),
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            auto_offset_reset="earliest",
            # El commit se hace después de persistir el lote: al menos una vez.
            enable_auto_commit=False,
        )
        logger.info(
            "Conectado a Kafka %s topic=%s group=%s",
            self.config.kafka_bootstrap,
            self.config.kafka_topic,
            self.config.kafka_group,
        )

    # ----- procesamiento -----

    def process_payload(self, value: object) -> bool:
        """Procesa un mensaje ya deserializado. Devuelve False si se descarta."""
        if not isinstance(value, dict) or "@timestamp" not in value:
            logger.warning("Mensaje sin @timestamp: %r", value)
            return False
        try:
            timestamp = parse_timestamp(value["@timestamp"])
        except (TypeError, ValueError) as exc:
            logger.warning("Timestamp inválido: %s", exc)
            return False

        self.counter.add(timestamp)
        self._pending += 1
        return True

    def flush(self) -> int:
        """Escribe las filas acumuladas y confirma la transacción.

        Devuelve el número de filas escritas, 0 si no había nada y -1 si falló
        (en ese caso conserva el contador para reintentar y no commitea offset).
        """
        rows = [(key, value, count) for (key, value), count in self.counter.counts.items()]
        if not rows or self.cur is None:
            return 0
        try:
            self.cur.executemany(self.insert_sql, rows)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            logger.exception("Error almacenando %d filas KPI", len(rows))
            return -1
        self.counter.drain()
        self._pending = 0
        logger.info("Flush: %d filas KPI", len(rows))
        return len(rows)

    def _flush_and_commit(self) -> None:
        written = self.flush()
        if written >= 0 and self.consumer is not None:
            with contextlib.suppress(Exception):
                self.consumer.commit()

    def _shutdown(self, signum: int | None = None, _frame: object = None) -> None:
        logger.info("Deteniendo procesador (signal=%s)...", signum)
        self.running = False

    def _close(self) -> None:
        self._flush_and_commit()
        if self.consumer is not None:
            with contextlib.suppress(Exception):
                self.consumer.close()
        if self.cur is not None:
            with contextlib.suppress(Exception):
                self.cur.close()
        if self.conn is not None:
            with contextlib.suppress(Exception):
                self.conn.close()
        logger.info("Procesador detenido.")

    def run(self) -> None:
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

        self.connect_db()
        self.connect_kafka()
        self.running = True
        last_flush = time.monotonic()

        logger.info("Procesador iniciado. Esperando mensajes...")
        try:
            while self.running:
                records = self.consumer.poll(timeout_ms=500)
                for messages in records.values():
                    for msg in messages:
                        logger.debug(
                            "Mensaje recibido: topic=%s partition=%s offset=%s",
                            msg.topic,
                            msg.partition,
                            msg.offset,
                        )
                        self.process_payload(msg.value)
                        if self._pending >= self.config.flush_max_rows:
                            self._flush_and_commit()
                            last_flush = time.monotonic()
                if time.monotonic() - last_flush >= self.config.flush_interval:
                    self._flush_and_commit()
                    last_flush = time.monotonic()
        except Exception:
            logger.exception("Error en el loop principal")
        finally:
            self._close()


def main() -> int:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    try:
        config = load_config()
    except RuntimeError as exc:
        logger.critical("%s", exc)
        return 2

    KpiProcessor(config).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
