import datetime
import json
import logging
import os
import signal
import sys
from dotenv import load_dotenv
from kafka import KafkaConsumer, errors as kafka_errors
import psycopg2
from psycopg2 import errors as pg_errors

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

DB_NAME = os.environ["DB_NAME"]
DB_HOST = os.environ["DB_HOST"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_PORT = os.environ.get("DB_PORT", "5432")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "test")
KAFKA_GROUP = os.environ.get("KAFKA_GROUP", "kpi-processor")
DB_TABLE = os.environ.get("DB_TABLE", "test.kpis")


class KpiProcessor:
    def __init__(self):
        self.conn = None
        self.cur = None
        self.consumer = None
        self.running = False

    def connect_db(self):
        try:
            self.conn = psycopg2.connect(
                database=DB_NAME,
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                port=DB_PORT
            )
            self.cur = self.conn.cursor()
            logger.info("Conectado a PostgreSQL %s@%s:%s/%s", DB_USER, DB_HOST, DB_PORT, DB_NAME)
        except Exception as e:
            logger.critical("No se pudo conectar a PostgreSQL: %s", e)
            sys.exit(1)

    def connect_kafka(self):
        try:
            self.consumer = KafkaConsumer(
                KAFKA_TOPIC,
                group_id=KAFKA_GROUP,
                bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True
            )
            logger.info("Conectado a Kafka %s topic=%s group=%s", KAFKA_BOOTSTRAP, KAFKA_TOPIC, KAFKA_GROUP)
        except Exception as e:
            logger.critical("No se pudo conectar a Kafka: %s", e)
            sys.exit(1)

    def _build_kpi_value(self, timestamp, parts):
        try:
            dt = datetime.datetime.fromtimestamp(timestamp)
            if parts == 3:
                return dt.strftime("%Y-%m-%d")
            elif parts == 4:
                return dt.strftime("%Y-%m-%d-%H")
            elif parts == 5:
                return dt.strftime("%Y-%m-%d-%H-%M")
            else:
                return dt.strftime("%Y-%m-%d-%H-%M")[:parts]
        except (TypeError, ValueError, OSError) as e:
            logger.warning("Timestamp inválido %s: %s", timestamp, e)
            return None

    def _store_kpi(self, kpi_key, kpi_value, vcount=1):
        if kpi_value is None:
            return
        sql = f"""
        INSERT INTO {DB_TABLE} (kpi_key, kpi_value, vcount)
        VALUES (%s, %s, %s)
        ON CONFLICT (kpi_key, kpi_value)
        DO UPDATE SET vcount = {DB_TABLE}.vcount + EXCLUDED.vcount;
        """
        try:
            self.cur.execute(sql, (kpi_key, kpi_value, vcount))
            self.conn.commit()
        except Exception as e:
            logger.error("Error almacenando KPI %s=%s: %s", kpi_key, kpi_value, e)
            self.conn.rollback()

    def _process_message(self, msg):
        try:
            value = msg.value
            if not isinstance(value, dict) or "@timestamp" not in value:
                logger.warning("Mensaje sin @timestamp: %s", value)
                return

            ts = value["@timestamp"]
            minute_val = self._build_kpi_value(ts, 5)
            hour_val = self._build_kpi_value(ts, 4)
            day_val = self._build_kpi_value(ts, 3)

            self._store_kpi("REQUEST_X_MINUTE", minute_val)
            self._store_kpi("REQUEST_X_HOUR", hour_val)
            self._store_kpi("REQUEST_X_DAY", day_val)

        except (KeyError, TypeError, json.JSONDecodeError) as e:
            logger.error("Error procesando mensaje: %s", e)

    def _shutdown(self, signum=None, frame=None):
        logger.info("Deteniendo procesador...")
        self.running = False
        if self.consumer:
            try:
                self.consumer.close()
            except Exception:
                pass
        if self.cur:
            self.cur.close()
        if self.conn:
            try:
                self.conn.commit()
                self.conn.close()
            except Exception:
                pass
        logger.info("Procesador detenido.")
        sys.exit(0)

    def run(self):
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

        self.connect_db()
        self.connect_kafka()
        self.running = True

        logger.info("Procesador iniciado. Esperando mensajes...")
        try:
            for msg in self.consumer:
                if not self.running:
                    break
                logger.debug("Mensaje recibido: topic=%s partition=%s offset=%s",
                             msg.topic, msg.partition, msg.offset)
                self._process_message(msg)
        except Exception as e:
            logger.error("Error en el loop principal: %s", e)
        finally:
            self._shutdown()


if __name__ == "__main__":
    processor = KpiProcessor()
    processor.run()
