"""Tests del procesador con cursor y consumer falsos."""

import pytest

from app import Config, KpiProcessor, load_config


def make_config(**overrides):
    base = dict(db_name="db", db_host="localhost", db_user="user", db_password="pass")
    base.update(overrides)
    return Config(**base)


class FakeCursor:
    def __init__(self, fail=False):
        self.rows = None
        self.calls = 0
        self.fail = fail

    def executemany(self, _sql, rows):
        if self.fail:
            raise RuntimeError("db caída")
        self.rows = list(rows)
        self.calls += 1

    def close(self):
        pass


class FakeConn:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        pass


class FakeConsumer:
    def __init__(self):
        self.commits = 0

    def commit(self):
        self.commits += 1

    def close(self):
        pass


def test_load_config_falla_con_mensaje_claro():
    with pytest.raises(RuntimeError, match="DB_NAME"):
        load_config(env={"DB_HOST": "h", "DB_USER": "u", "DB_PASSWORD": "p"})


def test_load_config_defaults():
    cfg = load_config(env={"DB_NAME": "d", "DB_HOST": "h", "DB_USER": "u", "DB_PASSWORD": "p"})
    assert cfg.db_port == "5432"
    assert cfg.kafka_topic == "test"
    assert cfg.flush_max_rows == 500


def test_process_payload_valido_acumula_tres_ventanas():
    processor = KpiProcessor(make_config())
    assert processor.process_payload({"@timestamp": 1767227045}) is True
    assert len(processor.counter) == 3


@pytest.mark.parametrize("payload", [None, {}, {"@timestamp": "no"}, {"otra": 1}, []])
def test_process_payload_invalido_se_descarta(payload):
    processor = KpiProcessor(make_config())
    assert processor.process_payload(payload) is False
    assert len(processor.counter) == 0


def test_flush_escribe_y_commitea():
    processor = KpiProcessor(make_config())
    processor.cur = FakeCursor()
    processor.conn = FakeConn()
    processor.process_payload({"@timestamp": 1767227045})

    assert processor.flush() == 3
    assert processor.cur.calls == 1
    assert processor.conn.commits == 1
    assert len(processor.cur.rows) == 3


def test_flush_sin_datos_no_hace_nada():
    processor = KpiProcessor(make_config())
    processor.cur = FakeCursor()
    processor.conn = FakeConn()
    assert processor.flush() == 0
    assert processor.cur.calls == 0


def test_flush_fallido_conserva_datos_y_no_commitea_offset():
    processor = KpiProcessor(make_config())
    processor.cur = FakeCursor(fail=True)
    processor.conn = FakeConn()
    processor.consumer = FakeConsumer()
    processor.process_payload({"@timestamp": 1767227045})

    processor._flush_and_commit()

    assert processor.conn.rollbacks == 1
    assert len(processor.counter) == 3
    assert processor.consumer.commits == 0


def test_insert_sql_es_upsert():
    sql = KpiProcessor(make_config()).insert_sql
    assert "ON CONFLICT (kpi_key, kpi_value)" in sql
    assert "EXCLUDED.vcount" in sql


def test_shutdown_marca_running_false():
    processor = KpiProcessor(make_config())
    processor.running = True
    processor._shutdown()
    assert processor.running is False
