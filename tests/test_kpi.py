"""Tests de las ventanas KPI sin Kafka ni PostgreSQL."""

import datetime as dt

import pytest

from kpi import KpiCounter, parse_timestamp, window_keys


def test_parse_timestamp_acepta_numero_y_texto():
    assert parse_timestamp(1700000000) == 1700000000.0
    assert parse_timestamp("1700000000.5") == 1700000000.5


@pytest.mark.parametrize("valor", [None, True, [], {}, "no-numero", -5, 0])
def test_parse_timestamp_rechaza_invalidos(valor):
    with pytest.raises((TypeError, ValueError)):
        parse_timestamp(valor)


def test_window_keys_usa_utc():
    ts = dt.datetime(2026, 1, 2, 3, 4, 5, tzinfo=dt.UTC).timestamp()
    assert window_keys(ts) == {
        "REQUEST_X_MINUTE": "2026-01-02-03-04",
        "REQUEST_X_HOUR": "2026-01-02-03",
        "REQUEST_X_DAY": "2026-01-02",
    }


def test_counter_acumula_y_drena():
    counter = KpiCounter()
    ts = dt.datetime(2026, 1, 2, 3, 4, 5, tzinfo=dt.UTC).timestamp()

    counter.add(ts)
    counter.add(ts)
    assert len(counter) == 3

    rows = dict(((k, v), c) for k, v, c in counter.drain())
    assert rows[("REQUEST_X_MINUTE", "2026-01-02-03-04")] == 2
    assert rows[("REQUEST_X_HOUR", "2026-01-02-03")] == 2
    assert rows[("REQUEST_X_DAY", "2026-01-02")] == 2
    assert len(counter) == 0
