"""Tests del generador de eventos de demo."""


from scripts.produce_demo import build_event, main


def test_build_event_tiene_timestamp_y_campos():
    event = build_event(timestamp=1767227045.0)
    assert event["@timestamp"] == 1767227045.0
    assert event["service"] == "api"
    assert event["status"] == 200
    assert 5 <= event["latency_ms"] <= 500


def test_main_envia_con_producer_falso(monkeypatch, capsys):
    enviados = []

    class FakeProducer:
        def __init__(self, **_kwargs):
            pass

        def send(self, topic, value):
            enviados.append((topic, value))

        def flush(self):
            pass

        def close(self):
            pass

    monkeypatch.setattr("scripts.produce_demo.KafkaProducer", FakeProducer)
    assert main(["--count", "3", "--interval", "0", "--topic", "demo"]) == 0
    assert len(enviados) == 3
    assert all(topic == "demo" for topic, _ in enviados)
    assert "Enviados 3 eventos" in capsys.readouterr().out
