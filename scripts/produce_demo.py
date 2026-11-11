"""Genera eventos JSON de ejemplo para probar el pipeline sin una app real."""

from __future__ import annotations

import argparse
import json
import os
import random
import time
from typing import Any

from kafka import KafkaProducer


def build_event(timestamp: float | None = None, service: str = "api", status: int = 200) -> dict[str, Any]:
    return {
        "@timestamp": time.time() if timestamp is None else timestamp,
        "service": service,
        "status": status,
        "latency_ms": round(random.uniform(5, 500), 2),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Envía eventos de demo a Kafka")
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--interval", type=float, default=0.02, help="Segundos entre eventos")
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "test"))
    parser.add_argument(
        "--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:29092")
    )
    args = parser.parse_args(argv)

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap.split(","),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        linger_ms=50,
    )
    for _ in range(args.count):
        producer.send(args.topic, build_event())
        if args.interval:
            time.sleep(args.interval)
    producer.flush()
    producer.close()
    print(f"Enviados {args.count} eventos a {args.topic} ({args.bootstrap})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
