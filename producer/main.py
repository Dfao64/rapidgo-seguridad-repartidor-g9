"""Configurable synthetic events; run from repository root: python -m producer."""
import argparse
from datetime import datetime, timedelta, timezone
import json
import math
import os
import random
import sys
import time
import uuid

TOPIC = "rapidgo.seguridad.repartidor.v1"


def utc(value):
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
        return parsed.replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Use YYYY-MM-DDThh:mm:ssZ") from exc


def config(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", TOPIC))
    p.add_argument("--interval", type=float, default=os.getenv("PRODUCER_INTERVAL", "5"), help="Seconds per rider round (>=1)")
    p.add_argument("--riders", type=int, default=os.getenv("PRODUCER_RIDERS", "3"))
    p.add_argument("--rounds", type=int, default=os.getenv("PRODUCER_ROUNDS", "0"), help="0 = continuous")
    p.add_argument("--seed", type=int, default=os.getenv("PRODUCER_SEED", "42"))
    p.add_argument("--start-time", type=utc, default=os.getenv("PRODUCER_START_TIME"))
    p.add_argument("--run-id", default=os.getenv("PRODUCER_RUN_ID"), help="Reuse only for intentional replay")
    p.add_argument("--dry-run", action="store_true", help="JSONL to stdout; no Kafka")
    a = p.parse_args(argv)
    if not math.isfinite(a.interval) or a.interval < 1 or not a.interval.is_integer():
        p.error("interval must be a finite whole number >= 1 (contract timestamps have seconds)")
    if a.riders < 1 or a.rounds < 0:
        p.error("riders >= 1 and rounds >= 0 required")
    if not a.bootstrap_servers.strip() or not a.topic.strip() or a.run_id == "":
        p.error("bootstrap servers, topic and run id must not be empty")
    a.start_time = a.start_time or datetime.now(timezone.utc).replace(microsecond=0)
    a.run_id = a.run_id or str(uuid.uuid4())
    return a


def events(a):
    """One event per rider per round; deterministic with seed/start/run id.

    Reference route is a north-south meridian at longitude -74.264.
    Deviation is the great-circle cross-track distance to that meridian.
    All coordinates and operational values are synthetic, not a traffic model.
    """
    rng = random.Random(a.seed)
    round_no = 0
    namespace = uuid.uuid5(uuid.NAMESPACE_URL, a.run_id)
    while a.rounds == 0 or round_no < a.rounds:
        for rider in range(1, a.riders + 1):
            lat = round(4.732 + (round_no % 100) * 0.0001 + rng.uniform(-0.00005, 0.00005), 6)
            lon = round(-74.264 + rng.uniform(-0.005, 0.005), 6)
            deviation = 6371000 * math.asin(abs(math.cos(math.radians(lat)) * math.sin(math.radians(lon + 74.264))))
            state = rng.choice(("EN_RUTA", "ENTREGADO", "PAUSADO"))
            yield {
                "event_id": str(uuid.uuid5(namespace, f"{round_no}:{rider}")),
                "repartidor_id": f"REP-{rider:03d}",
                "pedido_id": f"PED-{rider:03d}-{round_no // 12:06d}",
                "timestamp": (a.start_time + timedelta(seconds=round_no * a.interval)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "gps": {"latitud": lat, "longitud": lon},
                "velocidad_kmh": round(rng.uniform(0, 85), 2) if state == "EN_RUTA" else 0,
                "bateria_pct": round(rng.uniform(0, 100), 2),
                "estado": state,
                "desvio_metros": round(deviation, 2),
            }
        round_no += 1


def encode(event):
    return json.dumps(event, ensure_ascii=False, allow_nan=False, sort_keys=True).encode("utf-8")


def publish(a, client, sleep=time.sleep, clock=time.monotonic):
    """Wait for broker ACK before reporting success; retain per-key order."""
    total = 0
    deadline = clock()
    for event in events(a):
        if total and total % a.riders == 0:
            deadline += a.interval
            sleep(max(0, deadline - clock()))
        result = client.send(a.topic, key=event["repartidor_id"].encode("utf-8"), value=encode(event)).get(timeout=30)
        total += 1
        print(json.dumps({"event_id": event["event_id"], "key": event["repartidor_id"], "topic": result.topic, "partition": result.partition, "offset": result.offset}), flush=True)
    client.flush(timeout=30)
    return total


def main(argv=None):
    a = config(argv)
    if a.dry_run:
        try:
            for event in events(a):
                print(encode(event).decode("utf-8"))
        except KeyboardInterrupt:
            return 130
        return 0
    client = None
    try:
        from kafka import KafkaProducer
        client = KafkaProducer(
            bootstrap_servers=a.bootstrap_servers.split(","),
            client_id="rapidgo-daniel-producer", acks="all", retries=5,
            max_in_flight_requests_per_connection=1,
            max_block_ms=30000, request_timeout_ms=15000,
        )
        publish(a, client)
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"Producer failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if client is not None:
            client.close(timeout=30)
