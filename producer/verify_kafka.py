"""Read back an isolated test run, verify contract, keys and deterministic payloads."""
import argparse
import json
import time
from collections import Counter

from kafka import KafkaConsumer, TopicPartition

from producer.main import config, events
from spark.reglas import es_evento_valido


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap-servers", default="127.0.0.1:19092")
    p.add_argument("--rounds", type=int, default=4)
    p.add_argument("--run-id", default="evidence-week2")
    a = p.parse_args()
    expected = list(events(config(["--rounds", str(a.rounds), "--seed", "42", "--start-time", "2026-10-30T14:00:00Z", "--run-id", a.run_id])))
    topic = "rapidgo.seguridad.repartidor.v1"
    consumer = KafkaConsumer(bootstrap_servers=a.bootstrap_servers.split(","), enable_auto_commit=False, group_id=None)
    try:
        partitions = consumer.partitions_for_topic(topic)
        assert partitions == {0, 1, 2}, partitions
        consumer.assign([TopicPartition(topic, n) for n in sorted(partitions)])
        consumer.seek_to_beginning()
        wanted = {e["event_id"]: e for e in expected}
        found, by_rider, counts = {}, {}, Counter()
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline and len(found) < len(wanted):
            for records in consumer.poll(timeout_ms=1000).values():
                for record in records:
                    event = json.loads(record.value)
                    if event.get("event_id") not in wanted:
                        continue
                    assert es_evento_valido(event) and event == wanted[event["event_id"]]
                    assert record.key.decode() == event["repartidor_id"]
                    assert event["event_id"] not in found, "duplicate event"
                    found[event["event_id"]] = event
                    rider = event["repartidor_id"]
                    previous = by_rider.get(rider)
                    if previous:
                        assert previous[0] == record.partition and previous[1] < record.offset
                        assert previous[2] < event["timestamp"]
                    by_rider[rider] = (record.partition, record.offset, event["timestamp"])
                    counts[record.partition] += 1
                    print(json.dumps({"key": rider, "partition": record.partition, "offset": record.offset, "event": event}, sort_keys=True))
        assert set(found) == set(wanted), f"received {len(found)}/{len(wanted)}"
        print(json.dumps({"verified": len(found), "partitions": sorted(partitions), "counts": dict(counts), "contract": "v1", "run_id": a.run_id}, sort_keys=True))
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
