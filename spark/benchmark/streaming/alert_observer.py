import argparse
import json
import time

from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer, TopicPartition


parser = argparse.ArgumentParser()

parser.add_argument("--run-name", required=True)
parser.add_argument("--timeout", type=int, default=600)

args = parser.parse_args()

if not args.run_name.replace("-", "").replace("_", "").isalnum():
    parser.error("Invalid run name")

if args.timeout <= 0:
    parser.error("Timeout must be positive")


TOPIC = "ais-benchmark-alerts"

OUTPUT = (
    Path(__file__).resolve().parent
    / "results"
    / args.run_name
)

OUTPUT.mkdir(parents=True, exist_ok=True)

destination = OUTPUT / "observed_alerts.jsonl"

consumer = KafkaConsumer(
    bootstrap_servers=["127.0.0.1:9092"],
    enable_auto_commit=False,
    group_id=None,
    auto_offset_reset="latest",
    request_timeout_ms=10000,
    api_version_auto_timeout_ms=5000
)

received = 0
invalid = 0
seen = set()

try:

    partitions = consumer.partitions_for_topic(TOPIC)

    if not partitions:
        raise RuntimeError("Kafka partitions unavailable")

    assignments = [
        TopicPartition(TOPIC, p)
        for p in sorted(partitions)
    ]

    consumer.assign(assignments)

    offsets = consumer.end_offsets(assignments)

    for partition, offset in offsets.items():
        consumer.seek(partition, offset)

    with destination.open(
        "x",
        encoding="utf-8"
    ) as output:

        print("OBSERVER READY", flush=True)
        print("Initial offsets:", offsets, flush=True)

        start = time.monotonic()

        while time.monotonic() - start < args.timeout:

            records = consumer.poll(
                timeout_ms=500,
                max_records=500
            )

            observed_at = datetime.now(timezone.utc)

            for messages in records.values():

                for message in messages:

                    try:

                        alert = json.loads(
                            message.value.decode("utf-8")
                        )

                        ingestion = datetime.fromisoformat(
                            alert["ingestion_timestamp"]
                            .replace("Z", "+00:00")
                        )

                        alert_id = alert["alert_id"]
                        replay_id = alert["replay_id"]

                        latency = (
                            observed_at - ingestion
                        ).total_seconds() * 1000

                        duplicate = alert_id in seen

                        seen.add(alert_id)

                        record = {
                            "run_name": args.run_name,
                            "alert_id": alert_id,
                            "replay_id": replay_id,
                            "gap_id": alert["gap_id"],
                            "type_alerte": alert["type_alerte"],
                            "ingestion_timestamp": (
                                ingestion.isoformat()
                            ),
                            "observed_at": (
                                observed_at.isoformat()
                            ),
                            "end_to_end_ms": latency,
                            "duplicate": duplicate,
                            "partition": message.partition,
                            "offset": message.offset
                        }

                        output.write(
                            json.dumps(record, allow_nan=False)
                            + "\n"
                        )

                        output.flush()

                        received += 1

                        print(
                            f"Alert {received} | "
                            f"Latency: {latency:.2f} ms",
                            flush=True
                        )

                    except (
                        ValueError,
                        KeyError,
                        TypeError,
                        AttributeError
                    ) as exc:

                        invalid += 1

                        print("Invalid alert:", exc)

except KeyboardInterrupt:

    print("Observer stopped manually")

finally:

    consumer.close()

    print("Received alerts:", received)
    print("Unique alerts:", len(seen))
    print("Duplicates:", received - len(seen))
    print("Invalid alerts:", invalid)
    print("Output:", destination)