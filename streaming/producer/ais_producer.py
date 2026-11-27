
import argparse
import csv
import json
import time
import uuid

from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer


BASE_DIR = Path(__file__).resolve().parents[1]

DATA_PATH = BASE_DIR / "data" / "ais_events.csv"

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TOPIC = "ais-events"

COLUMNS = [
    "gap_id",
    "mmsi",
    "vessel_class",
    "pavillon",
    "gap_start_timestamp",
    "gap_end_timestamp",
    "mois",
    "latitude",
    "longitude",
    "latitude_fin",
    "longitude_fin",
    "distance_cote_km",
    "gap_hours",
    "zone_distance",
    "cellule_lat",
    "cellule_lon",
    "duree_extreme",
    "mmsi_format_atypique"
]

FLOAT_COLUMNS = [
    "latitude",
    "longitude",
    "latitude_fin",
    "longitude_fin",
    "distance_cote_km",
    "gap_hours"
]

INT_COLUMNS = [
    "cellule_lat",
    "cellule_lon",
    "duree_extreme",
    "mmsi_format_atypique"
]

TEST_START = datetime(
    2019, 7, 1, tzinfo=timezone.utc
)

TEST_END = datetime(
    2020, 1, 1, tzinfo=timezone.utc
)


def load_events():

    events = []

    with open(
        DATA_PATH,
        encoding="utf-8-sig",
        newline=""
    ) as file:

        reader = csv.DictReader(
            file,
            fieldnames=COLUMNS
        )

        for row in reader:

            end_timestamp = datetime.fromisoformat(
                row["gap_end_timestamp"]
            )

            if not TEST_START <= end_timestamp < TEST_END:
                continue

            for column in FLOAT_COLUMNS:
                row[column] = float(row[column])

            for column in INT_COLUMNS:
                row[column] = int(row[column])

            events.append(row)

    events.sort(
        key=lambda event: (
            datetime.fromisoformat(
                event["gap_end_timestamp"]
            ),
            event["gap_id"]
        )
    )

    return events


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--limit",
        type=int,
        default=100
    )

    parser.add_argument(
        "--rate",
        type=float,
        default=10
    )

    parser.add_argument(
        "--dry-run",
        action="store_true"
    )

    args = parser.parse_args()

    if args.limit < 0 or args.rate <= 0:
        parser.error("limit >= 0 et rate > 0 requis")

    events = load_events()

    print("Evenements de test disponibles :", len(events))

    assert len(events) == 11123

    assert len({
        event["gap_id"] for event in events
    }) == len(events)

    if args.limit > 0:
        events = events[:args.limit]

    print("Evenements selectionnes :", len(events))

    if args.dry_run:

        print(json.dumps(
            events[0],
            indent=4,
            ensure_ascii=False
        ))

        print("VALIDATION DES DONNEES REUSSIE")
        return

    run_id = str(uuid.uuid4())

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(
            value,
            allow_nan=False
        ).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=3,
        max_in_flight_requests_per_connection=1
    )

    sent = 0
    start = time.perf_counter()

    print("\n=== DARKSEA AIS PRODUCER ===")
    print("Run ID :", run_id)
    print("Topic :", TOPIC)
    print("Debit cible :", args.rate, "events/s")

    try:

        for index, event in enumerate(events):

            target_time = start + index / args.rate

            delay = target_time - time.perf_counter()

            if delay > 0:
                time.sleep(delay)

            event["replay_id"] = run_id

            event["ingestion_timestamp"] = (
                datetime.now(timezone.utc).isoformat()
            )

            metadata = producer.send(
                TOPIC,
                key=event["mmsi"],
                value=event
            ).get(timeout=15)

            sent += 1

            if sent % 100 == 0 or sent == 1:

                print(
                    f"Envoyes={sent} | "
                    f"MMSI={event['mmsi']} | "
                    f"Partition={metadata.partition} | "
                    f"Offset={metadata.offset}"
                )

    finally:

        producer.close(timeout=15)

    duration = time.perf_counter() - start

    print("\n=== RESULTATS PRODUCTEUR ===")
    print("Run ID :", run_id)
    print("Messages confirmes :", sent)
    print("Duree :", round(duration, 3), "secondes")
    print(
        "Debit reel :",
        round(sent / max(duration, 0.001), 2),
        "events/s"
    )

    assert sent == len(events)

    print("REPLAY KAFKA TERMINE AVEC SUCCES")


if __name__ == "__main__":
    main()
