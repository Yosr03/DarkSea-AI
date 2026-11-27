
import json
import time

from kafka import KafkaConsumer


BOOTSTRAP_SERVERS = ["127.0.0.1:9092"]
ALERT_TOPIC = "ais-alerts"


def decode_alert(value):
    """Décoder et vérifier un message Kafka."""

    try:
        alert = json.loads(value.decode("utf-8"))

        if not isinstance(alert, dict):
            return None

        if not alert.get("alert_id"):
            return None

        if not alert.get("replay_id"):
            return None

        return alert

    except (UnicodeDecodeError, json.JSONDecodeError, AttributeError):
        return None


def merge_alerts(existing, incoming, max_alerts=20000):
    """Fusionner les alertes sans doublons."""

    merged = {}

    for alert in [*existing, *incoming]:
        if alert.get("alert_id"):
            merged[alert["alert_id"]] = alert

    return list(merged.values())[-max_alerts:]


def create_consumer():
    """Créer un consommateur indépendant du pipeline Spark."""

    return KafkaConsumer(
        ALERT_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=None,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        request_timeout_ms=10000,
        api_version_auto_timeout_ms=5000,
        max_poll_records=500
    )


def poll_alerts(consumer, max_seconds=2, max_records=2000):
    """Lire un nombre limité de messages sans bloquer le dashboard."""

    alerts = []
    invalid_messages = 0
    received = 0

    deadline = time.monotonic() + max_seconds

    while time.monotonic() < deadline and received < max_records:

        remaining_ms = max(
            1,
            int((deadline - time.monotonic()) * 1000)
        )

        batches = consumer.poll(
            timeout_ms=min(500, remaining_ms),
            max_records=min(500, max_records - received)
        )

        for messages in batches.values():
            for message in messages:

                received += 1
                alert = decode_alert(message.value)

                if alert is None:
                    invalid_messages += 1
                else:
                    alerts.append(alert)

    return alerts, invalid_messages
