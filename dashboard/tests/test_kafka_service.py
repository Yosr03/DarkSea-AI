
import sys
import json

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1])
)

from services.kafka_service import (
    decode_alert,
    merge_alerts,
    poll_alerts
)


def make_alert(identifier):
    return {
        "alert_id": f"replay-1:{identifier}",
        "replay_id": "replay-1",
        "gap_id": identifier,
        "mmsi": "123456789",
        "type_alerte": "ANOMALIE_STATISTIQUE"
    }


def test_decode_valid_alert():
    alert = make_alert("gap-1")
    payload = json.dumps(alert).encode("utf-8")

    assert decode_alert(payload) == alert


def test_decode_invalid_json():
    assert decode_alert(b"invalid-json") is None


def test_decode_invalid_utf8():
    assert decode_alert(b"\xff") is None


def test_decode_missing_alert_id():
    assert decode_alert(b'{"replay_id":"r1"}') is None


def test_decode_missing_replay_id():
    assert decode_alert(b'{"alert_id":"a1"}') is None


def test_decode_non_dictionary():
    assert decode_alert(b'["alert"]') is None


def test_merge_alerts_without_duplicates():
    first = make_alert("gap-1")
    second = make_alert("gap-2")

    result = merge_alerts(
        [first],
        [first, second]
    )

    assert len(result) == 2


def test_merge_alerts_updates_existing():
    original = make_alert("gap-1")
    updated = {
        **original,
        "distance_centroide": 3.5
    }

    result = merge_alerts(
        [original],
        [updated]
    )

    assert len(result) == 1
    assert result[0]["distance_centroide"] == 3.5


def test_merge_alerts_memory_limit():
    alerts = [
        make_alert(f"gap-{i}")
        for i in range(10)
    ]

    result = merge_alerts(
        [],
        alerts,
        max_alerts=3
    )

    assert len(result) == 3
    assert result[-1]["gap_id"] == "gap-9"


def test_different_replays_are_not_duplicates():
    first = make_alert("gap-1")

    second = {
        **first,
        "replay_id": "replay-2",
        "alert_id": "replay-2:gap-1"
    }

    result = merge_alerts(
        [first],
        [second]
    )

    assert len(result) == 2


def test_poll_alerts():
    consumer = Mock()

    valid = SimpleNamespace(
        value=json.dumps(
            make_alert("gap-1")
        ).encode("utf-8")
    )

    invalid = SimpleNamespace(
        value=b"invalid-json"
    )

    consumer.poll.return_value = {
        "partition-0": [valid, invalid]
    }

    alerts, invalid_count = poll_alerts(
        consumer,
        max_seconds=1,
        max_records=2
    )

    assert len(alerts) == 1
    assert invalid_count == 1
