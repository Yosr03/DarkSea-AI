import json
import sys
import time

from pathlib import Path

import pytest


STREAMING_DIR = Path(__file__).resolve().parents[1]

sys.path.insert(0, str(STREAMING_DIR))

import batch_metrics

from batch_metrics import BatchMetrics


@pytest.fixture
def isolated_output(tmp_path, monkeypatch):

    monkeypatch.setattr(
        batch_metrics,
        "RESULTS_DIR",
        tmp_path / "results"
    )

    return tmp_path


def test_initialization():

    timer = BatchMetrics("test", 1)

    assert timer.batch_id == 1
    assert timer.run_name == "test"
    assert timer.metrics["status"] == "running"


def test_measure_duration():

    timer = BatchMetrics("test", 1)

    with timer.measure("scoring"):
        time.sleep(0.01)

    assert timer.metrics["scoring_ms"] > 0


def test_custom_metrics():

    timer = BatchMetrics("test", 1)

    timer.set(
        input_rows=100,
        alert_count=5
    )

    assert timer.metrics["input_rows"] == 100
    assert timer.metrics["alert_count"] == 5


def test_finish_success(isolated_output):

    timer = BatchMetrics("test", 1)

    result = timer.finish()

    assert result["status"] == "success"
    assert result["batch_total_ms"] >= 0


def test_json_export(isolated_output):

    timer = BatchMetrics("test", 2)

    timer.set(input_rows=100)

    timer.finish()

    path = (
        isolated_output
        / "results"
        / "test"
        / "batch_00000002.json"
    )

    assert path.exists()

    data = json.loads(
        path.read_text(encoding="utf-8")
    )

    assert data["input_rows"] == 100
    assert data["batch_id"] == 2


def test_failed_status(isolated_output):

    timer = BatchMetrics("test", 3)

    result = timer.finish(status="failed")

    assert result["status"] == "failed"


def test_measure_exception():

    timer = BatchMetrics("test", 1)

    with pytest.raises(ValueError):

        with timer.measure("scoring"):
            raise ValueError("Test error")

    assert "scoring_ms" in timer.metrics


def test_atomic_export(isolated_output):

    timer = BatchMetrics("test", 4)

    timer.finish()

    temporary_files = list(
        isolated_output.rglob("*.tmp")
    )

    assert temporary_files == []