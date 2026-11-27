import json
import time

from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

RESULTS_DIR = Path(__file__).resolve().parent / "results"

class BatchMetrics:

    def __init__(self, run_name, batch_id):

        self.run_name = run_name
        self.batch_id = int(batch_id)
        self.started = time.perf_counter()

        self.metrics = {
            "run_name": run_name,
            "batch_id": self.batch_id,
            "started_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "status": "running"
        }

    @contextmanager
    def measure(self, name):

        start = time.perf_counter()

        try:
            yield

        finally:
            elapsed = (
                time.perf_counter() - start
            ) * 1000

            self.metrics[f"{name}_ms"] = round(
                elapsed, 3
            )

    def set(self, **values):

        self.metrics.update(values)

    def finish(self, status="success"):

        self.metrics["status"] = status

        self.metrics["batch_total_ms"] = round(
            (time.perf_counter() - self.started)
            * 1000,
            3
        )

        self.metrics["finished_at"] = datetime.now(
            timezone.utc
        ).isoformat()

        output = RESULTS_DIR / self.run_name

        output.mkdir(
            parents=True,
            exist_ok=True
        )

        destination = (
            output
            / f"batch_{self.batch_id:08d}.json"
        )

        temporary = destination.with_suffix(".tmp")

        temporary.write_text(
            json.dumps(
                self.metrics,
                indent=4,
                allow_nan=False
            ),
            encoding="utf-8"
        )

        temporary.replace(destination)

        print(
            "BENCHMARK_METRICS",
            json.dumps(self.metrics)
        )

        return dict(self.metrics)