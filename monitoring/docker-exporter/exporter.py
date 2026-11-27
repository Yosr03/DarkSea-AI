
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

import docker
from prometheus_client import start_http_server, REGISTRY
from prometheus_client.core import (
    GaugeMetricFamily,
    CounterMetricFamily,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

LOGGER = logging.getLogger("darksea-exporter")

COLLECTION_INTERVAL = 5
PREFIX = "darksea-"

def extract_metrics(stats):
    cpu = stats.get("cpu_stats", {})
    previous = stats.get("precpu_stats", {})

    cpu_total = cpu.get(
        "cpu_usage", {}
    ).get("total_usage", 0)

    previous_total = previous.get(
        "cpu_usage", {}
    ).get("total_usage", 0)

    cpu_delta = cpu_total - previous_total

    system_delta = (
        cpu.get("system_cpu_usage", 0)
        - previous.get("system_cpu_usage", 0)
    )

    cores = cpu.get("online_cpus") or len(
        cpu.get("cpu_usage", {}).get(
            "percpu_usage", []
        )
    )

    cpu_percent = None

    if (
        previous.get("system_cpu_usage", 0) > 0
        and system_delta > 0
        and cpu_delta >= 0
        and cores > 0
    ):
        cpu_percent = (
            cpu_delta / system_delta * cores * 100
        )

    memory = stats.get("memory_stats", {})
    usage = memory.get("usage", 0)
    limit = memory.get("limit", 0)

    memory_details = memory.get("stats", {})

    cache = memory_details.get(
        "total_inactive_file",
        memory_details.get("inactive_file", 0),
    )

    working_set = (
        usage - cache if 0 <= cache < usage
        else usage
    )

    networks = stats.get("networks", {})

    rx_bytes = sum(
        interface.get("rx_bytes", 0)
        for interface in networks.values()
    )

    tx_bytes = sum(
        interface.get("tx_bytes", 0)
        for interface in networks.values()
    )

    disk_entries = stats.get(
        "blkio_stats", {}
    ).get("io_service_bytes_recursive") or []

    read_bytes = sum(
        entry.get("value", 0)
        for entry in disk_entries
        if entry.get("op", "").lower() == "read"
    )

    write_bytes = sum(
        entry.get("value", 0)
        for entry in disk_entries
        if entry.get("op", "").lower() == "write"
    )

    return {
        "cpu_percent": cpu_percent,
        "cpu_seconds": cpu_total / 1e9,
        "memory_bytes": working_set,
        "memory_limit_bytes": limit,
        "memory_percent": (
            working_set / limit * 100
            if limit > 0 else None
        ),
        "network_rx_bytes": rx_bytes,
        "network_tx_bytes": tx_bytes,
        "disk_read_bytes": read_bytes,
        "disk_write_bytes": write_bytes,
        "pids": stats.get(
            "pids_stats", {}
        ).get("current", 0),
    }

class DockerCollector:

    def __init__(self):
        self.client = docker.from_env(timeout=10)
        self.lock = Lock()
        self.snapshot = []
        self.success = 0
        self.last_success = 0
        self.duration = 0

    def inspect_container(self, container):
        row = {
            "name": container.name,
            "id": container.id,
            "running": int(
                container.status == "running"
            ),
            "stats_available": 0,
            "metrics": None,
        }

        if not row["running"]:
            return row

        try:
            stats = container.stats(stream=False)
            row["metrics"] = extract_metrics(stats)
            row["stats_available"] = 1

        except Exception as exc:
            LOGGER.warning(
                "Stats unavailable for %s: %s",
                container.name,
                exc,
            )

        return row

    def refresh(self):
        start = time.perf_counter()

        try:
            containers = [
                container
                for container in
                self.client.containers.list(all=True)
                if container.name.startswith(PREFIX)
            ]

            with ThreadPoolExecutor(
                max_workers=4
            ) as executor:
                rows = list(
                    executor.map(
                        self.inspect_container,
                        containers,
                    )
                )

            success = int(all(
                not row["running"]
                or row["stats_available"]
                for row in rows
            ))

            with self.lock:
                self.snapshot = rows
                self.success = success
                self.duration = (
                    time.perf_counter() - start
                )

                if success:
                    self.last_success = time.time()

            LOGGER.info(
                "Containers=%s | Collection=%.3fs",
                len(rows),
                self.duration,
            )

        except Exception:
            LOGGER.exception("Docker collection failed")

            with self.lock:
                self.snapshot = []
                self.success = 0
                self.duration = (
                    time.perf_counter() - start
                )

    def collect(self):
        with self.lock:
            rows = list(self.snapshot)
            success = self.success
            duration = self.duration
            last_success = self.last_success

        yield GaugeMetricFamily(
            "darksea_docker_collection_success",
            "Docker collection success",
            value=success,
        )

        yield GaugeMetricFamily(
            "darksea_docker_collection_duration_seconds",
            "Docker collection duration",
            value=duration,
        )

        yield GaugeMetricFamily(
            "darksea_docker_last_success_timestamp_seconds",
            "Last successful collection",
            value=last_success,
        )

        definitions = [
            ("container_running", "Container running",
             "running", False),
            ("container_stats_available", "Stats available",
             "stats_available", False),
            ("container_cpu_percent", "CPU usage percent",
             "cpu_percent", False),
            ("container_cpu_usage_seconds_total", "CPU seconds",
             "cpu_seconds", True),
            ("container_memory_bytes", "Memory working set",
             "memory_bytes", False),
            ("container_memory_limit_bytes", "Memory limit",
             "memory_limit_bytes", False),
            ("container_memory_percent", "Memory percent",
             "memory_percent", False),
            ("container_network_receive_bytes_total", "Received bytes",
             "network_rx_bytes", True),
            ("container_network_transmit_bytes_total", "Sent bytes",
             "network_tx_bytes", True),
            ("container_disk_read_bytes_total", "Disk read bytes",
             "disk_read_bytes", True),
            ("container_disk_write_bytes_total", "Disk written bytes",
             "disk_write_bytes", True),
            ("container_pids", "Container processes",
             "pids", False),
        ]

        for name, description, key, counter in definitions:

            metric_type = (
                CounterMetricFamily
                if counter else GaugeMetricFamily
            )

            metric = metric_type(
                f"darksea_{name}",
                description,
                labels=["container", "container_id"],
            )

            for row in rows:
                if key in ("running", "stats_available"):
                    value = row[key]
                elif row["metrics"] is not None:
                    value = row["metrics"].get(key)
                else:
                    value = None

                if value is not None:
                    metric.add_metric(
                        [row["name"], row["id"]],
                        value,
                    )

            yield metric

def main():
    collector = DockerCollector()

    REGISTRY.register(collector)

    start_http_server(9100)

    LOGGER.info("DarkSea Docker Exporter started")

    while True:
        start = time.monotonic()

        collector.refresh()

        elapsed = time.monotonic() - start

        time.sleep(
            max(0, COLLECTION_INTERVAL - elapsed)
        )

if __name__ == "__main__":
    main()