
import logging
import math
import os
import time

from concurrent.futures import ThreadPoolExecutor

import requests

from prometheus_client import (
    CollectorRegistry,
    start_http_server,
)
from prometheus_client.core import GaugeMetricFamily


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

LOGGER = logging.getLogger("darksea-bigdata")

SPARK_MASTER_URL = os.getenv(
    "SPARK_MASTER_URL",
    "http://spark-master:8080/json/",
)

SPARK_WORKER_URL = os.getenv(
    "SPARK_WORKER_URL",
    "http://spark-worker:8081/json/",
)

HDFS_URL = os.getenv(
    "HDFS_URL",
    "http://namenode:9870/jmx",
)

PORT = int(os.getenv("EXPORTER_PORT", "9200"))

TIMEOUT = 3

MIB = 1024 ** 2


def fetch_json(url, params=None):
    response = requests.get(
        url,
        params=params,
        timeout=TIMEOUT,
    )

    response.raise_for_status()

    data = response.json()

    if not isinstance(data, dict):
        raise ValueError("Expected a JSON object")

    return data


def number(data, key):
    value = data[key]

    if isinstance(value, bool):
        raise ValueError(f"Invalid number: {key}")

    value = float(value)

    if not math.isfinite(value) or value < 0:
        raise ValueError(f"Invalid number: {key}")

    return value


def parse_spark_master(data):
    workers = data["workers"]
    active = data["activeapps"]
    completed = data["completedapps"]

    cores = number(data, "cores")
    cores_used = number(data, "coresused")

    memory = number(data, "memory")
    memory_used = number(data, "memoryused")

    if cores_used > cores or memory_used > memory:
        raise ValueError("Inconsistent Spark resources")

    return {
        "spark_master_alive":
            int(data["status"] == "ALIVE"),

        "spark_workers_total":
            len(workers),

        "spark_workers_alive":
            sum(w["state"] == "ALIVE" for w in workers),

        "spark_cores_total":
            cores,

        "spark_cores_used":
            cores_used,

        "spark_cores_available":
            cores - cores_used,

        "spark_memory_total_bytes":
            memory * MIB,

        "spark_memory_used_bytes":
            memory_used * MIB,

        "spark_memory_available_bytes":
            (memory - memory_used) * MIB,

        "spark_active_applications":
            len(active),

        "spark_completed_applications":
            len(completed),

        "spark_finished_applications":
            sum(a["state"] == "FINISHED" for a in completed),

        "spark_killed_applications":
            sum(a["state"] == "KILLED" for a in completed),

        "spark_failed_applications":
            sum(a["state"] == "FAILED" for a in completed),
    }


def parse_spark_worker(data):
    return {
        "spark_worker_cores_total":
            number(data, "cores"),

        "spark_worker_cores_used":
            number(data, "coresused"),

        "spark_worker_memory_total_bytes":
            number(data, "memory") * MIB,

        "spark_worker_memory_used_bytes":
            number(data, "memoryused") * MIB,

        "spark_worker_active_executors":
            len(data["executors"]),

        "spark_worker_finished_executors":
            len(data["finishedexecutors"]),
    }


HDFS_FIELDS = {
    "CapacityTotal": "capacity_total_bytes",
    "CapacityUsed": "capacity_used_bytes",
    "CapacityRemaining": "capacity_remaining_bytes",
    "CapacityUsedNonDFS": "non_dfs_used_bytes",
    "BlocksTotal": "blocks_total",
    "FilesTotal": "files_total",
    "UnderReplicatedBlocks": "under_replicated_blocks",
    "MissingBlocks": "missing_blocks",
    "CorruptBlocks": "corrupt_blocks",
    "NumLiveDataNodes": "live_datanodes",
    "NumDeadDataNodes": "dead_datanodes",
    "PendingReplicationBlocks": "pending_replication_blocks",
    "VolumeFailuresTotal": "volume_failures",
}


def parse_hdfs(data):
    beans = data["beans"]

    if len(beans) != 1:
        raise ValueError("Expected one FSNamesystem bean")

    bean = beans[0]

    if bean["name"] != (
        "Hadoop:service=NameNode,name=FSNamesystem"
    ):
        raise ValueError("Unexpected JMX bean")

    metrics = {
        f"hdfs_{metric}": number(bean, field)
        for field, metric in HDFS_FIELDS.items()
    }

    capacity = metrics["hdfs_capacity_total_bytes"]
    used = metrics["hdfs_capacity_used_bytes"]

    if capacity > 0:
        metrics["hdfs_capacity_used_percent"] = (
            100 * used / capacity
        )

    blocks = metrics["hdfs_blocks_total"]

    if blocks > 0:
        metrics["hdfs_under_replication_percent"] = (
            100 * metrics["hdfs_under_replicated_blocks"]
            / blocks
        )

    return metrics


def collect_spark_master():
    return parse_spark_master(
        fetch_json(SPARK_MASTER_URL)
    )


def collect_spark_worker():
    return parse_spark_worker(
        fetch_json(SPARK_WORKER_URL)
    )


def collect_hdfs():
    data = fetch_json(
        HDFS_URL,
        params={
            "qry": "Hadoop:service=NameNode,name=FSNamesystem"
        },
    )

    return parse_hdfs(data)


class BigDataCollector:

    def __init__(self):
        self.sources = {
            "spark_master": collect_spark_master,
            "spark_worker": collect_spark_worker,
            "hdfs": collect_hdfs,
        }

    @staticmethod
    def collect_source(source, function):
        started = time.monotonic()

        try:
            metrics = function()
            success = 1

        except Exception as exc:
            LOGGER.warning(
                "Collection failed for %s: %s",
                source,
                exc,
            )

            metrics = {}
            success = 0

        duration = time.monotonic() - started

        return source, success, duration, metrics

    def collect(self):
        availability = GaugeMetricFamily(
            "darksea_bigdata_source_up",
            "Successful collection from the source",
            labels=["source"],
        )

        durations = GaugeMetricFamily(
            "darksea_bigdata_collection_duration_seconds",
            "Collection duration by source",
            labels=["source"],
        )

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(
                    self.collect_source,
                    source,
                    function,
                )
                for source, function in self.sources.items()
            ]

            results = [
                future.result()
                for future in futures
            ]

        for source, success, duration, metrics in results:
            availability.add_metric([source], success)
            durations.add_metric([source], duration)

            for name, value in metrics.items():
                yield GaugeMetricFamily(
                    f"darksea_{name}",
                    name.replace("_", " "),
                    value=value,
                )

        yield availability
        yield durations


def main():
    registry = CollectorRegistry()

    registry.register(BigDataCollector())

    start_http_server(
        PORT,
        registry=registry,
    )

    LOGGER.info(
        "DarkSea Big Data Exporter started on port %s",
        PORT,
    )

    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
