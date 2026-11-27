
import pytest

from exporter import (
    BigDataCollector,
    HDFS_FIELDS,
    number,
    parse_hdfs,
    parse_spark_master,
    parse_spark_worker,
)


@pytest.fixture
def master():
    return {
        "status": "ALIVE",
        "cores": 2,
        "coresused": 0,
        "memory": 2048,
        "memoryused": 0,
        "workers": [{"state": "ALIVE"}],
        "activeapps": [],
        "completedapps": (
            [{"state": "FINISHED"}] * 5
            + [{"state": "KILLED"}] * 3
        ),
    }


@pytest.fixture
def hdfs():
    bean = {
        key: 0
        for key in HDFS_FIELDS
    }

    bean.update({
        "name": "Hadoop:service=NameNode,name=FSNamesystem",
        "CapacityTotal": 1081101176832,
        "CapacityUsed": 421527552,
        "CapacityRemaining": 985612156928,
        "CapacityUsedNonDFS": 40075137024,
        "BlocksTotal": 352,
        "FilesTotal": 621,
        "UnderReplicatedBlocks": 322,
        "NumLiveDataNodes": 1,
    })

    return {"beans": [bean]}


def test_master_alive(master):
    assert parse_spark_master(master)["spark_master_alive"] == 1


def test_worker_count(master):
    assert parse_spark_master(master)["spark_workers_alive"] == 1


def test_available_cores(master):
    assert parse_spark_master(master)["spark_cores_available"] == 2


def test_memory_conversion(master):
    assert (
        parse_spark_master(master)["spark_memory_total_bytes"]
        == 2147483648
    )


def test_application_states(master):
    metrics = parse_spark_master(master)

    assert metrics["spark_completed_applications"] == 8
    assert metrics["spark_finished_applications"] == 5
    assert metrics["spark_killed_applications"] == 3


def test_invalid_spark_resources(master):
    master["coresused"] = 3

    with pytest.raises(ValueError):
        parse_spark_master(master)


def test_worker_metrics():
    data = {
        "cores": 2,
        "coresused": 1,
        "memory": 2048,
        "memoryused": 1024,
        "executors": [{}],
        "finishedexecutors": [],
    }

    metrics = parse_spark_worker(data)

    assert metrics["spark_worker_active_executors"] == 1
    assert metrics["spark_worker_memory_used_bytes"] == 1073741824


def test_hdfs_capacity(hdfs):
    assert (
        parse_hdfs(hdfs)["hdfs_capacity_used_bytes"]
        == 421527552
    )


def test_hdfs_blocks(hdfs):
    metrics = parse_hdfs(hdfs)

    assert metrics["hdfs_blocks_total"] == 352
    assert metrics["hdfs_missing_blocks"] == 0
    assert metrics["hdfs_corrupt_blocks"] == 0


def test_under_replication(hdfs):
    assert parse_hdfs(hdfs)[
        "hdfs_under_replication_percent"
    ] == pytest.approx(322 / 352 * 100)


def test_hdfs_datanodes(hdfs):
    assert parse_hdfs(hdfs)["hdfs_live_datanodes"] == 1


def test_missing_jmx_bean():
    with pytest.raises(ValueError):
        parse_hdfs({"beans": []})


def test_missing_metric(hdfs):
    del hdfs["beans"][0]["CapacityUsed"]

    with pytest.raises(KeyError):
        parse_hdfs(hdfs)


@pytest.mark.parametrize(
    "value",
    [-1, float("nan"), float("inf"), True],
)
def test_invalid_numbers(value):
    with pytest.raises(ValueError):
        number({"metric": value}, "metric")


def test_source_failure():
    def failing_source():
        raise ConnectionError("Unavailable")

    source, success, duration, metrics = (
        BigDataCollector.collect_source(
            "hdfs",
            failing_source,
        )
    )

    assert source == "hdfs"
    assert success == 0
    assert duration >= 0
    assert metrics == {}


def test_partial_collection():
    collector = BigDataCollector()

    def unavailable():
        raise ConnectionError("Unavailable")

    collector.sources = {
        "spark_master": lambda: {"spark_cores_total": 2},
        "hdfs": unavailable,
    }

    metrics = list(collector.collect())

    availability = next(
        metric for metric in metrics
        if metric.name == "darksea_bigdata_source_up"
    )

    values = {
        sample.labels["source"]: sample.value
        for sample in availability.samples
    }

    assert values["spark_master"] == 1
    assert values["hdfs"] == 0

    assert any(
        metric.name == "darksea_spark_cores_total"
        for metric in metrics
    )
