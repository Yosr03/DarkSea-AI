
import pytest

from exporter import extract_metrics

@pytest.fixture
def sample_stats():
    return {
        "cpu_stats": {
            "cpu_usage": {
                "total_usage": 2000000000
            },
            "system_cpu_usage": 10000000000,
            "online_cpus": 4,
        },
        "precpu_stats": {
            "cpu_usage": {
                "total_usage": 1900000000
            },
            "system_cpu_usage": 9000000000,
        },
        "memory_stats": {
            "usage": 500000000,
            "limit": 1000000000,
            "stats": {
                "inactive_file": 100000000
            },
        },
        "networks": {
            "eth0": {
                "rx_bytes": 1000,
                "tx_bytes": 2000,
            },
            "eth1": {
                "rx_bytes": 500,
                "tx_bytes": 700,
            },
        },
        "blkio_stats": {
            "io_service_bytes_recursive": [
                {"op": "Read", "value": 3000},
                {"op": "Write", "value": 4000},
            ]
        },
        "pids_stats": {"current": 12},
    }

def test_cpu_percent(sample_stats):
    result = extract_metrics(sample_stats)
    assert result["cpu_percent"] == pytest.approx(40)

def test_cpu_seconds(sample_stats):
    assert extract_metrics(
        sample_stats
    )["cpu_seconds"] == 2

def test_memory_working_set(sample_stats):
    assert extract_metrics(
        sample_stats
    )["memory_bytes"] == 400000000

def test_memory_percent(sample_stats):
    assert extract_metrics(
        sample_stats
    )["memory_percent"] == 40

def test_memory_limit(sample_stats):
    assert extract_metrics(
        sample_stats
    )["memory_limit_bytes"] == 1000000000

def test_network_receive(sample_stats):
    assert extract_metrics(
        sample_stats
    )["network_rx_bytes"] == 1500

def test_network_transmit(sample_stats):
    assert extract_metrics(
        sample_stats
    )["network_tx_bytes"] == 2700

def test_disk_read(sample_stats):
    assert extract_metrics(
        sample_stats
    )["disk_read_bytes"] == 3000

def test_disk_write(sample_stats):
    assert extract_metrics(
        sample_stats
    )["disk_write_bytes"] == 4000

def test_pids(sample_stats):
    assert extract_metrics(
        sample_stats
    )["pids"] == 12

def test_empty_stats():
    result = extract_metrics({})
    assert result["cpu_percent"] is None
    assert result["memory_percent"] is None

def test_invalid_cpu_delta(sample_stats):
    sample_stats["cpu_stats"][
        "system_cpu_usage"
    ] = 0

    assert extract_metrics(
        sample_stats
    )["cpu_percent"] is None

def test_cgroup_v1_memory(sample_stats):
    sample_stats["memory_stats"]["stats"] = {
        "total_inactive_file": 200000000
    }

    assert extract_metrics(
        sample_stats
    )["memory_bytes"] == 300000000

def test_null_disk_stats(sample_stats):
    sample_stats["blkio_stats"][
        "io_service_bytes_recursive"
    ] = None

    result = extract_metrics(sample_stats)

    assert result["disk_read_bytes"] == 0
    assert result["disk_write_bytes"] == 0