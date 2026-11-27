
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "dashboards"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DATASOURCE = {
    "type": "prometheus",
    "uid": "darksea-prometheus",
}

panels = []


def add_panel(
    title,
    expression,
    x,
    y,
    w=12,
    h=8,
    unit="short",
    panel_type="timeseries",
    legend="{{container}}",
    description="",
    thresholds=None,
):
    panel = {
        "id": len(panels) + 1,
        "title": title,
        "description": description,
        "type": panel_type,
        "datasource": DATASOURCE,
        "gridPos": {
            "x": x,
            "y": y,
            "w": w,
            "h": h,
        },
        "targets": [
            {
                "refId": "A",
                "expr": expression,
                "legendFormat": legend,
                "datasource": DATASOURCE,
                "range": panel_type != "stat",
                "instant": panel_type == "stat",
            }
        ],
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "color": {
                    "mode": "palette-classic"
                },
            },
            "overrides": [],
        },
        "options": {},
    }

    if panel_type == "stat":
        panel["fieldConfig"]["defaults"]["color"] = {
            "mode": "thresholds"
        }

        panel["fieldConfig"]["defaults"]["thresholds"] = {
            "mode": "absolute",
            "steps": thresholds or [
                {"color": "green", "value": None}
            ],
        }

        panel["options"] = {
            "reduceOptions": {
                "calcs": ["lastNotNull"],
                "fields": "",
                "values": False,
            },
            "colorMode": "value",
            "graphMode": "none",
            "justifyMode": "auto",
        }

    else:
        panel["fieldConfig"]["defaults"]["custom"] = {
            "drawStyle": "line",
            "lineWidth": 2,
            "fillOpacity": 10,
            "showPoints": "never",
            "spanNulls": False,
        }

        panel["options"] = {
            "legend": {
                "displayMode": "table",
                "placement": "bottom",
                "calcs": ["lastNotNull", "max"],
            },
            "tooltip": {
                "mode": "multi",
                "sort": "desc",
            },
        }

    panels.append(panel)


def add_row(title, y):
    panels.append({
        "id": len(panels) + 1,
        "title": title,
        "type": "row",
        "collapsed": False,
        "gridPos": {
            "x": 0,
            "y": y,
            "w": 24,
            "h": 1,
        },
        "panels": [],
    })


# ==========================================
# 1. INFRASTRUCTURE OVERVIEW
# ==========================================

add_row("01 - Infrastructure Overview", 0)

add_panel(
    "Active containers",
    "sum(darksea_container_running)",
    0, 1, 6, 4,
    panel_type="stat",
)

add_panel(
    "Containers with metrics",
    "sum(darksea_container_stats_available)",
    6, 1, 6, 4,
    panel_type="stat",
)

add_panel(
    "Total memory usage",
    "sum(darksea_container_memory_bytes)",
    12, 1, 6, 4,
    unit="bytes",
    panel_type="stat",
)

add_panel(
    "Total CPU usage",
    "sum(darksea_container_cpu_percent)",
    18, 1, 6, 4,
    unit="percent",
    panel_type="stat",
    description="100% represents one logical CPU core.",
)

add_panel(
    "Collection success",
    "darksea_docker_collection_success * 100",
    0, 5, 6, 4,
    unit="percent",
    panel_type="stat",
    thresholds=[
        {"color": "red", "value": None},
        {"color": "green", "value": 100},
    ],
)

add_panel(
    "Kafka brokers",
    "kafka_brokers",
    6, 5, 6, 4,
    panel_type="stat",
)

add_panel(
    "Collection duration",
    "darksea_docker_collection_duration_seconds",
    12, 5, 6, 4,
    unit="s",
    panel_type="stat",
)

add_panel(
    "Data freshness",
    "time() - (darksea_docker_last_success_timestamp_seconds > 0)",
    18, 5, 6, 4,
    unit="s",
    panel_type="stat",
    thresholds=[
        {"color": "green", "value": None},
        {"color": "orange", "value": 15},
        {"color": "red", "value": 30},
    ],
)


# ==========================================
# 2. CONTAINER RESOURCES
# ==========================================

add_row("02 - Container Resources", 9)

add_panel(
    "CPU usage by container",
    "darksea_container_cpu_percent",
    0, 10,
    unit="percent",
)

add_panel(
    "Memory usage by container",
    "darksea_container_memory_bytes",
    12, 10,
    unit="bytes",
)

add_panel(
    "Memory utilization by container",
    "darksea_container_memory_percent",
    0, 18,
    unit="percent",
)

add_panel(
    "Container PIDs",
    "darksea_container_pids",
    12, 18,
)


# ==========================================
# 3. NETWORK AND STORAGE
# ==========================================

add_row("03 - Network and Storage", 26)

add_panel(
    "Network receive throughput",
    "rate(darksea_container_network_receive_bytes_total[1m])",
    0, 27,
    unit="Bps",
)

add_panel(
    "Network transmit throughput",
    "rate(darksea_container_network_transmit_bytes_total[1m])",
    12, 27,
    unit="Bps",
)

add_panel(
    "Disk read throughput",
    "rate(darksea_container_disk_read_bytes_total[1m])",
    0, 35,
    unit="Bps",
)

add_panel(
    "Disk write throughput",
    "rate(darksea_container_disk_write_bytes_total[1m])",
    12, 35,
    unit="Bps",
)


# ==========================================
# 4. KAFKA MONITORING
# ==========================================

add_row("04 - Kafka Monitoring", 43)

add_panel(
    "Partitions by topic",
    'kafka_topic_partitions{topic=~"ais-.*"}',
    0, 44,
    legend="{{topic}}",
)

add_panel(
    "Kafka topic offsets",
    'sum by (topic) (kafka_topic_partition_current_offset{topic=~"ais-.*"})',
    12, 44,
    legend="{{topic}}",
)

add_panel(
    "Kafka message throughput",
    'sum by (topic) (clamp_min(delta(kafka_topic_partition_current_offset{topic=~"ais-.*"}[1m]), 0)) / 60',
    0, 52,
    unit="ops",
    legend="{{topic}}",
    description="Estimated messages per second from offset changes.",
)

add_panel(
    "Kafka consumer group lag",
    'sum by (consumergroup, topic) (kafka_consumergroup_lag{topic=~"ais-.*"})',
    12, 52,
    legend="{{consumergroup}} - {{topic}}",
    description="Available only for consumer groups exposing committed offsets.",
)


# ==========================================
# 5. MONITORING HEALTH
# ==========================================

add_row("05 - Monitoring Health", 60)

add_panel(
    "Prometheus target availability",
    'up{instance=~"docker-exporter:9100|kafka-exporter:9308|localhost:9090|bigdata-exporter:9200"}',
    0, 61,
    legend="{{job}}",
)

add_panel(
    "Exporter collection duration",
    "darksea_docker_collection_duration_seconds",
    12, 61,
    unit="s",
    legend="Collection duration",
)

add_panel(
    "Metrics freshness",
    "time() - (darksea_docker_last_success_timestamp_seconds > 0)",
    0, 69,
    unit="s",
    legend="Data age",
)

add_panel(
    "Available container statistics",
    "sum(darksea_container_stats_available)",
    12, 69,
    legend="Containers",
)


# ==========================================
# DASHBOARD CONFIGURATION
# ==========================================

dashboard = {
    "id": None,
    "uid": "darksea-infrastructure",
    "title": "DarkSea-AI - Infrastructure Monitoring",
    "description": (
        "Technical monitoring of the DarkSea-AI "
        "Big Data and streaming infrastructure."
    ),
    "tags": [
        "DarkSea-AI",
        "Big Data",
        "Docker",
        "Spark",
        "Kafka",
        "Hadoop",
    ],
    "timezone": "browser",
    "schemaVersion": 41,
    "version": 1,
    "refresh": "5s",
    "editable": True,
    "time": {
        "from": "now-15m",
        "to": "now",
    },
    "templating": {
        "list": [
            {
                "name": "DS_PROMETHEUS",
                "label": "Prometheus",
                "type": "datasource",
                "query": "prometheus",
                "refresh": 1,
                "hide": 0,
            }
        ]
    },
    "panels": panels,
}

output = OUTPUT_DIR / "darksea_infrastructure.json"

with output.open("w", encoding="utf-8") as file:
    json.dump(
        dashboard,
        file,
        indent=2,
        ensure_ascii=False,
    )

print("Dashboard generated successfully")
print("Destination:", output)
print("Visualization panels:", sum(
    p["type"] != "row" for p in panels
))
print("Sections:", sum(
    p["type"] == "row" for p in panels
))

