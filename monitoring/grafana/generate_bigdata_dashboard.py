
import json
from pathlib import Path


OUTPUT = (
    Path(__file__).resolve().parent
    / "dashboards"
    / "darksea_bigdata.json"
)

DATASOURCE = {
    "type": "prometheus",
    "uid": "darksea-prometheus",
}

panels = []


def row(title, y):
    panels.append({
        "id": len(panels) + 1,
        "title": title,
        "type": "row",
        "collapsed": False,
        "gridPos": {
            "x": 0, "y": y, "w": 24, "h": 1,
        },
        "panels": [],
    })


def panel(
    title,
    expr,
    x,
    y,
    w=6,
    h=4,
    unit="short",
    kind="stat",
    warning=None,
    critical=None,
    legend="",
):
    steps = [
        {"color": "green", "value": None}
    ]

    if warning is not None:
        steps.append({
            "color": "orange",
            "value": warning,
        })

    if critical is not None:
        steps.append({
            "color": "red",
            "value": critical,
        })

    defaults = {
        "unit": unit,
        "thresholds": {
            "mode": "absolute",
            "steps": steps,
        },
        "color": {
            "mode": (
                "thresholds"
                if kind == "stat"
                else "palette-classic"
            )
        },
    }

    if kind == "stat":
        options = {
            "reduceOptions": {
                "calcs": ["lastNotNull"],
                "values": False,
            },
            "colorMode": "value",
            "graphMode": "none",
        }
    else:
        options = {
            "legend": {
                "displayMode": "table",
                "placement": "bottom",
                "calcs": ["lastNotNull", "max"],
            },
            "tooltip": {"mode": "multi"},
        }

        defaults["custom"] = {
            "drawStyle": "line",
            "lineWidth": 2,
            "fillOpacity": 10,
            "spanNulls": False,
        }

    panels.append({
        "id": len(panels) + 1,
        "title": title,
        "type": kind,
        "datasource": DATASOURCE,
        "gridPos": {
            "x": x, "y": y, "w": w, "h": h,
        },
        "targets": [{
            "refId": "A",
            "expr": expr,
            "legendFormat": legend,
            "datasource": DATASOURCE,
            "instant": kind == "stat",
            "range": kind != "stat",
        }],
        "fieldConfig": {
            "defaults": defaults,
            "overrides": [],
        },
        "options": options,
    })


# 01 - SPARK

row("01 - Spark Cluster", 0)

spark_stats = [
    ("Active workers", "spark_workers_alive", "short"),
    ("Available cores", "spark_cores_available", "short"),
    ("Available memory", "spark_memory_available_bytes", "bytes"),
    ("Active applications", "spark_active_applications", "short"),
    ("Finished applications", "spark_finished_applications", "short"),
    ("Killed applications", "spark_killed_applications", "short"),
    ("Active executors", "spark_worker_active_executors", "short"),
    ("Used executor memory", "spark_worker_memory_used_bytes", "bytes"),
]

for i, (title, metric, unit) in enumerate(spark_stats):
    panel(
        title,
        f"darksea_{metric}",
        (i % 4) * 6,
        1 + (i // 4) * 4,
        unit=unit,
    )

panel(
    "Spark cores used",
    "darksea_spark_cores_used",
    0, 9, 12, 8,
    kind="timeseries",
)

panel(
    "Spark allocated memory",
    "darksea_spark_memory_used_bytes",
    12, 9, 12, 8,
    unit="bytes",
    kind="timeseries",
)

panel(
    "Active Spark applications",
    "darksea_spark_active_applications",
    0, 17, 12, 8,
    kind="timeseries",
)

panel(
    "Active Spark executors",
    "darksea_spark_worker_active_executors",
    12, 17, 12, 8,
    kind="timeseries",
)


# 02 - HDFS

row("02 - Hadoop HDFS", 25)

hdfs_stats = [
    ("Live DataNodes", "live_datanodes", "short", None, None),
    ("Dead DataNodes", "dead_datanodes", "short", None, 1),
    ("HDFS used", "capacity_used_bytes", "bytes", None, None),
    ("HDFS remaining", "capacity_remaining_bytes", "bytes", None, None),
    ("Total blocks", "blocks_total", "short", None, None),
    ("Under-replicated blocks", "under_replicated_blocks", "short", 1, None),
    ("Missing blocks", "missing_blocks", "short", None, 1),
    ("Corrupt blocks", "corrupt_blocks", "short", None, 1),
    ("Namespace files and directories", "files_total", "short", None, None),
    ("HDFS capacity used", "capacity_used_percent", "percent", 75, 90),
    ("Under-replication rate", "under_replication_percent", "percent", 1, 100),
    ("Volume failures", "volume_failures", "short", None, 1),
]

for i, item in enumerate(hdfs_stats):
    title, metric, unit, warning, critical = item

    panel(
        title,
        f"darksea_hdfs_{metric}",
        (i % 4) * 6,
        26 + (i // 4) * 4,
        unit=unit,
        warning=warning,
        critical=critical,
    )

panel(
    "HDFS storage usage",
    "darksea_hdfs_capacity_used_bytes",
    0, 38, 12, 8,
    unit="bytes",
    kind="timeseries",
)

panel(
    "HDFS under-replicated blocks",
    "darksea_hdfs_under_replicated_blocks",
    12, 38, 12, 8,
    kind="timeseries",
)


# 03 - MONITORING HEALTH

row("03 - Big Data Monitoring Health", 46)

panel(
    "Source availability",
    "darksea_bigdata_source_up",
    0, 47, 12, 8,
    kind="timeseries",
    legend="{{source}}",
)

panel(
    "Collection duration",
    "darksea_bigdata_collection_duration_seconds",
    12, 47, 12, 8,
    unit="s",
    kind="timeseries",
    legend="{{source}}",
)


dashboard = {
    "id": None,
    "uid": "darksea-bigdata",
    "title": "DarkSea-AI - Big Data Monitoring",
    "description": "Internal Spark and Hadoop HDFS metrics",
    "tags": ["DarkSea-AI", "Spark", "Hadoop", "Big Data"],
    "timezone": "browser",
    "schemaVersion": 41,
    "version": 1,
    "refresh": "15s",
    "editable": True,
    "time": {
        "from": "now-15m",
        "to": "now",
    },
    "panels": panels,
}

OUTPUT.parent.mkdir(
    parents=True,
    exist_ok=True,
)

OUTPUT.write_text(
    json.dumps(
        dashboard,
        indent=2,
        ensure_ascii=False,
    ),
    encoding="utf-8",
)

print("Dashboard generated successfully")
print("Destination:", OUTPUT)
print("Visualization panels:", sum(
    p["type"] != "row" for p in panels
))
print("Sections:", sum(
    p["type"] == "row" for p in panels
))
