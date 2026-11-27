from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent

source = BASE_DIR / "consumer_v1.py"
destination = BASE_DIR / "consumer_v1_instrumented.py"

content = source.read_text(encoding="utf-8")


def replace_once(old, new):

    global content

    count = content.count(old)

    if count != 1:
        raise RuntimeError(
            f"Expected one occurrence, found {count}: {old}"
        )

    content = content.replace(old, new, 1)


replace_once(
    "import argparse",
    "import argparse\nfrom batch_metrics import BatchMetrics"
)

replace_once(
    "    if batch_df.isEmpty():\n        return",
    """    timer = BatchMetrics(args.run_name, batch_id)

    with timer.measure("empty_check"):
        empty = batch_df.isEmpty()

    if empty:
        return"""
)

replace_once(
    "        total = checked.count()",
    """        with timer.measure("validation"):
            total = checked.count()"""
)

replace_once(
    "        invalid_count = invalid.count()",
    """        with timer.measure("invalid_count"):
            invalid_count = invalid.count()"""
)

replace_once(
    "        valid_count = total - invalid_count",
    """        valid_count = total - invalid_count

        timer.set(
            input_rows=total,
            valid_rows=valid_count,
            invalid_rows=invalid_count
        )"""
)

replace_once(
    "        scored_count = scored.count()",
    """        with timer.measure("scoring"):
            scored_count = scored.count()"""
)

replace_once(
    "        alert_count = alerts.count()",
    """        with timer.measure("alert_count"):
            alert_count = alerts.count()"""
)

replace_once(
    """        ml_count = scored.filter(
            F.col("anomalie_potentielle")
        ).count()""",
    """        with timer.measure("ml_count"):
            ml_count = scored.filter(
                F.col("anomalie_potentielle")
            ).count()

        timer.set(
            scored_rows=scored_count,
            alert_count=alert_count,
            ml_count=ml_count
        )"""
)

replace_once(
    """        scored.write.mode("overwrite").parquet(
            f"{RUN_BASE}/results/"
            f"batch_{batch_id:08d}"
        )""",
    """        with timer.measure("hdfs_results"):
            scored.write.mode("overwrite").parquet(
                f"{RUN_BASE}/results/"
                f"batch_{batch_id:08d}"
            )"""
)

replace_once(
    """            alerts.write.mode("overwrite").parquet(
                f"{RUN_BASE}/alerts/"
                f"batch_{batch_id:08d}"
            )""",
    """            with timer.measure("hdfs_alerts"):
                alerts.write.mode("overwrite").parquet(
                    f"{RUN_BASE}/alerts/"
                    f"batch_{batch_id:08d}"
                )"""
)

replace_once(
    """            publish_kafka(
                alerts,
                "ais-benchmark-alerts",
                "alert_id"
            )""",
    """            with timer.measure("kafka_publish"):
                publish_kafka(
                    alerts,
                    "ais-benchmark-alerts",
                    "alert_id"
                )"""
)

replace_once(
    "        if valid_count == 0:\n            return",
    """        if valid_count == 0:
            timer.set(
                scored_rows=0,
                alert_count=0,
                ml_count=0
            )
            return"""
)

replace_once(
    "    finally:\n\n        if scored is not None:",
    """    except Exception:
        timer.set(status="failed")
        raise

    finally:

        if scored is not None:"""
)

replace_once(
    "        checked.unpersist()",
    """        checked.unpersist()

        timer.finish(
            status=(
                "failed"
                if timer.metrics["status"] == "failed"
                else "success"
            )
        )"""
)

replace_once(
    "from batch_metrics import BatchMetrics",
    """from batch_metrics import BatchMetrics
import batch_metrics
from pathlib import Path

batch_metrics.RESULTS_DIR = Path(
    "/workspace/spark/benchmark/streaming/results"
)"""
)

replace_once(
    """        scored.groupBy(
            "type_alerte"
        ).count().show(truncate=False)""",
    """        with timer.measure("type_statistics"):
            scored.groupBy(
                "type_alerte"
            ).count().show(truncate=False)"""
)

replace_once(
    """        scored.agg(
            F.avg("latence_ms").alias(
                "latence_moyenne_ms"
            ),
            F.expr(
                "percentile_approx(latence_ms, 0.95)"
            ).alias("latence_p95_ms"),
            F.max("latence_ms").alias(
                "latence_max_ms"
            )
        ).show(truncate=False)""",
    """        with timer.measure("latency_statistics"):
            scored.agg(
                F.avg("latence_ms").alias(
                    "latence_moyenne_ms"
                ),
                F.expr(
                    "percentile_approx(latence_ms, 0.95)"
                ).alias("latence_p95_ms"),
                F.max("latence_ms").alias(
                    "latence_max_ms"
                )
            ).show(truncate=False)"""
)

replace_once(
    "\nquery.awaitTermination()\n",
    """
import time

if not args.once:

    readiness_deadline = time.monotonic() + 180

    jvm = spark._jvm

    hadoop_conf = spark._jsc.hadoopConfiguration()

    source_path = jvm.org.apache.hadoop.fs.Path(
        CHECKPOINT + "/sources/0/0"
    )

    filesystem = source_path.getFileSystem(
        hadoop_conf
    )

    while query.isActive:

        offsets_initialized = filesystem.exists(
            source_path
        )

        if (
            offsets_initialized
            and query.lastProgress is not None
        ):
            break

        if time.monotonic() >= readiness_deadline:

            query.stop()

            raise TimeoutError(
                "Spark initialization timeout"
            )

        time.sleep(1)

    if not query.isActive:

        raise RuntimeError(
            "Streaming query stopped before readiness"
        )

    print(
        "SPARK BENCHMARK READY",
        flush=True
    )

    print(
        "INITIAL PROGRESS:",
        query.lastProgress,
        flush=True
    )

query.awaitTermination()
"""
)

compile(content, str(destination), "exec")

destination.write_text(
    content,
    encoding="utf-8"
)

print("INSTRUMENTATION V1 REUSSIE")
print("Fichier genere :", destination)