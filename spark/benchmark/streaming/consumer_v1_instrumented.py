
import argparse
from batch_metrics import BatchMetrics
import batch_metrics
from pathlib import Path

batch_metrics.RESULTS_DIR = Path(
    "/workspace/spark/benchmark/streaming/results"
)
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

from event_features import (
    validate_events,
    prepare_event_features
)


# ==========================================
# 1. PARAMETRES
# ==========================================

parser = argparse.ArgumentParser()

parser.add_argument(
    "--once",
    action="store_true"
)

parser.add_argument("--run-name", required=True)
parser.add_argument("--trigger", type=int, default=5)
parser.add_argument("--max-offsets", type=int, default=1000)
parser.add_argument("--shuffle-partitions", type=int, default=4)

args = parser.parse_args()

if not args.run_name.replace("-", "").replace("_", "").isalnum():
    parser.error("Invalid run name")

if min(args.trigger, args.max_offsets, args.shuffle_partitions) <= 0:
    parser.error("Positive parameters required")

BASE = "hdfs://namenode:9000/darksea"

KAFKA_SERVER = "kafka:29092"

RUN_BASE = f"{BASE}/benchmark/streaming/{args.run_name}"

PHYSICAL_THRESHOLD = 80.0

CHECKPOINT = (
    f"{RUN_BASE}/checkpoint"
)


# ==========================================
# 2. INITIALISATION SPARK
# ==========================================

spark = (
    SparkSession.builder
    .appName(f"DarkSea-Benchmark-{args.run_name}")
    .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ==========================================
# 3. CHARGEMENT DU MODELE
# ==========================================

model = PipelineModel.load(
    f"{BASE}/models/kmeans_events"
)

metrics_text = (
    spark.sparkContext
    .wholeTextFiles(
        f"{BASE}/models/config/event_kmeans_metrics.json"
    )
    .first()[1]
)

metrics = json.loads(metrics_text)

threshold = float(metrics["anomaly_threshold"])

centers = model.stages[-1].clusterCenters()

assert len(centers) == metrics["best_k"]

centers_array = F.array(*[
    F.array(*[
        F.lit(float(value))
        for value in center
    ])
    for center in centers
])

print("\n=== MODELE EVENEMENTIEL CHARGE ===")
print("Clusters :", len(centers))
print("Seuil ML :", threshold)
print("Seuil physique :", PHYSICAL_THRESHOLD)


# ==========================================
# 4. SCHEMA DES MESSAGES JSON
# ==========================================

schema = """
    gap_id STRING,
    mmsi STRING,
    vessel_class STRING,
    pavillon STRING,
    gap_start_timestamp STRING,
    gap_end_timestamp STRING,
    mois STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    latitude_fin DOUBLE,
    longitude_fin DOUBLE,
    distance_cote_km DOUBLE,
    gap_hours DOUBLE,
    zone_distance STRING,
    cellule_lat INT,
    cellule_lon INT,
    duree_extreme INT,
    mmsi_format_atypique INT,
    replay_id STRING,
    ingestion_timestamp STRING,
    _corrupt_record STRING
"""


# ==========================================
# 5. CONNEXION KAFKA
# ==========================================

raw_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_SERVER
    )
    .option("subscribe", "ais-benchmark-events")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "true")
    .option("maxOffsetsPerTrigger", str(args.max_offsets))
    .load()
)

events = raw_stream.select(
    F.col("value").cast("string").alias("raw_json"),
    F.col("partition").alias("kafka_partition"),
    F.col("offset").alias("kafka_offset"),
    F.col("timestamp").alias("kafka_timestamp")
)

events = events.withColumn(
    "data",
    F.from_json(
        "raw_json",
        schema,
        {
            "mode": "PERMISSIVE",
            "columnNameOfCorruptRecord": "_corrupt_record"
        }
    )
)

events = events.select(
    "data.*",
    "raw_json",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp"
)


# ==========================================
# 6. FONCTION DE SCORING
# ==========================================

def add_anomaly_score(df):

    df = df.withColumn(
        "feature_array",
        vector_to_array("features")
    )

    df = df.withColumn(
        "center_array",
        F.element_at(
            centers_array,
            F.col("prediction") + F.lit(1)
        )
    )

    squared = F.zip_with(
        F.col("feature_array"),
        F.col("center_array"),
        lambda x, y: (x - y) * (x - y)
    )

    return df.withColumn(
        "distance_centroide",
        F.sqrt(
            F.aggregate(
                squared,
                F.lit(0.0),
                lambda acc, x: acc + x
            )
        )
    )


# ==========================================
# 7. PUBLICATION KAFKA
# ==========================================

def publish_kafka(df, topic, key_column):

    payload = df.select(
        F.col(key_column)
        .cast("string")
        .alias("key"),

        F.to_json(
            F.struct(*[
                F.col(c) for c in df.columns
            ])
        ).alias("value")
    )

    (
        payload.write
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_SERVER
        )
        .option("topic", topic)
        .save()
    )


# ==========================================
# 8. TRAITEMENT D'UN MICRO-BATCH
# ==========================================

def process_batch(batch_df, batch_id):

    timer = BatchMetrics(args.run_name, batch_id)

    with timer.measure("empty_check"):
        empty = batch_df.isEmpty()

    if empty:
        return

    checked = validate_events(batch_df)

    checked = (
        checked
        .withColumn(
            "start_ts",
            F.to_timestamp(
                "gap_start_timestamp",
                "yyyy-MM-dd HH:mm:ssXXX"
            )
        )
        .withColumn(
            "end_ts",
            F.to_timestamp(
                "gap_end_timestamp",
                "yyyy-MM-dd HH:mm:ssXXX"
            )
        )
        .withColumn(
            "ingestion_ts",
            F.to_timestamp("ingestion_timestamp")
        )
    )

    checked = checked.withColumn(
        "ecart_duree_h",
        F.abs(
            F.col("gap_hours")
            - (
                F.col("end_ts").cast("long")
                - F.col("start_ts").cast("long")
            ) / 3600.0
        )
    )

    checked = checked.withColumn(
        "event_valid",
        F.coalesce(
            F.col("event_valid")
            & F.col("_corrupt_record").isNull()
            & F.col("start_ts").isNotNull()
            & F.col("end_ts").isNotNull()
            & (F.col("end_ts") > F.col("start_ts"))
            & F.col("ingestion_ts").isNotNull()
            & F.col("replay_id").isNotNull()
            & (F.length(F.trim("replay_id")) > 0)
            & (F.col("ecart_duree_h") <= 1.0 / 60.0),
            F.lit(False)
        )
    )

    checked = checked.withColumn(
        "event_key",
        F.concat_ws(
            ":",
            F.col("kafka_partition"),
            F.col("kafka_offset")
        )
    ).cache()

    invalid = None
    scored = None

    try:

        with timer.measure("validation"):
            total = checked.count()

        invalid = checked.filter(
            ~F.col("event_valid")
        ).withColumn(
            "motif",
            F.lit("INVALID_JSON_OR_EVENT_FIELDS")
        ).cache()

        with timer.measure("invalid_count"):
            invalid_count = invalid.count()

        valid = checked.filter(
            F.col("event_valid")
        )

        valid_count = total - invalid_count

        timer.set(
            input_rows=total,
            valid_rows=valid_count,
            invalid_rows=invalid_count
        )

        print(f"\n=== MICRO-BATCH {batch_id} ===")
        print("Messages recus :", total)
        print("Messages valides :", valid_count)
        print("Messages invalides :", invalid_count)

        # ----------------------------------
        # 9. GESTION DES INVALIDES
        # ----------------------------------

        if invalid_count > 0:

            invalid.write.mode("overwrite").parquet(
                f"{RUN_BASE}/invalid/"
                f"batch_{batch_id:08d}"
            )

            publish_kafka(
                invalid,
                "ais-benchmark-invalid",
                "event_key"
            )

        if valid_count == 0:
            timer.set(
                scored_rows=0,
                alert_count=0,
                ml_count=0
            )
            return

        # ----------------------------------
        # 10. INFERENCE MACHINE LEARNING
        # ----------------------------------

        features = prepare_event_features(valid)

        predictions = model.transform(features)

        scored = add_anomaly_score(predictions)

        scored = scored.withColumn(
            "anomalie_potentielle",
            F.col("distance_centroide") > threshold
        )

        scored = scored.withColumn(
            "incoherence_physique_potentielle",
            F.col("vitesse_apparente_kmh")
            > PHYSICAL_THRESHOLD
        )

        # ----------------------------------
        # 11. CLASSIFICATION DES ALERTES
        # ----------------------------------

        scored = scored.withColumn(
            "type_alerte",
            F.when(
                F.col(
                    "incoherence_physique_potentielle"
                ),
                "COHERENCE_PHYSIQUE"
            ).when(
                F.col("anomalie_potentielle"),
                "ANOMALIE_STATISTIQUE"
            ).otherwise(
                "NON_SIGNALE"
            )
        )

        scored = scored.withColumn(
            "explication",
            F.when(
                F.col(
                    "incoherence_physique_potentielle"
                ),
                F.lit(
                    "Vitesse apparente superieure "
                    "au seuil exploratoire de 80 km/h"
                )
            ).when(
                F.col("anomalie_potentielle"),
                F.lit(
                    "Distance au centroide superieure "
                    "au seuil P95 de validation"
                )
            ).otherwise(
                F.lit(
                    "Aucun des deux seuils depasse"
                )
            )
        )

        scored = scored.withColumn(
            "alert_id",
            F.concat_ws(
                ":",
                "replay_id",
                "gap_id"
            )
        )

        scored = scored.withColumn(
            "model_version",
            F.lit("kmeans_events_v1")
        )

        scored = scored.withColumn(
            "seuil_anomalie",
            F.lit(threshold)
        )

        # ----------------------------------
        # 12. MESURE DE LATENCE
        # ----------------------------------

        scored = scored.withColumn(
            "processing_timestamp",
            F.current_timestamp()
        )

        scored = scored.withColumn(
            "latence_ms",
            (
                F.col("processing_timestamp")
                .cast("double")
                - F.col("ingestion_ts")
                .cast("double")
            ) * 1000.0
        )

        scored = (
            scored
            .withColumnRenamed(
                "prediction",
                "cluster"
            )
            .drop(
                "raw_features",
                "features",
                "feature_array",
                "center_array",
                "raw_json",
                "_corrupt_record"
            )
            .cache()
        )

        # Materialisation avant les ecritures

        with timer.measure("scoring"):
            scored_count = scored.count()

        assert scored_count == valid_count

        alerts = scored.filter(
            F.col("type_alerte") != "NON_SIGNALE"
        )

        with timer.measure("alert_count"):
            alert_count = alerts.count()

        with timer.measure("ml_count"):
            ml_count = scored.filter(
                F.col("anomalie_potentielle")
            ).count()

        timer.set(
            scored_rows=scored_count,
            alert_count=alert_count,
            ml_count=ml_count
        )

        # ----------------------------------
        # 13. SAUVEGARDE HDFS
        # ----------------------------------

        with timer.measure("hdfs_results"):
            scored.write.mode("overwrite").parquet(
                f"{RUN_BASE}/results/"
                f"batch_{batch_id:08d}"
            )

        if alert_count > 0:

            with timer.measure("hdfs_alerts"):
                alerts.write.mode("overwrite").parquet(
                    f"{RUN_BASE}/alerts/"
                    f"batch_{batch_id:08d}"
                )

            # ------------------------------
            # 14. PUBLICATION DES ALERTES
            # ------------------------------

            with timer.measure("kafka_publish"):
                publish_kafka(
                    alerts,
                    "ais-benchmark-alerts",
                    "alert_id"
                )

        # ----------------------------------
        # 15. INDICATEURS DU MICRO-BATCH
        # ----------------------------------

        print("Evenements scores :", scored_count)
        print("Anomalies ML :", ml_count)
        print("Alertes publiees :", alert_count)

        with timer.measure("type_statistics"):
            scored.groupBy(
                "type_alerte"
            ).count().show(truncate=False)

        with timer.measure("latency_statistics"):
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
            ).show(truncate=False)

        print("MICRO-BATCH TRAITE AVEC SUCCES")

    except Exception:
        timer.set(status="failed")
        raise

    finally:

        if scored is not None:
            scored.unpersist()

        if invalid is not None:
            invalid.unpersist()

        checked.unpersist()

        timer.finish(
            status=(
                "failed"
                if timer.metrics["status"] == "failed"
                else "success"
            )
        )


# ==========================================
# 16. DEMARRAGE DU STREAMING
# ==========================================

writer = (
    events.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .outputMode("append")
)

if args.once:

    writer = writer.trigger(
        availableNow=True
    )

else:

    writer = writer.trigger(
        processingTime=f"{args.trigger} seconds"
    )

query = writer.start()

print("\n=== DARKSEA STREAMING DEMARRE ===")
print("Source :", KAFKA_SERVER)
print("Topic source : ais-benchmark-events")
print("Checkpoint :", CHECKPOINT)

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

spark.stop()
