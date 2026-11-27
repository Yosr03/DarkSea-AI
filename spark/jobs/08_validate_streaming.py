
import argparse
import json

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


parser = argparse.ArgumentParser()

parser.add_argument(
    "--replay-id",
    required=True
)

args = parser.parse_args()

BASE = "hdfs://namenode:9000/darksea"

spark = (
    SparkSession.builder
    .appName("DarkSea-Streaming-Validation")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ==========================================
# 1. CHARGEMENT DES RESULTATS
# ==========================================

batch = (
    spark.read.parquet(
        f"{BASE}/results/evenements_scores"
    )
    .filter(F.col("data_split") == "TEST")
    .cache()
)

stream = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet(f"{BASE}/streaming/results")
    .filter(F.col("replay_id") == args.replay_id)
    .cache()
)

batch_count = batch.count()
stream_count = stream.count()

unique_events = stream.select(
    "gap_id"
).distinct().count()

duplicates = stream_count - unique_events

missing = batch.select("gap_id").join(
    stream.select("gap_id"),
    "gap_id",
    "left_anti"
).count()

unexpected = stream.select("gap_id").join(
    batch.select("gap_id"),
    "gap_id",
    "left_anti"
).count()

print("\n=== CONTROLE DES EVENEMENTS ===")

print("Evenements batch :", batch_count)
print("Evenements streaming :", stream_count)
print("Evenements uniques :", unique_events)
print("Doublons :", duplicates)
print("Evenements manquants :", missing)
print("Evenements inattendus :", unexpected)


# ==========================================
# 2. COMPARAISON DES PREDICTIONS
# ==========================================

comparison = batch.alias("b").join(
    stream.alias("s"),
    "gap_id",
    "inner"
).select(
    "gap_id",

    F.col("b.cluster").alias("cluster_batch"),
    F.col("s.cluster").alias("cluster_stream"),

    F.col("b.distance_centroide").alias(
        "score_batch"
    ),

    F.col("s.distance_centroide").alias(
        "score_stream"
    ),

    F.col("b.anomalie_potentielle").alias(
        "anomalie_batch"
    ),

    F.col("s.anomalie_potentielle").alias(
        "anomalie_stream"
    )
).cache()

compared = comparison.count()

cluster_diff = comparison.filter(
    ~F.col("cluster_batch").eqNullSafe(
        F.col("cluster_stream")
    )
).count()

score_diff = comparison.filter(
    F.col("score_batch").isNull()
    | F.col("score_stream").isNull()
    | F.isnan("score_batch")
    | F.isnan("score_stream")
    | (
        F.abs(
            F.col("score_batch")
            - F.col("score_stream")
        ) > 0.000001
    )
).count()

anomaly_diff = comparison.filter(
    ~F.col("anomalie_batch").eqNullSafe(
        F.col("anomalie_stream")
    )
).count()

batch_anomalies = batch.filter(
    F.col("anomalie_potentielle")
).count()

stream_anomalies = stream.filter(
    F.col("anomalie_potentielle")
).count()

print("\n=== COMPARAISON BATCH / STREAMING ===")

print("Evenements compares :", compared)
print("Differences clusters :", cluster_diff)
print("Differences scores :", score_diff)
print("Differences anomalies :", anomaly_diff)

print("Anomalies batch :", batch_anomalies)
print("Anomalies streaming :", stream_anomalies)


# ==========================================
# 3. ANALYSE DES ALERTES
# ==========================================

print("\n=== TYPES D'ALERTES ===")

stream.groupBy(
    "type_alerte"
).count().show(truncate=False)

physical_count = stream.filter(
    F.col("incoherence_physique_potentielle")
).count()

alerts_count = stream.filter(
    F.col("type_alerte") != "NON_SIGNALE"
).count()


# ==========================================
# 4. ANALYSE DES PERFORMANCES
# ==========================================

latency = stream.agg(

    F.min("latence_ms").alias("min_ms"),

    F.avg("latence_ms").alias("mean_ms"),

    F.expr(
        "percentile_approx(latence_ms, 0.50)"
    ).alias("p50_ms"),

    F.expr(
        "percentile_approx(latence_ms, 0.95)"
    ).alias("p95_ms"),

    F.expr(
        "percentile_approx(latence_ms, 0.99)"
    ).alias("p99_ms"),

    F.max("latence_ms").alias("max_ms")

).first().asDict()

negative_latency = stream.filter(
    F.col("latence_ms") < 0
).count()

print("\n=== PERFORMANCES STREAMING ===")

for key, value in latency.items():

    print(key, ":", value)

print(
    "Latences negatives :",
    negative_latency
)


# ==========================================
# 5. RAPPORT DE VALIDATION
# ==========================================

validated = (
    batch_count == 11123
    and stream_count == 11123
    and compared == 11123
    and duplicates == 0
    and missing == 0
    and unexpected == 0
    and cluster_diff == 0
    and score_diff == 0
    and anomaly_diff == 0
    and batch_anomalies == 551
    and stream_anomalies == 551
    and negative_latency == 0
)

report = {
    "replay_id": args.replay_id,
    "batch_events": batch_count,
    "streaming_events": stream_count,
    "unique_events": unique_events,
    "compared_events": compared,
    "duplicates": duplicates,
    "missing_events": missing,
    "unexpected_events": unexpected,
    "cluster_differences": cluster_diff,
    "score_differences": score_diff,
    "anomaly_differences": anomaly_diff,
    "batch_anomalies": batch_anomalies,
    "streaming_anomalies": stream_anomalies,
    "physical_inconsistencies": physical_count,
    "total_alerts": alerts_count,
    "latency": latency,
    "negative_latency_count": negative_latency,
    "validation_passed": validated
}

output = Path(
    "/workspace/spark/results"
)

output.mkdir(
    parents=True,
    exist_ok=True
)

with open(
    output / "streaming_validation.json",
    "w",
    encoding="utf-8"
) as file:

    json.dump(
        report,
        file,
        indent=4
    )

print("\n=== RESULTAT FINAL ===")

if validated:
    print("VALIDATION STREAMING REUSSIE")
else:
    print("VALIDATION STREAMING NON CONFORME")

comparison.unpersist()
batch.unpersist()
stream.unpersist()

spark.stop()

assert validated, "Echec de validation streaming"
