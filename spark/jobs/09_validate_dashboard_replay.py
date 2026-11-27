from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("DarkSea-Dashboard-Final-Validation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

replay_id = "ef569776-f0bc-4769-8cb9-9983895308e6"

stream = spark.read.parquet(
    "hdfs://namenode:9000/darksea/streaming/results/batch_*"
).filter(F.col("replay_id") == replay_id)

batch = spark.read.parquet(
    "hdfs://namenode:9000/darksea/results/evenements_scores"
)

total = stream.count()
unique = stream.select("gap_id").distinct().count()

print("=== VALIDATION DU NOUVEAU REPLAY ===")
print("Evenements traites :", total)
print("Evenements uniques :", unique)
print("Doublons :", total - unique)

stream.groupBy("type_alerte").count().show(truncate=False)

comparison = stream.alias("s").join(
    batch.alias("b"),
    F.col("s.gap_id") == F.col("b.gap_id"),
    "left"
)

missing = comparison.filter(
    F.col("b.gap_id").isNull()
).count()

differences = comparison.filter(
    F.col("b.gap_id").isNotNull() &
    (
        (~F.col("s.cluster").eqNullSafe(F.col("b.cluster"))) |
        (~F.col("s.anomalie_potentielle").eqNullSafe(
            F.col("b.anomalie_potentielle")
        )) |
        (F.abs(
            F.col("s.distance_centroide") -
            F.col("b.distance_centroide")
        ) > 0.000001)
    )
).count()

print("Evenements sans correspondance :", missing)
print("Differences Batch/Streaming :", differences)

assert total == 1000
assert unique == 1000
assert missing == 0
assert differences == 0
assert stream.filter(
    F.col("type_alerte") == "ANOMALIE_STATISTIQUE"
).count() == 45
assert stream.filter(
    F.col("type_alerte") == "COHERENCE_PHYSIQUE"
).count() == 0

print("VALIDATION FINALE REUSSIE")

spark.stop()
