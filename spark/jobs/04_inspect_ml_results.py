
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("DarkSea-Inspect-ML-Results")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

BASE = "hdfs://namenode:9000/darksea"

scores = spark.read.parquet(
    f"{BASE}/results/profils_scores"
)

print("\n=== SCHEMA DES RESULTATS ML ===")

scores.printSchema()

print("\n=== EXEMPLES DE RESULTATS ===")

scores.show(5, truncate=False)

print("\n=== CONTROLE DES IDENTIFIANTS ===")

scores.agg(
    F.count("*").alias("total_profils"),
    F.countDistinct("mmsi").alias("mmsi_uniques"),
    F.sum(
        F.when(F.col("mmsi").isNull(), 1)
        .otherwise(0)
    ).alias("mmsi_null")
).show()

print("\n=== CONTROLE DES SCORES ===")

scores.agg(
    F.min("distance_centroide").alias("score_min"),
    F.avg("distance_centroide").alias("score_moyen"),
    F.max("distance_centroide").alias("score_max"),
    F.sum(
        F.when(
            F.col("distance_centroide").isNull()
            | F.isnan("distance_centroide")
            | (F.col("distance_centroide") < 0),
            1
        ).otherwise(0)
    ).alias("scores_invalides")
).show(truncate=False)

print("\n=== DISTRIBUTION DES ANOMALIES ===")

scores.groupBy(
    "anomalie_potentielle"
).count().show()

print("\n=== DISTRIBUTION PAR CLUSTER ===")

scores.groupBy(
    "cluster",
    "anomalie_potentielle"
).count().orderBy(
    "cluster",
    "anomalie_potentielle"
).show()

print("\n=== COMPARAISON REGLES HIVE / ML ===")

prioritaires = spark.read.parquet(
    f"{BASE}/analytics/navires_prioritaires"
).select("mmsi").distinct().withColumn(
    "prioritaire_hive",
    F.lit(True)
)

comparison = scores.join(
    prioritaires,
    "mmsi",
    "left"
).fillna({
    "prioritaire_hive": False
})

comparison.groupBy(
    "prioritaire_hive",
    "anomalie_potentielle"
).count().orderBy(
    "prioritaire_hive",
    "anomalie_potentielle"
).show()

print("\nINSPECTION TERMINEE")

spark.stop()
