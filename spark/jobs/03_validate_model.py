
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

spark = (
    SparkSession.builder
    .appName("DarkSea-Model-Validation")
    .config("spark.sql.shuffle.partitions", "4")
    .config(
        "spark.hadoop.fs.defaultFS",
        "hdfs://namenode:9000"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

BASE = "hdfs://namenode:9000/darksea"

# 1. Rechargement du modele

model = PipelineModel.load(
    f"{BASE}/models/kmeans_profiles"
)

print("\n=== MODELE RECHARGE ===")

print("Nombre de clusters :", model.stages[-1].getK())

# 2. Chargement des metriques

with open(
    "/workspace/spark/results/kmeans_metrics.json",
    encoding="utf-8"
) as f:
    metrics = json.load(f)

threshold = metrics["anomaly_threshold"]

# 3. Lecture des profils

df = spark.read.parquet(
    f"{BASE}/analytics/profils_enrichis"
)

# 4. Nouvelles predictions

predictions = model.transform(df)

centers = model.stages[-1].clusterCenters()

centers_array = F.array(*[
    F.array(*[
        F.lit(float(value))
        for value in center
    ])
    for center in centers
])

predictions = predictions.withColumn(
    "feature_array",
    vector_to_array("features")
).withColumn(
    "center_array",
    F.element_at(
        centers_array,
        F.col("prediction") + F.lit(1)
    )
)

squared_distances = F.zip_with(
    F.col("feature_array"),
    F.col("center_array"),
    lambda x, y: (x - y) * (x - y)
)

predictions = predictions.withColumn(
    "distance_recalculee",
    F.sqrt(
        F.aggregate(
            squared_distances,
            F.lit(0.0),
            lambda acc, x: acc + x
        )
    )
).withColumn(
    "anomalie_recalculee",
    F.col("distance_recalculee") > threshold
)

# 5. Comparaison avec les resultats sauvegardes

saved = spark.read.parquet(
    f"{BASE}/results/profils_scores"
)

comparison = predictions.select(
    "mmsi",
    "prediction",
    "distance_recalculee",
    "anomalie_recalculee"
).join(
    saved.select(
        "mmsi",
        "cluster",
        "distance_centroide",
        "anomalie_potentielle"
    ),
    "mmsi",
    "inner"
).cache()

total = comparison.count()

cluster_errors = comparison.filter(
    F.col("prediction") != F.col("cluster")
).count()

distance_errors = comparison.filter(
    F.abs(
        F.col("distance_recalculee")
        - F.col("distance_centroide")
    ) > 0.000001
).count()

anomaly_errors = comparison.filter(
    F.col("anomalie_recalculee")
    != F.col("anomalie_potentielle")
).count()

print("\n=== TEST DE REPRODUCTIBILITE ===")

print("Profils compares :", total)
print("Differences clusters :", cluster_errors)
print("Differences distances :", distance_errors)
print("Differences anomalies :", anomaly_errors)

assert total == df.count() == saved.count() == 5269
assert cluster_errors == 0
assert distance_errors == 0
assert anomaly_errors == 0

print("\nMODELE RECHARGE ET VALIDE AVEC SUCCES")

spark.stop()
