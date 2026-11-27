
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

from event_features import (
    prepare_event_features,
    validate_events
)

spark = (
    SparkSession.builder
    .appName("DarkSea-Event-Model-Validation")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

BASE = "hdfs://namenode:9000/darksea"

# ==========================================
# 1. CHARGEMENT DU MODELE ET DES METRIQUES
# ==========================================

model = PipelineModel.load(
    f"{BASE}/models/kmeans_events"
)

with open(
    "/workspace/spark/results/event_kmeans_metrics.json",
    encoding="utf-8"
) as f:
    metrics = json.load(f)

threshold = metrics["anomaly_threshold"]

centers = model.stages[-1].clusterCenters()

print("\n=== MODELE RECHARGE ===")
print("Nombre de clusters :", len(centers))
print("Seuil :", threshold)

assert len(centers) == metrics["best_k"]

# ==========================================
# 2. CHARGEMENT DES RESULTATS HISTORIQUES
# ==========================================

saved = spark.read.parquet(
    f"{BASE}/results/evenements_scores"
).cache()

total = saved.count()

assert total == metrics["valid_events"]

print("\nNombre d'evenements sauvegardes :", total)

# ==========================================
# 3. RECALCUL DES VARIABLES
# ==========================================

source = saved.select(
    "gap_id",
    "mmsi",
    "latitude",
    "longitude",
    "latitude_fin",
    "longitude_fin",
    "gap_hours",
    "distance_cote_km"
)

source = validate_events(source)

assert source.filter(
    ~F.col("event_valid")
).count() == 0

source = prepare_event_features(source)

predictions = model.transform(source)

# ==========================================
# 4. RECALCUL DU SCORE D'ANOMALIE
# ==========================================

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
)

predictions = predictions.withColumn(
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
    "new_score",
    F.sqrt(
        F.aggregate(
            squared_distances,
            F.lit(0.0),
            lambda acc, x: acc + x
        )
    )
)

predictions = predictions.withColumn(
    "new_anomaly",
    F.col("new_score") > threshold
)

# ==========================================
# 5. COMPARAISON AVEC LES RESULTATS SAUVES
# ==========================================

comparison = saved.select(
    "gap_id",
    "cluster",
    "distance_centroide",
    "anomalie_potentielle"
).join(
    predictions.select(
        "gap_id",
        "prediction",
        "new_score",
        "new_anomaly"
    ),
    on="gap_id",
    how="inner"
).cache()

n_compared = comparison.count()

cluster_diff = comparison.filter(
    ~F.col("cluster").eqNullSafe(
        F.col("prediction")
    )
).count()

score_diff = comparison.filter(
    F.col("new_score").isNull()
    | F.isnan(F.col("new_score"))
    | (
        F.abs(
            F.col("distance_centroide")
            - F.col("new_score")
        ) > 1e-6
    )
).count()

anomaly_diff = comparison.filter(
    ~F.col("anomalie_potentielle").eqNullSafe(
        F.col("new_anomaly")
    )
).count()

print("\n=== TEST DE REPRODUCTIBILITE ===")
print("Evenements compares :", n_compared)
print("Differences clusters :", cluster_diff)
print("Differences scores :", score_diff)
print("Differences anomalies :", anomaly_diff)

assert n_compared == total
assert cluster_diff == 0
assert score_diff == 0
assert anomaly_diff == 0

# ==========================================
# 6. AUDIT DE COHERENCE PHYSIQUE
# ==========================================

print("\n=== AUDIT DES VITESSES APPARENTES ===")

for limit in [40, 60, 80, 100, 200]:

    subset = saved.filter(
        F.col("vitesse_apparente_kmh") > limit
    )

    count = subset.count()

    flagged = subset.filter(
        F.col("anomalie_potentielle")
    ).count()

    print(
        f"Vitesse > {limit} km/h : "
        f"{count} evenements, "
        f"dont {flagged} signales par ML"
    )

# Seuil exploratoire de coherence physique

audit = saved.withColumn(
    "incoherence_physique_potentielle",
    F.col("vitesse_apparente_kmh") > 80
)

print("\n=== COMPARAISON QUALITE ET ML ===")

audit.groupBy(
    "incoherence_physique_potentielle",
    "anomalie_potentielle"
).count().show()

print("\n=== INDICATEURS DE QUALITE ===")

audit.groupBy(
    "mmsi_format_atypique",
    "duree_extreme",
    "incoherence_physique_potentielle"
).count().show(truncate=False)

print("\n=== VITESSES EXTREMES ===")

audit.select(
    "gap_id",
    "mmsi",
    "vessel_class",
    "latitude",
    "longitude",
    "latitude_fin",
    "longitude_fin",
    "gap_hours",
    "deplacement_km",
    "vitesse_apparente_kmh",
    "distance_centroide"
).orderBy(
    F.desc("vitesse_apparente_kmh")
).show(15, truncate=False)

print("\n=== TAUX ANOMALIES PAR CLUSTER ===")

audit.groupBy("cluster").agg(
    F.count("*").alias("total"),
    F.sum(
        F.col("anomalie_potentielle").cast("int")
    ).alias("anomalies"),
    F.sum(
        F.col(
            "incoherence_physique_potentielle"
        ).cast("int")
    ).alias("incoherences_physiques"),
    (
        F.avg(
            F.col("anomalie_potentielle").cast("double")
        ) * 100
    ).alias("taux_anomalies_pct")
).orderBy("cluster").show(truncate=False)

print("\nMODELE EVENEMENTIEL RECHARGE ET VALIDE")
print("AUDIT DE QUALITE TERMINE")

spark.stop()
