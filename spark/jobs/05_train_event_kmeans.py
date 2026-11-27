
import json
import time

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    SQLTransformer,
    VectorAssembler,
    StandardScaler
)
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.functions import vector_to_array

from event_features import (
    prepare_event_features,
    validate_events,
    FEATURE_COLUMNS
)


# ==========================================
# 1. INITIALISATION SPARK
# ==========================================

spark = (
    SparkSession.builder
    .appName("DarkSea-Event-KMeans")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "UTC")
    .config(
        "spark.hadoop.fs.defaultFS",
        "hdfs://namenode:9000"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

start_time = time.perf_counter()

BASE = "hdfs://namenode:9000/darksea"

MODEL_PATH = f"{BASE}/models/kmeans_events"
RESULT_PATH = f"{BASE}/results/evenements_scores"

LOCAL_RESULTS = Path("/workspace/spark/results")


# ==========================================
# 2. CHARGEMENT DES CONNAISSANCES PIG
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
    mmsi_format_atypique INT
"""

raw = (
    spark.read
    .option("header", "false")
    .schema(schema)
    .csv(
        f"{BASE}/analytics/evenements_cartographie/part-*"
    )
    .cache()
)

n_raw = raw.count()

print("\n=== DONNEES SOURCES ===")
print("Nombre d'evenements :", n_raw)

assert n_raw == 55368

assert (
    raw.select("gap_id").distinct().count()
    == n_raw
)


# ==========================================
# 3. VALIDATION ET FEATURE ENGINEERING
# ==========================================

df = validate_events(raw)

df = (
    df.withColumn(
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
)

df = df.withColumn(
    "event_valid",
    F.coalesce(
        F.col("event_valid")
        & F.col("start_ts").isNotNull()
        & F.col("end_ts").isNotNull()
        & (F.col("end_ts") > F.col("start_ts")),
        F.lit(False)
    )
)

invalid_count = df.filter(
    ~F.col("event_valid")
).count()

df = df.filter(F.col("event_valid"))

df = prepare_event_features(df)

df = df.withColumn(
    "duree_calculee_h",
    (
        F.col("end_ts").cast("long")
        - F.col("start_ts").cast("long")
    ) / 3600.0
)

df = df.withColumn(
    "ecart_duree_h",
    F.abs(
        F.col("gap_hours")
        - F.col("duree_calculee_h")
    )
)

df = df.withColumn(
    "ecart_duree_significatif",
    F.col("ecart_duree_h") > (1.0 / 60.0)
)

df = df.repartition(4).cache()

n_valid = df.count()

assert n_valid + invalid_count == n_raw

print("\n=== QUALITE DES DONNEES ===")
print("Evenements valides :", n_valid)
print("Evenements invalides :", invalid_count)

df.select(
    "gap_hours",
    "distance_cote_km",
    "deplacement_km",
    "vitesse_apparente_kmh",
    "ecart_duree_h"
).describe().show(truncate=False)

duration_mismatch = df.filter(
    F.col("ecart_duree_significatif")
).count()

print(
    "Ecarts de duree superieurs a une minute :",
    duration_mismatch
)


# ==========================================
# 4. SEPARATION CHRONOLOGIQUE
# ==========================================

train = df.filter(
    F.col("end_ts") < F.lit("2019-01-01").cast("timestamp")
).cache()

validation = df.filter(
    (F.col("end_ts") >= F.lit("2019-01-01").cast("timestamp"))
    & (F.col("end_ts") < F.lit("2019-07-01").cast("timestamp"))
).cache()

test = df.filter(
    F.col("end_ts") >= F.lit("2019-07-01").cast("timestamp")
).cache()

n_train = train.count()
n_val = validation.count()
n_test = test.count()

print("\n=== SEPARATION CHRONOLOGIQUE ===")
print("Train :", n_train)
print("Validation :", n_val)
print("Test :", n_test)

assert n_train > 0
assert n_val > 0
assert n_test > 0

assert n_train + n_val + n_test == n_valid


# ==========================================
# 5. PREPROCESSING
# ==========================================

log_columns = [
    f"log_{name}" for name in FEATURE_COLUMNS
]

expressions = ", ".join(
    f"LOG1P({name}) AS log_{name}"
    for name in FEATURE_COLUMNS
)

log_transformer = SQLTransformer(
    statement=f"SELECT *, {expressions} FROM __THIS__"
)

assembler = VectorAssembler(
    inputCols=log_columns,
    outputCol="raw_features"
)

scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withMean=True,
    withStd=True
)

preprocessing = Pipeline(
    stages=[
        log_transformer,
        assembler,
        scaler
    ]
)

preprocessing_model = preprocessing.fit(train)

train_features = (
    preprocessing_model
    .transform(train)
    .select("features")
    .cache()
)

val_features = (
    preprocessing_model
    .transform(validation)
    .select("features")
    .cache()
)

train_features.count()
val_features.count()


# ==========================================
# 6. COMPARAISON DES VALEURS DE K
# ==========================================

evaluator = ClusteringEvaluator(
    featuresCol="features",
    predictionCol="prediction",
    metricName="silhouette",
    distanceMeasure="squaredEuclidean"
)

experiments = []

best_k = None
best_score = float("-inf")

print("\n=== RECHERCHE DU MEILLEUR K ===")

for k in range(2, 9):

    t0 = time.perf_counter()

    algorithm = KMeans(
        k=k,
        seed=42,
        maxIter=50,
        featuresCol="features",
        predictionCol="prediction"
    )

    model = algorithm.fit(train_features)

    predictions = model.transform(val_features)

    score = evaluator.evaluate(predictions)

    duration = time.perf_counter() - t0

    experiments.append({
        "k": k,
        "silhouette_validation": float(score),
        "training_cost": float(
            model.summary.trainingCost
        ),
        "duration_seconds": round(duration, 3)
    })

    print(
        f"K={k} | "
        f"Silhouette={score:.6f} | "
        f"Temps={duration:.2f}s"
    )

    if score > best_score:
        best_score = score
        best_k = k

print("\nMeilleur K :", best_k)
print("Silhouette validation :", best_score)


# ==========================================
# 7. ENTRAINEMENT FINAL
# ==========================================

final_pipeline = Pipeline(
    stages=[
        log_transformer,
        assembler,
        scaler,
        KMeans(
            k=best_k,
            seed=42,
            maxIter=50,
            featuresCol="features",
            predictionCol="prediction"
        )
    ]
)

final_model = final_pipeline.fit(train)

kmeans_model = final_model.stages[-1]

test_predictions = final_model.transform(test)

test_score = evaluator.evaluate(test_predictions)

print("\nSilhouette test :", test_score)


# ==========================================
# 8. SCORE D'ANOMALIE
# ==========================================

centers = kmeans_model.clusterCenters()

centers_array = F.array(*[
    F.array(*[
        F.lit(float(value))
        for value in center
    ])
    for center in centers
])


def add_distance(predictions):

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

    return predictions.withColumn(
        "distance_centroide",
        F.sqrt(
            F.aggregate(
                squared_distances,
                F.lit(0.0),
                lambda acc, x: acc + x
            )
        )
    )


# ==========================================
# 9. CALIBRATION DU SEUIL
# ==========================================

val_scored = add_distance(
    final_model.transform(validation)
).cache()

thresholds = val_scored.approxQuantile(
    "distance_centroide",
    [0.90, 0.95, 0.99],
    0.001
)

threshold = thresholds[1]

print("\n=== SEUILS D'ANOMALIE ===")

for q, value in zip(
    [0.90, 0.95, 0.99],
    thresholds
):
    print(f"P{int(q * 100)} : {value}")

print("Seuil retenu P95 :", threshold)


# ==========================================
# 10. EVALUATION SUR LE TEST
# ==========================================

test_scored = add_distance(
    test_predictions
).cache()

test_anomalies = test_scored.filter(
    F.col("distance_centroide") > threshold
).count()

print("\n=== EVALUATION TEST ===")
print("Evenements test :", n_test)
print("Anomalies test :", test_anomalies)

print(
    "Taux anomalies test :",
    round(test_anomalies / n_test * 100, 3),
    "%"
)


# ==========================================
# 11. SCORING DES EVENEMENTS HISTORIQUES
# ==========================================

scored = add_distance(
    final_model.transform(df)
)

scored = scored.withColumn(
    "anomalie_potentielle",
    F.col("distance_centroide") > threshold
)

scored = scored.withColumn(
    "data_split",
    F.when(
        F.col("end_ts") < F.lit("2019-01-01").cast("timestamp"),
        "TRAIN"
    ).when(
        F.col("end_ts") < F.lit("2019-07-01").cast("timestamp"),
        "VALIDATION"
    ).otherwise("TEST")
)

results = scored.select(
    "gap_id",
    "mmsi",
    "vessel_class",
    "pavillon",
    "gap_start_timestamp",
    "gap_end_timestamp",
    "latitude",
    "longitude",
    "latitude_fin",
    "longitude_fin",
    "gap_hours",
    "distance_cote_km",
    "deplacement_km",
    "vitesse_apparente_kmh",
    "zone_distance",
    "duree_extreme",
    "mmsi_format_atypique",
    "ecart_duree_h",
    "ecart_duree_significatif",
    "data_split",
    F.col("prediction").alias("cluster"),
    "distance_centroide",
    "anomalie_potentielle"
).cache()

total = results.count()

anomalies = results.filter(
    F.col("anomalie_potentielle")
).count()

assert total == n_valid

assert (
    results.select("gap_id").distinct().count()
    == total
)

assert results.filter(
    F.col("distance_centroide").isNull()
    | F.isnan(F.col("distance_centroide"))
    | (F.col("distance_centroide") < 0)
    | (F.col("distance_centroide") == float("inf"))
).count() == 0


# ==========================================
# 12. ANALYSE DES CLUSTERS
# ==========================================

print("\n=== SYNTHESE DES CLUSTERS ===")

results.groupBy("cluster").agg(
    F.count("*").alias("nb_evenements"),
    F.avg("gap_hours").alias("duree_moyenne_h"),
    F.avg("distance_cote_km").alias("distance_cote_moyenne"),
    F.avg("deplacement_km").alias("deplacement_moyen"),
    F.avg("vitesse_apparente_kmh").alias("vitesse_moyenne"),
    F.avg("distance_centroide").alias("score_moyen"),
    F.sum(
        F.col("anomalie_potentielle").cast("int")
    ).alias("nb_anomalies")
).orderBy("cluster").show(truncate=False)

print("\n=== ANOMALIES PAR SPLIT ===")

results.groupBy(
    "data_split",
    "anomalie_potentielle"
).count().show()

print("\n=== TOP 10 EVENEMENTS ATYPIQUES ===")

results.select(
    "gap_id",
    "mmsi",
    "gap_hours",
    "distance_cote_km",
    "deplacement_km",
    "vitesse_apparente_kmh",
    "cluster",
    "distance_centroide"
).orderBy(
    F.desc("distance_centroide")
).show(10, truncate=False)


# ==========================================
# 13. SAUVEGARDE HDFS
# ==========================================

final_model.write().overwrite().save(MODEL_PATH)

results.write.mode("overwrite").parquet(RESULT_PATH)


# ==========================================
# 14. SAUVEGARDE DES METRIQUES
# ==========================================

metrics = {
    "model": "KMeans event-level",
    "spark_version": spark.version,
    "spark_master": spark.sparkContext.master,
    "features": FEATURE_COLUMNS,
    "seed": 42,
    "raw_events": n_raw,
    "valid_events": n_valid,
    "invalid_events": invalid_count,
    "duration_mismatch_count": duration_mismatch,
    "split_strategy": "chronological_by_end_timestamp",
    "train_count": n_train,
    "validation_count": n_val,
    "test_count": n_test,
    "experiments": experiments,
    "best_k": best_k,
    "silhouette_validation": float(best_score),
    "silhouette_test": float(test_score),
    "threshold_p90": float(thresholds[0]),
    "threshold_p95": float(thresholds[1]),
    "threshold_p99": float(thresholds[2]),
    "anomaly_threshold": float(threshold),
    "test_anomalies": test_anomalies,
    "test_anomaly_percentage": round(
        test_anomalies / n_test * 100, 3
    ),
    "total_events": total,
    "total_anomalies": anomalies,
    "anomaly_percentage": round(
        anomalies / total * 100, 3
    ),
    "model_path": MODEL_PATH,
    "result_path": RESULT_PATH,
    "total_duration_seconds": round(
        time.perf_counter() - start_time, 3
    )
}

LOCAL_RESULTS.mkdir(
    parents=True,
    exist_ok=True
)

with open(
    LOCAL_RESULTS / "event_kmeans_metrics.json",
    "w",
    encoding="utf-8"
) as f:
    json.dump(
        metrics,
        f,
        indent=4,
        ensure_ascii=False
    )

print("\n=== METRIQUES FINALES ===")

print(json.dumps(metrics, indent=4))

print("\nENTRAINEMENT EVENEMENTIEL TERMINE AVEC SUCCES")

spark.stop()
