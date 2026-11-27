
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


# 1. Initialisation de Spark

spark = (
    SparkSession.builder
    .appName("DarkSea-KMeans-Training")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

start_time = time.perf_counter()

BASE = "hdfs://namenode:9000/darksea"

MODEL_PATH = f"{BASE}/models/kmeans_profiles"
RESULT_PATH = f"{BASE}/results/profils_scores"

LOCAL_RESULTS = Path("/workspace/spark/results")


# 2. Lecture des connaissances Hive

df = spark.read.parquet(
    f"{BASE}/analytics/profils_enrichis"
)

print("\n=== DATASET INITIAL ===")

df.printSchema()

print("Nombre de profils :", df.count())


# 3. Indicateurs de qualité

df = df.withColumn(
    "mmsi_format_atypique",
    ~F.col("mmsi").rlike("^[0-9]{9}$")
).withColumn(
    "duree_extreme",
    F.col("duree_max_h") > 8760
)

df = df.filter(
    (F.col("nb_interruptions") > 0)
    & (F.col("duree_moyenne_h") > 0)
    & (F.col("duree_max_h") >= F.col("duree_moyenne_h"))
)

df = df.repartition(4).cache()

print("Profils exploitables :", df.count())


# 4. Separation train / validation / test

train, validation, test = df.randomSplit(
    [0.7, 0.15, 0.15],
    seed=42
)

train = train.cache()
validation = validation.cache()
test = test.cache()

n_train = train.count()
n_val = validation.count()
n_test = test.count()

print("\n=== REPARTITION DES DONNEES ===")

print("Train :", n_train)
print("Validation :", n_val)
print("Test :", n_test)


# 5. Feature Engineering

log_transformer = SQLTransformer(
    statement="""
        SELECT *,
            LOG1P(nb_interruptions) AS log_nb,
            LOG1P(duree_moyenne_h) AS log_moyenne,
            LOG1P(duree_max_h) AS log_max
        FROM __THIS__
    """
)

assembler = VectorAssembler(
    inputCols=[
        "log_nb",
        "log_moyenne",
        "log_max"
    ],
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
    preprocessing_model.transform(train).cache()
)

val_features = (
    preprocessing_model.transform(validation).cache()
)

train_features.count()
val_features.count()


# 6. Comparaison des valeurs de K

evaluator = ClusteringEvaluator(
    featuresCol="features",
    predictionCol="prediction",
    metricName="silhouette",
    distanceMeasure="squaredEuclidean"
)

experiments = []

best_k = None
best_score = float("-inf")

print("\n=== COMPARAISON DES MODELES ===")

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


print("\n=== MEILLEUR MODELE ===")

print("Meilleur K :", best_k)
print("Silhouette validation :", best_score)


# 7. Pipeline final

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

print("Silhouette test :", test_score)


# 8. Calcul des distances aux centroides

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


# 9. Calibration du seuil sur la validation

val_scored = add_distance(
    final_model.transform(validation)
)

threshold = val_scored.approxQuantile(
    "distance_centroide",
    [0.95],
    0.001
)[0]

print("\n=== SEUIL D'ANOMALIE ===")

print("Seuil P95 :", threshold)


# 10. Scoring de tous les profils

scored = add_distance(
    final_model.transform(df)
)

scored = scored.withColumn(
    "anomalie_potentielle",
    F.col("distance_centroide") > threshold
)

results = scored.select(
    "mmsi",
    "nb_interruptions",
    "duree_moyenne_h",
    "duree_max_h",
    "duree_totale_h",
    "classe_frequence",
    "mmsi_format_atypique",
    "duree_extreme",
    F.col("prediction").alias("cluster"),
    "distance_centroide",
    "anomalie_potentielle"
).cache()

print("\n=== DISTRIBUTION DES CLUSTERS ===")

results.groupBy("cluster").agg(
    F.count("*").alias("nombre_profils"),
    F.round(F.avg("nb_interruptions"), 2)
        .alias("moyenne_interruptions"),
    F.round(F.avg("duree_moyenne_h"), 2)
        .alias("moyenne_duree_h"),
    F.round(F.avg("duree_max_h"), 2)
        .alias("moyenne_max_h")
).orderBy("cluster").show(truncate=False)


print("\n=== DISTRIBUTION DES ANOMALIES ===")

results.groupBy(
    "anomalie_potentielle"
).count().show()


print("\n=== TOP 20 PROFILS ATYPIQUES ===")

results.orderBy(
    F.desc("distance_centroide")
).show(20, truncate=False)


# 11. Sauvegarde du modèle et des résultats

final_model.write().overwrite().save(
    MODEL_PATH
)

results.write.mode("overwrite").parquet(
    RESULT_PATH
)

total = results.count()

nb_anomalies = results.filter(
    F.col("anomalie_potentielle")
).count()


# 12. Sauvegarde des métriques

metrics = {
    "spark_version": spark.version,
    "spark_master": spark.sparkContext.master,
    "features": [
        "nb_interruptions",
        "duree_moyenne_h",
        "duree_max_h"
    ],
    "seed": 42,
    "train_count": n_train,
    "validation_count": n_val,
    "test_count": n_test,
    "experiments": experiments,
    "best_k": best_k,
    "silhouette_validation": float(best_score),
    "silhouette_test": float(test_score),
    "anomaly_threshold": float(threshold),
    "threshold_quantile": 0.95,
    "total_profiles": total,
    "anomalies": nb_anomalies,
    "anomaly_percentage": round(
        nb_anomalies / total * 100, 3
    ),
    "total_duration_seconds": round(
        time.perf_counter() - start_time, 3
    ),
    "model_path": MODEL_PATH,
    "result_path": RESULT_PATH
}

LOCAL_RESULTS.mkdir(
    parents=True,
    exist_ok=True
)

with open(
    LOCAL_RESULTS / "kmeans_metrics.json",
    "w",
    encoding="utf-8"
) as f:

    json.dump(
        metrics,
        f,
        indent=4,
        ensure_ascii=False
    )

print("\n=== RESULTATS FINAUX ===")

print(json.dumps(metrics, indent=4))

assert total == df.count()

print("\nENTRAINEMENT TERMINE AVEC SUCCES")

spark.stop()
