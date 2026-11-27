
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("DarkSea-Integration-Test")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n=== ENVIRONNEMENT SPARK ===")
print("Spark version :", spark.version)
print("Spark master :", spark.sparkContext.master)

base_path = "hdfs://namenode:9000/darksea/analytics"

print("\n=== LECTURE DES PROFILS HIVE ===")

df = spark.read.parquet(
    f"{base_path}/profils_enrichis"
)

df.printSchema()
df.show(10, truncate=False)

nombre_profils = df.count()

print("Nombre de profils :", nombre_profils)

print("\n=== STATISTIQUES GLOBALES ===")

stats = spark.read.parquet(
    f"{base_path}/statistiques_globales"
)

stats.show(truncate=False)

print("\n=== NAVIRES PRIORITAIRES ===")

prioritaires = spark.read.parquet(
    f"{base_path}/navires_prioritaires"
)

nombre_prioritaires = prioritaires.count()

print("Nombre de profils prioritaires :", nombre_prioritaires)

print("\n=== TEST SPARK SQL ===")

df.createOrReplaceTempView("profils")

spark.sql("""
    SELECT
        classe_frequence,
        COUNT(*) AS nombre_navires,
        ROUND(AVG(duree_moyenne_h), 2)
            AS duree_moyenne_h
    FROM profils
    GROUP BY classe_frequence
    ORDER BY nombre_navires DESC
""").show()

print("\n=== VALIDATION ===")

total_interruptions = df.agg(
    F.sum("nb_interruptions")
).first()[0]

assert nombre_profils == 5269
assert nombre_prioritaires == 1884
assert total_interruptions == 55368
assert spark.sparkContext.master == "spark://spark-master:7077"

print("Tous les tests d'integration sont reussis.")

spark.stop()
