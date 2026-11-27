import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW_PATH = "hdfs://namenode:9000/darksea/raw/ais_disabling_events.csv"
OUTPUT_PATH = "hdfs://namenode:9000/darksea/benchmark/datasets"

EXPECTED_BASE_COUNT = 55368


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--factors", nargs="+", type=int, default=[1, 10])
    args = parser.parse_args()

    if any(factor < 1 for factor in args.factors):
        parser.error("Les facteurs doivent etre positifs.")

    spark = (
        SparkSession.builder
        .appName("DarkSea-Benchmark-Generation")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.hadoop.dfs.replication", "1")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        raw = spark.read.option("header", True).csv(RAW_PATH)

        base = raw.select(
            F.col("gap_id").cast("string"),
            F.col("mmsi").cast("string"),
            F.col("gap_hours").cast("double")
        )

        base = base.filter(
            F.col("gap_id").isNotNull()
            & F.col("mmsi").isNotNull()
            & F.col("gap_hours").isNotNull()
            & (F.col("gap_hours") > 0)
            & (~F.isnan("gap_hours"))
        ).cache()

        base_count = base.count()

        print("\n=== DATASET DE REFERENCE ===")
        print("Evenements :", base_count)

        assert base_count == EXPECTED_BASE_COUNT
        assert base.select("gap_id").distinct().count() == base_count

        for factor in args.factors:

            start = time.perf_counter()

            copies = spark.range(factor).withColumnRenamed(
                "id", "copy_id"
            )

            expanded = base.crossJoin(F.broadcast(copies))

            dataset = expanded.select(
                F.concat_ws(
                    "_",
                    F.col("gap_id"),
                    F.col("copy_id").cast("string")
                ).alias("gap_id"),
                F.col("mmsi"),
                F.col("gap_hours")
            )

            expected = base_count * factor

            partitions = min(16, max(2, factor // 5))

            destination = f"{OUTPUT_PATH}/x{factor}"

            (
                dataset.repartition(partitions)
                .write
                .mode("overwrite")
                .option("compression", "snappy")
                .parquet(destination)
            )

            saved = spark.read.parquet(destination)

            actual = saved.count()
            unique = saved.select("gap_id").distinct().count()

            assert actual == expected
            assert unique == expected

            elapsed = time.perf_counter() - start

            print("\n" + "=" * 50)
            print("FACTEUR :", factor)
            print("Lignes attendues :", expected)
            print("Lignes obtenues :", actual)
            print("Identifiants uniques :", unique)
            print("Partitions ecriture :", partitions)
            print("Duree generation :", round(elapsed, 3), "s")
            print("Destination :", destination)
            print("VALIDATION REUSSIE")

        base.unpersist()

        print("\nGENERATION TERMINEE AVEC SUCCES")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
