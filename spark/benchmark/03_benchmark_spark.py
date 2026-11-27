import argparse
import json
import statistics
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
RESULTS = ROOT / "results"

BASE_COUNT = 55368
EXPECTED_VESSELS = 5269


def workload(spark, factor):

    path = f"file://{DATA / f'x{factor}'}"

    df = (
        spark.read.parquet(path)
        .select("mmsi", "gap_hours")
        .filter(
            F.col("mmsi").isNotNull()
            & F.col("gap_hours").isNotNull()
            & (~F.isnan("gap_hours"))
            & (F.col("gap_hours") > 0)
        )
    )

    profiles = (
        df.groupBy("mmsi")
        .agg(
            F.count("gap_hours").alias("nb_interruptions"),
            F.avg("gap_hours").alias("duree_moyenne_h"),
            F.max("gap_hours").alias("duree_max_h"),
            F.sum("gap_hours").alias("duree_totale_h")
        )
        .orderBy(
            F.desc("nb_interruptions"),
            F.asc("mmsi")
        )
    )

    return profiles.toPandas()


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--engine",
        choices=["spark_local", "spark_standalone"],
        required=True
    )

    parser.add_argument(
        "--factors",
        nargs="+",
        type=int,
        default=[1, 10, 50, 100]
    )

    parser.add_argument("--repetitions", type=int, default=3)

    args = parser.parse_args()

    RESULTS.mkdir(parents=True, exist_ok=True)

    startup = time.perf_counter()

    spark = (
        SparkSession.builder
        .appName(f"DarkSea-Benchmark-{args.engine}")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

    startup_s = time.perf_counter() - startup

    spark.sparkContext.setLogLevel("ERROR")

    try:

        master = spark.sparkContext.master

        if args.engine == "spark_local":
            assert master == "local[2]"
        else:
            assert master.startswith("spark://")

        print("=== BENCHMARK SPARK ===")
        print("Version :", spark.version)
        print("Master :", master)
        print("Startup :", startup_s)

        print("Warm-up...")
        workload(spark, 1)

        for factor in args.factors:

            expected = BASE_COUNT * factor

            source_rows = spark.read.parquet(
                f"file://{DATA / f'x{factor}'}"
            ).count()

            assert source_rows == expected

            times = []

            for run in range(args.repetitions):

                start = time.perf_counter()
                profiles = workload(spark, factor)
                elapsed = time.perf_counter() - start

                assert len(profiles) == EXPECTED_VESSELS

                assert int(
                    profiles["nb_interruptions"].sum()
                ) == expected

                times.append(elapsed)

                print(
                    f"x{factor} | run {run + 1} | "
                    f"{elapsed:.4f} s",
                    flush=True
                )

            median = statistics.median(times)

            result = {
                "engine": args.engine,
                "master": master,
                "factor": factor,
                "rows": expected,
                "profiles": len(profiles),
                "times_s": times,
                "median_s": median,
                "throughput_rows_s": expected / median,
                "spark_session_startup_s": startup_s,
                "validation_passed": True
            }

            profiles.to_csv(
                RESULTS / f"{args.engine}_x{factor}_profiles.csv",
                index=False
            )

            with open(
                RESULTS / f"{args.engine}_x{factor}.json",
                "w",
                encoding="utf-8"
            ) as f:
                json.dump(result, f, indent=2)

            print(json.dumps(result, indent=2), flush=True)

        print("BENCHMARK SPARK TERMINE")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
