import argparse
import json
import time
from pathlib import Path


def run_pandas(path):
    import pandas as pd

    start = time.perf_counter()

    df = pd.read_parquet(
        path,
        columns=["mmsi", "gap_hours"]
    )

    rows = len(df)

    result = (
        df.groupby("mmsi", as_index=False)
        .agg(
            nb_interruptions=("gap_hours", "count"),
            duree_moyenne_h=("gap_hours", "mean"),
            duree_max_h=("gap_hours", "max"),
            duree_totale_h=("gap_hours", "sum"),
        )
        .sort_values("mmsi")
    )

    elapsed = time.perf_counter() - start

    return result, rows, elapsed


def run_spark(path):
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (
        SparkSession.builder
        .appName("DarkSea-Memory-Benchmark")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        start = time.perf_counter()

        df = spark.read.parquet(str(path)).select(
            "mmsi", "gap_hours"
        )

        result = (
            df.groupBy("mmsi")
            .agg(
                F.count("gap_hours").alias("nb_interruptions"),
                F.avg("gap_hours").alias("duree_moyenne_h"),
                F.max("gap_hours").alias("duree_max_h"),
                F.sum("gap_hours").alias("duree_totale_h"),
            )
            .orderBy("mmsi")
            .toPandas()
        )

        elapsed = time.perf_counter() - start

        rows = int(result["nb_interruptions"].sum())

        return result, rows, elapsed

    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--engine",
        required=True,
        choices=["pandas", "spark_local", "spark_standalone"]
    )

    parser.add_argument(
        "--factor",
        type=int,
        required=True,
        choices=[1, 10, 50, 100]
    )

    parser.add_argument("--output", required=True)

    args = parser.parse_args()

    base = Path(__file__).resolve().parent
    path = base / "data" / f"x{args.factor}"

    if not path.exists():
        raise FileNotFoundError(path)

    if args.engine == "pandas":
        result, rows, elapsed = run_pandas(path)
    else:
        result, rows, elapsed = run_spark(path)

    expected_rows = 55368 * args.factor

    if rows != expected_rows:
        raise ValueError(
            f"Expected {expected_rows}, received {rows}"
        )

    if len(result) != 5269:
        raise ValueError("Unexpected number of profiles")

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    result.to_csv(
        output.with_suffix(".csv"),
        index=False
    )

    metrics = {
        "engine": args.engine,
        "factor": args.factor,
        "rows": rows,
        "profiles": len(result),
        "elapsed_s": elapsed,
        "throughput_rows_s": rows / elapsed,
    }

    output.write_text(
        json.dumps(metrics, indent=2),
        encoding="utf-8"
    )

    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()