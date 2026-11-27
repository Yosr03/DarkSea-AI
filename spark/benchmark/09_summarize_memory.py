import argparse
import json
from pathlib import Path
from itertools import product

import numpy as np
import pandas as pd


ENGINES = [
    "pandas",
    "spark_local",
    "spark_standalone",
]

FACTORS = [1, 10, 50, 100]

METRICS = [
    "baseline_mib",
    "peak_total_mib",
    "peak_incremental_mib",
    "mean_workload_mib",
    "elapsed_s",
    "wall_time_s",
    "throughput_rows_s",
]


def validate_runs(runs, validation):

    df = pd.DataFrame(runs)

    if len(df) != 36:
        raise ValueError("Expected exactly 36 runs")

    expected = set(
        product(ENGINES, FACTORS, [1, 2, 3])
    )

    actual = set(
        df[["engine", "factor", "repetition"]]
        .itertuples(index=False, name=None)
    )

    if actual != expected:
        raise ValueError("Invalid experiment coverage")

    if df["run_id"].duplicated().any():
        raise ValueError("Duplicate run ID")

    if not (df["rows"] == 55368 * df["factor"]).all():
        raise ValueError("Invalid row count")

    if not (df["profiles"] == 5269).all():
        raise ValueError("Invalid profile count")

    if not np.isfinite(df[METRICS].to_numpy()).all():
        raise ValueError("Non-finite metrics")

    if (df[METRICS] < 0).any().any():
        raise ValueError("Negative metrics")

    if (df["elapsed_s"] <= 0).any():
        raise ValueError("Invalid execution time")

    if (df["wall_time_s"] < df["elapsed_s"]).any():
        raise ValueError("Invalid wall time")

    if (df["baseline_samples"] < 20).any():
        raise ValueError("Insufficient baseline samples")

    if (df["workload_samples"] <= 0).any():
        raise ValueError("Missing workload samples")

    expected_increment = (
        df["peak_total_mib"] - df["baseline_mib"]
    ).clip(lower=0)

    if not np.allclose(
        expected_increment,
        df["peak_incremental_mib"],
        atol=1e-6,
        rtol=0,
    ):
        raise ValueError("Inconsistent memory metrics")

    if not validation["validation_passed"]:
        raise ValueError("Numerical validation failed")

    checks = validation["results"]

    if len(checks) != 36:
        raise ValueError("Incomplete validation")

    if validation["total_runs"] != 36:
        raise ValueError("Invalid validation count")

    if {r["run_id"] for r in checks} != set(df["run_id"]):
        raise ValueError("Validation run ID mismatch")

    for row in checks:

        if not row["validation_passed"]:
            raise ValueError("Invalid profile comparison")

        if row["concordance_pct"] != 100:
            raise ValueError("Numerical differences")

        if row["different_profiles"] != 0:
            raise ValueError("Different profiles")

    return df


def summarize(df):

    records = []

    for (engine, factor), group in df.groupby(
        ["engine", "factor"]
    ):

        record = {
            "engine": engine,
            "factor": int(factor),
            "rows": int(group["rows"].iloc[0]),
            "profiles": int(group["profiles"].iloc[0]),
            "repetitions": len(group),
            "concordance_pct": 100.0,
            "validation_passed": True,
        }

        for metric in METRICS:

            values = group[metric]

            record[f"{metric}_median"] = float(
                values.median()
            )

            record[f"{metric}_mean"] = float(
                values.mean()
            )

            record[f"{metric}_min"] = float(
                values.min()
            )

            record[f"{metric}_max"] = float(
                values.max()
            )

            record[f"{metric}_std"] = float(
                values.std(ddof=1)
            )

        record["peak_incremental_samples_mib"] = (
            group.sort_values("repetition")
            ["peak_incremental_mib"]
            .tolist()
        )

        record["min_workload_samples"] = int(
            group["workload_samples"].min()
        )

        records.append(record)

    return records


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True)

    args = parser.parse_args()

    source = Path(args.input)

    validation_path = source.with_name(
        source.stem + "_validation.json"
    )

    runs = json.loads(
        source.read_text(encoding="utf-8")
    )

    validation = json.loads(
        validation_path.read_text(encoding="utf-8")
    )

    df = validate_runs(runs, validation)

    records = summarize(df)

    summary = {
        "schema_version": 1,
        "source": source.name,
        "total_runs": len(df),
        "total_configurations": len(records),
        "total_rows_processed": int(df["rows"].sum()),
        "validation_passed": True,
        "concordance_pct": 100.0,
        "sampling_interval_ms": 100,
        "memory_scope": "Spark Master + Spark Worker",
        "memory_measure": "cgroup v2 working set",
        "baseline_duration_s": 5,
        "results": records,
    }

    json_path = source.parent / (
        "memory_benchmark_summary.json"
    )

    csv_path = source.parent / (
        "memory_benchmark_summary.csv"
    )

    json_path.write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )

    pd.DataFrame(records).to_csv(
        csv_path,
        index=False,
    )

    print("\nMEMORY BENCHMARK SUMMARY")

    print(
        pd.DataFrame(records)[[
            "engine",
            "factor",
            "rows",
            "peak_total_mib_median",
            "peak_incremental_mib_median",
            "elapsed_s_median",
        ]].round(3).to_string(index=False)
    )

    print(f"\nTotal runs: {len(df)}")
    print(f"Configurations: {len(records)}")
    print(f"Total rows: {df['rows'].sum()}")
    print(f"JSON: {json_path}")
    print(f"CSV: {csv_path}")
    print("SUMMARY VALIDATED")


if __name__ == "__main__":
    main()