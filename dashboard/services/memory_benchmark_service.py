import json
from itertools import product
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st


BASE_DIR = Path(__file__).resolve().parents[1]

SUMMARY_PATH = (
    BASE_DIR
    / "data"
    / "benchmark"
    / "memory_benchmark_summary.json"
)

ENGINES = {
    "pandas": "Pandas",
    "spark_local": "Spark Local",
    "spark_standalone": "Spark Standalone",
}

FACTORS = [1, 10, 50, 100]


def validate_summary(summary):

    if summary["schema_version"] != 1:
        raise ValueError("Unsupported schema version")

    if summary["total_runs"] != 36:
        raise ValueError("Invalid run count")

    if summary["total_configurations"] != 12:
        raise ValueError("Invalid configuration count")

    if not summary["validation_passed"]:
        raise ValueError("Benchmark not validated")

    if summary["concordance_pct"] != 100:
        raise ValueError("Numerical concordance failed")

    df = pd.DataFrame(summary["results"])

    if len(df) != 12:
        raise ValueError("Incomplete summary")

    expected = set(product(ENGINES, FACTORS))

    actual = set(
        df[["engine", "factor"]]
        .itertuples(index=False, name=None)
    )

    if actual != expected:
        raise ValueError("Invalid experiment coverage")

    if not (df["repetitions"] == 3).all():
        raise ValueError("Invalid repetition count")

    if not (df["rows"] == df["factor"] * 55368).all():
        raise ValueError("Invalid dataset size")

    if not (df["profiles"] == 5269).all():
        raise ValueError("Invalid profile count")

    if not df["validation_passed"].all():
        raise ValueError("Invalid configuration")

    if not (df["concordance_pct"] == 100).all():
        raise ValueError("Invalid concordance")

    if int((df["rows"] * df["repetitions"]).sum()) != (
        summary["total_rows_processed"]
    ):
        raise ValueError("Invalid cumulative row count")

    metrics = [
        "baseline_mib_median",
        "peak_total_mib_median",
        "peak_incremental_mib_median",
        "mean_workload_mib_median",
        "elapsed_s_median",
        "wall_time_s_median",
    ]

    if not np.isfinite(df[metrics].to_numpy()).all():
        raise ValueError("Non-finite metrics")

    if (df[metrics] < 0).any().any():
        raise ValueError("Negative metrics")

    for _, row in df.iterrows():

        samples = np.asarray(
            row["peak_incremental_samples_mib"],
            dtype=float,
        )

        if len(samples) != 3:
            raise ValueError("Invalid memory samples")

        if not np.isfinite(samples).all():
            raise ValueError("Non-finite samples")

        if (samples < 0).any():
            raise ValueError("Negative memory samples")

        if not np.isclose(
            np.median(samples),
            row["peak_incremental_mib_median"],
        ):
            raise ValueError("Invalid median")

    df["engine_label"] = df["engine"].map(ENGINES)

    df["volume"] = df["factor"].map(
        lambda factor: f"x{factor}"
    )

    return df.sort_values(
        ["factor", "engine"]
    ).reset_index(drop=True)


@st.cache_data(ttl=300, show_spinner=False)
def load_memory_benchmark():

    summary = json.loads(
        SUMMARY_PATH.read_text(encoding="utf-8")
    )

    df = validate_summary(summary)

    return summary, df


def build_repetition_dataframe(df):

    records = []

    for _, row in df.iterrows():

        for repetition, value in enumerate(
            row["peak_incremental_samples_mib"],
            start=1,
        ):

            records.append({
                "engine": row["engine"],
                "engine_label": row["engine_label"],
                "factor": row["factor"],
                "volume": row["volume"],
                "repetition": repetition,
                "memory_mib": value,
            })

    return pd.DataFrame(records)