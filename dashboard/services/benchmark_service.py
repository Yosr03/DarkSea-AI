import json
from pathlib import Path

import pandas as pd
import streamlit as st

DATA_DIR = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "benchmark"
)

ENGINE_NAMES = {
    "pandas": "Pandas",
    "spark_local": "Spark Local",
    "spark_standalone": "Spark Standalone"
}

EXPECTED_FACTORS = {1, 10, 50, 100}


def validate_benchmark(df):

    required = {
        "engine",
        "factor",
        "rows",
        "profiles",
        "times_s",
        "median_s",
        "throughput_rows_s",
        "speedup_vs_pandas",
        "profiles_concordance",
        "validation_passed"
    }

    if not required.issubset(df.columns):
        raise ValueError("Colonnes du benchmark manquantes.")

    expected = {
        (engine, factor)
        for engine in ENGINE_NAMES
        for factor in EXPECTED_FACTORS
    }

    actual = set(zip(df["engine"], df["factor"]))

    if len(df) != 12 or actual != expected:
        raise ValueError("Configurations incompletes ou dupliquees.")

    if not (df["rows"] == df["factor"] * 55368).all():
        raise ValueError("Volumes incorrects.")

    if not (df["profiles"] == 5269).all():
        raise ValueError("Nombre de profils incorrect.")

    if not (df["median_s"] > 0).all():
        raise ValueError("Durees invalides.")

    if not df["validation_passed"].eq(True).all():
        raise ValueError("Validation du benchmark echouee.")

    if not df["profiles_concordance"].eq(1.0).all():
        raise ValueError("Divergence des profils.")

    if not df["times_s"].apply(
        lambda values: len(values) == 3
        and all(v > 0 for v in values)
    ).all():
        raise ValueError("Repetitions invalides.")

    return True


@st.cache_data(ttl=300, show_spinner=False)
def load_benchmark():

    path = DATA_DIR / "benchmark_summary.json"

    with open(path, encoding="utf-8") as f:
        records = json.load(f)

    df = pd.DataFrame(records)

    validate_benchmark(df)

    df["engine_label"] = df["engine"].map(ENGINE_NAMES)
    df["volume"] = df["factor"].map(lambda x: f"x{x}")

    return df.sort_values(
        ["factor", "engine"]
    ).reset_index(drop=True)


def get_benchmark_overview(df):

    return {
        "max_rows": int(df["rows"].max()),
        "engines": int(df["engine"].nunique()),
        "experiments": int(df["times_s"].apply(len).sum()),
        "configurations": len(df),
        "concordance": float(
            df["profiles_concordance"].min() * 100
        )
    }
