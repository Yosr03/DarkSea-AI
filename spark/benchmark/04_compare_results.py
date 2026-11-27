import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
RESULTS = ROOT / "results"

ENGINES = [
    "pandas",
    "spark_local",
    "spark_standalone"
]

METRICS = [
    "duree_moyenne_h",
    "duree_max_h",
    "duree_totale_h"
]


def compare_profiles(reference, candidate):

    reference = reference.sort_values("mmsi").reset_index(drop=True)
    candidate = candidate.sort_values("mmsi").reset_index(drop=True)

    assert len(reference) == len(candidate)
    assert reference["mmsi"].is_unique
    assert candidate["mmsi"].is_unique

    assert reference["mmsi"].equals(candidate["mmsi"])

    np.testing.assert_array_equal(
        reference["nb_interruptions"],
        candidate["nb_interruptions"]
    )

    for column in METRICS:

        np.testing.assert_allclose(
            reference[column],
            candidate[column],
            rtol=1e-9,
            atol=1e-6
        )


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--factors",
        nargs="+",
        type=int,
        default=[1, 10, 50, 100]
    )
    args = parser.parse_args()

    summary = []

    for factor in args.factors:

        reference = pd.read_csv(
            RESULTS / f"pandas_x{factor}_profiles.csv",
            dtype={"mmsi": str}
        )

        with open(RESULTS / f"pandas_x{factor}.json") as f:
            baseline = json.load(f)

        for engine in ENGINES:

            profiles = pd.read_csv(
                RESULTS / f"{engine}_x{factor}_profiles.csv",
                dtype={"mmsi": str}
            )

            compare_profiles(reference, profiles)

            with open(
                RESULTS / f"{engine}_x{factor}.json"
            ) as f:
                result = json.load(f)

            assert result["validation_passed"]
            assert result["rows"] == 55368 * factor
            assert result["profiles"] == 5269

            result["speedup_vs_pandas"] = (
                baseline["median_s"] / result["median_s"]
            )

            result["profiles_concordance"] = 1.0

            summary.append(result)

            print(
                f"x{factor} | {engine} | "
                f"{result['median_s']:.4f} s | "
                f"speedup={result['speedup_vs_pandas']:.3f} | "
                "VALIDATION OK"
            )

    with open(
        RESULTS / "benchmark_summary.json",
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(summary, f, indent=2)

    table = pd.DataFrame(summary)

    columns = [
        "engine",
        "factor",
        "rows",
        "median_s",
        "throughput_rows_s",
        "speedup_vs_pandas",
        "profiles_concordance"
    ]

    table[columns].to_csv(
        RESULTS / "benchmark_summary.csv",
        index=False
    )

    print("\n=== RESULTATS COMPARATIFS ===")
    print(table[columns].round(4).to_string(index=False))

    print("\nVALIDATION COMPARATIVE REUSSIE")


if __name__ == "__main__":
    main()
