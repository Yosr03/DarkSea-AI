import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


BASE = Path(__file__).resolve().parent
ROOT = BASE / "results" / "memory"

ENGINES = [
    "pandas",
    "spark_local",
    "spark_standalone",
]

COLUMNS = [
    "nb_interruptions",
    "duree_moyenne_h",
    "duree_max_h",
    "duree_totale_h",
]


def load_profiles(run_id):

    path = ROOT / run_id / "workload.csv"

    df = pd.read_csv(
        path,
        dtype={"mmsi": str}
    )

    if len(df) != 5269:
        raise ValueError("Invalid profile count")

    if df["mmsi"].duplicated().any():
        raise ValueError("Duplicate MMSI")

    if df[["mmsi"] + COLUMNS].isna().any().any():
        raise ValueError("Missing values")

    if not np.isfinite(df[COLUMNS].to_numpy()).all():
        raise ValueError("Non-finite values")

    return df.sort_values("mmsi").reset_index(drop=True)


def compare_profiles(reference, candidate):

    if not reference["mmsi"].equals(candidate["mmsi"]):
        raise ValueError("MMSI mismatch")

    counts_equal = (
        reference["nb_interruptions"]
        == candidate["nb_interruptions"]
    ).to_numpy()

    numeric_equal = np.isclose(
        reference[COLUMNS[1:]].to_numpy(),
        candidate[COLUMNS[1:]].to_numpy(),
        rtol=1e-8,
        atol=1e-6,
    ).all(axis=1)

    matches = counts_equal & numeric_equal

    return {
        "profiles": len(reference),
        "matching_profiles": int(matches.sum()),
        "different_profiles": int((~matches).sum()),
        "concordance_pct": float(matches.mean() * 100),
        "validation_passed": bool(matches.all()),
    }


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        required=True,
    )

    args = parser.parse_args()

    source = Path(args.input)

    runs = json.loads(
        source.read_text(encoding="utf-8")
    )

    results = []

    factors = sorted({
        run["factor"] for run in runs
    })

    for factor in factors:

        selected = [
            run for run in runs
            if run["factor"] == factor
        ]

        if {r["engine"] for r in selected} != set(ENGINES):
            raise ValueError(f"Missing engine for x{factor}")

        reference_run = next(
            run for run in selected
            if run["engine"] == "pandas"
        )

        reference = load_profiles(
            reference_run["run_id"]
        )

        for run in selected:

            candidate = load_profiles(run["run_id"])

            expected_rows = 55368 * factor

            if int(candidate["nb_interruptions"].sum()) != expected_rows:
                raise ValueError("Unexpected row count")

            comparison = compare_profiles(
                reference,
                candidate,
            )

            record = {
                "engine": run["engine"],
                "factor": factor,
                "repetition": run["repetition"],
                "run_id": run["run_id"],
                **comparison,
            }

            results.append(record)

            print(
                f"{run['engine']} x{factor} "
                f"r{run['repetition']}: "
                f"{comparison['concordance_pct']:.2f}%"
            )

    passed = all(
        row["validation_passed"]
        for row in results
    )

    report = {
        "total_runs": len(results),
        "validation_passed": passed,
        "results": results,
    }

    output = source.with_name(
        source.stem + "_validation.json"
    )

    output.write_text(
        json.dumps(report, indent=2),
        encoding="utf-8"
    )

    print(f"\nReport: {output}")

    if not passed:
        raise SystemExit("VALIDATION FAILED")

    print("VALIDATION PASSED")


if __name__ == "__main__":
    main()