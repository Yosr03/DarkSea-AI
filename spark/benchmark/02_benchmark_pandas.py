import argparse
import gc
import json
import resource
import statistics
import time
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
RESULTS = ROOT / "results"

BASE_COUNT = 55368
EXPECTED_VESSELS = 5269


def workload(factor):

    path = DATA / f"x{factor}"

    df = pd.read_parquet(
        path,
        columns=["mmsi", "gap_hours"]
    )

    df = df[
        df["mmsi"].notna()
        & df["gap_hours"].notna()
        & (df["gap_hours"] > 0)
    ]

    profiles = (
        df.groupby("mmsi", as_index=False)
        .agg(
            nb_interruptions=("gap_hours", "count"),
            duree_moyenne_h=("gap_hours", "mean"),
            duree_max_h=("gap_hours", "max"),
            duree_totale_h=("gap_hours", "sum")
        )
        .sort_values(
            ["nb_interruptions", "mmsi"],
            ascending=[False, True]
        )
        .reset_index(drop=True)
    )

    return profiles


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--factors",
        nargs="+",
        type=int,
        default=[1, 10, 50, 100]
    )
    parser.add_argument("--repetitions", type=int, default=3)
    args = parser.parse_args()

    RESULTS.mkdir(parents=True, exist_ok=True)

    print("=== BENCHMARK PANDAS ===")
    print("Version :", pd.__version__)

    print("Warm-up...")
    workload(1)
    gc.collect()

    for factor in args.factors:

        expected = BASE_COUNT * factor

        source_rows = sum(
            pq.ParquetFile(p).metadata.num_rows
            for p in (DATA / f"x{factor}").glob("*.parquet")
        )

        assert source_rows == expected

        times = []

        for run in range(args.repetitions):

            gc.collect()

            start = time.perf_counter()
            profiles = workload(factor)
            elapsed = time.perf_counter() - start

            assert len(profiles) == EXPECTED_VESSELS
            assert int(profiles["nb_interruptions"].sum()) == expected

            times.append(elapsed)

            print(
                f"x{factor} | run {run + 1} | "
                f"{elapsed:.4f} s",
                flush=True
            )

        median = statistics.median(times)

        result = {
            "engine": "pandas",
            "factor": factor,
            "rows": expected,
            "profiles": len(profiles),
            "times_s": times,
            "median_s": median,
            "throughput_rows_s": expected / median,
            "python_peak_rss_mib_cumulative":
                resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
            "validation_passed": True
        }

        profiles.to_csv(
            RESULTS / f"pandas_x{factor}_profiles.csv",
            index=False
        )

        with open(
            RESULTS / f"pandas_x{factor}.json",
            "w",
            encoding="utf-8"
        ) as f:
            json.dump(result, f, indent=2)

        print(json.dumps(result, indent=2), flush=True)

    print("BENCHMARK PANDAS TERMINE")


if __name__ == "__main__":
    main()
