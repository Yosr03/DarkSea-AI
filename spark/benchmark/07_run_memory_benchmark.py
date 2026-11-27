import argparse
import csv
import json
import random
import statistics
import subprocess
import time
import uuid
from pathlib import Path


BASE = Path(__file__).resolve().parent
ROOT = BASE / "results" / "memory"
REMOTE = "/workspace/spark/benchmark"
MASTER = "darksea-spark-master"
WORKER = "darksea-spark-worker"

ENGINES = [
    "pandas",
    "spark_local",
    "spark_standalone",
]


def remote_path(path):
    return f"{REMOTE}/{path.relative_to(BASE).as_posix()}"


def workload_command(engine, factor, output):

    script = f"{REMOTE}/05_memory_workload.py"

    command = ["docker", "exec", MASTER]

    if engine == "pandas":
        command += ["python3"]

    elif engine == "spark_local":
        command += [
            "/opt/spark/bin/spark-submit",
            "--master", "local[2]",
            "--driver-memory", "1g",
        ]

    else:
        command += [
            "/opt/spark/bin/spark-submit",
            "--master", "spark://spark-master:7077",
            "--deploy-mode", "client",
            "--driver-memory", "1g",
            "--executor-memory", "1g",
            "--executor-cores", "2",
            "--total-executor-cores", "2",
            "--conf", "spark.driver.host=spark-master",
            "--conf", "spark.driver.bindAddress=0.0.0.0",
        ]

    return command + [
        script,
        "--engine", engine,
        "--factor", str(factor),
        "--output", remote_path(output),
    ]


def start_sampler(container, output, stop):

    return subprocess.Popen([
        "docker", "exec", container,
        "python3", f"{REMOTE}/06_memory_sampler.py",
        "--output", remote_path(output),
        "--stop-file", remote_path(stop),
        "--interval", "0.1",
        "--max-seconds", "900",
    ])


def read_samples(path):

    with path.open(newline="") as file:
        return [
            {
                "timestamp": float(row["timestamp"]),
                "memory": int(row["working_set_bytes"]),
            }
            for row in csv.DictReader(file)
        ]


def synchronize(master, worker):

    pairs = []
    j = 0

    for m in master:

        while (
            j + 1 < len(worker)
            and abs(
                worker[j + 1]["timestamp"] - m["timestamp"]
            ) < abs(
                worker[j]["timestamp"] - m["timestamp"]
            )
        ):
            j += 1

        w = worker[j]

        if abs(w["timestamp"] - m["timestamp"]) <= 0.1:

            pairs.append({
                "timestamp": (
                    m["timestamp"] + w["timestamp"]
                ) / 2,
                "memory": m["memory"] + w["memory"],
            })

    return pairs


def mib(value):
    return value / (1024 ** 2)


def run_one(engine, factor, repetition):

    run_id = (
        f"{engine}_x{factor}_r{repetition}_"
        f"{uuid.uuid4().hex[:8]}"
    )

    folder = ROOT / run_id
    folder.mkdir(parents=True)

    master_csv = folder / "master.csv"
    worker_csv = folder / "worker.csv"
    stop = folder / "sampler.stop"
    output = folder / "workload.json"

    print(f"\n=== {run_id} ===", flush=True)

    samplers = []

    try:
        samplers.append(
            start_sampler(MASTER, master_csv, stop)
        )

        samplers.append(
            start_sampler(WORKER, worker_csv, stop)
        )

        deadline = time.monotonic() + 20

        while True:

            if any(p.poll() is not None for p in samplers):
                raise RuntimeError("Sampler stopped unexpectedly")

            if all(
                p.exists() and p.stat().st_size > 150
                for p in [master_csv, worker_csv]
            ):
                break

            if time.monotonic() > deadline:
                raise RuntimeError("Samplers not ready")

            time.sleep(0.2)

        baseline_start = time.time()
        time.sleep(5)

        workload_start = time.time()
        clock_start = time.perf_counter()

        with (folder / "workload.log").open("w") as log:

            subprocess.run(
                workload_command(engine, factor, output),
                stdout=log,
                stderr=subprocess.STDOUT,
                check=True,
            )

        workload_end = time.time()

        wall_time = time.perf_counter() - clock_start

        time.sleep(3)

    finally:

        stop.touch()

        for process in samplers:
            process.wait(timeout=20)

    if any(p.returncode != 0 for p in samplers):
        raise RuntimeError("Memory sampling failed")

    master = read_samples(master_csv)
    worker = read_samples(worker_csv)

    if not master or not worker:
        raise RuntimeError("Missing memory samples")

    samples = synchronize(master, worker)

    baseline = [
        s["memory"] for s in samples
        if baseline_start <= s["timestamp"] < workload_start
    ]

    workload = [
        s["memory"] for s in samples
        if workload_start <= s["timestamp"] <= workload_end
    ]

    if len(baseline) < 20 or not workload:
        raise RuntimeError("Insufficient synchronized samples")

    baseline_memory = statistics.median(baseline)
    peak_memory = max(workload)

    metrics = json.loads(output.read_text(encoding="utf-8"))

    metrics.update({
        "run_id": run_id,
        "repetition": repetition,
        "wall_time_s": wall_time,
        "baseline_mib": mib(baseline_memory),
        "peak_total_mib": mib(peak_memory),
        "peak_incremental_mib": mib(
            max(0, peak_memory - baseline_memory)
        ),
        "mean_workload_mib": mib(
            statistics.mean(workload)
        ),
        "baseline_samples": len(baseline),
        "workload_samples": len(workload),
        "master_samples": len(master),
        "worker_samples": len(worker),
    })

    (folder / "metrics.json").write_text(
        json.dumps(metrics, indent=2),
        encoding="utf-8"
    )

    print(json.dumps(metrics, indent=2), flush=True)

    return metrics


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--factors",
        nargs="+",
        type=int,
        default=[1, 10, 50, 100],
        choices=[1, 10, 50, 100],
    )

    parser.add_argument(
        "--repetitions",
        type=int,
        default=3,
    )

    args = parser.parse_args()

    if args.repetitions < 1:
        raise ValueError("Invalid repetitions")

    ROOT.mkdir(parents=True, exist_ok=True)

    plan = [
        (engine, factor, repetition)
        for repetition in range(1, args.repetitions + 1)
        for factor in args.factors
        for engine in ENGINES
    ]

    random.Random(42).shuffle(plan)

    results = []

    summary = ROOT / (
        f"runs_{uuid.uuid4().hex[:8]}.json"
    )

    for index, (engine, factor, repetition) in enumerate(
        plan, start=1
    ):

        print(f"\nRun {index}/{len(plan)}", flush=True)

        time.sleep(5)

        results.append(
            run_one(engine, factor, repetition)
        )

        summary.write_text(
            json.dumps(results, indent=2),
            encoding="utf-8",
        )

    print(f"\nCompleted: {len(results)} runs")
    print(f"Results: {summary}")


if __name__ == "__main__":
    main()