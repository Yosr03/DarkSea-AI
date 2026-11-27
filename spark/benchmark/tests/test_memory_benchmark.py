import importlib.util
import json
from pathlib import Path

import pytest


BASE = Path(__file__).resolve().parents[1]

SPEC = importlib.util.spec_from_file_location(
    "memory_summary",
    BASE / "09_summarize_memory.py",
)

module = importlib.util.module_from_spec(SPEC)

SPEC.loader.exec_module(module)


@pytest.fixture
def experiment():

    runs = []
    checks = []

    for engine in module.ENGINES:
        for factor in module.FACTORS:
            for repetition in [1, 2, 3]:

                run_id = (
                    f"{engine}_{factor}_{repetition}"
                )

                runs.append({
                    "run_id": run_id,
                    "engine": engine,
                    "factor": factor,
                    "repetition": repetition,
                    "rows": 55368 * factor,
                    "profiles": 5269,
                    "baseline_mib": 680.0,
                    "peak_total_mib": 780.0,
                    "peak_incremental_mib": 100.0,
                    "mean_workload_mib": 730.0,
                    "elapsed_s": 2.0,
                    "wall_time_s": 3.0,
                    "throughput_rows_s": (
                        55368 * factor / 2
                    ),
                    "baseline_samples": 50,
                    "workload_samples": 30,
                })

                checks.append({
                    "run_id": run_id,
                    "validation_passed": True,
                    "concordance_pct": 100.0,
                    "different_profiles": 0,
                })

    validation = {
        "validation_passed": True,
        "total_runs": 36,
        "results": checks,
    }

    return runs, validation


def test_complete_experiment(experiment):

    runs, validation = experiment

    df = module.validate_runs(runs, validation)

    assert len(df) == 36


def test_summary_configurations(experiment):

    runs, validation = experiment

    df = module.validate_runs(runs, validation)

    records = module.summarize(df)

    assert len(records) == 12

    assert all(
        r["repetitions"] == 3
        for r in records
    )


def test_median_memory(experiment):

    runs, validation = experiment

    df = module.validate_runs(runs, validation)

    records = module.summarize(df)

    assert all(
        r["peak_incremental_mib_median"] == 100
        for r in records
    )


def test_missing_run(experiment):

    runs, validation = experiment

    with pytest.raises(ValueError):
        module.validate_runs(runs[:-1], validation)


def test_duplicate_run(experiment):

    runs, validation = experiment

    runs[0]["run_id"] = runs[1]["run_id"]

    with pytest.raises(ValueError):
        module.validate_runs(runs, validation)


def test_invalid_row_count(experiment):

    runs, validation = experiment

    runs[0]["rows"] = 10

    with pytest.raises(ValueError):
        module.validate_runs(runs, validation)


def test_invalid_memory(experiment):

    runs, validation = experiment

    runs[0]["peak_incremental_mib"] = -10

    with pytest.raises(ValueError):
        module.validate_runs(runs, validation)


def test_inconsistent_memory(experiment):

    runs, validation = experiment

    runs[0]["peak_incremental_mib"] = 999

    with pytest.raises(ValueError):
        module.validate_runs(runs, validation)


def test_failed_concordance(experiment):

    runs, validation = experiment

    validation["validation_passed"] = False

    with pytest.raises(ValueError):
        module.validate_runs(runs, validation)


def test_insufficient_samples(experiment):

    runs, validation = experiment

    runs[0]["baseline_samples"] = 0

    with pytest.raises(ValueError):
        module.validate_runs(runs, validation)


def test_json_serialization(experiment):

    runs, validation = experiment

    df = module.validate_runs(runs, validation)

    records = module.summarize(df)

    assert json.loads(json.dumps(records)) == records