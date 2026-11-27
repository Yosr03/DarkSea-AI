import copy
import json
import sys
from pathlib import Path

import pytest


DASHBOARD = Path(__file__).resolve().parents[1]

sys.path.insert(0, str(DASHBOARD))

from services.memory_benchmark_service import (
    validate_summary,
    build_repetition_dataframe,
)


@pytest.fixture
def summary():

    path = (
        DASHBOARD
        / "data"
        / "benchmark"
        / "memory_benchmark_summary.json"
    )

    return json.loads(
        path.read_text(encoding="utf-8")
    )


def test_valid_summary(summary):

    df = validate_summary(summary)

    assert len(df) == 12


def test_maximum_volume(summary):

    df = validate_summary(summary)

    assert df["rows"].max() == 5536800


def test_repetitions(summary):

    df = validate_summary(summary)

    repetitions = build_repetition_dataframe(df)

    assert len(repetitions) == 36


def test_all_engines(summary):

    df = validate_summary(summary)

    assert df["engine"].nunique() == 3


def test_all_volumes(summary):

    df = validate_summary(summary)

    assert set(df["factor"]) == {1, 10, 50, 100}


def test_invalid_run_count(summary):

    invalid = copy.deepcopy(summary)

    invalid["total_runs"] = 35

    with pytest.raises(ValueError):
        validate_summary(invalid)


def test_invalid_validation(summary):

    invalid = copy.deepcopy(summary)

    invalid["validation_passed"] = False

    with pytest.raises(ValueError):
        validate_summary(invalid)


def test_missing_configuration(summary):

    invalid = copy.deepcopy(summary)

    invalid["results"].pop()

    with pytest.raises(ValueError):
        validate_summary(invalid)


def test_invalid_memory(summary):

    invalid = copy.deepcopy(summary)

    invalid["results"][0][
        "peak_incremental_mib_median"
    ] = -10

    with pytest.raises(ValueError):
        validate_summary(invalid)


def test_invalid_median(summary):

    invalid = copy.deepcopy(summary)

    invalid["results"][0][
        "peak_incremental_mib_median"
    ] = 99999

    with pytest.raises(ValueError):
        validate_summary(invalid)


def test_invalid_samples(summary):

    invalid = copy.deepcopy(summary)

    invalid["results"][0][
        "peak_incremental_samples_mib"
    ] = [1, 2]

    with pytest.raises(ValueError):
        validate_summary(invalid)


def test_invalid_concordance(summary):

    invalid = copy.deepcopy(summary)

    invalid["concordance_pct"] = 99

    with pytest.raises(ValueError):
        validate_summary(invalid)