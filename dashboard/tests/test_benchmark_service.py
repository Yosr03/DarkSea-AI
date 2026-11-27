import pytest

from services.benchmark_service import (
    load_benchmark,
    validate_benchmark,
    get_benchmark_overview
)


def test_load_benchmark():
    assert len(load_benchmark()) == 12


def test_engine_count():
    assert load_benchmark()["engine"].nunique() == 3


def test_max_volume():
    assert load_benchmark()["rows"].max() == 5536800


def test_experiment_count():
    overview = get_benchmark_overview(load_benchmark())
    assert overview["experiments"] == 36


def test_concordance():
    assert load_benchmark()["profiles_concordance"].eq(1).all()


def test_missing_configuration():
    df = load_benchmark().iloc[:-1].copy()

    with pytest.raises(ValueError):
        validate_benchmark(df)


def test_invalid_duration():
    df = load_benchmark().copy()
    df.loc[0, "median_s"] = -1

    with pytest.raises(ValueError):
        validate_benchmark(df)


def test_invalid_volume():
    df = load_benchmark().copy()
    df.loc[0, "rows"] = 999

    with pytest.raises(ValueError):
        validate_benchmark(df)
