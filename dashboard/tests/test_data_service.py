import sys
from pathlib import Path

import pytest


sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1])
)

from services.data_service import (
    load_dataset,
    load_metrics,
    get_overview,
)


@pytest.mark.parametrize(
    "name, expected_count",
    [
        ("profils_scores", 5269),
        ("evenements_scores", 55368),
        ("profils_enrichis", 5269),
        ("statistiques_globales", 1),
        ("anomalies_ml", 327),
        ("synthese_clusters", 2),
        ("comparaison_regles_ml", 4),
        ("zones_maritimes", 994),
        ("statistiques_mensuelles", 36),
        ("statistiques_pavillons", 102),
        ("statistiques_types_navires", 5),
        ("statistiques_distance_cote", 3),
    ],
)
def test_dataset_count(name, expected_count):

    data = load_dataset(name)

    assert len(data) == expected_count


@pytest.mark.parametrize(
    "name",
    [
        "zones_maritimes",
        "statistiques_mensuelles",
        "statistiques_pavillons",
        "statistiques_types_navires",
        "statistiques_distance_cote",
    ],
)
def test_aggregations(name):

    data = load_dataset(name)

    assert data["nb_interruptions"].sum() == 55368


def test_unique_events():

    events = load_dataset("evenements_scores")

    assert events["gap_id"].nunique() == 55368


def test_unique_profiles():

    profiles = load_dataset("profils_scores")

    assert profiles["mmsi"].nunique() == 5269


def test_overview():

    overview = get_overview()

    assert overview["total_events"] == 55368
    assert overview["total_vessels"] == 5269
    assert overview["profile_anomalies"] == 327
    assert overview["event_anomalies"] == 2742
    assert overview["total_zones"] == 994


def test_streaming_metrics():

    metrics = load_metrics("streaming_validation")

    assert metrics["validation_passed"] is True
    assert metrics["streaming_events"] == 11123
    assert metrics["total_alerts"] == 551
    assert metrics["duplicates"] == 0
    assert metrics["missing_events"] == 0
    assert metrics["score_differences"] == 0


def test_models():

    profiles = load_metrics("kmeans_metrics")
    events = load_metrics("event_kmeans_metrics")

    assert profiles["best_k"] == 2
    assert events["best_k"] == 3