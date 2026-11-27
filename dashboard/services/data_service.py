import json
from pathlib import Path

import pandas as pd
import streamlit as st


BASE_DIR = Path(__file__).resolve().parents[1]
HISTORICAL_DIR = BASE_DIR / "data" / "historical"
METRICS_DIR = BASE_DIR / "data" / "metrics"


PARQUET_DATASETS = {
    "profils_scores",
    "evenements_scores",
    "profils_enrichis",
    "statistiques_globales",
    "anomalies_ml",
    "synthese_clusters",
    "comparaison_regles_ml",
}


COMMON_COLUMNS = [
    "nb_interruptions",
    "nb_navires",
    "duree_moyenne_h",
    "duree_max_h",
    "duree_totale_h",
]


CSV_SCHEMAS = {
    "zones_maritimes": [
        "cellule_lat",
        "cellule_lon",
        *COMMON_COLUMNS,
    ],
    "statistiques_mensuelles": [
        "mois",
        *COMMON_COLUMNS,
    ],
    "statistiques_pavillons": [
        "pavillon",
        *COMMON_COLUMNS,
    ],
    "statistiques_types_navires": [
        "vessel_class",
        *COMMON_COLUMNS,
    ],
    "statistiques_distance_cote": [
        "zone_distance",
        *COMMON_COLUMNS,
    ],
}


@st.cache_data(ttl=300, show_spinner=False)
def load_dataset(name):

    directory = HISTORICAL_DIR / name

    if not directory.exists():
        raise FileNotFoundError(directory)

    files = sorted(
        file
        for file in directory.iterdir()
        if file.is_file()
        and not file.name.startswith(("_", "."))
    )

    if not files:
        raise ValueError(f"Dataset vide : {name}")

    if name in PARQUET_DATASETS:
        return pd.concat(
            [pd.read_parquet(file) for file in files],
            ignore_index=True,
        )

    if name in CSV_SCHEMAS:
        return pd.concat(
            [
                pd.read_csv(
                    file,
                    header=None,
                    names=CSV_SCHEMAS[name],
                )
                for file in files
            ],
            ignore_index=True,
        )

    raise ValueError(f"Dataset inconnu : {name}")


@st.cache_data(ttl=300, show_spinner=False)
def load_metrics(name):

    path = METRICS_DIR / f"{name}.json"

    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def get_overview():

    profiles = load_dataset("profils_scores")
    events = load_dataset("evenements_scores")
    zones = load_dataset("zones_maritimes")

    return {
        "total_events": len(events),
        "total_vessels": profiles["mmsi"].nunique(),
        "profile_anomalies": int(
            profiles["anomalie_potentielle"].sum()
        ),
        "event_anomalies": int(
            events["anomalie_potentielle"].sum()
        ),
        "total_zones": len(zones),
        "mean_gap_hours": float(
            events["gap_hours"].mean()
        ),
        "total_gap_hours": float(
            events["gap_hours"].sum()
        ),
    }