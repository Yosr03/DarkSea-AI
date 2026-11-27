
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from components.layout import configure_page, page_header, format_number
from services.data_service import load_dataset, load_metrics

configure_page("Analyse des navires")

page_header(
    "Analyse des navires",
    "Profils comportementaux et historique des interruptions AIS"
)

try:
    profiles = load_dataset("profils_scores").copy()
    events = load_dataset("evenements_scores").copy()
    metrics = load_metrics("kmeans_metrics")
except Exception as exc:
    st.error(f"Erreur de chargement : {exc}")
    st.stop()

threshold = float(metrics["anomaly_threshold"])

profiles["mmsi"] = profiles["mmsi"].astype(str)
events["mmsi"] = events["mmsi"].astype(str)

# ==================================================
# RECHERCHE ET FILTRES
# ==================================================

st.subheader("Recherche d'un profil maritime")

c1, c2 = st.columns(2)

with c1:
    status = st.selectbox(
        "Statut du profil",
        ["Tous", "Atypiques", "Non signalés"]
    )

with c2:
    cluster = st.selectbox(
        "Cluster",
        ["Tous"] + sorted(profiles["cluster"].unique().tolist())
    )

available = profiles.copy()

if status == "Atypiques":
    available = available[available["anomalie_potentielle"]]
elif status == "Non signalés":
    available = available[~available["anomalie_potentielle"]]

if cluster != "Tous":
    available = available[available["cluster"] == cluster]

available = available.sort_values(
    "distance_centroide",
    ascending=False
)

if available.empty:
    st.warning("Aucun profil disponible.")
    st.stop()

selected_mmsi = st.selectbox(
    "Sélectionner ou rechercher un MMSI",
    available["mmsi"].tolist()
)

profile = available[
    available["mmsi"] == selected_mmsi
].iloc[0]

history = events[
    events["mmsi"] == selected_mmsi
].copy()

history["date"] = pd.to_datetime(
    history["gap_start_timestamp"],
    utc=True,
    errors="coerce"
)

history = history.sort_values("date")

# ==================================================
# FICHE DU NAVIRE
# ==================================================

st.subheader(f"Profil maritime — {selected_mmsi}")

if bool(profile["anomalie_potentielle"]):
    st.warning("Profil comportemental atypique selon KMeans.")
else:
    st.success("Profil non signalé par le modèle KMeans.")

c1, c2, c3, c4 = st.columns(4)

c1.metric(
    "Interruptions",
    format_number(int(profile["nb_interruptions"]))
)

c2.metric(
    "Durée moyenne",
    f'{profile["duree_moyenne_h"]:.2f} h'
)

c3.metric(
    "Durée maximale",
    f'{profile["duree_max_h"]:.2f} h'
)

c4.metric(
    "Durée cumulée",
    f'{profile["duree_totale_h"] / 24:.2f} j'
)

c1, c2, c3, c4 = st.columns(4)

c1.metric("Cluster", int(profile["cluster"]))
c2.metric("Fréquence", profile["classe_frequence"])
c3.metric("Score ML", f'{profile["distance_centroide"]:.4f}')
c4.metric(
    "Événements atypiques",
    int(history["anomalie_potentielle"].sum())
)

if bool(profile["mmsi_format_atypique"]):
    st.warning("Indicateur qualité : format MMSI atypique.")

if bool(profile["duree_extreme"]):
    st.warning("Indicateur qualité : interruption de durée extrême.")

# ==================================================
# SCORE D'ANOMALIE
# ==================================================

left, right = st.columns(2)

with left:
    score = float(profile["distance_centroide"])
    max_score = max(
        float(profiles["distance_centroide"].max()),
        threshold
    ) * 1.1

    gauge = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score,
        title={"text": "Distance au centroïde"},
        number={"valueformat": ".4f"},
        gauge={
            "axis": {"range": [0, max_score]},
            "bar": {"color": "#22D3EE"},
            "steps": [
                {"range": [0, threshold], "color": "#164E63"},
                {"range": [threshold, max_score], "color": "#7C2D12"}
            ],
            "threshold": {
                "line": {"color": "#F97316", "width": 4},
                "thickness": 0.8,
                "value": threshold
            }
        }
    ))

    gauge.update_layout(
        template="plotly_dark",
        height=300,
        margin=dict(l=30, r=30, t=60, b=20)
    )

    st.plotly_chart(gauge, use_container_width=True)
    st.caption(f"Seuil P95 de validation : {threshold:.4f}")

with right:
    cluster_data = profiles[
        profiles["cluster"] == profile["cluster"]
    ]

    comparison = pd.DataFrame({
        "Indicateur": [
            "Interruptions",
            "Durée moyenne (h)",
            "Durée maximale (h)",
            "Score ML"
        ],
        "Navire": [
            profile["nb_interruptions"],
            profile["duree_moyenne_h"],
            profile["duree_max_h"],
            profile["distance_centroide"]
        ],
        "Moyenne du cluster": [
            cluster_data["nb_interruptions"].mean(),
            cluster_data["duree_moyenne_h"].mean(),
            cluster_data["duree_max_h"].mean(),
            cluster_data["distance_centroide"].mean()
        ]
    })

    st.markdown("**Comparaison avec le cluster**")

    st.dataframe(
        comparison.round(3),
        use_container_width=True,
        hide_index=True
    )

# ==================================================
# HISTORIQUE DES INTERRUPTIONS
# ==================================================

st.subheader("Historique des interruptions AIS")

if history.empty:
    st.warning("Aucun événement historique disponible.")
    st.stop()

history["statut"] = history["anomalie_potentielle"].map({
    True: "Atypique",
    False: "Non signalé"
})

fig = px.scatter(
    history,
    x="date",
    y="gap_hours",
    color="statut",
    hover_data=[
        "gap_id",
        "pavillon",
        "distance_centroide"
    ],
    color_discrete_map={
        "Atypique": "#F97316",
        "Non signalé": "#22D3EE"
    },
    labels={
        "date": "Date UTC",
        "gap_hours": "Durée (heures)"
    },
    title="Interruptions observées dans le temps",
    template="plotly_dark"
)

fig.update_traces(marker=dict(size=9))

st.plotly_chart(fig, use_container_width=True)

# ==================================================
# LOCALISATION
# ==================================================

st.subheader("Localisation des interruptions")

geo_fig = px.scatter_geo(
    history,
    lat="latitude",
    lon="longitude",
    color="statut",
    hover_data=[
        "gap_start_timestamp",
        "gap_hours",
        "distance_centroide"
    ],
    projection="natural earth",
    color_discrete_map={
        "Atypique": "#F97316",
        "Non signalé": "#22D3EE"
    }
)

geo_fig.update_geos(
    showland=True,
    landcolor="#263449",
    showocean=True,
    oceancolor="#0B1120",
    showcountries=True,
    countrycolor="#475569",
    bgcolor="#0B1120"
)

geo_fig.update_layout(
    template="plotly_dark",
    height=450,
    margin=dict(l=0, r=0, t=0, b=0)
)

st.plotly_chart(geo_fig, use_container_width=True)

st.caption(
    "Les points représentent les positions de début "
    "des interruptions et non une trajectoire continue."
)

# ==================================================
# TABLE ET EXPORT
# ==================================================

st.subheader("Données historiques détaillées")

columns = [
    "gap_id",
    "gap_start_timestamp",
    "gap_end_timestamp",
    "pavillon",
    "vessel_class",
    "gap_hours",
    "deplacement_km",
    "vitesse_apparente_kmh",
    "distance_centroide",
    "statut"
]

st.dataframe(
    history[columns],
    use_container_width=True,
    hide_index=True
)

st.download_button(
    "Exporter l'historique du navire",
    data=history[columns].to_csv(index=False).encode("utf-8-sig"),
    file_name=f"darksea_navire_{selected_mmsi}.csv",
    mime="text/csv"
)

st.info(
    "Le score mesure un écart statistique au comportement "
    "du cluster. Il ne représente pas une probabilité "
    "de fraude ou d'activité illégale."
)