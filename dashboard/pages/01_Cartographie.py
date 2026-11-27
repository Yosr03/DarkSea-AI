
import pandas as pd
import plotly.express as px
import streamlit as st

from components.layout import configure_page, page_header, format_number
from services.data_service import load_dataset

configure_page("Cartographie maritime")

page_header(
    "Cartographie maritime",
    "Exploration géographique des interruptions AIS historiques"
)

try:
    df = load_dataset("evenements_scores").copy()
except Exception as exc:
    st.error(f"Impossible de charger les données : {exc}")
    st.stop()

df["date"] = pd.to_datetime(
    df["gap_start_timestamp"], utc=True, errors="coerce"
)
df["annee"] = df["date"].dt.year
df["mois"] = df["date"].dt.strftime("%Y-%m")
df["statut"] = df["anomalie_potentielle"].map({
    True: "Atypique",
    False: "Non signalé"
})

# ==================================================
# FILTRES
# ==================================================

st.subheader("Filtres d'exploration")

c1, c2, c3 = st.columns(3)

with c1:
    annees = st.multiselect(
        "Années",
        sorted(df["annee"].dropna().astype(int).unique())
    )

with c2:
    pavillons = st.multiselect(
        "Pavillons",
        sorted(df["pavillon"].dropna().unique())
    )

with c3:
    types = st.multiselect(
        "Types de navires",
        sorted(df["vessel_class"].dropna().unique())
    )

c4, c5, c6 = st.columns(3)

with c4:
    zones = st.multiselect(
        "Distance de la côte",
        sorted(df["zone_distance"].dropna().unique())
    )

with c5:
    statut = st.selectbox(
        "Statut ML",
        ["Tous", "Atypique", "Non signalé"]
    )

with c6:
    mmsi = st.text_input("MMSI exact").strip()

duree_min = st.number_input(
    "Durée minimale de l'interruption (heures)",
    min_value=0.0,
    value=0.0,
    step=12.0
)

filtered = df.copy()

if annees:
    filtered = filtered[filtered["annee"].isin(annees)]

if pavillons:
    filtered = filtered[filtered["pavillon"].isin(pavillons)]

if types:
    filtered = filtered[filtered["vessel_class"].isin(types)]

if zones:
    filtered = filtered[filtered["zone_distance"].isin(zones)]

if statut != "Tous":
    filtered = filtered[filtered["statut"] == statut]

if mmsi:
    filtered = filtered[
        filtered["mmsi"].astype(str) == mmsi
    ]

filtered = filtered[filtered["gap_hours"] >= duree_min]

if filtered.empty:
    st.warning("Aucun événement ne correspond aux filtres.")
    st.stop()

# ==================================================
# INDICATEURS
# ==================================================

total = len(filtered)
anomalies = int(filtered["anomalie_potentielle"].sum())
taux = 100 * anomalies / total

c1, c2, c3, c4 = st.columns(4)

c1.metric("Interruptions", format_number(total))
c2.metric("Navires", format_number(filtered["mmsi"].nunique()))
c3.metric("Événements atypiques", format_number(anomalies))
c4.metric("Taux d'anomalies", f"{taux:.2f} %")

# ==================================================
# CARTE INTERACTIVE
# ==================================================

st.subheader("Carte mondiale des interruptions AIS")

max_points = st.select_slider(
    "Nombre maximal de points affichés",
    options=[1000, 5000, 10000, 20000, 60000],
    value=10000
)

# Priorité aux anomalies lorsque la carte est limitée.
atypiques = filtered[filtered["anomalie_potentielle"]]
normaux = filtered[~filtered["anomalie_potentielle"]]

if len(filtered) > max_points:
    if len(atypiques) >= max_points:
        map_data = atypiques.nlargest(
            max_points, "distance_centroide"
        )
    else:
        remaining = max_points - len(atypiques)
        map_data = pd.concat([
            atypiques,
            normaux.sample(
                n=min(remaining, len(normaux)),
                random_state=42
            )
        ])
else:
    map_data = filtered

fig = px.scatter_geo(
    map_data,
    lat="latitude",
    lon="longitude",
    color="statut",
    hover_name="mmsi",
    hover_data=[
        "pavillon",
        "vessel_class",
        "gap_hours",
        "distance_centroide",
        "gap_start_timestamp"
    ],
    color_discrete_map={
        "Atypique": "#F97316",
        "Non signalé": "#22D3EE"
    },
    projection="natural earth"
)

fig.update_traces(marker=dict(size=4, opacity=0.65))

fig.update_geos(
    showland=True,
    landcolor="#263449",
    showocean=True,
    oceancolor="#0B1120",
    showcountries=True,
    countrycolor="#475569",
    coastlinecolor="#64748B",
    bgcolor="#0B1120"
)

fig.update_layout(
    template="plotly_dark",
    height=600,
    paper_bgcolor="#0B1120",
    margin=dict(l=0, r=0, t=20, b=0),
    legend_title_text="Statut ML"
)

st.plotly_chart(fig, use_container_width=True)

st.caption(
    f"{len(map_data):,} points affichés sur {total:,} événements. "
    "Les indicateurs utilisent toutes les données filtrées. "
    "Les positions correspondent au début des interruptions."
)

# ==================================================
# ANALYSES COMPLÉMENTAIRES
# ==================================================

left, right = st.columns(2)

with left:
    monthly = (
        filtered.groupby("mois", as_index=False)
        .agg(
            interruptions=("gap_id", "count"),
            anomalies=("anomalie_potentielle", "sum")
        )
        .sort_values("mois")
    )

    fig_month = px.line(
        monthly,
        x="mois",
        y=["interruptions", "anomalies"],
        markers=True,
        title="Évolution mensuelle",
        template="plotly_dark"
    )

    st.plotly_chart(fig_month, use_container_width=True)

with right:
    flags = (
        filtered.groupby("pavillon", dropna=False)
        .size()
        .reset_index(name="interruptions")
        .nlargest(10, "interruptions")
        .sort_values("interruptions")
    )

    fig_flags = px.bar(
        flags,
        x="interruptions",
        y="pavillon",
        orientation="h",
        title="Top 10 des pavillons par volume",
        template="plotly_dark",
        color="interruptions",
        color_continuous_scale="Teal"
    )

    st.plotly_chart(fig_flags, use_container_width=True)

# ==================================================
# ZONES GÉOGRAPHIQUES
# ==================================================

st.subheader("Zones géographiques les plus concernées")

geo = filtered.copy()

geo["cellule_lat"] = (
    geo["latitude"] // 5 * 5
).astype(int).clip(-90, 85)

geo["cellule_lon"] = (
    geo["longitude"] // 5 * 5
).astype(int).clip(-180, 175)

ranking = (
    geo.groupby(
        ["cellule_lat", "cellule_lon"],
        as_index=False
    )
    .agg(
        interruptions=("gap_id", "count"),
        navires=("mmsi", "nunique"),
        anomalies=("anomalie_potentielle", "sum"),
        duree_moyenne_h=("gap_hours", "mean")
    )
    .sort_values("interruptions", ascending=False)
)

st.dataframe(
    ranking.head(20),
    use_container_width=True,
    hide_index=True
)

st.caption(
    "Cellules géographiques de 5° × 5°. "
    "Il s'agit d'un classement par volume observé, "
    "et non d'une mesure du risque maritime réel."
)

# ==================================================
# DÉTAIL ET EXPORT
# ==================================================

st.subheader("Détail des événements")

details = filtered.sort_values(
    "distance_centroide",
    ascending=False
)

columns = [
    "gap_id",
    "mmsi",
    "pavillon",
    "vessel_class",
    "gap_start_timestamp",
    "gap_hours",
    "distance_cote_km",
    "distance_centroide",
    "statut"
]

st.dataframe(
    details[columns].head(1000),
    use_container_width=True,
    hide_index=True
)

st.download_button(
    "Exporter tous les événements filtrés",
    data=details[columns].to_csv(index=False).encode("utf-8-sig"),
    file_name="darksea_evenements_filtres.csv",
    mime="text/csv"
)

st.info(
    "Une interruption AIS atypique ne constitue pas "
    "une preuve d'activité maritime illégale."
)
