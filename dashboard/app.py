import streamlit as st
import plotly.express as px

from components.layout import (
    configure_page,
    page_header,
    format_number,
)

from services.data_service import (
    load_dataset,
    load_metrics,
    get_overview,
)


configure_page("Vue générale")

page_header(
    "Maritime Intelligence Center",
    "Analyse intelligente des interruptions AIS "
    "et détection des comportements atypiques.",
)


try:

    overview = get_overview()

    monthly = load_dataset(
        "statistiques_mensuelles"
    ).sort_values("mois")

    vessel_types = load_dataset(
        "statistiques_types_navires"
    )

    flags = load_dataset(
        "statistiques_pavillons"
    )

    zones = load_dataset(
        "zones_maritimes"
    ).copy()

    streaming = load_metrics(
        "streaming_validation"
    )

except Exception as error:

    st.error(
        f"Impossible de charger les données : {error}"
    )

    st.stop()


st.caption(
    "Période historique : 2017–2019 | "
    "Source : Global Fishing Watch"
)


# ==================================================
# INDICATEURS PRINCIPAUX
# ==================================================

st.subheader("Indicateurs maritimes")

c1, c2, c3, c4 = st.columns(4)

c1.metric(
    "Événements AIS",
    format_number(overview["total_events"]),
)

c2.metric(
    "Navires analysés",
    format_number(overview["total_vessels"]),
)

c3.metric(
    "Profils atypiques",
    format_number(overview["profile_anomalies"]),
)

c4.metric(
    "Événements atypiques",
    format_number(overview["event_anomalies"]),
)


c5, c6, c7, c8 = st.columns(4)

c5.metric(
    "Cellules géographiques",
    format_number(overview["total_zones"]),
)

c6.metric(
    "Durée moyenne AIS",
    f"{overview['mean_gap_hours']:.2f} h",
)

c7.metric(
    "Taux d'événements atypiques",
    f"{overview['event_anomalies'] / overview['total_events'] * 100:.2f} %",
)

c8.metric(
    "Taux de profils atypiques",
    f"{overview['profile_anomalies'] / overview['total_vessels'] * 100:.2f} %",
)


st.divider()


# ==================================================
# CARTOGRAPHIE MONDIALE
# ==================================================

st.subheader("Distribution mondiale des interruptions AIS")

zones["latitude_centre"] = (
    zones["cellule_lat"] + 2.5
)

zones["longitude_centre"] = (
    zones["cellule_lon"] + 2.5
)

fig_map = px.scatter_geo(
    zones,
    lat="latitude_centre",
    lon="longitude_centre",
    size="nb_interruptions",
    color="nb_interruptions",
    color_continuous_scale="Turbo",
    projection="natural earth",
    size_max=28,
    hover_data={
        "nb_interruptions": True,
        "nb_navires": True,
        "duree_moyenne_h": ":.2f",
        "latitude_centre": False,
        "longitude_centre": False,
    },
    labels={
        "nb_interruptions": "Interruptions",
        "nb_navires": "Navires",
        "duree_moyenne_h": "Durée moyenne (h)",
    },
    template="plotly_dark",
)

fig_map.update_geos(
    showland=True,
    landcolor="#25364B",
    showocean=True,
    oceancolor="#0B1120",
    showcountries=True,
    countrycolor="#42546B",
    coastlinecolor="#42546B",
    bgcolor="#0B1120",
)

fig_map.update_layout(
    height=530,
    margin=dict(l=0, r=0, t=10, b=0),
    paper_bgcolor="#0B1120",
)

st.plotly_chart(
    fig_map,
    use_container_width=True,
)

st.caption(
    "Agrégation géographique Pig : cellules de 5° × 5°. "
    "Les positions représentent les centres des cellules, "
    "et non les trajectoires exactes des navires."
)


# ==================================================
# ANALYSE TEMPORELLE
# ==================================================

st.subheader("Évolution temporelle des interruptions")

fig_monthly = px.area(
    monthly,
    x="mois",
    y="nb_interruptions",
    markers=True,
    template="plotly_dark",
    color_discrete_sequence=["#22D3EE"],
    labels={
        "mois": "Période",
        "nb_interruptions": "Interruptions AIS",
    },
)

fig_monthly.update_layout(
    height=340,
    paper_bgcolor="#0B1120",
    plot_bgcolor="#101B2D",
)

st.plotly_chart(
    fig_monthly,
    use_container_width=True,
)


# ==================================================
# TYPES DE NAVIRES ET PAVILLONS
# ==================================================

left, right = st.columns(2)

with left:

    st.subheader("Types de navires")

    fig_types = px.pie(
        vessel_types,
        names="vessel_class",
        values="nb_interruptions",
        hole=0.55,
        template="plotly_dark",
        color_discrete_sequence=[
            "#22D3EE",
            "#3B82F6",
            "#F59E0B",
            "#10B981",
            "#A78BFA",
        ],
    )

    fig_types.update_layout(
        height=400,
        paper_bgcolor="#0B1120",
        legend=dict(orientation="h"),
    )

    st.plotly_chart(
        fig_types,
        use_container_width=True,
    )


with right:

    st.subheader("Top 10 des pavillons")

    top_flags = flags.nlargest(
        10,
        "nb_interruptions",
    ).sort_values("nb_interruptions")

    fig_flags = px.bar(
        top_flags,
        x="nb_interruptions",
        y="pavillon",
        orientation="h",
        template="plotly_dark",
        color="nb_interruptions",
        color_continuous_scale="Blues",
        labels={
            "pavillon": "Pavillon",
            "nb_interruptions": "Interruptions",
        },
    )

    fig_flags.update_layout(
        height=400,
        paper_bgcolor="#0B1120",
        plot_bgcolor="#101B2D",
        coloraxis_showscale=False,
    )

    st.plotly_chart(
        fig_flags,
        use_container_width=True,
    )


st.caption(
    "Les volumes par pavillon ou type de navire "
    "ne constituent pas des taux de risque."
)


# ==================================================
# DERNIERE SIMULATION STREAMING
# ==================================================

st.divider()

st.subheader("Dernière simulation Kafka / Spark")

s1, s2, s3, s4 = st.columns(4)

s1.metric(
    "Événements traités",
    format_number(streaming["streaming_events"]),
)

s2.metric(
    "Alertes générées",
    format_number(streaming["total_alerts"]),
)

s3.metric(
    "Latence moyenne de scoring",
    f"{streaming['latency']['mean_ms'] / 1000:.2f} s",
)

s4.metric(
    "Latence P95",
    f"{streaming['latency']['p95_ms'] / 1000:.2f} s",
)


if streaming["validation_passed"]:

    st.success(
        "Validation Batch / Streaming réussie : "
        "aucune différence de prédiction, "
        "aucun doublon et aucun événement manquant."
    )

else:

    st.error(
        "La validation Streaming a échoué."
    )


st.caption(
    "Résultats de la dernière simulation historique. "
    "Il ne s'agit pas d'un flux AIS maritime en direct."
)


st.divider()

st.info(
    "Une anomalie statistique constitue un signal "
    "à examiner et non une preuve d'activité illégale."
)