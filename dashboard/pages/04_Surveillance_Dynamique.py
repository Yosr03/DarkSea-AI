
import pandas as pd
import plotly.express as px
import streamlit as st

from streamlit_autorefresh import st_autorefresh

from components.layout import (
    configure_page,
    page_header,
    format_number
)

from services.kafka_service import (
    create_consumer,
    poll_alerts,
    merge_alerts
)

configure_page("Surveillance dynamique")

page_header(
    "Surveillance dynamique",
    "Réception et visualisation des alertes Kafka"
)

st.info(
    "Le flux actuel repose sur le replay d'événements AIS "
    "historiques. Il ne s'agit pas d'un flux maritime réel."
)

# ==================================================
# INITIALISATION DE LA SESSION
# ==================================================

defaults = {
    "kafka_consumer": None,
    "kafka_connected": False,
    "live_alerts": [],
    "invalid_messages": 0
}

for key, value in defaults.items():
    if key not in st.session_state:
        st.session_state[key] = value

# ==================================================
# CONTRÔLES DE CONNEXION
# ==================================================

st.subheader("Connexion au flux Kafka")

c1, c2, c3 = st.columns(3)

with c1:
    if st.button(
        "Connecter Kafka",
        disabled=st.session_state.kafka_connected,
        use_container_width=True
    ):
        try:
            st.session_state.kafka_consumer = create_consumer()
            st.session_state.kafka_connected = True
        except Exception as exc:
            st.error(f"Connexion impossible : {exc}")

with c2:
    if st.button(
        "Déconnecter Kafka",
        disabled=not st.session_state.kafka_connected,
        use_container_width=True
    ):
        try:
            st.session_state.kafka_consumer.close()
        finally:
            st.session_state.kafka_consumer = None
            st.session_state.kafka_connected = False

with c3:
    if st.button(
        "Effacer les alertes affichées",
        use_container_width=True
    ):
        st.session_state.live_alerts = []
        st.session_state.invalid_messages = 0

st.caption(
    "Effacer les alertes vide uniquement la mémoire du dashboard. "
    "Les messages Kafka ne sont pas supprimés."
)

auto_refresh = st.toggle(
    "Actualisation automatique",
    value=True
)

refresh_seconds = st.select_slider(
    "Intervalle d'actualisation",
    options=[3, 5, 10, 15, 30],
    value=5
)

if st.session_state.kafka_connected:

    if auto_refresh:
        st_autorefresh(
            interval=refresh_seconds * 1000,
            key="darksea_live_refresh"
        )

    st.button("Actualiser maintenant")

    try:
        incoming, invalid = poll_alerts(
            st.session_state.kafka_consumer
        )

        st.session_state.live_alerts = merge_alerts(
            st.session_state.live_alerts,
            incoming
        )

        st.session_state.invalid_messages += invalid

        st.success("Kafka connecté — topic ais-alerts")

    except Exception as exc:
        st.error(f"Erreur de lecture Kafka : {exc}")

        try:
            st.session_state.kafka_consumer.close()
        except Exception:
            pass

        st.session_state.kafka_consumer = None
        st.session_state.kafka_connected = False

else:
    st.warning("Kafka déconnecté.")

# ==================================================
# PRÉPARATION DES ALERTES
# ==================================================

if not st.session_state.live_alerts:
    st.info(
        "Aucune alerte chargée. Connectez Kafka "
        "pour consulter les messages disponibles."
    )
    st.stop()

df = pd.DataFrame(st.session_state.live_alerts)

for column in [
    "distance_centroide",
    "gap_hours",
    "latitude",
    "longitude",
    "latence_ms",
    "vitesse_apparente_kmh"
]:
    if column in df.columns:
        df[column] = pd.to_numeric(
            df[column], errors="coerce"
        )

if "processing_timestamp" in df.columns:
    df["date_traitement"] = pd.to_datetime(
        df["processing_timestamp"],
        utc=True,
        errors="coerce"
    )
else:
    df["date_traitement"] = pd.NaT

if "type_alerte" not in df.columns:
    df["type_alerte"] = "NON_PRECISE"

df["type_alerte"] = df["type_alerte"].fillna(
    "NON_PRECISE"
)

# ==================================================
# FILTRES
# ==================================================

st.subheader("Filtres de surveillance")

c1, c2 = st.columns(2)

replays = sorted(
    df["replay_id"].dropna().astype(str).unique()
)

with c1:
    replay = st.selectbox(
        "Session de replay",
        ["Tous"] + replays
    )

with c2:
    types = st.multiselect(
        "Types d'alertes",
        sorted(df["type_alerte"].unique())
    )

filtered = df.copy()

if replay != "Tous":
    filtered = filtered[
        filtered["replay_id"].astype(str) == replay
    ]

if types:
    filtered = filtered[
        filtered["type_alerte"].isin(types)
    ]

if filtered.empty:
    st.warning("Aucune alerte pour ces filtres.")
    st.stop()

# ==================================================
# INDICATEURS DYNAMIQUES
# ==================================================

st.subheader("Indicateurs de surveillance")

statistical = int(
    (filtered["type_alerte"] == "ANOMALIE_STATISTIQUE").sum()
)

physical = int(
    (filtered["type_alerte"] == "COHERENCE_PHYSIQUE").sum()
)

c1, c2, c3, c4 = st.columns(4)

c1.metric(
    "Alertes uniques chargées",
    format_number(len(filtered))
)

c2.metric(
    "Anomalies statistiques",
    format_number(statistical)
)

c3.metric(
    "Incohérences physiques",
    format_number(physical)
)

vessels = (
    filtered["mmsi"].nunique()
    if "mmsi" in filtered.columns
    else 0
)

c4.metric("Navires concernés", format_number(vessels))

st.caption(
    f"Messages non conformes reçus : "
    f"{st.session_state.invalid_messages}. "
    "Historique local limité à 20 000 alertes uniques."
)

# ==================================================
# LATENCE
# ==================================================

if "latence_ms" in filtered.columns:

    latency = filtered["latence_ms"].dropna()
    latency = latency[latency >= 0] / 1000

    if not latency.empty:

        st.subheader("Latence du scoring des alertes")

        c1, c2, c3, c4 = st.columns(4)

        c1.metric("Moyenne", f"{latency.mean():.2f} s")
        c2.metric("P50", f"{latency.quantile(0.50):.2f} s")
        c3.metric("P95", f"{latency.quantile(0.95):.2f} s")
        c4.metric("Maximum", f"{latency.max():.2f} s")

        st.caption(
            "Ces mesures concernent uniquement les alertes "
            "chargées, pas tous les événements traités par Spark."
        )

# ==================================================
# ÉVOLUTION TEMPORELLE
# ==================================================

st.subheader("Évolution des alertes")

timeline = filtered.dropna(
    subset=["date_traitement"]
).copy()

if not timeline.empty:

    timeline["minute"] = (
        timeline["date_traitement"].dt.floor("min")
    )

    counts = (
        timeline.groupby(
            ["minute", "type_alerte"],
            as_index=False
        )
        .size()
        .rename(columns={"size": "alertes"})
    )

    fig = px.bar(
        counts,
        x="minute",
        y="alertes",
        color="type_alerte",
        title="Alertes par minute de traitement",
        template="plotly_dark",
        color_discrete_map={
            "ANOMALIE_STATISTIQUE": "#22D3EE",
            "COHERENCE_PHYSIQUE": "#F97316"
        }
    )

    st.plotly_chart(fig, use_container_width=True)

# ==================================================
# CARTOGRAPHIE
# ==================================================

st.subheader("Localisation des alertes")

if {"latitude", "longitude"}.issubset(filtered.columns):

    geo = filtered.dropna(
        subset=["latitude", "longitude"]
    )

    geo = geo[
        geo["latitude"].between(-90, 90)
        & geo["longitude"].between(-180, 180)
    ]

    if not geo.empty:

        hover_columns = [
            column for column in [
                "mmsi",
                "gap_hours",
                "distance_centroide",
                "explication"
            ]
            if column in geo.columns
        ]

        fig = px.scatter_geo(
            geo,
            lat="latitude",
            lon="longitude",
            color="type_alerte",
            hover_data=hover_columns,
            projection="natural earth",
            color_discrete_map={
                "ANOMALIE_STATISTIQUE": "#22D3EE",
                "COHERENCE_PHYSIQUE": "#F97316"
            }
        )

        fig.update_geos(
            showland=True,
            landcolor="#263449",
            showocean=True,
            oceancolor="#0B1120",
            showcountries=True,
            bgcolor="#0B1120"
        )

        fig.update_layout(
            template="plotly_dark",
            height=500,
            margin=dict(l=0, r=0, t=0, b=0)
        )

        st.plotly_chart(fig, use_container_width=True)

# ==================================================
# TABLE DES ALERTES
# ==================================================

st.subheader("Journal des alertes")

columns = [
    column for column in [
        "processing_timestamp",
        "mmsi",
        "type_alerte",
        "gap_hours",
        "distance_centroide",
        "vitesse_apparente_kmh",
        "latence_ms",
        "explication",
        "replay_id",
        "alert_id"
    ]
    if column in filtered.columns
]

latest = filtered.sort_values(
    "date_traitement",
    ascending=False
)

st.dataframe(
    latest[columns].head(1000),
    use_container_width=True,
    hide_index=True
)

st.download_button(
    "Exporter les alertes filtrées",
    data=latest[columns].to_csv(
        index=False
    ).encode("utf-8-sig"),
    file_name="darksea_alertes_kafka.csv",
    mime="text/csv"
)

st.warning(
    "Les alertes constituent des signaux d'aide à la décision. "
    "Elles ne prouvent pas une activité maritime illégale."
)
