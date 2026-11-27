
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

from components.layout import configure_page, page_header, format_number
from services.data_service import load_dataset, load_metrics

configure_page("Intelligence artificielle")

page_header(
    "Intelligence artificielle",
    "Évaluation, interprétation et validation des modèles Spark MLlib"
)

try:
    profiles = load_dataset("profils_scores").copy()
    events = load_dataset("evenements_scores").copy()
    clusters = load_dataset("synthese_clusters")
    rules = load_dataset("comparaison_regles_ml")

    profile_metrics = load_metrics("kmeans_metrics")
    event_metrics = load_metrics("event_kmeans_metrics")
    streaming = load_metrics("streaming_validation")

except Exception as exc:
    st.error(f"Erreur de chargement : {exc}")
    st.stop()

# ==================================================
# COMPARAISON DES MODÈLES
# ==================================================

st.subheader("Modèles de détection d'anomalies")

comparison = pd.DataFrame([
    {
        "Modèle": "Profils des navires",
        "Observations": profile_metrics["total_profiles"],
        "K optimal": profile_metrics["best_k"],
        "Silhouette validation": profile_metrics["silhouette_validation"],
        "Silhouette test": profile_metrics["silhouette_test"],
        "Anomalies": profile_metrics["anomalies"],
        "Taux (%)": profile_metrics["anomaly_percentage"]
    },
    {
        "Modèle": "Événements AIS",
        "Observations": event_metrics["total_events"],
        "K optimal": event_metrics["best_k"],
        "Silhouette validation": event_metrics["silhouette_validation"],
        "Silhouette test": event_metrics["silhouette_test"],
        "Anomalies": event_metrics["total_anomalies"],
        "Taux (%)": event_metrics["anomaly_percentage"]
    }
])

st.dataframe(
    comparison.round(4),
    use_container_width=True,
    hide_index=True
)

st.caption(
    "Les deux modèles répondent à des objectifs différents. "
    "Le score de silhouette mesure la qualité de la séparation "
    "des clusters et non la précision de détection des anomalies."
)

# ==================================================
# CHOIX DE K
# ==================================================

st.subheader("Sélection du nombre optimal de clusters")

exp_profiles = pd.DataFrame(profile_metrics["experiments"])
exp_profiles["modele"] = "Profils"

exp_events = pd.DataFrame(event_metrics["experiments"])
exp_events["modele"] = "Événements"

experiments = pd.concat(
    [exp_profiles, exp_events],
    ignore_index=True
)

left, right = st.columns(2)

with left:
    fig = px.line(
        experiments,
        x="k",
        y="silhouette_validation",
        color="modele",
        markers=True,
        title="Score de silhouette",
        template="plotly_dark"
    )

    fig.update_xaxes(dtick=1)

    st.plotly_chart(fig, use_container_width=True)

with right:
    fig = px.line(
        experiments,
        x="k",
        y="training_cost",
        color="modele",
        markers=True,
        log_y=True,
        title="Coût d'entraînement — échelle logarithmique",
        template="plotly_dark"
    )

    fig.update_xaxes(dtick=1)

    st.plotly_chart(fig, use_container_width=True)

st.caption(
    "Le coût KMeans est interprété au sein de chaque modèle. "
    "Les coûts absolus ne sont pas directement comparables "
    "entre des datasets et des espaces de variables différents."
)

# ==================================================
# EXPLORATION DES CLUSTERS
# ==================================================

st.subheader("Exploration interactive des clusters")

selected_model = st.radio(
    "Modèle à explorer",
    ["Profils des navires", "Événements AIS"],
    horizontal=True
)

if selected_model == "Profils des navires":
    selected = profiles.copy()
    metrics = profile_metrics
    x_feature = "nb_interruptions"
    y_feature = "duree_max_h"
    identifier = "mmsi"

else:
    selected = events.copy()
    metrics = event_metrics
    x_feature = "gap_hours"
    y_feature = "deplacement_km"
    identifier = "gap_id"

threshold = float(metrics["anomaly_threshold"])

c1, c2, c3, c4 = st.columns(4)

c1.metric("K optimal", metrics["best_k"])
c2.metric(
    "Silhouette test",
    f'{metrics["silhouette_test"]:.4f}'
)
c3.metric("Seuil ML", f"{threshold:.4f}")
c4.metric(
    "Anomalies",
    format_number(int(selected["anomalie_potentielle"].sum()))
)

st.markdown("**Variables utilisées par le modèle**")
st.write(", ".join(metrics["features"]))

st.markdown("**Répartition des données**")

splits = pd.DataFrame({
    "Ensemble": ["Entraînement", "Validation", "Test"],
    "Observations": [
        metrics["train_count"],
        metrics["validation_count"],
        metrics["test_count"]
    ]
})

fig_split = px.bar(
    splits,
    x="Observations",
    y="Ensemble",
    orientation="h",
    text="Observations",
    color="Ensemble",
    template="plotly_dark"
)

fig_split.update_layout(
    showlegend=False,
    height=260
)

st.plotly_chart(fig_split, use_container_width=True)

# ==================================================
# VISUALISATION DES CLUSTERS
# ==================================================

selected["cluster_label"] = (
    "Cluster " + selected["cluster"].astype(str)
)

selected["statut"] = selected["anomalie_potentielle"].map({
    True: "Atypique",
    False: "Non signalé"
})

# Conserver les anomalies et limiter les points normaux.
atypiques = selected[selected["anomalie_potentielle"]]
normaux = selected[~selected["anomalie_potentielle"]]

remaining = max(0, 10000 - len(atypiques))

sample = pd.concat([
    atypiques,
    normaux.sample(
        n=min(remaining, len(normaux)),
        random_state=42
    )
])

sample["x_log"] = np.log1p(sample[x_feature])
sample["y_log"] = np.log1p(sample[y_feature])

fig = px.scatter(
    sample,
    x="x_log",
    y="y_log",
    color="cluster_label",
    symbol="statut",
    hover_name=identifier,
    hover_data=[
        x_feature,
        y_feature,
        "distance_centroide"
    ],
    labels={
        "x_log": f"log(1 + {x_feature})",
        "y_log": f"log(1 + {y_feature})",
        "cluster_label": "Cluster"
    },
    template="plotly_dark",
    title="Projection descriptive des clusters"
)

fig.update_traces(marker=dict(size=5, opacity=0.65))
fig.update_layout(height=500)

st.plotly_chart(fig, use_container_width=True)

st.caption(
    "Projection sur deux variables transformées. "
    "Le modèle utilise davantage de variables standardisées. "
    "Cette représentation ne reproduit pas tout l'espace KMeans."
)

# ==================================================
# DISTRIBUTION DES SCORES
# ==================================================

st.subheader("Distribution des scores d'anomalie")

fig_scores = px.histogram(
    selected,
    x="distance_centroide",
    color="statut",
    nbins=60,
    barmode="overlay",
    opacity=0.75,
    color_discrete_map={
        "Atypique": "#F97316",
        "Non signalé": "#22D3EE"
    },
    template="plotly_dark"
)

fig_scores.add_vline(
    x=threshold,
    line_dash="dash",
    line_color="#FACC15",
    annotation_text="Seuil P95"
)

st.plotly_chart(fig_scores, use_container_width=True)

# ==================================================
# ANALYSE DE SENSIBILITÉ
# ==================================================

st.subheader("Analyse de sensibilité des seuils")

if selected_model == "Événements AIS":

    threshold_options = {
        "P90": event_metrics["threshold_p90"],
        "P95": event_metrics["threshold_p95"],
        "P99": event_metrics["threshold_p99"]
    }

    quantile = st.select_slider(
        "Seuil de validation à simuler",
        options=list(threshold_options),
        value="P95"
    )

    simulated_threshold = threshold_options[quantile]

    # Évaluation sur les événements du test chronologique.
    test_data = selected[
        selected["data_split"] == "test"
    ]

    if len(test_data) != event_metrics["test_count"]:
        st.warning(
            "La taille du test ne correspond pas aux métriques. "
            "Vérifier les valeurs de data_split."
        )
    else:
        count = int(
            (test_data["distance_centroide"] > simulated_threshold).sum()
        )

        rate = 100 * count / len(test_data)

        c1, c2, c3 = st.columns(3)

        c1.metric("Seuil simulé", f"{simulated_threshold:.4f}")
        c2.metric("Événements signalés sur le test", count)
        c3.metric("Taux simulé", f"{rate:.2f} %")

        sensitivity = pd.DataFrame([
            {
                "Seuil": name,
                "Anomalies": int(
                    (test_data["distance_centroide"] > value).sum()
                )
            }
            for name, value in threshold_options.items()
        ])

        fig_sensitivity = px.bar(
            sensitivity,
            x="Seuil",
            y="Anomalies",
            color="Seuil",
            text="Anomalies",
            template="plotly_dark"
        )

        st.plotly_chart(
            fig_sensitivity,
            use_container_width=True
        )

    st.info(
        "Simulation analytique uniquement : "
        "le modèle de production et le seuil Kafka "
        "ne sont pas modifiés."
    )

else:
    st.info(
        "Le modèle de profils utilise un seuil P95 "
        "calculé sur l'ensemble de validation."
    )

# ==================================================
# SYNTHÈSE DES CLUSTERS
# ==================================================

st.subheader("Synthèse des clusters de navires — Hive")

st.dataframe(
    clusters.round(3),
    use_container_width=True,
    hide_index=True
)

fig_clusters = px.bar(
    clusters,
    x=clusters["cluster"].astype(str),
    y="nombre_navires",
    color="nombre_anomalies",
    text="nombre_navires",
    labels={"x": "Cluster"},
    color_continuous_scale="Teal",
    template="plotly_dark"
)

st.plotly_chart(fig_clusters, use_container_width=True)

# ==================================================
# HIVE VS MACHINE LEARNING
# ==================================================

st.subheader("Comparaison des règles Hive et du Machine Learning")

labels = {
    "HIVE_ET_ML": "Hive et ML",
    "HIVE_UNIQUEMENT": "Hive uniquement",
    "ML_UNIQUEMENT": "ML uniquement",
    "NON_SIGNALE": "Non signalé"
}

rules = rules.copy()
rules["categorie_label"] = rules["categorie"].map(labels)

fig_rules = px.bar(
    rules,
    x="categorie_label",
    y="nombre_navires",
    color="categorie_label",
    text="nombre_navires",
    template="plotly_dark",
    color_discrete_map={
        "Hive et ML": "#F97316",
        "Hive uniquement": "#EAB308",
        "ML uniquement": "#22D3EE",
        "Non signalé": "#64748B"
    }
)

fig_rules.update_layout(
    showlegend=False,
    xaxis_title="Catégorie"
)

st.plotly_chart(fig_rules, use_container_width=True)

st.dataframe(
    rules.drop(columns=["categorie_label"]).round(3),
    use_container_width=True,
    hide_index=True
)

st.caption(
    "Les profils détectés uniquement par ML constituent "
    "des signaux complémentaires aux règles Hive. "
    "Sans vérité terrain, cette comparaison ne permet pas "
    "de mesurer les faux positifs ou la précision."
)

# ==================================================
# VALIDATION BATCH / STREAMING
# ==================================================

st.subheader("Validation Batch / Streaming")

c1, c2, c3, c4 = st.columns(4)

c1.metric(
    "Événements comparés",
    format_number(streaming["compared_events"])
)

c2.metric("Alertes", streaming["total_alerts"])
c3.metric("Écarts de score", streaming["score_differences"])
c4.metric("Doublons", streaming["duplicates"])

if streaming["validation_passed"]:
    st.success(
        "Validation réussie : les résultats Batch et Streaming "
        "sont concordants pour le replay testé."
    )
else:
    st.error("La validation Batch / Streaming a échoué.")

latency = pd.DataFrame({
    "Indicateur": ["Minimum", "P50", "Moyenne", "P95", "P99", "Maximum"],
    "Secondes": [
        streaming["latency"]["min_ms"] / 1000,
        streaming["latency"]["p50_ms"] / 1000,
        streaming["latency"]["mean_ms"] / 1000,
        streaming["latency"]["p95_ms"] / 1000,
        streaming["latency"]["p99_ms"] / 1000,
        streaming["latency"]["max_ms"] / 1000
    ]
})

fig_latency = px.bar(
    latency,
    x="Indicateur",
    y="Secondes",
    color="Secondes",
    text_auto=".2f",
    color_continuous_scale="Teal",
    template="plotly_dark"
)

st.plotly_chart(fig_latency, use_container_width=True)

st.caption(
    "Latence mesurée entre l'horodatage d'ingestion "
    "et le scoring Spark lors du replay historique validé."
)

st.warning(
    "Le système utilise un apprentissage non supervisé. "
    "Les scores d'anomalie ne constituent ni des probabilités "
    "de fraude ni des preuves d'activités illégales."
)