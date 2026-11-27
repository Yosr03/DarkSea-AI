import pandas as pd
import plotly.express as px
import streamlit as st

from components.layout import configure_page, page_header

from services.memory_benchmark_service import (
    ENGINES,
    load_memory_benchmark,
    build_repetition_dataframe,
)


configure_page("Analyse mémoire")

page_header(
    "Analyse de la consommation mémoire",
    "Comparaison expérimentale de Pandas, Spark Local "
    "et Spark Standalone",
)


COLORS = {
    "Pandas": "#22D3EE",
    "Spark Local": "#A78BFA",
    "Spark Standalone": "#FB923C",
}

ORDER = ["x1", "x10", "x50", "x100"]


def display_chart(fig):

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#0B1120",
        plot_bgcolor="#101B2D",
        font_color="#E2E8F0",
        legend_title_text="Moteur",
        margin=dict(l=20, r=20, t=40, b=20),
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )


try:

    summary, df = load_memory_benchmark()

except (OSError, ValueError, KeyError, TypeError) as error:

    st.error(
        f"Impossible de charger le benchmark : {error}"
    )

    st.stop()


# ==================================================
# GLOBAL KPIs
# ==================================================

cols = st.columns(4)

cols[0].metric(
    "Exécutions",
    summary["total_runs"],
)

cols[1].metric(
    "Concordance numérique",
    f"{summary['concordance_pct']:.0f} %",
)

cols[2].metric(
    "Volume maximal",
    f"{df['rows'].max() / 1_000_000:.2f} M",
)

cols[3].metric(
    "Configurations",
    summary["total_configurations"],
)

st.success(
    "Les 36 exécutions ont été validées avec "
    "une concordance numérique de 100 %."
)

st.caption(
    "Nombre cumulé de lignes traitées : "
    f"{summary['total_rows_processed']:,}"
    .replace(",", " ")
)

st.divider()


# ==================================================
# FILTERS
# ==================================================

st.subheader("Filtres expérimentaux")

left, right = st.columns(2)

with left:

    selected_engines = st.multiselect(
        "Moteurs de traitement",
        options=list(ENGINES.values()),
        default=list(ENGINES.values()),
    )

with right:

    selected_factors = st.multiselect(
        "Volumes de données",
        options=[1, 10, 50, 100],
        default=[1, 10, 50, 100],
        format_func=lambda value: f"x{value}",
    )

filtered = df[
    df["engine_label"].isin(selected_engines)
    & df["factor"].isin(selected_factors)
].copy()

if filtered.empty:

    st.warning(
        "Sélectionnez au moins un moteur et un volume."
    )

    st.stop()


# ==================================================
# MEMORY EVOLUTION
# ==================================================

st.subheader("1. Évolution de la consommation mémoire")

metric_options = {
    "Pic mémoire supplémentaire": (
        "peak_incremental_mib_median"
    ),
    "Pic mémoire total": (
        "peak_total_mib_median"
    ),
    "Mémoire moyenne pendant le traitement": (
        "mean_workload_mib_median"
    ),
}

metric_label = st.radio(
    "Indicateur mémoire",
    list(metric_options),
    horizontal=True,
)

metric = metric_options[metric_label]

fig = px.line(
    filtered.sort_values("factor"),
    x="volume",
    y=metric,
    color="engine_label",
    markers=True,
    color_discrete_map=COLORS,
    category_orders={"volume": ORDER},
    labels={
        "volume": "Volume de données",
        metric: "Mémoire (Mio)",
        "engine_label": "Moteur",
    },
)

fig.update_traces(
    line=dict(width=3),
    marker=dict(size=9),
)

fig.update_yaxes(rangemode="tozero")

display_chart(fig)


# ==================================================
# MEMORY COMPARISON
# ==================================================

st.subheader("2. Comparaison par volume")

factor = st.selectbox(
    "Volume à comparer",
    options=sorted(selected_factors),
    index=len(selected_factors) - 1,
    format_func=lambda value: f"x{value}",
)

comparison = filtered[
    filtered["factor"] == factor
].copy()

fig = px.bar(
    comparison,
    x="engine_label",
    y="peak_incremental_mib_median",
    color="engine_label",
    text_auto=".2f",
    color_discrete_map=COLORS,
    labels={
        "engine_label": "Moteur",
        "peak_incremental_mib_median": (
            "Mémoire supplémentaire (Mio)"
        ),
    },
)

fig.update_layout(showlegend=False)

display_chart(fig)


# ==================================================
# TOTAL MEMORY
# ==================================================

st.subheader("3. Empreinte mémoire totale")

fig = px.bar(
    filtered,
    x="volume",
    y="peak_total_mib_median",
    color="engine_label",
    barmode="group",
    color_discrete_map=COLORS,
    category_orders={"volume": ORDER},
    labels={
        "volume": "Volume",
        "peak_total_mib_median": "Pic total (Mio)",
        "engine_label": "Moteur",
    },
)

display_chart(fig)


# ==================================================
# REPETITION VARIABILITY
# ==================================================

st.subheader("4. Variabilité entre les répétitions")

repetitions = build_repetition_dataframe(
    filtered
)

fig = px.box(
    repetitions,
    x="volume",
    y="memory_mib",
    color="engine_label",
    points="all",
    color_discrete_map=COLORS,
    category_orders={"volume": ORDER},
    labels={
        "volume": "Volume",
        "memory_mib": "Pic supplémentaire (Mio)",
        "engine_label": "Moteur",
    },
)

display_chart(fig)

st.caption(
    "Chaque configuration contient trois répétitions. "
    "Les distributions sont descriptives."
)


# ==================================================
# TIME VS MEMORY
# ==================================================

st.subheader("5. Compromis temps / mémoire")

fig = px.scatter(
    filtered,
    x="elapsed_s_median",
    y="peak_incremental_mib_median",
    color="engine_label",
    size="rows",
    hover_name="volume",
    color_discrete_map=COLORS,
    labels={
        "elapsed_s_median": "Temps de traitement (s)",
        "peak_incremental_mib_median": (
            "Mémoire supplémentaire (Mio)"
        ),
        "engine_label": "Moteur",
        "rows": "Nombre de lignes",
    },
)

fig.update_xaxes(type="log")

display_chart(fig)

st.caption(
    "Axe temporel logarithmique. La taille des points "
    "représente le nombre de lignes."
)


# ==================================================
# STATISTICAL TABLE
# ==================================================

st.subheader("6. Résultats statistiques détaillés")

columns = {
    "engine_label": "Moteur",
    "factor": "Facteur",
    "rows": "Lignes",
    "repetitions": "Répétitions",
    "baseline_mib_median": "Référence (Mio)",
    "peak_total_mib_median": "Pic total (Mio)",
    "peak_incremental_mib_median": "Pic ajouté (Mio)",
    "peak_incremental_mib_min": "Minimum (Mio)",
    "peak_incremental_mib_max": "Maximum (Mio)",
    "peak_incremental_mib_std": "Écart-type (Mio)",
    "elapsed_s_median": "Temps médian (s)",
    "wall_time_s_median": "Temps total (s)",
    "min_workload_samples": "Échantillons min.",
    "concordance_pct": "Concordance (%)",
}

table = filtered[list(columns)].rename(
    columns=columns
)

st.dataframe(
    table.round(3),
    use_container_width=True,
    hide_index=True,
)

st.download_button(
    "Exporter les résultats CSV",
    data=table.to_csv(index=False).encode("utf-8-sig"),
    file_name="darksea_memory_benchmark.csv",
    mime="text/csv",
)


# ==================================================
# SCIENTIFIC INTERPRETATION
# ==================================================

st.divider()

st.subheader("7. Interprétation scientifique")

st.markdown(
    """
    **Résultats observés sur l'ensemble du benchmark :**

    - Pandas présente la consommation mémoire la plus
      faible sur les volumes testés.
    - Son pic mémoire supplémentaire augmente
      avec le volume des données.
    - Spark Local présente une empreinte mémoire
      relativement stable.
    - Spark Standalone introduit un coût mémoire
      supplémentaire lié à son architecture distribuée.
    - Les trois moteurs produisent les mêmes résultats
      analytiques selon les tolérances définies.
    """
)

with st.expander(
    "Méthodologie expérimentale",
    expanded=True,
):

    st.markdown(
        """
        - Quatre volumes : x1, x10, x50 et x100.
        - Trois moteurs et trois répétitions.
        - Mesures Linux cgroup v2.
        - Mémoire working set des conteneurs
          Spark Master et Spark Worker.
        - Intervalle cible : 100 ms.
        - Référence mémoire : médiane sur cinq secondes.
        - Pic supplémentaire : maximum observé
          moins la mémoire de référence, borné à zéro.
        - Exécutions dans un ordre pseudo-aléatoire.
        - Nouvelle application pour chaque exécution.
        """
    )

with st.expander("Limites expérimentales"):

    st.markdown(
        """
        - Cluster installé sur une seule machine physique.
        - Données agrandies par duplication contrôlée.
        - Nombre de groupes MMSI constant.
        - Cache système non contrôlé entre les exécutions.
        - Pics très courts potentiellement manqués
          par l'échantillonnage à 100 ms.
        - La mesure mémoire inclut le démarrage
          et l'arrêt des applications.
        - La mesure ne correspond pas à la RAM
          totale de la machine.
        - Le benchmark mesure une agrégation analytique,
          et non directement l'entraînement MLlib.
        - Les temps ne sont pas directement comparables
          au précédent benchmark utilisant un autre
          protocole d'exécution.
        """
    )

st.info(
    "La stabilité mémoire observée avec Spark constitue "
    "un résultat intéressant, mais ne démontre pas "
    "une supériorité générale sur Pandas."
)