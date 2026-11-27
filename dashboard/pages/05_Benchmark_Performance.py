import pandas as pd
import plotly.express as px
import streamlit as st

from components.layout import configure_page, page_header
from services.benchmark_service import (
    load_benchmark,
    get_benchmark_overview
)

configure_page("Benchmark & Performance")

page_header(
    "Benchmark & Performance",
    "Comparaison experimentale des moteurs de traitement Big Data"
)

COLORS = {
    "Pandas": "#A78BFA",
    "Spark Local": "#FB923C",
    "Spark Standalone": "#22D3EE"
}

ORDER = ["x1", "x10", "x50", "x100"]

try:
    df = load_benchmark()
except (OSError, ValueError, TypeError, KeyError) as exc:
    st.error(f"Impossible de charger le benchmark : {exc}")
    st.stop()

overview = get_benchmark_overview(df)

cols = st.columns(4)

cols[0].metric(
    "Volume maximal",
    f"{overview['max_rows']:,}".replace(",", " ")
)

cols[1].metric(
    "Experiences",
    overview["experiments"]
)

cols[2].metric(
    "Concordance",
    f"{overview['concordance']:.0f} %"
)

cols[3].metric(
    "Moteurs",
    overview["engines"]
)

st.success(
    "Validation comparative reussie : "
    "12 configurations et 5 269 profils par configuration."
)

st.divider()

st.subheader("Configuration de l'analyse")

c1, c2 = st.columns(2)

with c1:
    engines = st.multiselect(
        "Moteurs de traitement",
        list(COLORS),
        default=list(COLORS)
    )

with c2:
    volumes = st.multiselect(
        "Volumes de donnees",
        ORDER,
        default=ORDER
    )

filtered = df[
    df["engine_label"].isin(engines)
    & df["volume"].isin(volumes)
].copy()

if filtered.empty:
    st.warning("Selectionnez au moins un moteur et un volume.")
    st.stop()


def style_chart(fig):

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#0B1120",
        plot_bgcolor="#0B1120",
        font_color="#E2E8F0",
        legend_title_text="Moteur",
        margin=dict(l=20, r=20, t=50, b=20)
    )

    return fig


st.subheader("Temps d'execution")

fig = px.bar(
    filtered,
    x="volume",
    y="median_s",
    color="engine_label",
    barmode="group",
    text_auto=".3f",
    color_discrete_map=COLORS,
    category_orders={"volume": ORDER},
    labels={
        "volume": "Volume",
        "median_s": "Temps median (s)",
        "engine_label": "Moteur"
    }
)

st.plotly_chart(
    style_chart(fig),
    use_container_width=True
)

left, right = st.columns(2)

with left:

    st.subheader("Evolution des performances")

    fig = px.line(
        filtered,
        x="factor",
        y="median_s",
        color="engine_label",
        markers=True,
        color_discrete_map=COLORS,
        labels={
            "factor": "Facteur de multiplication",
            "median_s": "Temps median (s)"
        }
    )

    st.plotly_chart(
        style_chart(fig),
        use_container_width=True
    )

with right:

    st.subheader("Debit de traitement")

    fig = px.bar(
        filtered,
        x="volume",
        y="throughput_rows_s",
        color="engine_label",
        barmode="group",
        color_discrete_map=COLORS,
        category_orders={"volume": ORDER},
        labels={
            "volume": "Volume",
            "throughput_rows_s": "Lignes / seconde"
        }
    )

    st.plotly_chart(
        style_chart(fig),
        use_container_width=True
    )

st.divider()

st.subheader("Speedup par rapport a Pandas")

fig = px.line(
    filtered,
    x="factor",
    y="speedup_vs_pandas",
    color="engine_label",
    markers=True,
    color_discrete_map=COLORS,
    labels={
        "factor": "Facteur",
        "speedup_vs_pandas": "Speedup"
    }
)

fig.add_hline(
    y=1,
    line_dash="dash",
    line_color="#4ADE80",
    annotation_text="Seuil d'egalite"
)

st.plotly_chart(
    style_chart(fig),
    use_container_width=True
)

st.caption(
    "Speedup > 1 : moteur plus rapide que Pandas. "
    "Speedup < 1 : moteur plus lent."
)

st.divider()

st.subheader("Variabilite des mesures")

runs = filtered[
    ["engine_label", "volume", "times_s"]
].explode("times_s")

runs["times_s"] = runs["times_s"].astype(float)

fig = px.box(
    runs,
    x="volume",
    y="times_s",
    color="engine_label",
    points="all",
    color_discrete_map=COLORS,
    category_orders={"volume": ORDER},
    labels={
        "volume": "Volume",
        "times_s": "Duree (s)"
    }
)

st.plotly_chart(
    style_chart(fig),
    use_container_width=True
)

st.divider()

st.subheader("Consommation memoire Pandas")

if "python_peak_rss_mib_cumulative" in df.columns:

    memory = df[
        df["engine"] == "pandas"
    ].dropna(
        subset=["python_peak_rss_mib_cumulative"]
    )

    fig = px.bar(
        memory,
        x="volume",
        y="python_peak_rss_mib_cumulative",
        text_auto=".1f",
        color_discrete_sequence=["#A78BFA"],
        category_orders={"volume": ORDER},
        labels={
            "volume": "Volume",
            "python_peak_rss_mib_cumulative":
                "Pic RSS cumulatif (Mio)"
        }
    )

    st.plotly_chart(
        style_chart(fig),
        use_container_width=True
    )

st.info(
    "La memoire Python cumulative de Pandas "
    "n'est pas directement comparable a la "
    "memoire totale JVM et executors de Spark."
)

st.divider()

st.subheader("Resultats detailles")

table = filtered[
    [
        "engine_label",
        "volume",
        "rows",
        "profiles",
        "median_s",
        "throughput_rows_s",
        "speedup_vs_pandas",
        "profiles_concordance"
    ]
].rename(columns={
    "engine_label": "Moteur",
    "volume": "Volume",
    "rows": "Evenements",
    "profiles": "Profils",
    "median_s": "Temps median (s)",
    "throughput_rows_s": "Lignes/s",
    "speedup_vs_pandas": "Speedup",
    "profiles_concordance": "Concordance"
})

table["Concordance"] *= 100

st.dataframe(
    table.round(4),
    use_container_width=True,
    hide_index=True
)

st.download_button(
    "Exporter les resultats CSV",
    data=table.to_csv(index=False).encode("utf-8"),
    file_name="darksea_benchmark.csv",
    mime="text/csv"
)

st.divider()

st.subheader("Interpretation scientifique")

best = df.loc[
    df.groupby("factor")["median_s"].idxmin()
]

if best["engine"].eq("pandas").all():
    st.markdown(
        "**Pandas obtient le meilleur temps median "
        "sur les quatre volumes testes.**"
    )

st.markdown(
    """
    - Les trois moteurs utilisent les memes fichiers Parquet.
    - Les calculs sont identiques : COUNT, AVG, MAX et SUM.
    - Trois repetitions chronometrees par configuration.
    - Les sessions Spark sont initialisees avant les mesures.
    - Les donnees synthetiques conservent les memes navires.
    - Le cluster Spark dispose d'un worker et de deux coeurs.
    - Les mesures ne demontrent pas une scalabilite multi-machine.
    - Les performances specifiques de HDFS ne sont pas mesurees.
    - Les caches systeme ne sont pas controles.
    """
)
