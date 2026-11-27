from pathlib import Path
from html import escape

import streamlit as st


BASE_DIR = Path(__file__).resolve().parents[1]


def configure_page(title):

    st.set_page_config(
        page_title=f"{title} | DarkSea-AI",
        page_icon="⚓",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    css_path = BASE_DIR / "assets" / "style.css"

    st.markdown(
        f"<style>{css_path.read_text(encoding='utf-8')}</style>",
        unsafe_allow_html=True,
    )

    with st.sidebar:

        st.title("⚓ DarkSea-AI")

        st.caption(
            "Maritime Intelligence Platform"
        )

        st.divider()

        st.markdown("**Architecture Big Data**")

        st.caption("Hadoop • Pig • Hive")
        st.caption("Spark • MLlib • Kafka")

        st.divider()

        st.caption(
            "Source : Global Fishing Watch"
        )

        st.caption(
            "Analyse historique et streaming simulé"
        )


def page_header(title, subtitle):

    st.markdown(
        f"""
        <div class="darksea-header">
            <div class="section-label">
                DARKSEA INTELLIGENCE SYSTEM
            </div>
            <div class="darksea-title">
                {escape(title)}
            </div>
            <div class="darksea-subtitle">
                {escape(subtitle)}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def format_number(value):

    return f"{value:,.0f}".replace(",", " ")