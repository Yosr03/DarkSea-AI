from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent

source = BASE_DIR / "consumer_v1_instrumented.py"
destination = BASE_DIR / "consumer_v2.py"

content = source.read_text(encoding="utf-8")


def replace_once(old, new):

    global content

    if content.count(old) != 1:
        raise RuntimeError(
            f"Unexpected source pattern: {old}"
        )

    content = content.replace(old, new, 1)


# 1. Suppression de l'action isEmpty()

replace_once(
    """    with timer.measure("empty_check"):
        empty = batch_df.isEmpty()

    if empty:
        return""",
    ""
)


# 2. Regroupement des statistiques de validation

replace_once(
    """        with timer.measure("validation"):
            total = checked.count()

        invalid = checked.filter(
            ~F.col("event_valid")
        ).withColumn(
            "motif",
            F.lit("INVALID_JSON_OR_EVENT_FIELDS")
        ).cache()

        with timer.measure("invalid_count"):
            invalid_count = invalid.count()

        valid = checked.filter(
            F.col("event_valid")
        )

        valid_count = total - invalid_count""",
    """        with timer.measure("validation"):
            counts = checked.agg(
                F.count("*").alias("total"),
                F.count(
                    F.when(F.col("event_valid"), 1)
                ).alias("valid_count")
            ).first()

        total = counts["total"]
        valid_count = counts["valid_count"]
        invalid_count = total - valid_count

        if total == 0:
            timer.set(
                input_rows=0,
                valid_rows=0,
                invalid_rows=0,
                scored_rows=0,
                alert_count=0,
                ml_count=0
            )
            return

        invalid = checked.filter(
            ~F.col("event_valid")
        ).withColumn(
            "motif",
            F.lit("INVALID_JSON_OR_EVENT_FIELDS")
        )

        valid = checked.filter(
            F.col("event_valid")
        )"""
)


# 3. Regroupement du scoring et des statistiques

start_marker = '        with timer.measure("scoring"):'

end_marker = (
    "        # ----------------------------------\n"
    "        # 13. SAUVEGARDE HDFS"
)

if content.count(start_marker) != 1:
    raise RuntimeError("Scoring marker not found")

if content.count(end_marker) != 1:
    raise RuntimeError("HDFS marker not found")

start = content.index(start_marker)
end = content.index(end_marker, start)

optimized_scoring = '''        with timer.measure("scoring_and_statistics"):

            stats = scored.agg(
                F.count("*").alias("scored_count"),

                F.count(
                    F.when(
                        F.col("type_alerte") != "NON_SIGNALE",
                        1
                    )
                ).alias("alert_count"),

                F.count(
                    F.when(
                        F.col("anomalie_potentielle"),
                        1
                    )
                ).alias("ml_count"),

                F.count(
                    F.when(
                        F.col("type_alerte") == "COHERENCE_PHYSIQUE",
                        1
                    )
                ).alias("physical_count"),

                F.count(
                    F.when(
                        F.col("type_alerte") == "ANOMALIE_STATISTIQUE",
                        1
                    )
                ).alias("statistical_count"),

                F.count(
                    F.when(
                        F.col("type_alerte") == "NON_SIGNALE",
                        1
                    )
                ).alias("normal_count"),

                F.avg("latence_ms").alias("latency_mean_ms"),

                F.expr(
                    "percentile_approx(latence_ms, 0.95)"
                ).alias("latency_p95_ms"),

                F.max("latence_ms").alias("latency_max_ms")

            ).first().asDict()

        scored_count = stats["scored_count"]
        alert_count = stats["alert_count"]
        ml_count = stats["ml_count"]

        assert scored_count == valid_count

        assert (
            stats["physical_count"]
            + stats["statistical_count"]
            + stats["normal_count"]
        ) == scored_count

        alerts = scored.filter(
            F.col("type_alerte") != "NON_SIGNALE"
        )

        timer.set(
            scored_rows=scored_count,
            **stats
        )

'''

content = (
    content[:start]
    + optimized_scoring
    + content[end:]
)


# 4. Suppression des actions statistiques redondantes

start_marker = (
    '        with timer.measure("type_statistics"):'
)

end_marker = (
    '        print("MICRO-BATCH TRAITE AVEC SUCCES")'
)

if content.count(start_marker) != 1:
    raise RuntimeError("Statistics marker not found")

if content.count(end_marker) != 1:
    raise RuntimeError("End marker not found")

start = content.index(start_marker)
end = content.index(end_marker, start)

content = (
    content[:start]
    + '        print("Statistiques :", json.dumps(stats))\n\n'
    + content[end:]
)


# 5. Suppression du unpersist des invalides non caches

replace_once(
    """        if invalid is not None:
            invalid.unpersist()

""",
    ""
)


# 6. Identification de la version

replace_once(
    "    timer = BatchMetrics(args.run_name, batch_id)",
    """    timer = BatchMetrics(args.run_name, batch_id)
    timer.set(consumer_version="v2")"""
)


# 7. Verification syntaxique

compile(content, str(destination), "exec")

destination.write_text(
    content,
    encoding="utf-8"
)

print("GENERATION V2 REUSSIE")
print("Fichier genere :", destination)