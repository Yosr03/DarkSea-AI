
import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BASE = "hdfs://namenode:9000/darksea"

SCORE_TOLERANCE = 0.000001

OUTPUT = Path(
    "/workspace/spark/benchmark/streaming/results"
)


def parse_args():

    parser = argparse.ArgumentParser()

    parser.add_argument("--run-v1", required=True)
    parser.add_argument("--run-v2", required=True)

    parser.add_argument("--replay-v1", required=True)
    parser.add_argument("--replay-v2", required=True)

    parser.add_argument(
        "--limit",
        type=int,
        default=1000
    )

    parser.add_argument(
        "--expected-alerts",
        type=int,
        required=True
    )

    args = parser.parse_args()

    if args.limit <= 0:
        parser.error("Limit must be positive")

    if not 0 <= args.expected_alerts <= args.limit:
        parser.error("Invalid expected alert count")

    for name in (args.run_v1, args.run_v2):

        if not name.replace("_", "").replace("-", "").isalnum():
            parser.error("Invalid run name")

    if args.run_v1 == args.run_v2:
        parser.error("Run names must be different")

    return args


def invalid_number(column):

    return (
        column.isNull()
        | F.isnan(column)
        | (F.abs(column) == float("inf"))
    )


def score_difference(left, right):

    return (
        invalid_number(left)
        | invalid_number(right)
        | (
            F.abs(left - right)
            > SCORE_TOLERANCE
        )
    )


def count_if(condition, name):

    return F.count(
        F.when(condition, 1)
    ).alias(name)


def load_stream(spark, run_name):

    return (
        spark.read
        .option("recursiveFileLookup", "true")
        .parquet(
            f"{BASE}/benchmark/streaming/"
            f"{run_name}/results"
        )
        .cache()
    )


def load_reference(spark, limit):

    reference = (
        spark.read.parquet(
            f"{BASE}/results/evenements_scores"
        )
        .filter(F.col("data_split") == "TEST")
    )

    reference = reference.withColumn(
        "_benchmark_end_time",
        F.to_timestamp("gap_end_timestamp")
    )

    if reference.filter(
        F.col("_benchmark_end_time").isNull()
    ).count() > 0:

        raise ValueError(
            "Invalid reference timestamps"
        )

    return (
        reference
        .orderBy(
            "_benchmark_end_time",
            "gap_id"
        )
        .limit(limit)
        .drop("_benchmark_end_time")
        .cache()
    )


def validate_stream(
    reference,
    stream,
    replay_id,
    expected_count,
    expected_alerts
):

    stats = stream.agg(

        F.count("*").alias("streaming_events"),

        F.countDistinct("gap_id").alias(
            "unique_events"
        ),

        count_if(
            F.col("gap_id").isNull(),
            "null_ids"
        ),

        count_if(
            ~F.col("replay_id").eqNullSafe(
                F.lit(replay_id)
            ),
            "unexpected_replay_rows"
        ),

        count_if(
            F.col("anomalie_potentielle"),
            "streaming_anomalies"
        ),

        count_if(
            F.col("type_alerte") != "NON_SIGNALE",
            "total_alerts"
        ),

        count_if(
            F.col("incoherence_physique_potentielle"),
            "physical_inconsistencies"
        ),

        count_if(
            F.col("latence_ms") < 0,
            "negative_latencies"
        ),

        count_if(
            invalid_number(F.col("latence_ms")),
            "invalid_latencies"
        ),

        count_if(
            F.col("cluster").isNull()
            | F.col("anomalie_potentielle").isNull()
            | F.col("incoherence_physique_potentielle").isNull()
            | F.col("type_alerte").isNull(),
            "null_predictions"
        )

    ).first().asDict()

    stats["duplicates"] = (
        stats["streaming_events"]
        - stats["unique_events"]
    )

    reference_ids = reference.select("gap_id")

    stream_ids = stream.select("gap_id")

    stats["missing_events"] = (
        reference_ids.join(
            stream_ids,
            "gap_id",
            "left_anti"
        ).count()
    )

    stats["unexpected_events"] = (
        stream_ids.join(
            reference_ids,
            "gap_id",
            "left_anti"
        ).count()
    )

    comparison = (
        reference.alias("b")
        .join(
            stream.alias("s"),
            "gap_id",
            "inner"
        )
    )

    prediction_stats = comparison.agg(

        F.count("*").alias(
            "compared_events"
        ),

        count_if(
            ~F.col("b.cluster").eqNullSafe(
                F.col("s.cluster")
            ),
            "cluster_differences"
        ),

        count_if(
            score_difference(
                F.col("b.distance_centroide"),
                F.col("s.distance_centroide")
            ),
            "score_differences"
        ),

        count_if(
            ~F.col("b.anomalie_potentielle").eqNullSafe(
                F.col("s.anomalie_potentielle")
            ),
            "anomaly_differences"
        ),

        F.max(
            F.abs(
                F.col("b.distance_centroide")
                - F.col("s.distance_centroide")
            )
        ).alias("max_score_difference")

    ).first().asDict()

    stats.update(prediction_stats)

    stats["reference_events"] = reference.count()

    stats["reference_anomalies"] = reference.filter(
        F.col("anomalie_potentielle")
    ).count()

    zero_fields = [

        "duplicates",
        "null_ids",
        "unexpected_replay_rows",
        "missing_events",
        "unexpected_events",
        "cluster_differences",
        "score_differences",
        "anomaly_differences",
        "negative_latencies",
        "invalid_latencies",
        "null_predictions"

    ]

    stats["validation_passed"] = (

        stats["reference_events"] == expected_count

        and stats["streaming_events"] == expected_count

        and stats["unique_events"] == expected_count

        and stats["compared_events"] == expected_count

        and stats["total_alerts"] == expected_alerts

        and stats["reference_anomalies"]
        == stats["streaming_anomalies"]

        and all(
            stats[field] == 0
            for field in zero_fields
        )

    )

    return stats


def compare_versions(v1, v2, expected_count):

    comparison = (
        v1.alias("v1")
        .join(
            v2.alias("v2"),
            "gap_id",
            "inner"
        )
    )

    stats = comparison.agg(

        F.count("*").alias(
            "compared_events"
        ),

        count_if(
            ~F.col("v1.cluster").eqNullSafe(
                F.col("v2.cluster")
            ),
            "cluster_differences"
        ),

        count_if(
            score_difference(
                F.col("v1.distance_centroide"),
                F.col("v2.distance_centroide")
            ),
            "score_differences"
        ),

        count_if(
            ~F.col("v1.anomalie_potentielle").eqNullSafe(
                F.col("v2.anomalie_potentielle")
            ),
            "anomaly_differences"
        ),

        count_if(
            ~F.col("v1.type_alerte").eqNullSafe(
                F.col("v2.type_alerte")
            ),
            "alert_type_differences"
        ),

        count_if(
            ~F.col(
                "v1.incoherence_physique_potentielle"
            ).eqNullSafe(
                F.col(
                    "v2.incoherence_physique_potentielle"
                )
            ),
            "physical_rule_differences"
        ),

        F.max(
            F.abs(
                F.col("v1.distance_centroide")
                - F.col("v2.distance_centroide")
            )
        ).alias("max_score_difference")

    ).first().asDict()

    stats["missing_in_v2"] = (
        v1.select("gap_id")
        .join(
            v2.select("gap_id"),
            "gap_id",
            "left_anti"
        ).count()
    )

    stats["unexpected_in_v2"] = (
        v2.select("gap_id")
        .join(
            v1.select("gap_id"),
            "gap_id",
            "left_anti"
        ).count()
    )

    stats["validation_passed"] = (

        stats["compared_events"] == expected_count

        and all(

            value == 0

            for key, value in stats.items()

            if key.endswith("_differences")
            or key in (
                "missing_in_v2",
                "unexpected_in_v2"
            )

        )

    )

    return stats


def main():

    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("DarkSea-Benchmark-Validation")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    cached = []

    try:

        print("\n=== CHARGEMENT REFERENCE BATCH ===")

        reference = load_reference(
            spark,
            args.limit
        )

        cached.append(reference)

        reference_count = reference.count()

        reference_unique = (
            reference.select("gap_id")
            .distinct()
            .count()
        )

        print(
            "Reference events:",
            reference_count
        )

        if (
            reference_count != args.limit
            or reference_unique != args.limit
            or reference.filter(
                F.col("gap_id").isNull()
            ).count() > 0
        ):

            raise ValueError(
                "Invalid reference dataset"
            )

        v1 = load_stream(
            spark,
            args.run_v1
        )

        cached.append(v1)

        v2 = load_stream(
            spark,
            args.run_v2
        )

        cached.append(v2)

        print("\n=== REFERENCE BATCH / V1 ===")

        report_v1 = validate_stream(
            reference,
            v1,
            args.replay_v1,
            args.limit,
            args.expected_alerts
        )

        print(json.dumps(
            report_v1,
            indent=4
        ))

        print("\n=== REFERENCE BATCH / V2 ===")

        report_v2 = validate_stream(
            reference,
            v2,
            args.replay_v2,
            args.limit,
            args.expected_alerts
        )

        print(json.dumps(
            report_v2,
            indent=4
        ))

        print("\n=== COMPARAISON V1 / V2 ===")

        comparison = compare_versions(
            v1,
            v2,
            args.limit
        )

        print(json.dumps(
            comparison,
            indent=4
        ))

        validated = all([

            report_v1["validation_passed"],

            report_v2["validation_passed"],

            comparison["validation_passed"]

        ])

        report = {

            "run_v1": args.run_v1,

            "run_v2": args.run_v2,

            "replay_v1": args.replay_v1,

            "replay_v2": args.replay_v2,

            "expected_events": args.limit,

            "expected_alerts": args.expected_alerts,

            "score_tolerance": SCORE_TOLERANCE,

            "batch_vs_v1": report_v1,

            "batch_vs_v2": report_v2,

            "v1_vs_v2": comparison,

            "validation_passed": validated

        }

        destination = OUTPUT / (
            f"validation_{args.run_v1}"
            f"_vs_{args.run_v2}.json"
        )

        OUTPUT.mkdir(
            parents=True,
            exist_ok=True
        )

        temporary = destination.with_suffix(".tmp")

        temporary.write_text(
            json.dumps(
                report,
                indent=4,
                allow_nan=False
            ),
            encoding="utf-8"
        )

        temporary.replace(destination)

        print("\n=== RESULTAT FINAL ===")

        print("Report:", destination)

        if validated:

            print(
                "BENCHMARK VALIDATION PASSED"
            )

        else:

            print(
                "BENCHMARK VALIDATION FAILED"
            )

            raise RuntimeError(
                "Prediction concordance failed"
            )

    finally:

        for dataframe in cached:

            dataframe.unpersist()

        spark.stop()


if __name__ == "__main__":

    main()
