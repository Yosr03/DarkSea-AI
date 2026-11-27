import ast

from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]

V1 = BASE_DIR / "consumer_v1_instrumented.py"
V2 = BASE_DIR / "consumer_v2.py"


def get_source(path):

    return path.read_text(encoding="utf-8")


def get_batch_function(path):

    tree = ast.parse(get_source(path))

    return next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "process_batch"
    )


def count_actions(path):

    function = get_batch_function(path)

    actions = {
        "count",
        "isEmpty",
        "first",
        "show",
        "collect"
    }

    total = 0

    for node in ast.walk(function):

        if not isinstance(node, ast.Call):
            continue

        if not isinstance(node.func, ast.Attribute):
            continue

        if node.func.attr not in actions:
            continue

        receiver = node.func.value

        # Exclure les fonctions SQL : F.count(...)
        if (
            isinstance(receiver, ast.Name)
            and receiver.id == "F"
        ):
            continue

        # Exclure groupBy(...).count()
        # qui retourne un DataFrame
        if (
            node.func.attr == "count"
            and isinstance(receiver, ast.Call)
            and isinstance(receiver.func, ast.Attribute)
            and receiver.func.attr in {
                "groupBy",
                "groupby",
                "rollup",
                "cube"
            }
        ):
            continue

        total += 1

    return total

def test_v2_exists():

    assert V2.exists()


def test_v2_syntax():

    ast.parse(get_source(V2))


def test_v1_action_count():

    assert count_actions(V1) == 8


def test_v2_action_count():

    assert count_actions(V2) == 2


def test_model_preserved():

    assert "/models/kmeans_events" in get_source(V2)

    assert "model.transform(features)" in get_source(V2)


def test_threshold_preserved():

    assert "PHYSICAL_THRESHOLD = 80.0" in get_source(V2)

    assert 'metrics["anomaly_threshold"]' in get_source(V2)


def test_isolated_checkpoint():

    assert "{RUN_BASE}/checkpoint" in get_source(V2)


def test_hdfs_outputs_preserved():

    content = get_source(V2)

    for directory in [
        "results",
        "alerts",
        "invalid"
    ]:
        assert f"{{RUN_BASE}}/{directory}/" in content


def test_kafka_topics_preserved():

    content = get_source(V2)

    for topic in [
        "ais-benchmark-events",
        "ais-benchmark-alerts",
        "ais-benchmark-invalid"
    ]:
        assert topic in content


def test_cache_cleanup():

    content = get_source(V2)

    assert "scored.unpersist()" in content
    assert "checked.unpersist()" in content
    assert "invalid.unpersist()" not in content


def test_alert_categories():

    content = get_source(V2)

    for category in [
        "NON_SIGNALE",
        "ANOMALIE_STATISTIQUE",
        "COHERENCE_PHYSIQUE"
    ]:
        assert category in content


def test_metrics_instrumentation():

    content = get_source(V2)

    assert "scoring_and_statistics" in content
    assert "timer.finish(" in content
    assert 'consumer_version="v2"' in content

def test_v1_readiness_signal():

    content = V1.read_text(encoding="utf-8")

    assert "SPARK BENCHMARK READY" in content

    assert "query.lastProgress" in content


def test_v2_readiness_signal():

    content = V2.read_text(encoding="utf-8")

    assert "SPARK BENCHMARK READY" in content

    assert "query.lastProgress" in content


def test_v1_initial_offsets_verification():

    content = V1.read_text(encoding="utf-8")

    assert "/sources/0/0" in content

    assert "filesystem.exists(" in content


def test_v2_initial_offsets_verification():

    content = V2.read_text(encoding="utf-8")

    assert "/sources/0/0" in content

    assert "filesystem.exists(" in content