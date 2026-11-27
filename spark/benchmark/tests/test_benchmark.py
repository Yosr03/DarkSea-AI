import importlib.util
import unittest
from pathlib import Path

import pandas as pd

path = (
    Path(__file__).resolve().parents[1]
    / "04_compare_results.py"
)

spec = importlib.util.spec_from_file_location(
    "compare_results", path
)

module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class TestBenchmark(unittest.TestCase):

    def setUp(self):

        self.reference = pd.DataFrame({
            "mmsi": ["111111111", "222222222"],
            "nb_interruptions": [2, 3],
            "duree_moyenne_h": [10.0, 20.0],
            "duree_max_h": [15.0, 25.0],
            "duree_totale_h": [20.0, 60.0]
        })

    def test_identical_results(self):

        module.compare_profiles(
            self.reference,
            self.reference.copy()
        )

    def test_different_order(self):

        module.compare_profiles(
            self.reference,
            self.reference.iloc[::-1]
        )

    def test_wrong_count(self):

        candidate = self.reference.copy()
        candidate.loc[0, "nb_interruptions"] = 99

        with self.assertRaises(AssertionError):
            module.compare_profiles(
                self.reference, candidate
            )

    def test_wrong_duration(self):

        candidate = self.reference.copy()
        candidate.loc[0, "duree_totale_h"] = 999

        with self.assertRaises(AssertionError):
            module.compare_profiles(
                self.reference, candidate
            )

    def test_missing_vessel(self):

        with self.assertRaises(AssertionError):
            module.compare_profiles(
                self.reference,
                self.reference.iloc[:1]
            )

    def test_float_tolerance(self):

        candidate = self.reference.copy()
        candidate.loc[0, "duree_totale_h"] += 1e-8

        module.compare_profiles(
            self.reference, candidate
        )


if __name__ == "__main__":
    unittest.main()
