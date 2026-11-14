"""Offline syntax and privacy-constrained real TLC notebook review."""
import ast
import unittest
from pathlib import Path

NOTEBOOK = Path("notebooks/03_real_tlc_zones_weather.py")

class RealTLCNotebookTests(unittest.TestCase):
    def test_python_syntax(self):
        ast.parse(NOTEBOOK.read_text(encoding="utf-8"))

    def test_three_source_gold_and_reconciliation(self):
        source = NOTEBOOK.read_text(encoding="utf-8")
        for key in ("fhvhv_tripdata_", "taxi_zone_lookup.csv", "nyc_weather_",
                    "silver_valid_trips", "silver_enriched_trips", "silver_trip_quarantine",
                    "gold_zone_day_operations",
                    "assert valid_n + quarantine_n == total",
                    "assert enriched.count() == valid_n"):
            self.assertIn(key, source)

if __name__ == "__main__":
    unittest.main()
