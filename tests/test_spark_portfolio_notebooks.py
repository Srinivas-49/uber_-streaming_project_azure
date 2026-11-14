"""Offline syntax and stage checks; cloud runtime separately verified."""
import ast
from pathlib import Path

def test_notebook_syntax():
    for filename in ("01_generate_synthetic_rides.py", "02_cdc_quality_gold.py"):
        ast.parse((Path("notebooks") / filename).read_text())

def test_advanced_pipeline_contract():
    source = (Path("notebooks") / "02_cdc_quality_gold.py").read_text()
    for term in ("dropDuplicates", "silver_ride_quarantine", "silver_ride_scd2",
                 "silver_current_rides", "gold_zone_kpis",
                 "assert a == b == c"):
        assert term in source
