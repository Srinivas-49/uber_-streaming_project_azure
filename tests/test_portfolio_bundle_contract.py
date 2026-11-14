"""Offline CI preflight; Databricks job and catalog must be tested live."""
import ast
from pathlib import Path

def test_deployment_notebooks_compile():
    for file in ("deployment/00_bootstrap_workspace.py", "deployment/90_validate_uber.py"):
        ast.parse(Path(file).read_text())

def test_synthetic_bundle_paused_no_paid_cluster():
    cfg = Path("portfolio_bundle/databricks.yml").read_text()
    assert "pause_status: PAUSED" in cfg
    assert "new_cluster:" not in cfg and "existing_cluster_id:" not in cfg
    assert "../notebooks/01_generate_synthetic_rides.py" in cfg
    assert "../notebooks/02_cdc_quality_gold.py" in cfg
    assert "../deployment/90_validate_uber.py" in cfg
    assert cfg.index("task_key: bootstrap") < cfg.index("task_key: bronze_events")
    assert cfg.index("task_key: bronze_events") < cfg.index("task_key: cdc_and_gold")
    assert cfg.index("task_key: cdc_and_gold") < cfg.index("task_key: validate")
