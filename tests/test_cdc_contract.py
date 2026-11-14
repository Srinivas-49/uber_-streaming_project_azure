"""Offline structural checks; no claim of a Databricks cloud test."""
import ast
from pathlib import Path

MODEL = Path("uber_event_streaming/transformations/model.py")
INGEST = Path("uber_event_streaming/transformations/ingest.py")

def test_unique_cdc_view_functions():
    module = ast.parse(MODEL.read_text())
    functions = [node.name for node in module.body if isinstance(node, ast.FunctionDef)]
    assert len(functions) == len(set(functions))
    assert len([node for node in ast.walk(module) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "create_auto_cdc_flow"]) == 7

def test_time_based_sequence_columns():
    source = MODEL.read_text()
    assert 'sequence_by=F.col("booking_timestamp")' in source
    assert 'sequence_by=F.col("city_updated_at")' in source
    assert 'sequence_by=F.col("ride_id")' not in source

def test_eventhub_configuration_and_fail_closed_offset_loss():
    source = INGEST.read_text()
    assert '"failOnDataLoss": "true"' in source
    assert 'spark.conf.get("uber.eventhubs.namespace"' in source
    assert 'dbutils.secrets.get(' in source
    assert 'event-uber-0000' not in source
