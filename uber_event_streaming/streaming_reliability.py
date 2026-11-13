"""Reusable bounded-microbatch validation/metrics hook.

Use in a separate Structured Streaming foreachBatch query when appropriate.
Do not attach blindly to Lakeflow managed streaming tables; Lakeflow manages
checkpoints and execution, and production integrations need separate design.
"""
import json
import logging
from .quality import quality_metrics

logger = logging.getLogger(__name__)

def audit_batch(batch_df, batch_id: int) -> None:
    """Log counts for each microbatch; does not guarantee exactly-once writes."""
    metrics = quality_metrics(batch_df)
    logger.info(json.dumps({"event": "ride_batch_quality",
                            "batch_id": batch_id, **metrics}))
    if metrics["null_ride_ids"] or metrics["duplicate_ride_ids"]:
        raise ValueError(f"Batch {batch_id} failed quality checks: {metrics}")

def example_query(stream_df, checkpoint_location: str):
    """Example only; requires a durable, unique checkpoint path per query."""
    return (stream_df.writeStream
            .foreachBatch(audit_batch)
            .option("checkpointLocation", checkpoint_location)
            .start())
