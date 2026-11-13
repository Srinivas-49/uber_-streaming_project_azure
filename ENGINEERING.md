# Engineering reliability foundation (Databricks)

This branch introduces **local PySpark data-quality tests**, a GitHub Actions test workflow and a **sample microbatch audit/checkpoint helper**. These are foundational utilities; they are **not yet wired into the existing Lakeflow pipeline**, and no production cloud deployment or benchmark has been verified.

## Local tests
```bash
python -m pip install -r requirements-dev.txt
python -m unittest discover -s tests -v
```

## Quality checks
`uber_event_streaming/quality.py` checks required columns, null ride IDs and duplicate ride IDs on **bounded** DataFrames. The streaming helper in `streaming_reliability.py` shows a `foreachBatch` audit pattern; do not run it against an unbounded DataFrame directly. It logs JSON-formatted counts to the driver log.

## Reliability and replay
Use a persistent, unique checkpoint path per query; avoid deleting checkpoints as a routine retry strategy. Replaying events requires idempotent sinks and a defined deduplication key, which are **not implemented** by this example. Lakeflow manages its own streaming state; integrate quality expectations there separately.

## Observability
Suggested production metrics: event-time lag, input rows/sec, processed rows/sec, batch duration, bad-record counts, pipeline failure rate and recovery time. Connect Lakeflow event logs and Azure Monitor alerts after deploying the workspace and choosing alert thresholds.

## Performance benchmark plan
1. Generate deterministic synthetic ride events with a known event count.
2. Record event rate, p50/p95 end-to-end latency, processing throughput, compute size and cost.
3. Test late events, duplicate IDs, schema drift and restart behavior.
4. Publish measurements and test conditions; do not claim benchmark results without running them.

## Deployment roadmap
Add Databricks Asset Bundles and a Terraform-managed workspace/permissions setup once workspace identifiers, authentication method and target environments are defined. Keep secrets in Azure Key Vault or Databricks secret scopes; do not commit connection strings.

## Known existing issues
The existing `sliver.py` filename is intentionally unchanged. Align inconsistent table names and review CDC sequence keys before deploying the Gold models. This PR adds no changes to live transformations.
