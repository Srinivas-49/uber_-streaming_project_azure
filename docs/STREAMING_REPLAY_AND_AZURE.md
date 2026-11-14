# Uber streaming: replay, recovery and cloud guardrails

## Offline validation (no cloud spend)
Run:
    python -m unittest discover -s tests -v
    python -m scripts.run_replay_demo --rides 30

The Python replay harness consumes deterministic synthetic ride events with time-zone-aware timestamps, fare validation, duplicate event IDs, late events, CDC snapshots and safe quarantine metadata. It does not record passenger contacts, payment credentials, location coordinates or raw invalid payloads. The latest record is selected by timestamp and event ID. Revenue uses Decimal, not binary floats.

Key result for default fixture: 39 received records, 1 duplicate, 1 invalid, 1 stale arrival, 30 current rides and 6 completed rides. CI tests verify these numbers and correct final state. This is NOT a proof of Spark/Kafka exactly-once processing.

## Real Azure path (manual and billable)
Infrastructure source: infra/azure/event_hubs.bicep. Standard Event Hubs namespaces have an ongoing hourly charge while present, even with no events; never leave the namespace idle after learning. Preview using Azure CLI what-if, compare regional cost, then explicitly deploy only during the planned workshop. Only synthetic events may be used.

Databricks Lakeflow source remains in uber_event_streaming/transformations. Set Event Hubs namespace/hub settings through pipeline configuration. Obtain credentials using Databricks secret scopes or a supported identity mechanism, never in GitHub. Run validation and Delta checkpoint replay checks in the actual workspace before claiming a live streaming deployment.

## Engineering gaps for production
Persistent event checkpoints, producer-side retries and source ordering, late-data policy/watermark, retention limits, per-ride idempotent sink, poison-record DLQ, Unity Catalog access control, encryption, audit log ingestion, SLO alerts and cost monitoring. Existing Lakeflow CDC code is not proven deployed.

Reuse the storage/ADF/Key Vault foundation in the enterprise-azure-databricks-lakehouse repository; DO NOT create a second Databricks workspace to demo this project.
