# CDC and Event Hubs hardening — deployment preflight

**This change is source code and offline checks, not evidence of a successful Azure deployment.**

## Changed
- Event Hubs namespace, hub and offset cap are configurable via pipeline Spark configuration; the connection string stays in Databricks secrets.
- Event Hubs consumer uses `failOnDataLoss=true` to make unavailable offsets visible instead of silently skipping data. A missing-offset failure requires explicit recovery procedures; no automatic replay guarantee is claimed.
- Seven Gold CDC definitions have uniquely named view functions and timestamp-based `sequence_by` fields (rather than entity IDs); location retains SCD Type 2.
- Gold names consistently specify the configured catalog.

## Still required before production
1. Configure `uber.eventhubs.namespace`, `uber.eventhubs.name`, `uber.catalog` and the Event Hubs secret; provide precreated Bronze/Silver/Gold schemas and reference tables.
2. Validate the Databricks Lakeflow runtime, bundle configuration, fully-qualified table names and permitted pipeline schemas.
3. Ensure CDC source has a stable ordering value per key. Multiple changes at the same timestamp require tie-breaking and source-specific semantics. Booking timestamps may not reflect actual updates to driver/payment entities; use authoritative change timestamps in real systems.
4. Reconcile bad JSON, late events, duplicates and PII handling; test checkpoints, offset retention, replay and observability.
5. Confirm the Gold `fact_ride` rename is reflected in notebooks, dashboards and dependencies.
6. Measure streaming throughput, failures and cost on a live workspace; do not report the 10,000-offset limit as measured messages per second.
