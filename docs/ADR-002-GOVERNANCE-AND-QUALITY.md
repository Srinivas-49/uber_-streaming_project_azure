# ADR-002 — Governance, lineage and data-quality policy

**Status:** design specification and committed policy template. No verified live grant or column mask.

## Problem
Ride events contain passenger/driver identifiers and locations. A wide Silver one-big-table risks exposing these values in dashboards and public analytics.

## Decision
Use Unity Catalog (UC) on Bronze/Silver/Gold objects with least-privilege groups. Use analyst-specific views and Unity Catalog column masks for sensitive phone/email fields; restrict raw access. Use UC lineage for source-to-table and column tracing once the pipeline is deployed.

## Tradeoffs
Column masking adds governance simplicity, but does not replace storage permission hardening or access reviews. Row filtering may be required for tenant/city scoping. UC lineage captures supported query/pipeline lineage; upstream Event Hubs producers and external HTTP steps need documented mappings.

## Quality gates
1. Source contract: expected JSON schema, presence of ride ID and timestamps.
2. Bronze: count Kafka offsets and malformed payloads; route bad data to an explicit quarantine table.
3. Silver: uniqueness and null-key checks, late-arrival indicators, accepted/rejected counts.
4. Gold: CDC type-1/type-2 correctness, SCD2 effective date continuity, no overlapping location records.
5. Observability: event log, freshness SLA, pipeline duration, error rate, quarantine count, retry/failure alert, resource DBUs.

## Validation
Run SQL tests after deployment using least-privileged identities, verify masking and lineage, and attach screenshots/job IDs to evidence artifacts. This ADR does not claim these runtime checks have already passed.

## References
- [Unity Catalog template](../governance/unity_catalog.sql)
- [Architecture rationale](ADR-001-UBER-ARCHITECTURE.md)
- [Bundle guide](BUNDLE_DEPLOYMENT_GUIDE.md)
