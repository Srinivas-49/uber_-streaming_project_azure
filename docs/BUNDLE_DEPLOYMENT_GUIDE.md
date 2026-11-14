# Secure Databricks bundle deployment checklist

This repository contains a deployment **configuration**, not evidence of a deployed service. The daily schedule is **PAUSED** by default.

## 1. Preflight (required before any deploy)
- Review Azure subscription budget and Databricks serverless availability.
- Prepare Event Hubs namespace/hub, Kafka-compatible access, and the `uber-streaming/eh-connection-string` secret scope.
- Provision an ADLS Gen2 account and run ADF reference ingestion first; compare `raw/igestion/` paths with Bronze notebooks.
- Create a Unity Catalog catalog and bronze/silver/gold schemas; replace current hardcoded catalog `uberxxxxxx000000` and align model table names.
- Review CDC ordering and SCD2 update timestamps, deduplication and retention; confirm the Lakeflow source files all load and publish to intended schemas.
- Set up a Databricks service principal with least-privilege workspace and Unity Catalog permissions.
- Add a GitHub OIDC federation policy specifically for this repository/environment (some newer repos use an immutable numeric claim format).
- Configure GitHub Environments `dev` and `prod`, requiring reviewers for `prod`; set environment variables `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `UC_CATALOG`, `ALERT_EMAIL`. Do not store secrets in Git.
- In GitHub Actions, manually start *Databricks bundle - manual validate and deploy* with `deploy=false`; fix all errors.

## 2. Deploy and validate
After approval, run workflow with `deploy=true` in `dev`. This uploads and configures resources. Verify actual pipeline runs separately (the schedule is paused). Use CLI:
```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run uber_daily_refresh --target dev
```
Observe pipeline update details, Bronze/Silver/Gold row counts and table lineage. Inject known duplicates, malformed events and a failed task; verify recovery and alerts.

## 3. Operational handoff
- Enable the paused schedule **only after** successful trials and cost approval.
- Capture GitHub workflow run URL, Databricks job ID, pipeline update ID, counts, DBUs, duration, retry behavior and alert evidence.
- Prevent PII exposure; run governance SQL from a privileged identity after replacing catalog/group placeholders.
- In production enforce separate run-as principal, storage grants, environment approval, alert recipients and explicit rollback procedures.

## Sources
https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/examples
https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/provider-github
https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/access-control/
