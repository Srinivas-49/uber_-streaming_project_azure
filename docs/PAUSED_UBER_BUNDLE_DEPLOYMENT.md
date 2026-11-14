# Databricks workspace: synthetic Uber project deployment

This repo contains TWO independent workloads:
1. A paid Azure Event Hubs/Lakeflow blueprint configured by root databricks.yml. It requires a real Azure workspace, secrets, source feeds, and deployment-specific corrections.
2. A zero-external-service **synthetic portfolio job** in portfolio_bundle/databricks.yml. Use this first in Databricks Free Edition.

No Databricks workspace has yet been logged into; files under portfolio_bundle are definitions, not an actual job.

## Required manual setup
1. Open the real Databricks workspace, not a Logic Apps HTTP trigger.
2. Create a Git folder: https://github.com/Srinivas-49/uber_-streaming_project_azure.git (main).
3. Run deployment/00_bootstrap_workspace.py; inspect whether ordinary tags were applied.
4. Run notebooks/01_generate_synthetic_rides.py and notebooks/02_cdc_quality_gold.py in order.
5. Run deployment/90_validate_uber.py and inspect the audit rows in uber_portfolio.ops_quality_audit.
6. Optionally run governance/POST_RUN_TAGS.sql if you have privileges.
7. Save actual run IDs, Delta table counts, query plans, job duration and screenshots of Gold tables.

## Optional jobs-as-code path
From a checked-out repo in your own authenticated terminal:

    cd portfolio_bundle
    databricks bundle validate --target dev
    databricks bundle deploy --target dev
    databricks bundle run uber_synthetic_quality --target dev

This job specification synchronizes ../notebooks and ../deployment, with a daily schedule PAUSED and only one run at a time. Validate the bundle on the target workspace before deployment. Running a serverless job on a paid Azure workspace can incur compute charges even if its schedule is paused.

## Production upgrade work
Use the separate Event Hubs/Lakeflow project with durable checkpoint storage, actual source schema contracts, real Kafka ingest, CDC sequence timestamps, streaming lateness policy, SCD history, Unity Catalog permissions, incident response and measured KPIs. Never call a synthetic batch notebook real-time production streaming.
