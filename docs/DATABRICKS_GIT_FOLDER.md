# Databricks Free Edition Git folder for Uber project

**Workspace folders have not yet been created.** Inside your own Databricks workspace, open Workspace -> your home folder -> Create -> Git folder. Clone:
https://github.com/Srinivas-49/uber_-streaming_project_azure.git

Suggested Git folder name: uber-azure-streaming. Select main.

Run in order:
1. notebooks/01_generate_synthetic_rides.py — Spark basics and Bronze synthetic data.
2. notebooks/02_cdc_quality_gold.py — quality quarantine, time-aware history, latest-state Silver and Gold analytics.

Managed tables are created in uber_portfolio. The repo also has actual Azure Event Hubs/Lakeflow implementation sources in uber_event_streaming/transformations, which require a separate cloud configuration and must be validated there. This standalone two-notebook series uses no Event Hubs, no authentication secrets and no live streaming. GitHub CI syntax tests are not proof of cloud runtime success.
