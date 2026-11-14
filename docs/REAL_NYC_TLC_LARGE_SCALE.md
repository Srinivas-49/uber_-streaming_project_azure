# Public ride operations data at real scale

**Official data:** NYC Taxi and Limousine Commission (TLC) trip records, https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page . The site publishes monthly Parquet files for high-volume for-hire vehicles, yellow taxi and green taxi. Taxi-zone lookup data is included. These are **TLC public records**, not proprietary Uber trips.

## Bounded sourcing

Preview the official URL plan without downloading anything:

    python -m scripts.fetch_tlc_public --months 2025-01 2025-02 --types fhvhv yellow

Explicitly download a known file after checking free disk, dataset size and network bandwidth:

    python -m scripts.fetch_tlc_public --months 2025-01 --types fhvhv --download --max-file-mb 750 --out raw-tlc

For months exceeding the cap the program raises an exception and deletes its partial file. It creates `manifest_tlc.json` with source URLs, lengths, SHA-256 hashes and provenance. Never commit Parquet or large raw downloads to GitHub.

## Databricks Free Edition vs full-scale

Upload `raw-tlc/trips/` and `raw-tlc/reference/` into your own UC Volume, then run `notebooks/03_real_tlc_fhv_zone_analytics.py`. Configure its landing_root widget to that volume and use the default **50,000-record sample**. The notebook performs Spark schema checks, Bronze ingestion, quality quarantine, validated Silver trip records, taxi-zone lookup joins and Gold daily metrics including p95 duration.

To analyse 12+ monthly Parquet files (potentially tens or hundreds of millions of rides), use a **properly sized, budget-approved Spark environment** and partition by service month/day; do not attempt to import all files through Free Edition in one run. Record actual row counts, throughput, bytes read, elapsed time and DBU/compute costs.

**Data modelling honesty:** TLC monthly trip files are completed historical records, not streaming Kafka events. They may have no globally unique trip ID. Never describe row-hash deduplication as true exactly-once processing. Existing synthetic Event Hubs/Lakeflow project remains a separate event-streaming exercise.

## Interview-grade next steps

Historical partitioned backfills, data-contract versioning, idempotent monthly file manifest, cloud object-store ingestion, observability, real shuffle optimization, adaptive query execution/skew metrics, GDPR/privacy controls, lineage and failure recovery. This repository's past local benchmark is not evidence for the official TLC dataset.
