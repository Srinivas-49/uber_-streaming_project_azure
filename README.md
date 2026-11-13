# Uber Event Streaming — Azure Databricks Lakeflow pipeline

[![Azure Databricks](https://img.shields.io/badge/Azure-Databricks-FF3621)](https://azure.microsoft.com/products/databricks)
[![Azure Event Hubs](https://img.shields.io/badge/Azure-Event%20Hubs-0078D4)](https://learn.microsoft.com/azure/event-hubs/)
[![PySpark](https://img.shields.io/badge/Apache-Spark-E25A1C)](https://spark.apache.org/)

> **Part 2 of a two-repository project.** This repository implements **Uber Event Streaming** in Databricks: Azure Event Hubs (Kafka-compatible) ingestion, Bronze/Silver/Gold processing and analytical explorations. The companion [Azure Data Factory repository](https://github.com/Srinivas-49/adf_uber_event_0000) ingests supporting JSON/reference data into ADLS Gen2.

## Architecture

```text
                 REAL-TIME RIDE EVENTS                     REFERENCE / BATCH DATA
                 Azure Event Hubs                          Public GitHub JSON files
                   (Kafka API)                                     |
                       |                                     Azure Data Factory
                       |                                    (HTTP_to_ADLS pipeline)
                       |                                            |
                       |                                     ADLS Gen2 raw/igestion/
                       |                                            |
                       v                                            v
            Databricks Bronze: rides_raw                  Bronze: bulk_rides, map_*
                       |                                            |
                       v                                            |
            Bronze: stg_rides (parsed JSON)                         |
                       |                                            |
                       +----------------+---------------------------+
                                        |
                                        v
                           Silver: silver_obt (enriched ride data)
                                        |
                                        v
                              Gold: dimensions + fact
                                        |
                                        v
                            KPI and trend exploration notebooks
```

The event ingestion uses Spark Structured Streaming with a Kafka source; the ADF pipeline supplies the separate reference-data path. The repository is **pipeline source code**, not a complete infrastructure-as-code or producer deployment.

## Implemented processing

### 1. Bronze — ingest events

[transformations/ingest.py](uber_event_streaming/transformations/ingest.py) defines a Lakeflow Declarative Pipelines table named `rides_raw`:

- Consumes Event Hubs through its Kafka-compatible endpoint on port `9093` with `SASL_SSL` / `PLAIN`.
- Reads the connection string with `dbutils.secrets.get(scope="uber-streaming", key="eh-connection-string")`.
- Subscribes to the configured event hub; casts the Kafka `value` and `key` into `rides` and `ride_key`.
- Sets `startingOffsets=earliest`, `maxOffsetsPerTrigger=10000`, and `failOnDataLoss=false`.

The current source hardcodes the Event Hubs namespace and hub name. Update these for your environment; do not commit credentials.

### 2. Bronze staging — parse JSON and include historical data

[transformations/sliver.py](uber_event_streaming/transformations/sliver.py) (filename is spelled `sliver.py` in the repository) defines a typed `rides_schema` and two append flows into `stg_rides`:

- `append_rides`: parses JSON event payloads from `bronze.rides_raw` with `from_json`.
- `rides`: appends existing `bronze.bulk_rides` records as a second input.

This combines live event records and historical/bulk records in staging. The expected columns include ride, passenger, driver, vehicle, location, booking timestamps, fare, tip, status and cancellation attributes.

### 3. Silver — enrich ride events

[transformations/silver_obt.sql](uber_event_streaming/transformations/silver_obt.sql) declares `uberxxxxxx000000.silver.silver_obt` as a streaming table. It:

- Streams `bronze.stg_rides` with a **3-minute booking-time watermark**.
- Left-joins Bronze reference tables for vehicle makes/types, status, payment methods, cities and cancellation reasons.
- Produces an **OBT (One Big Table)** combining ride records with readable reference attributes.

### 4. Gold — analytics-ready models

[transformations/model.py](uber_event_streaming/transformations/model.py) builds Gold tables using temporary views and `dlt.create_auto_cdc_flow`.

| Output | Intended purpose |
| --- | --- |
| `dim_passenger` | Latest passenger attributes; SCD Type 1 |
| `dim_driver` | Driver details; SCD Type 1 |
| `dim_vehicle` | Vehicle attributes; SCD Type 1 |
| `dim_payment_method` | Payment-method attributes; SCD Type 1 |
| `dim_bookings` | Booking/ride attributes |
| `dim_location_scd2` | Location history; SCD Type 2 |
| `fact` | Ride metrics and dimension keys; SCD Type 1 CDC flow |

**Note:** These are definitions and intended models, not a claim that all objects deploy successfully as-is. Some gold outputs use unqualified names, and CDC sequencing varies between entities. Confirm pipeline catalog/schema settings and CDC semantics before production use.

### 5. Explorations and analytics

- [explorations/bronze.py](uber_event_streaming/explorations/bronze.py): manually loads JSON reference files from ADLS into Bronze Delta tables, inspects records, and experiments with volumes/checkpoints.
- [explorations/silver.ipynb](uber_event_streaming/explorations/silver.ipynb): schema investigation, parsing, SQL-generation experiments, and exploratory table updates.
- [explorations/gold.py](uber_event_streaming/explorations/gold.py): Gold-level KPI and visual analyses, including completed/cancelled rides, trends, city-level performance, driver ratings and fleet mix.

**Use the `transformations/` files as the implementation reference.** The `explorations/` notebooks contain experimental operations, including overwrites and drops, and should not be executed unattended against production data.

## Repository structure

```text
README.md
LICENSE
uber_event_streaming/
├── transformations/
│   ├── ingest.py         # Event Hubs -> Bronze rides_raw
│   ├── sliver.py         # JSON parsing and bulk + streaming append to stg_rides
│   ├── silver_obt.sql    # Silver enrichment with Bronze lookup tables
│   └── model.py          # Gold dimensions and fact CDC flows
└── explorations/
    ├── bronze.py         # Reference-data loading and exploration
    ├── silver.ipynb      # Parsing and Silver experiments
    └── gold.py           # KPI charts and Gold exploration
```

## Prerequisites

- Azure Databricks workspace with access to **Lakeflow Declarative Pipelines** and Unity Catalog.
- Azure Event Hubs namespace, hub and SAS connection string permitted to consume messages (or a suitably configured authentication approach).
- A Databricks secret scope `uber-streaming` containing the secret `eh-connection-string`.
- ADLS Gen2 read access for the reference-data files written by the [ADF repository](https://github.com/Srinivas-49/adf_uber_event_0000).
- Unity Catalog catalog and Bronze/Silver/Gold schemas. The example code uses `uberxxxxxx000000` as its catalog identifier.
- Appropriate pipeline/service-principal permissions for streaming, creating tables and writing Delta data.

## Suggested setup and execution

1. **Configure Event Hubs.** Create/identify a namespace and event hub. Update `EH_NAMESPACE` and `EH_NAME` in `ingest.py`. Ensure messages contain JSON matching the fields in `rides_schema`. An event producer is not included in this repository.
2. **Create the secret securely.** Populate `uber-streaming/eh-connection-string` using your organization's Databricks secret-management process; never place the secret value in Git.
3. **Configure Unity Catalog.** Create the catalog and Bronze, Silver and Gold schemas, or adjust hardcoded identifiers consistently across source files.
4. **Load reference data.** Run the [ADF HTTP-to-ADLS pipeline](https://github.com/Srinivas-49/adf_uber_event_0000) first. Check that reference JSON exists in ADLS `raw/igestion/`. Adapt the Bronze reference-data import in `explorations/bronze.py` into a controlled job, and load required `map_*` tables plus `bulk_rides` if using the historical append flow.
5. **Create a Lakeflow Declarative Pipeline.** Add appropriate files from `transformations/` as pipeline source code. Configure target catalog/schema, storage/checkpoint behavior, compute, permissions and supported Python/SQL pipeline settings. Validate that cross-schema, fully qualified references work in your selected pipeline configuration.
6. **Run and monitor.** Validate Bronze ingestion, `stg_rides`, Silver enrichment and Gold table creation in that order. Inspect pipeline event logs and data-quality results before depending on the Gold tables.
7. **Explore results.** Run the Gold exploration notebook against confirmed table names. The analysis notebook currently references `dim_passenger_scd1`, while `model.py` defines `dim_passenger`; align this name before running that notebook.

### Basic validation SQL

```sql
SELECT COUNT(*) FROM uberxxxxxx000000.bronze.rides_raw;
SELECT COUNT(*) FROM uberxxxxxx000000.bronze.stg_rides;
SELECT COUNT(*) FROM uberxxxxxx000000.silver.silver_obt;
SELECT COUNT(*) FROM uberxxxxxx000000.gold.dim_bookings;
```

Run only after the corresponding pipeline objects exist.

## Configuration notes and limitations

| Area | What to verify |
| --- | --- |
| Event Hubs | Namespace/hub names are hardcoded; the producer is not present |
| Credentials | Secret scope/key must exist; do not check connection strings into Git |
| Data lake path | Bronze notebook expects **`igestion`**, matching the active ADF sink (intentional compatibility despite typo) |
| Bulk ingestion | `bronze.bulk_rides` must be prepared separately; code assumes it exists |
| Catalogs/schemas | Source files hardcode `uberxxxxxx000000` in multiple places |
| Table naming | Gold exploration references `dim_passenger_scd1`; model defines `dim_passenger` |
| CDC ordering | Some flows sequence by IDs rather than an event-time/update field; review before production use |
| Privacy | Ride events contain passenger/driver contact and location fields; restrict access and avoid real personal data in public exports |

## What this project demonstrates

- **Hybrid ingestion:** ADF batch reference-data ingestion plus Event Hubs event streaming.
- **Streaming ETL:** Spark Structured Streaming and Lakeflow Declarative Pipelines.
- **Medallion architecture:** Bronze raw/staging, Silver OBT enrichment and Gold dimensional/fact models.
- **CDC modeling:** SCD Type 1 and Type 2 flows in the Gold layer.
- **Analytics:** Uber booking trends, cancellations, vehicles, driver ratings and other exploratory KPIs.

**Related repository:** [Azure Data Factory — HTTP to ADLS ingestion](https://github.com/Srinivas-49/adf_uber_event_0000).
