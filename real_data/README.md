# Real Uber-style analytics: NYC TLC HVFHV + taxi zones + historical weather

**Official sources:**
- NYC Taxi & Limousine Commission High Volume For-Hire Vehicle (HVFHV) monthly **Parquet** data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- TLC Taxi Zone Lookup: same official TLC page
- Open-Meteo historical NYC precipitation and temperature: https://open-meteo.com/en/docs/historical-weather-api

This is **public for-hire trip data**, not proprietary Uber logs; trips can represent multiple ride-hailing providers. The 2025 HVFHV collection is about 244 million trips. Monthly source Parquet can be large, and its schema changes over time. Do not claim the whole collection has been processed from a one-month sample.

Network-enabled laptop example:
```bash
python -m real_data.tlc_sources --year 2025 --month 1 --max-mb 800 --confirm-large --out real-data/tlc
```
The default transfer cap is 100 MB and will deliberately reject a large monthly Parquet; the higher limit is an **explicit manual action**, not an estimate of file size. The script downloads 3 sources, streams to disk, validates approved domains and calculates per-file SHA-256 checksums. No files are fetched by GitHub CI and no Azure resources are provisioned.

After validating the local manifest, manually upload the three files to a Databricks Unity Catalog Volume (Free Edition internet access is restricted). Run the real-data Spark notebook in notebooks/03_real_tlc_zones_weather.py (introduced separately). Start with one month and only then consider 12-month backfill in paid Azure Databricks, with budget and permission checks.

**Privacy:** Do not publish individual trip-level outputs; share only grouped Gold zone/day summaries. **Licences:** TLC datasets may show an unspecified source licence; follow NYC Open Data terms and credit TLC. Open-Meteo use and redistributions are subject to its terms, including non-commercial access rules and attribution.

## Multi-month scale-up

Store each month in an independent directory and run downloads **one at a time**:

```bash
python -m real_data.tlc_sources --year 2025 --month 1 --max-mb 800 --confirm-large --out real-data/tlc/2025-01
python -m real_data.tlc_sources --year 2025 --month 2 --max-mb 800 --confirm-large --out real-data/tlc/2025-02
```

A verified manifest avoids downloading the same monthly Parquet again; it rejects mixing months or corrupted sources in one directory. Repeat across months only after reviewing disk capacity, source availability and Databricks compute/budget. For a full-year analytics run, upload partitioned data to ADLS/Unity Catalog Volumes, union schema-compatible monthly Parquet in Spark and record **actual** input row counts by month. Do not assume the publication's 244M source total is exactly your extracted output count.
