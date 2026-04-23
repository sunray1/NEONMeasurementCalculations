# NEON OLAP Pipeline

## Overview

This project processes NEON data products into a DuckDB-based OLAP
dataset.

Each job runs per:

(product, site, year, month)

For each job, the pipeline: - fetches NEON data URLs\
- downloads and extracts the product bundle\
- loads and normalizes the data\
- computes measurements\
- writes results to DuckDB

Raw data is not stored long-term. Everything is processed and reduced on
the fly.

------------------------------------------------------------------------

## Structure

olap_project/ neon_olap/ api.py bundling.py db.py adapters/
measurements/ jobs/ run.py build_aggregates.py tasks.csv

------------------------------------------------------------------------

## Core Concepts

### Adapters

Adapters are product-specific loaders and transformers.

Each NEON data product has its own structure (multiple CSVs, different
schemas, naming inconsistencies). The adapter handles that complexity.

Responsibilities: - select relevant CSV files\
- load into dataframes\
- join tables if needed\
- rename columns into a consistent format\
- return a clean dataframe

Adapters also define which measurements belong to the product.

Adapter = how raw NEON data becomes usable.

------------------------------------------------------------------------

### Measurements

Measurements are explicit calculations applied to normalized data.

Each measurement: - takes standardized dataframe\
- computes derived values\
- outputs:

measurement_id \| site_id \| date \| value

Measurements do not handle file structure or raw data quirks.

Measurement = what is computed from the data.

------------------------------------------------------------------------

## Data Flow

For each job:

1.  Discover URLs\
2.  Download and extract bundle\
3.  Adapter loads and normalizes data\
4.  Measurements compute outputs\
5.  Results written to DuckDB

------------------------------------------------------------------------

## Running the Pipeline

Run all jobs: python jobs/run.py --tasks-file tasks.csv

Run subset: python jobs/run.py --tasks-file tasks.csv --start 0 --end
100

Run single job: python jobs/run.py --product DP1.00002.001 --site HARV
--year 2021 --month 7

------------------------------------------------------------------------

## Tasks File

product,site,year,month DP1...,HARV,2021,7

Each row is one job.

------------------------------------------------------------------------

## Database

DuckDB file: olap.duckdb

Main table: fact_measurement(measurement_id, site_id, date, value,
product, year, month)

------------------------------------------------------------------------

## Aggregations

python jobs/build_aggregates.py

Creates summary tables for analysis.

------------------------------------------------------------------------

## Requirements

pip install duckdb pandas requests neonutilities

------------------------------------------------------------------------

## Notes

-   product-first processing\
-   adapters isolate schema complexity\
-   measurements remain simple\
-   easy to parallelize via tasks.csv\
-   raw data is not stored
