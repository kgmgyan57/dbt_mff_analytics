# dbt_mff_analytics

`dbt_mff_analytics` is a dbt + DuckDB analytics engineering project that implements a Medallion architecture (Bronze -> Silver -> Gold) on the Maven Analytics Toy Store e-commerce dataset.

It is designed for:
- Analytics engineers building trusted semantic layers
- Data engineers implementing lightweight local lakehouse workflows

## Dataset Context

Source dataset: [Maven Analytics Toy Store E-commerce Database](https://mavenanalytics.io/data-playground/toy-store-e-commerce-database)

The dataset contains transactional and behavioral e-commerce entities, including:
- Orders and order items
- Item-level refunds/returns
- Website sessions and pageviews
- Product catalog attributes

These sources support end-to-end analysis across acquisition, conversion, revenue, profitability, and returns.

## Architecture (Medallion)

This project follows a three-layer Medallion pattern:
- Bronze (`raw` schema): Source-aligned models over raw ingested data
- Silver (`silver` schema): Conformed dimensions and reusable facts at stable grains
- Gold (`gold` schema): Business-facing marts for daily performance and decision support

```text
Maven Toy Store CSVs
        |
        v
raw schema tables + partitioned Parquet (year/month)
        |
        v
Bronze (base_* normalization models)
        |
        v
Silver (conformed facts and dimensions)
        |
        v
Gold (business marts for analytics)
```

Current dbt model layout:
- Bronze
  - `base_orders`
  - `base_order_items`
  - `base_order_item_refunds`
  - `base_website_sessions`
  - `base_website_pageviews`
  - `base_products`
- Silver
  - `dim_products`: Conformed product dimension used as the canonical product reference across marts.
  - `fct_sales`: Core sales fact at order-item grain powering revenue, profit, refund, and return analytics.
  - `fct_sessions`: Core behavioral fact at session grain powering channel, conversion, and engagement analysis.
- Gold
  - `mart_product_daily_sales`: Daily product performance mart combining traffic, conversion, sales, and profitability.
  - `mart_product_daily_returns`: Daily product returns/refunds mart for return-rate and refund-efficiency analysis.
  - `mart_channel_daily_sessions`: Daily acquisition and channel performance mart for conversion and bounce diagnostics.
  - `mart_customer_daily_value`: Daily customer value mart for user engagement, purchasing, and monetization tracking.

## Modeling Philosophy

- Grain-first modeling: Every fact/dimension declares a clear grain before metric logic.
- Layered semantics: Source cleanup in Bronze, business logic in Silver, presentation in Gold.
- Reusability over duplication: Core KPIs are centralized in Silver facts and reused by marts.
- Explainability: Models and columns are documented in `schema.yaml` files close to code.

## Data Quality Strategy

Data quality is enforced with dbt tests and grain contracts:
- Entity integrity: `not_null` + `unique` tests on primary grain keys
- Required relationships: critical IDs and timestamps tested for nullability
- Metric safety: boolean flags and derived fields tested for completeness where appropriate

Recommended extensions:
- Add `relationships` tests between Silver facts and dimensions
- Add source freshness checks for ingestion monitoring
- Add singular tests for business rules (for example, non-negative revenue constraints)

## Incremental Modeling Overview

Current state:
- Bronze models are views
- Silver and Gold models are materialized as tables
- Full rebuilds are supported and deterministic

Incremental strategy (recommended next step):
- Promote large Silver facts (for example `fct_sales`, `fct_sessions`) to incremental models
- Use `created_at`/event timestamps as incremental watermarks
- Keep Gold marts table-based or incremental based on SLA and volume
- Run periodic full refreshes to handle late-arriving corrections

## Tech Stack

- dbt Core
- dbt-duckdb adapter
- DuckDB engine
- SQL + YAML model/test documentation

## Setup Instructions

### 1) Create and activate a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 2) Configure dbt profile

Create `~/.dbt/profiles.yml`:

```yaml
duckdb_analytics:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: mff_analytics.duckdb
      threads: 4
```

### 3) Install dependencies and run dbt

```bash
cd duckdb_analytics
dbt deps
dbt run
dbt test
```

## Data Ingestion Instructions

### 1) Download dataset files

Download from:
- [https://mavenanalytics.io/data-playground/toy-store-e-commerce-database](https://mavenanalytics.io/data-playground/toy-store-e-commerce-database)

### 2) Place raw CSV files in this structure

```text
<repo-root>/data/raw/maven_toy_store/
  orders.csv
  order_items.csv
  order_item_refunds.csv
  website_sessions.csv
  website_pageviews.csv
  products.csv
  maven_fuzzy_factory_data_dictionary.csv
```

### 3) Ingest Bronze data with `load_maven_bronze.sql`

From repository root:

```bash
duckdb mff_analytics.duckdb < load_maven_bronze.sql
```

What `load_maven_bronze.sql` does:
- Creates the `raw` schema
- Reads raw CSV files into staging tables
- Creates standardized raw tables
- Uses `COPY` to write Parquet datasets
- Partitions event tables by `year` and `month`
- Produces Hive-style partitioned paths for dbt source reads

This ingestion step must be executed before running dbt models.

### 4) Run dbt after ingestion

```bash
cd duckdb_analytics
dbt run
dbt test
```

## Source Path Configuration

`duckdb_analytics/models/bronze/_raw_sources.yaml` resolves external source files from:
- `MFF_DATA_LAKE_ROOT` environment variable, or
- Default `data_lake` path in the repository root

If needed:

```bash
export MFF_DATA_LAKE_ROOT=/absolute/path/to/
```

## Project Tree Layout

```text
quacker-data-lakehouse/
  README.md
  requirements.txt
  load_maven_bronze.sql
  mff_analytics.duckdb                  # created after ingestion/run
  data/
    raw/
      maven_toy_store/                  # downloaded CSV files
  data_lake/
    orders/year=YYYY/month=MM/*.parquet
    order_items/year=YYYY/month=MM/*.parquet
    order_item_refunds/year=YYYY/month=MM/*.parquet
    website_sessions/year=YYYY/month=MM/*.parquet
    website_pageviews/year=YYYY/month=MM/*.parquet
    products/*.parquet
    mff_data_dictionary/*.parquet
  duckdb_analytics/
    dbt_project.yml
    models/
      bronze/
      silver/
      gold/
```

## Roadmap

- Add incremental materializations for high-volume Silver facts
- Add relationship and business-rule tests for stronger contracts
- Add orchestration-ready commands and environment profiles (dev/prod)
- Add CI checks for `dbt build` + docs generation
- Add observability metadata and source freshness monitoring
