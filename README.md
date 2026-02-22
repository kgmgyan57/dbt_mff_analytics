# duckdb_analytics

dbt project for Maven Fuzzy Factory analytics using a medallion layout:
- `bronze`: raw source-aligned views
- `silver`: conformed dimensions and topic facts
- `gold`: business-facing marts for performance and customer analysis

## Project Structure

- `models/bronze`
  - source definitions in `_raw_sources.yaml`
  - base models: `base_orders`, `base_order_items`, `base_order_item_refunds`, `base_website_sessions`, `base_website_pageviews`, `base_products`
- `models/silver`
  - `dim_products`
  - `fct_sales` (order item grain)
  - `fct_sessions` (website session grain; includes product-view session signals)
- `models/gold`
  - `mart_product_daily_sales`
  - `mart_product_daily_returns`
  - `mart_channel_daily_sessions`
  - `mart_customer_daily_value`

## Core Modeling Principles

- Single source of truth per topic in silver:
  - sales metrics in `fct_sales`
  - session/channel/device behavior in `fct_sessions`
  - product attributes in `dim_products`
- Gold models are consumption-ready aggregations built on silver facts/dimensions.
- Daily grain is used for marts unless noted otherwise.

## How To Run

```bash
dbt deps
dbt run
dbt test
```

Useful targeted runs:

```bash
dbt run --select silver
dbt run --select gold
dbt test --select silver
dbt test --select gold
```

## Documentation

```bash
dbt docs generate
dbt docs serve
```

## Notes

- Project config is in `dbt_project.yml`.
- Bronze/silver/gold schemas are configured in `dbt_project.yml` under `models.duckdb_analytics`.
- Source file locations are currently configured in `models/bronze/_raw_sources.yaml`.
=======
# dbt_maven_analytics
