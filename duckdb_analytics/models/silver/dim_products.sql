{{
    config(
        materialized        = 'table',
        tags                = ['silver', 'products', 'dim_products']
    )
}}

WITH source_products AS (
    SELECT
        product_id,
        created_at,
        product_name
    FROM {{ ref('base_products') }}
),

clean_products AS (
    SELECT
        CAST(product_id AS INTEGER)                                 AS product_id,
        CAST(created_at AS TIMESTAMP)                               AS created_at_utc,
        CAST(CAST(created_at AS TIMESTAMP) AS DATE)                 AS created_date,
        TRIM(CAST(product_name AS VARCHAR))                         AS product_name
    FROM source_products
)

SELECT
    product_id,
    created_at_utc,
    created_date,
    product_name
FROM clean_products
