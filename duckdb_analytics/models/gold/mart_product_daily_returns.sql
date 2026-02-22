{{
    config(
        materialized        = 'incremental',
        incremental_strategy= 'merge',
        unique_key          = ['order_date', 'product_id'],
        partition_by        = ['order_date'],
        on_schema_change    = 'sync_all_columns',
        tags                = ['gold', 'returns', 'product_daily']
    )
}}

WITH product_list_price AS (
    SELECT
        product_id,
        MAX(gross_revenue_usd)                                      AS list_price_usd
    FROM {{ ref('fct_sales') }}
    GROUP BY 1
),

daily_product_returns AS (
    SELECT
        fs.order_date                                               AS order_date,
        fs.product_id,
        COUNT(*)                                                    AS order_items_sold_count,
        AVG(fs.gross_revenue_usd)                                   AS avg_selling_price_usd,
        MIN(fs.gross_revenue_usd)                                   AS min_selling_price_usd,
        MAX(fs.gross_revenue_usd)                                   AS max_selling_price_usd,
        SUM(CASE WHEN fs.has_returned THEN 1 ELSE 0 END)            AS returned_items_count,
        SUM(CASE WHEN fs.has_refund THEN 1 ELSE 0 END)              AS refunded_items_count,
        SUM(fs.refund_event_count)                                  AS refund_event_count,
        SUM(fs.refund_amount_usd)                                   AS refund_amount_usd,
        SUM(CASE WHEN fs.has_returned THEN fs.gross_revenue_usd ELSE 0 END)
                                                                    AS returned_gross_revenue_usd,
        SUM(fs.gross_revenue_usd)                                   AS gross_revenue_usd,
        SUM(fs.net_revenue_usd)                                     AS net_revenue_usd,
        SUM(fs.net_profit_usd)                                      AS net_profit_usd
    FROM {{ ref('fct_sales') }} fs
    {% if is_incremental() %}
    WHERE fs.order_date >= (
        SELECT COALESCE(MAX(order_date), DATE '1900-01-01') - INTERVAL 7 DAY
        FROM {{ this }}
    )
    {% endif %}
    GROUP BY 1, 2
)

SELECT
    dpr.order_date,
    dpr.product_id,
    dp.product_name,

    dpr.order_items_sold_count,
    dpr.returned_items_count,
    dpr.refunded_items_count,
    dpr.refund_event_count,

    dpr.avg_selling_price_usd,
    dpr.min_selling_price_usd,
    dpr.max_selling_price_usd,
    plp.list_price_usd,
    GREATEST(COALESCE(plp.list_price_usd, 0.0) - dpr.avg_selling_price_usd, 0.0)
                                                                    AS inferred_discount_usd,
    CASE
        WHEN COALESCE(plp.list_price_usd, 0.0) = 0 THEN null
        ELSE GREATEST(COALESCE(plp.list_price_usd, 0.0) - dpr.avg_selling_price_usd, 0.0)
            / plp.list_price_usd
    END                                                             AS inferred_discount_pct,

    dpr.refund_amount_usd,
    dpr.returned_gross_revenue_usd,
    dpr.gross_revenue_usd,
    dpr.net_revenue_usd,
    dpr.net_profit_usd,

    CASE
        WHEN dpr.order_items_sold_count = 0 THEN null
        ELSE dpr.returned_items_count::DOUBLE / dpr.order_items_sold_count::DOUBLE
    END                                                             AS item_return_rate,
    CASE
        WHEN dpr.order_items_sold_count = 0 THEN null
        ELSE dpr.refunded_items_count::DOUBLE / dpr.order_items_sold_count::DOUBLE
    END                                                             AS item_refund_rate,
    CASE
        WHEN dpr.returned_gross_revenue_usd = 0 THEN null
        ELSE dpr.refund_amount_usd / dpr.returned_gross_revenue_usd
    END                                                             AS refund_capture_rate
FROM daily_product_returns dpr
LEFT JOIN {{ ref('dim_products') }} dp
    ON dpr.product_id = dp.product_id
LEFT JOIN product_list_price plp
    ON dpr.product_id = plp.product_id
