{{
    config(
        materialized        = 'table',
        tags                = ['silver', 'sales', 'fct_sales']
    )
}}

WITH orders AS (
    SELECT
        CAST(order_id AS BIGINT)                                    AS order_id,
        CAST(created_at AS TIMESTAMP)                               AS order_created_at_utc,
        CAST(website_session_id AS BIGINT)                          AS website_session_id,
        CAST(user_id AS BIGINT)                                     AS user_id,
        CAST(primary_product_id AS BIGINT)                          AS primary_product_id,
        CAST(items_purchased AS BIGINT)                             AS items_purchased,
        CAST(price_usd AS DOUBLE)                                   AS order_revenue_usd,
        CAST(cogs_usd AS DOUBLE)                                    AS order_cogs_usd
    FROM {{ ref('base_orders') }}
),

order_items AS (
    SELECT
        CAST(order_item_id AS BIGINT)                               AS order_item_id,
        CAST(created_at AS TIMESTAMP)                               AS order_item_created_at_utc,
        CAST(order_id AS BIGINT)                                    AS order_id,
        CAST(product_id AS BIGINT)                                  AS product_id,
        CAST(is_primary_item AS BIGINT)                             AS is_primary_item_flag,
        CAST(price_usd AS DOUBLE)                                   AS item_revenue_usd,
        CAST(cogs_usd AS DOUBLE)                                    AS item_cogs_usd
    FROM {{ ref('base_order_items') }}
),

refunds_by_item AS (
    SELECT
        CAST(order_item_id AS BIGINT)                               AS order_item_id,
        CAST(order_id AS BIGINT)                                    AS order_id,
        COUNT(*)                                                    AS refund_event_count,
        SUM(CAST(refund_amount_usd AS DOUBLE))                      AS refund_amount_usd,
        MIN(CAST(created_at AS TIMESTAMP))                          AS first_refund_created_at_utc,
        MAX(CAST(created_at AS TIMESTAMP))                          AS last_refund_created_at_utc
    FROM {{ ref('base_order_item_refunds') }}
    GROUP BY 1, 2
)

SELECT
    oi.order_item_id,
    oi.order_id,
    oi.order_item_created_at_utc,
    CAST(oi.order_item_created_at_utc AS DATE)                      AS order_item_created_date,
    oi.product_id,
    oi.is_primary_item_flag,
    CASE
        WHEN oi.is_primary_item_flag = 1 THEN true
        ELSE false
    END                                                             AS is_primary_item,

    o.order_created_at_utc,
    CAST(o.order_created_at_utc AS DATE)                            AS order_date,
    o.website_session_id,
    o.user_id,
    o.primary_product_id,
    o.items_purchased,
    o.order_revenue_usd,
    o.order_cogs_usd,

    oi.item_revenue_usd                                              AS gross_revenue_usd,
    oi.item_cogs_usd                                                 AS item_cogs_usd,
    oi.item_revenue_usd - oi.item_cogs_usd                           AS gross_profit_usd,

    COALESCE(r.refund_event_count, 0)                                AS refund_event_count,
    COALESCE(r.refund_amount_usd, 0.0)                               AS refund_amount_usd,
    r.first_refund_created_at_utc,
    r.last_refund_created_at_utc,
    CASE
        WHEN COALESCE(r.refund_event_count, 0) > 0 THEN true
        ELSE false
    END                                                              AS has_returned,
    CASE
        WHEN COALESCE(r.refund_amount_usd, 0.0) > 0 THEN true
        ELSE false
    END                                                              AS has_refund,

    oi.item_revenue_usd - COALESCE(r.refund_amount_usd, 0.0)         AS net_revenue_usd,
    (oi.item_revenue_usd - COALESCE(r.refund_amount_usd, 0.0)) - oi.item_cogs_usd
                                                                     AS net_profit_usd,

    CASE
        WHEN oi.item_revenue_usd = 0 THEN null
        ELSE ((oi.item_revenue_usd - COALESCE(r.refund_amount_usd, 0.0)) - oi.item_cogs_usd)
            / oi.item_revenue_usd
    END                                                              AS net_margin_pct
FROM order_items oi
LEFT JOIN orders o
    ON oi.order_id = o.order_id
LEFT JOIN refunds_by_item r
    ON oi.order_item_id = r.order_item_id
    AND oi.order_id = r.order_id
