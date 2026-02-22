{{
    config(
        materialized        = 'incremental',
        incremental_strategy= 'merge',
        unique_key          = ['metric_date', 'product_id'],
        partition_by        = ['metric_date'],
        on_schema_change    = 'sync_all_columns',
        tags                = ['gold', 'sales', 'product_daily']
    )
}}

WITH product_pages AS (
    SELECT
        product_id,
        product_name,
        '/' || TRIM(BOTH '-' FROM REGEXP_REPLACE(LOWER(product_name), '[^a-z0-9]+', '-', 'g'))
                                                                    AS product_page_url
    FROM {{ ref('dim_products') }}
),

product_pageviews AS (
    SELECT
        CAST(pv.created_at AS DATE)                                 AS metric_date,
        pp.product_id,
        CAST(pv.website_session_id AS BIGINT)                       AS website_session_id
    FROM {{ ref('base_website_pageviews') }} pv
    INNER JOIN product_pages pp
        ON CAST(pv.pageview_url AS VARCHAR) = pp.product_page_url
    {% if is_incremental() %}
    WHERE CAST(pv.created_at AS DATE) >= (
        SELECT COALESCE(MAX(metric_date), DATE '1900-01-01') - INTERVAL 7 DAY
        FROM {{ this }}
    )
    {% endif %}
),

daily_product_traffic AS (
    SELECT
        ppv.metric_date,
        ppv.product_id,
        COUNT(*)                                                    AS product_pageviews_count,
        COUNT(DISTINCT ppv.website_session_id)                      AS product_sessions_count,
        COUNT(DISTINCT fsn.user_id)                                 AS product_viewers_count
    FROM product_pageviews ppv
    LEFT JOIN {{ ref('fct_sessions') }} fsn
        ON ppv.website_session_id = fsn.website_session_id
    GROUP BY 1, 2
),

session_product_touch AS (
    SELECT DISTINCT
        metric_date,
        product_id,
        website_session_id
    FROM product_pageviews
),

daily_session_to_product_conversion AS (
    SELECT
        spt.metric_date,
        spt.product_id,
        COUNT(DISTINCT fs.website_session_id)                       AS converted_sessions_count,
        COUNT(DISTINCT fs.order_id)                                 AS converted_orders_count,
        COUNT(*)                                                    AS converted_items_count
    FROM session_product_touch spt
    INNER JOIN {{ ref('fct_sales') }} fs
        ON spt.website_session_id = fs.website_session_id
        AND spt.product_id = fs.product_id
        AND spt.metric_date = fs.order_date
    GROUP BY 1, 2
),

daily_product_sales AS (
    SELECT
        fs.order_date                                               AS metric_date,
        fs.product_id,
        COUNT(*)                                                    AS order_items_sold_count,
        COUNT(DISTINCT fs.order_id)                                 AS orders_count,
        COUNT(DISTINCT fs.user_id)                                  AS purchasing_users_count,
        COUNT(DISTINCT fs.website_session_id)                       AS purchasing_sessions_count,
        SUM(fs.gross_revenue_usd)                                   AS gross_revenue_usd,
        SUM(fs.item_cogs_usd)                                       AS cogs_usd,
        SUM(fs.gross_profit_usd)                                    AS gross_profit_usd,
        SUM(fs.refund_event_count)                                  AS refund_event_count,
        SUM(CASE WHEN fs.has_returned THEN 1 ELSE 0 END)            AS returned_items_count,
        SUM(CASE WHEN fs.has_refund THEN 1 ELSE 0 END)              AS refunded_items_count,
        SUM(fs.refund_amount_usd)                                   AS refund_amount_usd,
        SUM(fs.net_revenue_usd)                                     AS net_revenue_usd,
        SUM(fs.net_profit_usd)                                      AS net_profit_usd
    FROM {{ ref('fct_sales') }} fs
    {% if is_incremental() %}
    WHERE fs.order_date >= (
        SELECT COALESCE(MAX(metric_date), DATE '1900-01-01') - INTERVAL 7 DAY
        FROM {{ this }}
    )
    {% endif %}
    GROUP BY 1, 2
),

daily_product_customer AS (
    SELECT
        order_date                                                  AS metric_date,
        product_id,
        COUNT(DISTINCT user_id)                                     AS unique_purchasers_count,
        COUNT(DISTINCT CASE WHEN has_returned THEN user_id END)     AS purchasers_with_returns_count
    FROM {{ ref('fct_sales') }}
    {% if is_incremental() %}
    WHERE order_date >= (
        SELECT COALESCE(MAX(metric_date), DATE '1900-01-01') - INTERVAL 7 DAY
        FROM {{ this }}
    )
    {% endif %}
    GROUP BY 1, 2
),

daily_product_repeat_buyers AS (
    SELECT
        metric_date,
        product_id,
        COUNT(DISTINCT user_id)                                     AS repeat_buyers_count
    FROM (
        SELECT
            order_date                                              AS metric_date,
            product_id,
            user_id,
            COUNT(DISTINCT order_id)                                AS orders_per_user
        FROM {{ ref('fct_sales') }}
        {% if is_incremental() %}
        WHERE order_date >= (
            SELECT COALESCE(MAX(metric_date), DATE '1900-01-01') - INTERVAL 7 DAY
            FROM {{ this }}
        )
        {% endif %}
        GROUP BY 1, 2, 3
    ) x
    WHERE orders_per_user > 1
    GROUP BY 1, 2
),

all_grains AS (
    SELECT metric_date, product_id FROM daily_product_traffic
    UNION
    SELECT metric_date, product_id FROM daily_product_sales
)

SELECT
    g.metric_date,
    g.product_id,
    dp.product_name,

    COALESCE(t.product_pageviews_count, 0)                         AS product_pageviews_count,
    COALESCE(t.product_sessions_count, 0)                          AS product_sessions_count,
    COALESCE(t.product_viewers_count, 0)                           AS product_viewers_count,

    COALESCE(c.converted_sessions_count, 0)                        AS converted_sessions_count,
    COALESCE(c.converted_orders_count, 0)                          AS converted_orders_count,
    COALESCE(c.converted_items_count, 0)                           AS converted_items_count,
    CASE
        WHEN COALESCE(t.product_sessions_count, 0) = 0 THEN null
        ELSE COALESCE(c.converted_sessions_count, 0)::DOUBLE / t.product_sessions_count::DOUBLE
    END                                                             AS product_session_conversion_rate,

    COALESCE(s.order_items_sold_count, 0)                          AS order_items_sold_count,
    COALESCE(s.orders_count, 0)                                     AS orders_count,
    COALESCE(s.purchasing_users_count, 0)                          AS purchasing_users_count,
    COALESCE(s.purchasing_sessions_count, 0)                       AS purchasing_sessions_count,
    COALESCE(s.gross_revenue_usd, 0.0)                             AS gross_revenue_usd,
    COALESCE(s.cogs_usd, 0.0)                                      AS cogs_usd,
    COALESCE(s.gross_profit_usd, 0.0)                              AS gross_profit_usd,
    COALESCE(s.refund_event_count, 0)                              AS refund_event_count,
    COALESCE(s.returned_items_count, 0)                            AS returned_items_count,
    COALESCE(s.refunded_items_count, 0)                            AS refunded_items_count,
    COALESCE(s.refund_amount_usd, 0.0)                             AS refund_amount_usd,
    COALESCE(s.net_revenue_usd, 0.0)                               AS net_revenue_usd,
    COALESCE(s.net_profit_usd, 0.0)                                AS net_profit_usd,

    COALESCE(pc.unique_purchasers_count, 0)                        AS unique_purchasers_count,
    COALESCE(pc.purchasers_with_returns_count, 0)                  AS purchasers_with_returns_count,
    COALESCE(rb.repeat_buyers_count, 0)                            AS repeat_buyers_count,

    CASE
        WHEN COALESCE(s.order_items_sold_count, 0) = 0 THEN null
        ELSE COALESCE(s.returned_items_count, 0)::DOUBLE / s.order_items_sold_count::DOUBLE
    END                                                             AS item_return_rate,
    CASE
        WHEN COALESCE(s.order_items_sold_count, 0) = 0 THEN null
        ELSE COALESCE(s.refunded_items_count, 0)::DOUBLE / s.order_items_sold_count::DOUBLE
    END                                                             AS item_refund_rate
FROM all_grains g
LEFT JOIN {{ ref('dim_products') }} dp
    ON g.product_id = dp.product_id
LEFT JOIN daily_product_traffic t
    ON g.metric_date = t.metric_date
    AND g.product_id = t.product_id
LEFT JOIN daily_session_to_product_conversion c
    ON g.metric_date = c.metric_date
    AND g.product_id = c.product_id
LEFT JOIN daily_product_sales s
    ON g.metric_date = s.metric_date
    AND g.product_id = s.product_id
LEFT JOIN daily_product_customer pc
    ON g.metric_date = pc.metric_date
    AND g.product_id = pc.product_id
LEFT JOIN daily_product_repeat_buyers rb
    ON g.metric_date = rb.metric_date
    AND g.product_id = rb.product_id
