{{
    config(
        materialized        = 'incremental',
        incremental_strategy= 'merge',
        unique_key          = ['metric_date'],
        partition_by        = ['metric_date'],
        on_schema_change    = 'sync_all_columns',
        tags                = ['gold', 'customer', 'daily']
    )
}}

WITH daily_sessions AS (
    SELECT
        session_date                                                AS metric_date,
        COUNT(*)                                                    AS sessions_count,
        COUNT(DISTINCT user_id)                                     AS active_users_count,
        COUNT(DISTINCT CASE WHEN is_repeat_session THEN user_id END)
                                                                    AS repeat_session_users_count,
        COUNT(DISTINCT CASE WHEN NOT is_repeat_session THEN user_id END)
                                                                    AS new_session_users_count,
        SUM(CASE WHEN is_bounced THEN 1 ELSE 0 END)                 AS bounced_sessions_count,
        SUM(product_pageviews_count)                                AS product_pageviews_count,
        COUNT(DISTINCT CASE WHEN has_product_pageview THEN website_session_id END)
                                                                    AS product_view_sessions_count,
        COUNT(DISTINCT CASE WHEN has_product_pageview THEN user_id END)
                                                                    AS users_with_product_views_count
    FROM {{ ref('fct_sessions') }}
    {% if is_incremental() %}
    WHERE session_date >= (
        SELECT COALESCE(MAX(metric_date), DATE '1900-01-01') - INTERVAL 7 DAY
        FROM {{ this }}
    )
    {% endif %}
    GROUP BY 1
),

daily_sales AS (
    SELECT
        order_date                                                  AS metric_date,
        COUNT(DISTINCT order_id)                                    AS orders_count,
        COUNT(*)                                                    AS order_items_sold_count,
        COUNT(DISTINCT user_id)                                     AS purchasing_users_count,
        COUNT(DISTINCT CASE WHEN has_returned THEN user_id END)     AS users_with_returns_count,
        SUM(gross_revenue_usd)                                      AS gross_revenue_usd,
        SUM(refund_amount_usd)                                      AS refund_amount_usd,
        SUM(net_revenue_usd)                                        AS net_revenue_usd,
        SUM(net_profit_usd)                                         AS net_profit_usd
    FROM {{ ref('fct_sales') }}
    {% if is_incremental() %}
    WHERE order_date >= (
        SELECT COALESCE(MAX(metric_date), DATE '1900-01-01') - INTERVAL 7 DAY
        FROM {{ this }}
    )
    {% endif %}
    GROUP BY 1
),

all_dates AS (
    SELECT metric_date FROM daily_sessions
    UNION
    SELECT metric_date FROM daily_sales
)

SELECT
    d.metric_date,
    COALESCE(ds.sessions_count, 0)                                  AS sessions_count,
    COALESCE(ds.active_users_count, 0)                              AS active_users_count,
    COALESCE(ds.repeat_session_users_count, 0)                      AS repeat_session_users_count,
    COALESCE(ds.new_session_users_count, 0)                         AS new_session_users_count,
    COALESCE(ds.bounced_sessions_count, 0)                          AS bounced_sessions_count,
    COALESCE(ds.product_pageviews_count, 0)                         AS product_pageviews_count,
    COALESCE(ds.product_view_sessions_count, 0)                     AS product_view_sessions_count,
    COALESCE(ds.users_with_product_views_count, 0)                  AS users_with_product_views_count,

    COALESCE(sl.orders_count, 0)                                    AS orders_count,
    COALESCE(sl.order_items_sold_count, 0)                          AS order_items_sold_count,
    COALESCE(sl.purchasing_users_count, 0)                          AS purchasing_users_count,
    COALESCE(sl.users_with_returns_count, 0)                        AS users_with_returns_count,
    COALESCE(sl.gross_revenue_usd, 0.0)                             AS gross_revenue_usd,
    COALESCE(sl.refund_amount_usd, 0.0)                             AS refund_amount_usd,
    COALESCE(sl.net_revenue_usd, 0.0)                               AS net_revenue_usd,
    COALESCE(sl.net_profit_usd, 0.0)                                AS net_profit_usd,

    CASE
        WHEN COALESCE(ds.sessions_count, 0) = 0 THEN null
        ELSE COALESCE(sl.orders_count, 0)::DOUBLE / ds.sessions_count::DOUBLE
    END                                                             AS orders_per_session,
    CASE
        WHEN COALESCE(ds.active_users_count, 0) = 0 THEN null
        ELSE COALESCE(ds.users_with_product_views_count, 0)::DOUBLE / ds.active_users_count::DOUBLE
    END                                                             AS product_view_user_penetration_rate,
    CASE
        WHEN COALESCE(sl.purchasing_users_count, 0) = 0 THEN null
        ELSE sl.net_revenue_usd / sl.purchasing_users_count::DOUBLE
    END                                                             AS net_revenue_per_purchasing_user
FROM all_dates d
LEFT JOIN daily_sessions ds
    ON d.metric_date = ds.metric_date
LEFT JOIN daily_sales sl
    ON d.metric_date = sl.metric_date
