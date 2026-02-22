{{
    config(
        materialized        = 'incremental',
        incremental_strategy= 'merge',
        unique_key          = ['session_date', 'utm_source', 'utm_campaign', 'utm_content', 'device_type'],
        partition_by        = ['session_date'],
        on_schema_change    = 'sync_all_columns',
        tags                = ['gold', 'sessions', 'channel_daily']
    )
}}

SELECT
    session_date                                                   AS session_date,
    COALESCE(utm_source, '(none)')                                 AS utm_source,
    COALESCE(utm_campaign, '(none)')                               AS utm_campaign,
    COALESCE(utm_content, '(none)')                                AS utm_content,
    COALESCE(device_type, '(none)')                                AS device_type,

    COUNT(*)                                                       AS sessions_count,
    SUM(CASE WHEN is_bounced THEN 1 ELSE 0 END)                    AS bounced_sessions_count,
    SUM(product_pageviews_count)                                   AS product_pageviews_count,
    SUM(CASE WHEN has_product_pageview THEN 1 ELSE 0 END)          AS product_view_sessions_count,
    SUM(orders_count)                                              AS orders_count,
    SUM(items_purchased_count)                                     AS items_purchased_count,

    CASE
        WHEN COUNT(*) = 0 THEN null
        ELSE SUM(CASE WHEN has_order THEN 1 ELSE 0 END)::DOUBLE / COUNT(*)::DOUBLE
    END                                                            AS session_conversion_rate,
    CASE
        WHEN COUNT(*) = 0 THEN null
        ELSE SUM(CASE WHEN is_bounced THEN 1 ELSE 0 END)::DOUBLE / COUNT(*)::DOUBLE
    END                                                            AS bounce_rate,
    CASE
        WHEN COUNT(*) = 0 THEN null
        ELSE SUM(CASE WHEN has_product_pageview THEN 1 ELSE 0 END)::DOUBLE / COUNT(*)::DOUBLE
    END                                                            AS product_view_session_rate,
    CASE
        WHEN SUM(CASE WHEN has_product_pageview THEN 1 ELSE 0 END) = 0 THEN null
        ELSE SUM(orders_count)::DOUBLE / SUM(CASE WHEN has_product_pageview THEN 1 ELSE 0 END)::DOUBLE
    END                                                            AS orders_per_product_view_session
FROM {{ ref('fct_sessions') }}
{% if is_incremental() %}
WHERE session_date >= (
    SELECT COALESCE(MAX(session_date), DATE '1900-01-01') - INTERVAL 7 DAY
    FROM {{ this }}
)
{% endif %}
GROUP BY 1, 2, 3, 4, 5
