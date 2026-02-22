{{
    config(
        materialized        = 'table',
        tags                = ['silver', 'sessions', 'fct_sessions']
    )
}}

WITH sessions AS (
    SELECT
        CAST(website_session_id AS BIGINT)                          AS website_session_id,
        CAST(created_at AS TIMESTAMP)                               AS session_created_at_utc,
        CAST(user_id AS BIGINT)                                     AS user_id,
        CAST(is_repeat_session AS BIGINT)                           AS is_repeat_session_flag,
        CAST(utm_source AS VARCHAR)                                 AS utm_source,
        CAST(utm_campaign AS VARCHAR)                               AS utm_campaign,
        CAST(utm_content AS VARCHAR)                                AS utm_content,
        CAST(device_type AS VARCHAR)                                AS device_type,
        CAST(http_referer AS VARCHAR)                               AS http_referer
    FROM {{ ref('base_website_sessions') }}
),

pageviews_ranked AS (
    SELECT
        CAST(website_pageview_id AS BIGINT)                         AS website_pageview_id,
        CAST(website_session_id AS BIGINT)                          AS website_session_id,
        CAST(created_at AS TIMESTAMP)                               AS pageview_created_at_utc,
        CAST(pageview_url AS VARCHAR)                               AS pageview_url,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(website_session_id AS BIGINT)
            ORDER BY CAST(created_at AS TIMESTAMP), CAST(website_pageview_id AS BIGINT)
        )                                                           AS pageview_number_asc,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(website_session_id AS BIGINT)
            ORDER BY CAST(created_at AS TIMESTAMP) DESC, CAST(website_pageview_id AS BIGINT) DESC
        )                                                           AS pageview_number_desc
    FROM {{ ref('base_website_pageviews') }}
),

pageviews_agg AS (
    SELECT
        website_session_id,
        COUNT(*)                                                    AS pageviews_count,
        MIN(pageview_created_at_utc)                                AS first_pageview_at_utc,
        MAX(pageview_created_at_utc)                                AS last_pageview_at_utc,
        MAX(
            CASE
                WHEN pageview_number_asc = 1 THEN pageview_url
                ELSE null
            END
        )                                                           AS landing_page_url,
        MAX(
            CASE
                WHEN pageview_number_desc = 1 THEN pageview_url
                ELSE null
            END
        )                                                           AS exit_page_url
    FROM pageviews_ranked
    GROUP BY 1
),

product_pages AS (
    SELECT
        product_id,
        -- Generate product page URL in the format: /product/product-name, where product-name is derived from the product_name column by converting it to lowercase, replacing non-alphanumeric characters with hyphens, and trimming any leading or trailing hyphens.
        '/' || TRIM(BOTH '-' FROM REGEXP_REPLACE(LOWER(product_name), '[^a-z0-9]+', '-', 'g'))
                                                                    AS product_page_url
    FROM {{ ref('dim_products') }}
),

product_pageviews_agg AS (
    SELECT
        pr.website_session_id,
        COUNT(*)                                                    AS product_pageviews_count,
        COUNT(DISTINCT pp.product_id)                               AS viewed_products_count
    FROM pageviews_ranked pr
    INNER JOIN product_pages pp
        ON pr.pageview_url = pp.product_page_url
    GROUP BY 1
),

orders_agg AS (
    SELECT
        CAST(website_session_id AS BIGINT)                          AS website_session_id,
        COUNT(DISTINCT CAST(order_id AS BIGINT))                    AS orders_count,
        SUM(CAST(items_purchased AS BIGINT))                        AS items_purchased_count
    FROM {{ ref('base_orders') }}
    GROUP BY 1
)

SELECT
    s.website_session_id,
    s.session_created_at_utc,
    CAST(s.session_created_at_utc AS DATE)                          AS session_date,
    s.user_id,
    s.is_repeat_session_flag,
    CASE
        WHEN s.is_repeat_session_flag = 1 THEN true
        ELSE false
    END                                                             AS is_repeat_session,
    s.utm_source,
    s.utm_campaign,
    s.utm_content,
    s.device_type,
    s.http_referer,

    p.pageviews_count,
    p.first_pageview_at_utc,
    p.last_pageview_at_utc,
    p.landing_page_url,
    p.exit_page_url,
    CASE
        WHEN p.pageviews_count = 1 THEN true
        ELSE false
    END                                                             AS is_bounced,
    CASE
        WHEN p.first_pageview_at_utc IS NULL OR p.last_pageview_at_utc IS NULL THEN null
        ELSE date_diff('second', p.first_pageview_at_utc, p.last_pageview_at_utc)
    END                                                             AS session_duration_seconds,
    COALESCE(pp.product_pageviews_count, 0)                         AS product_pageviews_count,
    COALESCE(pp.viewed_products_count, 0)                           AS viewed_products_count,
    CASE
        WHEN COALESCE(pp.product_pageviews_count, 0) > 0 THEN true
        ELSE false
    END                                                             AS has_product_pageview,

    COALESCE(o.orders_count, 0)                                     AS orders_count,
    CASE
        WHEN COALESCE(o.orders_count, 0) > 0 THEN true
        ELSE false
    END                                                             AS has_order,
    COALESCE(o.items_purchased_count, 0)                            AS items_purchased_count
FROM sessions s
LEFT JOIN pageviews_agg p
    ON s.website_session_id = p.website_session_id
LEFT JOIN product_pageviews_agg pp
    ON s.website_session_id = pp.website_session_id
LEFT JOIN orders_agg o
    ON s.website_session_id = o.website_session_id
