{{
    config(
        materialized        = 'view',
        tags                = ['bronze', 'sessions', 'website_sessions'],
        partition_by        = ['partition_year', 'partition_month'],
    )
}}

SELECT 
    *
FROM {{ source('raw', 'website_sessions') }}
