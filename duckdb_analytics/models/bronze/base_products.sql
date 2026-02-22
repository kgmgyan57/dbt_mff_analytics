{{
    config(
        materialized        = 'view',
        tags                = ['bronze', 'sales', 'products'],
    )
}}

SELECT 
    *
FROM {{ source('raw', 'products') }}
