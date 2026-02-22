{{
    config(
        materialized        = 'view',
        tags                = ['bronze', 'sales', 'orders'],
        partition_by        = ['partition_year', 'partition_month'],
    )
}}

SELECT 
    *
FROM {{ source('raw', 'orders') }}
