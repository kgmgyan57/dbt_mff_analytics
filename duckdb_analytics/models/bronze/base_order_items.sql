{{
    config(
        materialized        = 'view',
        tags                = ['bronze', 'sales', 'order_items'],
        partition_by        = ['partition_year', 'partition_month'],
    )
}}

SELECT 
    *
FROM {{ source('raw', 'order_items') }}
