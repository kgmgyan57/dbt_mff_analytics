{{
    config(
        materialized        = 'view',
        tags                = ['bronze', 'sales', 'order_item_refunds'],
        partition_by        = ['partition_year', 'partition_month'],
    )
}}

SELECT 
    *
FROM {{ source('raw', 'order_item_refunds') }}
