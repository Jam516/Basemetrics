{{ config
(
    materialized = 'incremental',
    unique_key = 'date'
)
}}

SELECT 
DATE_TRUNC('week',BLOCK_TIMESTAMP) as date,
COUNT(DISTINCT FROM_ADDRESS) as active_addresses
FROM {{ source('base_raw', 'transactions') }}
{% if is_incremental() %}
WHERE DATE_TRUNC('week',BLOCK_TIMESTAMP) >= DATE_TRUNC('week',CURRENT_DATE()) - interval '3 week' 
AND DATE_TRUNC('week',BLOCK_TIMESTAMP) < DATE_TRUNC('week',CURRENT_DATE())
{% endif %}
{% if not is_incremental() %}
WHERE DATE_TRUNC('week',BLOCK_TIMESTAMP) < DATE_TRUNC('week',CURRENT_DATE())
AND DATE_TRUNC('week',BLOCK_TIMESTAMP) >= TO_TIMESTAMP('2024-01-01', 'YYYY-MM-DD') 
{% endif %}
AND GAS_PRICE > 0
GROUP BY 1