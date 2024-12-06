{{ config
(
    materialized = 'incremental',
    unique_key = 'date'
)
}}

SELECT 
DATE_TRUNC('day',BLOCK_TIMESTAMP) as date,
COUNT(HASH) as num_txns
FROM {{ source('base_raw', 'transactions') }}
{% if is_incremental() %}
WHERE DATE_TRUNC('day',BLOCK_TIMESTAMP) >= CURRENT_DATE() - interval '3 day' 
AND DATE_TRUNC('day',BLOCK_TIMESTAMP) < CURRENT_DATE() 
{% endif %}
{% if not is_incremental() %}
WHERE DATE_TRUNC('day',BLOCK_TIMESTAMP) < CURRENT_DATE() 
AND DATE_TRUNC('day',BLOCK_TIMESTAMP) >= TO_TIMESTAMP('2024-01-01', 'YYYY-MM-DD') 
{% endif %}
AND GAS_PRICE > 0
GROUP BY 1