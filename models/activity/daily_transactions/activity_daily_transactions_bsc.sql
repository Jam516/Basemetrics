{{ config
(
    materialized = 'incremental',
    unique_key = 'date'
)
}}

SELECT 
DATE_TRUNC('day',BLOCK_TIMESTAMP) as date,
COUNT(HASH) as num_txns
FROM {{ source('bsc_raw', 'transactions') }}
{% if is_incremental() %}
WHERE DATE_TRUNC('day',BLOCK_TIMESTAMP) >= CURRENT_DATE() - interval '3 day' 
AND DATE_TRUNC('day',BLOCK_TIMESTAMP) < CURRENT_DATE() 
{% endif %}
{% if not is_incremental() %}
WHERE DATE_TRUNC('day',BLOCK_TIMESTAMP) < CURRENT_DATE() 
AND DATE_TRUNC('day',BLOCK_TIMESTAMP) >= TO_TIMESTAMP('2024-06-01', 'YYYY-MM-DD') 
{% endif %}
AND RECEIPT_STATUS = 1
GROUP BY 1