{{ config
(
    materialized = 'incremental',
    unique_key = 'day'
)
}}

SELECT 
DATE_TRUNC('day',BLOCK_TIMESTAMP) as day,
COUNT(TXN_ID) as num_txns
FROM {{ source('solana_raw', 'transactions') }}
{% if is_incremental() %}
WHERE DATE_TRUNC('day',BLOCK_TIMESTAMP) >= CURRENT_DATE() - interval '3 day' 
AND DATE_TRUNC('day',BLOCK_TIMESTAMP) < CURRENT_DATE() 
{% endif %}
{% if not is_incremental() %}
WHERE DATE_TRUNC('day',BLOCK_TIMESTAMP) < CURRENT_DATE() 
AND DATE_TRUNC('day',BLOCK_TIMESTAMP) >= TO_TIMESTAMP('2022-01-01', 'YYYY-MM-DD') 
{% endif %}
AND IS_VOTING = FALSE
GROUP BY 1