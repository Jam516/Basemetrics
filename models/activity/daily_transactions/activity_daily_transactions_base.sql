{{ config
(
    materialized = 'incremental',
    unique_key = 'day'
)
}}

SELECT 
DATE_TRUNC('day',BLOCK_TIMESTAMP) as day,
COUNT(HASH) as num_txns
FROM BASE.RAW.TRANSACTIONS
{% if is_incremental() %}
WHERE DATE_TRUNC('day',BLOCK_TIMESTAMP) >= CURRENT_DATE() - interval '3 day' 
AND DATE_TRUNC('day',BLOCK_TIMESTAMP) < CURRENT_DATE() 
{% endif %}
GROUP BY 1