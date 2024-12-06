{{ config
(
    materialized = 'incremental',
    unique_key = 'date'
)
}}

SELECT 
ACTIVITY_DATE as date,
SUCCESS_NON_VOTING_TX_COUNT as num_txns
FROM {{ source('solana_metrics', 'overview') }}
{% if is_incremental() %}
WHERE ACTIVITY_DATE >= CURRENT_DATE() - interval '3 day' 
AND ACTIVITY_DATE < CURRENT_DATE() 
{% endif %}
{% if not is_incremental() %}
WHERE ACTIVITY_DATE < CURRENT_DATE() 
AND ACTIVITY_DATE >= TO_TIMESTAMP('2024-06-01', 'YYYY-MM-DD') 
{% endif %}
