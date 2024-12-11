{{ config
(
    materialized = 'incremental',
    unique_key = 'date'
)
}}


SELECT
ACTIVITY_DATE AS DATE,
'solana' AS CHAIN,
SUCCESS_NON_VOTING_TX_COUNT AS NUM_TXNS,
ACTIVE_NON_VOTER_ADDRESSES AS ACTIVE_ADDRESSES,
TOTAL_TRANSACTIONS / 86400 AS TPS, 
null AS GAS_USED_PER_SECOND
FROM SOLANA__ENRICHED.METRICS.OVERVIEW
{% if is_incremental() %}
WHERE ACTIVITY_DATE >= CURRENT_DATE() - interval '3 day'
AND ACTIVITY_DATE < CURRENT_DATE() 
{% endif %}
{% if not is_incremental() %}
WHERE ACTIVITY_DATE < CURRENT_DATE() 
AND ACTIVITY_DATE >= TO_TIMESTAMP('2024-09-01', 'YYYY-MM-DD') 
{% endif %}
