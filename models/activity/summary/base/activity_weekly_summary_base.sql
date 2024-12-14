{{ config
(
    materialized = 'incremental',
    unique_key = 'date'
)
}}

WITH overview AS (
SELECT 
DATE_TRUNC('WEEK',DATE) AS DATE,
CHAIN,
SUM(SUCCESS_TRANSACTIONS) AS SUCCESS_TRANSACTIONS,
AVG(TPS) AS TPS
FROM (
SELECT
ACTIVITY_DATE AS DATE,
'base' AS CHAIN,
SUCCESS_TRANSACTIONS,
TOTAL_TRANSACTIONS / 86400 AS TPS
FROM BASE.METRICS.OVERVIEW
{% if is_incremental() %}
WHERE ACTIVITY_DATE >= CURRENT_DATE() - interval '3 day'
AND ACTIVITY_DATE < CURRENT_DATE() 
{% endif %}
{% if not is_incremental() %}
WHERE ACTIVITY_DATE < CURRENT_DATE() 
AND ACTIVITY_DATE >= TO_TIMESTAMP('2024-08-28', 'YYYY-MM-DD') 
{% endif %}
)
GROUP BY 1,2
)

, mgas AS (
SELECT 
DATE_TRUNC('WEEK',DATE) AS DATE,
AVG(MEGAGAS_USED_PER_SECOND) AS MEGAGAS_USED_PER_SECOND
FROM (
SELECT
DATE_TRUNC('DAY',BLOCK_TIMESTAMP) AS DATE,
(SUM(RECEIPT_GAS_USED) / 86400) / 1e6 AS MEGAGAS_USED_PER_SECOND
FROM BASE.RAW.TRANSACTIONS
{% if is_incremental() %}
WHERE DATE_TRUNC('WEEK',BLOCK_TIMESTAMP) >= DATE_TRUNC('WEEK',CURRENT_DATE()) - interval '3 week'
AND DATE_TRUNC('WEEK',BLOCK_TIMESTAMP) < DATE_TRUNC('WEEK',CURRENT_DATE())
{% endif %}
{% if not is_incremental() %}
WHERE DATE_TRUNC('WEEK',BLOCK_TIMESTAMP) < DATE_TRUNC('WEEK',CURRENT_DATE())
AND DATE_TRUNC('WEEK',BLOCK_TIMESTAMP) >= TO_TIMESTAMP('2024-08-28', 'YYYY-MM-DD') 
{% endif %}
GROUP BY 1
)
GROUP BY 1
)

, actives AS (
SELECT
DATE_TRUNC('WEEK', BLOCK_TIMESTAMP) AS DATE,
COUNT(DISTINCT FROM_ADDRESS) AS ACTIVE_ADDRESSES
FROM BASE.RAW.TRANSACTIONS
{% if is_incremental() %}
WHERE DATE_TRUNC('WEEK',BLOCK_TIMESTAMP) >= DATE_TRUNC('WEEK',CURRENT_DATE()) - interval '3 week'
AND DATE_TRUNC('WEEK',BLOCK_TIMESTAMP) < DATE_TRUNC('WEEK',CURRENT_DATE())
{% endif %}
{% if not is_incremental() %}
WHERE DATE_TRUNC('WEEK',BLOCK_TIMESTAMP) < DATE_TRUNC('WEEK',CURRENT_DATE())
AND DATE_TRUNC('WEEK',BLOCK_TIMESTAMP) >= TO_TIMESTAMP('2024-08-28', 'YYYY-MM-DD') 
{% endif %}
AND RECEIPT_STATUS = 1
GROUP BY 1
)

SELECT 
o.DATE,
CHAIN,
SUCCESS_TRANSACTIONS,
ACTIVE_ADDRESSES,
TPS,
MEGAGAS_USED_PER_SECOND
FROM overview o
INNER JOIN mgas m ON o.DATE = m.DATE
INNER JOIN actives a ON o.DATE = a.DATE