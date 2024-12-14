{{ config
(
    materialized = 'table'
)
}}

WITH contract_usage AS (
    SELECT 
        t.TO_ADDRESS as contract_address,
        COUNT(DISTINCT t.FROM_ADDRESS) as unique_callers,
        COUNT(DISTINCT CASE 
            WHEN t.FROM_ADDRESS IN (SELECT SENDER FROM BASEMETRICS.DBT.LABELS_LIKELY_BOTS)
            THEN t.FROM_ADDRESS 
        END) as bot_callers
    FROM BASE.RAW.TRANSACTIONS t
    INNER JOIN BASE.RAW.CONTRACTS c
        ON c.ADDRESS = t.TO_ADDRESS
    WHERE DATE_TRUNC('DAY',t.BLOCK_TIMESTAMP) >= TO_TIMESTAMP('2024-09-01', 'YYYY-MM-DD')
    GROUP BY t.TO_ADDRESS
)
SELECT contract_address
FROM contract_usage
WHERE bot_callers / unique_callers > 0.8