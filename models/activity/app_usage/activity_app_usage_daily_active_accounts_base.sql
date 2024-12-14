{{ config
(
    materialized = 'table'
)
}}

-- make this a grouped bar chart that isnt stacked maybe
-- or a table

WITH RankedProjects AS (
  SELECT
    DATE_TRUNC('day', t.BLOCK_TIMESTAMP) AS DATE,
    CASE WHEN c.ADDRESS IS NOT NULL THEN COALESCE(l.NAME, t.TO_ADDRESS)  
    WHEN to_double(VALUE) > 0 THEN 'EOA-EOA ETH transfer'
    ELSE 'empty_call'
    END AS PROJECT,
    COUNT(DISTINCT t.FROM_ADDRESS) AS ACTIVE_ADDRESSES,
    ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('day', t.BLOCK_TIMESTAMP) ORDER BY COUNT(DISTINCT t.FROM_ADDRESS) DESC) AS RN
  FROM
    BASE.RAW.TRANSACTIONS t
    LEFT JOIN BASE.RAW.CONTRACTS c ON t.TO_ADDRESS = c.ADDRESS 
    LEFT JOIN BASEMETRICS.DBT.LABELS_APPS l ON t.TO_ADDRESS = l.ADDRESS
    WHERE (c.ADDRESS IS NOT NULL OR to_double(VALUE) > 0)
    AND DATE_TRUNC('DAY',t.BLOCK_TIMESTAMP) >= TO_TIMESTAMP('2024-09-01', 'YYYY-MM-DD') 
    AND DATE_TRUNC('DAY',t.BLOCK_TIMESTAMP) < CURRENT_DATE() 
    AND RECEIPT_STATUS = 1
  GROUP BY
    1, 2
)
  SELECT
    DATE,
    PROJECT,
    ACTIVE_ADDRESSES
  FROM
    RankedProjects
  WHERE RN <= 10