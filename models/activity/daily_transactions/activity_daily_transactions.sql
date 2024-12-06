{{ config
(
    materialized = 'table'
)
}}

SELECT  *, 'arbitrum' AS chain FROM {{ ref('activity_daily_transactions_arbitrum') }}
UNION ALL
SELECT  *, 'base' AS chain FROM {{ ref('activity_daily_transactions_base') }}
UNION ALL
SELECT  *, 'bsc' AS chain FROM {{ ref('activity_daily_transactions_bsc') }}
UNION ALL
SELECT  *, 'solana' AS chain FROM {{ ref('activity_daily_transactions_solana') }}
UNION ALL
SELECT  *, 'tron' AS chain FROM {{ ref('activity_daily_transactions_tron') }}