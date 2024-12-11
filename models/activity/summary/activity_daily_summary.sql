{{ config
(
    materialized = 'table'
)
}}

SELECT  *, 'arbitrum' AS chain FROM {{ ref('activity_daily_summary_arbitrum') }}
UNION ALL
SELECT  *, 'base' AS chain FROM {{ ref('activity_daily_summary_base') }}
UNION ALL
SELECT  *, 'bsc' AS chain FROM {{ ref('activity_daily_summary_bsc') }}
UNION ALL
SELECT  *, 'solana' AS chain FROM {{ ref('activity_daily_summary_solana') }}
UNION ALL
SELECT  *, 'tron' AS chain FROM {{ ref('activity_daily_summary_tron') }}