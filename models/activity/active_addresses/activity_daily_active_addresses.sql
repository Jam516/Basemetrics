{{ config
(
    materialized = 'table'
)
}}

SELECT  *, 'arbitrum' AS chain FROM {{ ref('activity_daily_active_addresses_arbitrum') }}
UNION ALL
SELECT  *, 'base' AS chain FROM {{ ref('activity_daily_active_addresses_base') }}
UNION ALL
SELECT  *, 'bsc' AS chain FROM {{ ref('activity_daily_active_addresses_bsc') }}
UNION ALL
SELECT  *, 'solana' AS chain FROM {{ ref('activity_daily_active_addresses_solana') }}
UNION ALL
SELECT  *, 'tron' AS chain FROM {{ ref('activity_daily_active_addresses_tron') }}