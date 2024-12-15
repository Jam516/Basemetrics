{{ config
(
    materialized = 'table'
)
}}

WITH sender_transfer_rates AS (
    -- For each transaction sender, get their hourly transaction data
    SELECT 
    FROM_ADDRESS AS sender
    , DATE_TRUNC('hour',BLOCK_TIMESTAMP) AS hr
    , MIN(BLOCK_TIMESTAMP) AS min_block_time
    , MAX(BLOCK_TIMESTAMP) AS max_block_time
    , COUNT(*) AS hr_txs
    FROM BASE.RAW.TRANSACTIONS t
    WHERE DATE_TRUNC('DAY',t.BLOCK_TIMESTAMP) >= TO_TIMESTAMP('2024-09-01', 'YYYY-MM-DD') 
    GROUP BY 1,2
)

, first_pass_throughput_filter AS (
    -- Filter down this list a bit to help with later mappings
    SELECT 
    sender, 
    DATE_TRUNC('week',hr) AS wk, 
    SUM(hr_txs) AS wk_txs, 
    MAX(hr_txs) AS max_hr_txs, 
    cast(COUNT(*) as float) /cast(7.0*24.0 as float) AS pct_weekly_hours_active,
    MIN(min_block_time) AS min_block_time,
    MAX(max_block_time) AS max_block_time
    FROM sender_transfer_rates e
    GROUP BY 1,2
    HAVING MAX(hr_txs) >= 20 --had some high-ish frequency - gte 20 txs per hour at least once

)



        SELECT 
        sender
        ,MAX(wk_txs) AS max_wk_txs 
        ,MAX(max_hr_txs) AS max_hr_txs
        ,AVG(wk_txs) AS avg_wk_txs
        ,MIN(min_block_time) AS min_block_time
        ,MAX(max_block_time) AS max_block_time
        ,MAX(pct_weekly_hours_active) AS max_pct_weekly_hours_active
        ,AVG(pct_weekly_hours_active) AS avg_pct_weekly_hours_active
        ,SUM(wk_txs) AS num_txs
        FROM first_pass_throughput_filter f
            GROUP BY 1
            -- various cases to detect bots
            HAVING (MAX(wk_txs) >= 2000 AND MAX(max_hr_txs) >= 100) --frequency (gt 2k txs in one week and gt 100 txs in one hour)
                OR (MAX(wk_txs) >= 4000 AND MAX(max_hr_txs) >= 50) --frequency (gt 4k txs in one week and gt 50 txs in one hour)
                OR AVG(wk_txs) >= 1000 --frequency (avg 1k txs per week)
                OR AVG(pct_weekly_hours_active) > 0.5 -- aliveness: transacting at least 50% of hours per week
                OR MAX(pct_weekly_hours_active) > 0.95 -- aliveness: at peack, transacted at least 95% of hours in a week
                OR 
                (
                    cast(COUNT(*) as double) /
                        ( cast( DATEDIFF('second', MIN(min_block_time), MAX(max_block_time)) as double) / (60.0*60.0) ) >= 25
                    AND SUM(wk_txs) >= 100
                    ) --number of txns / number of hours > 25 per hour AND total txns > 100