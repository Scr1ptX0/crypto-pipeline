{{
  config(
    materialized='table',
    description='Top performing cryptocurrencies by various metrics'
  )
}}

WITH latest_metrics AS (
    SELECT *
    FROM {{ ref('crypto_daily_metrics') }}
    WHERE extract_date = (SELECT MAX(extract_date) FROM {{ ref('crypto_daily_metrics') }})
),

top_gainers_24h AS (
    SELECT 
        'Top Gainers 24h' as category,
        coin_id,
        symbol,
        name,
        current_price,
        price_change_percentage_24h as metric_value,
        market_cap_rank,
        ROW_NUMBER() OVER (ORDER BY price_change_percentage_24h DESC) as rank
    FROM latest_metrics
    WHERE price_change_percentage_24h IS NOT NULL
    ORDER BY price_change_percentage_24h DESC
    LIMIT 10
),

top_losers_24h AS (
    SELECT 
        'Top Losers 24h' as category,
        coin_id,
        symbol,
        name,
        current_price,
        price_change_percentage_24h as metric_value,
        market_cap_rank,
        ROW_NUMBER() OVER (ORDER BY price_change_percentage_24h ASC) as rank
    FROM latest_metrics
    WHERE price_change_percentage_24h IS NOT NULL
    ORDER BY price_change_percentage_24h ASC
    LIMIT 10
),

highest_volume AS (
    SELECT 
        'Highest Volume' as category,
        coin_id,
        symbol,
        name,
        current_price,
        total_volume as metric_value,
        market_cap_rank,
        ROW_NUMBER() OVER (ORDER BY total_volume DESC) as rank
    FROM latest_metrics
    WHERE total_volume IS NOT NULL
    ORDER BY total_volume DESC
    LIMIT 10
),

top_market_cap AS (
    SELECT 
        'Top Market Cap' as category,
        coin_id,
        symbol,
        name,
        current_price,
        market_cap as metric_value,
        market_cap_rank,
        ROW_NUMBER() OVER (ORDER BY market_cap DESC) as rank
    FROM latest_metrics
    WHERE market_cap IS NOT NULL
    ORDER BY market_cap DESC
    LIMIT 10
),

combined_results AS (
    SELECT * FROM top_gainers_24h
    UNION ALL
    SELECT * FROM top_losers_24h
    UNION ALL
    SELECT * FROM highest_volume
    UNION ALL
    SELECT * FROM top_market_cap
)

SELECT 
    category,
    rank,
    coin_id,
    symbol,
    name,
    current_price,
    metric_value,
    market_cap_rank,
    CURRENT_TIMESTAMP as created_at
FROM combined_results
ORDER BY category, rank