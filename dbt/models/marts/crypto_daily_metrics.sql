{{
  config(
    materialized='table',
    description='Daily aggregated metrics for cryptocurrencies'
  )
}}

WITH daily_crypto_data AS (
    SELECT 
        coin_id,
        symbol,
        name,
        DATE(extraction_timestamp) as extract_date,
        current_price,
        market_cap,
        market_cap_rank,
        total_volume,
        price_change_24h,
        price_change_percentage_24h,
        price_change_percentage_7d,
        circulating_supply,
        total_supply,
        ath,
        atl,
        ROW_NUMBER() OVER (
            PARTITION BY coin_id, DATE(extraction_timestamp) 
            ORDER BY extraction_timestamp DESC
        ) as rn
    FROM {{ ref('stg_crypto_prices') }}
),

latest_daily_prices AS (
    SELECT 
        coin_id,
        symbol,
        name,
        extract_date,
        current_price,
        market_cap,
        market_cap_rank,
        total_volume,
        price_change_24h,
        price_change_percentage_24h,
        price_change_percentage_7d,
        circulating_supply,
        total_supply,
        ath,
        atl
    FROM daily_crypto_data
    WHERE rn = 1
),

enriched_metrics AS (
    SELECT 
        coin_id,
        symbol,
        name,
        extract_date,
        current_price,
        market_cap,
        market_cap_rank,
        total_volume,
        price_change_24h,
        price_change_percentage_24h,
        price_change_percentage_7d,
        circulating_supply,
        total_supply,
        ath,
        atl,
        
        -- Розрахункові метрики
        CASE 
            WHEN circulating_supply > 0 
            THEN current_price * circulating_supply 
            ELSE NULL 
        END as calculated_market_cap,
        
        CASE 
            WHEN total_volume > 0 AND market_cap > 0 
            THEN (total_volume / market_cap) * 100 
            ELSE NULL 
        END as volume_to_mcap_ratio,
        
        CASE 
            WHEN ath > 0 
            THEN ((current_price - ath) / ath) * 100 
            ELSE NULL 
        END as distance_from_ath_pct,
        
        CASE 
            WHEN atl > 0 
            THEN ((current_price - atl) / atl) * 100 
            ELSE NULL 
        END as distance_from_atl_pct,
        
        -- Категоризація за ринковою капіталізацією
        CASE 
            WHEN market_cap_rank <= 10 THEN 'Large Cap'
            WHEN market_cap_rank <= 50 THEN 'Mid Cap'
            WHEN market_cap_rank <= 200 THEN 'Small Cap'
            ELSE 'Micro Cap'
        END as market_cap_category,
        
        -- Категоризація за зміною ціни
        CASE 
            WHEN price_change_percentage_24h >= 10 THEN 'Strong Pump'
            WHEN price_change_percentage_24h >= 5 THEN 'Pump'
            WHEN price_change_percentage_24h >= -5 THEN 'Stable'
            WHEN price_change_percentage_24h >= -10 THEN 'Dump'
            ELSE 'Strong Dump'
        END as price_movement_category
        
    FROM latest_daily_prices
)

SELECT * FROM enriched_metrics
ORDER BY market_cap_rank