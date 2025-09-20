{{
  config(
    materialized='view',
    description='Staging view for crypto prices with basic cleaning'
  )
}}

WITH raw_crypto_data AS (
    SELECT 
        id,
        coin_id,
        symbol,
        name,
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
        ath_date,
        atl,
        atl_date,
        last_updated,
        extraction_timestamp,
        created_at
    FROM {{ source('public', 'crypto_prices') }}
),

cleaned_data AS (
    SELECT 
        id,
        coin_id,
        UPPER(symbol) as symbol,
        name,
        CASE 
            WHEN current_price <= 0 THEN NULL 
            ELSE current_price 
        END as current_price,
        CASE 
            WHEN market_cap <= 0 THEN NULL 
            ELSE market_cap 
        END as market_cap,
        market_cap_rank,
        CASE 
            WHEN total_volume < 0 THEN NULL 
            ELSE total_volume 
        END as total_volume,
        price_change_24h,
        price_change_percentage_24h,
        price_change_percentage_7d,
        CASE 
            WHEN circulating_supply <= 0 THEN NULL 
            ELSE circulating_supply 
        END as circulating_supply,
        CASE 
            WHEN total_supply <= 0 THEN NULL 
            ELSE total_supply 
        END as total_supply,
        ath,
        ath_date::timestamp as ath_date,
        atl,
        atl_date::timestamp as atl_date,
        last_updated::timestamp as last_updated,
        extraction_timestamp::timestamp as extraction_timestamp,
        created_at
    FROM raw_crypto_data
    WHERE current_price IS NOT NULL
      AND name IS NOT NULL
      AND symbol IS NOT NULL
)

SELECT * FROM cleaned_data