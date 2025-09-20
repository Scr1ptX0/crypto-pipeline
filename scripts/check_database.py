#!/usr/bin/env python3
"""
Скрипт для перевірки результатів аналітики
"""
import psycopg2
import pandas as pd
from datetime import datetime

def check_analytics_results():
    """Перевірка результатів аналітичних моделей"""
    
    try:
        # Підключення до бази даних
        conn = psycopg2.connect(
            host="project-postgres",
            database="crypto_db",
            user="crypto_user",
            password="crypto_pass",
            port=5432
        )
        
        print("🔍 Перевіряємо результати аналітики...")
        print("="*60)
        
        # Перевірка створених схем
        cursor = conn.cursor()
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('staging', 'marts')
            ORDER BY schema_name
        """)
        
        schemas = cursor.fetchall()
        print(f"📁 Створено схем: {len(schemas)}")
        for schema in schemas:
            print(f"   - {schema[0]}")
        
        # Перевірка створених таблиць/в'ю
        cursor.execute("""
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables 
            WHERE table_schema IN ('staging', 'marts', 'public')
            AND table_name LIKE '%crypto%'
            ORDER BY table_schema, table_name
        """)
        
        tables = cursor.fetchall()
        print(f"\n📋 Знайдено таблиць/в'ю з криптовалютами: {len(tables)}")
        for schema, table, table_type in tables:
            print(f"   - {schema}.{table} ({table_type})")
        
        # Детальна статистика по кожній таблиці
        analytics_tables = [
            ('public', 'crypto_prices'),
            ('staging', 'stg_crypto_prices'),
            ('marts', 'crypto_daily_metrics'),
            ('marts', 'crypto_top_performers')
        ]
        
        print("\n📊 Статистика по таблицях:")
        for schema, table in analytics_tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
                count = cursor.fetchone()[0]
                print(f"   - {schema}.{table}: {count:,} записів")
                
                # Додаткова інформація для деяких таблиць
                if table == 'crypto_daily_metrics':
                    cursor.execute(f"""
                        SELECT 
                            COUNT(DISTINCT coin_id) as unique_coins,
                            COUNT(DISTINCT extract_date) as unique_dates,
                            MIN(extract_date) as first_date,
                            MAX(extract_date) as last_date
                        FROM {schema}.{table}
                    """)
                    stats = cursor.fetchone()
                    if stats:
                        unique_coins, unique_dates, first_date, last_date = stats
                        print(f"     └─ Унікальних монет: {unique_coins}")
                        print(f"     └─ Унікальних дат: {unique_dates}")
                        print(f"     └─ Період: {first_date} - {last_date}")
                
                elif table == 'crypto_top_performers':
                    cursor.execute(f"""
                        SELECT category, COUNT(*) as count
                        FROM {schema}.{table}
                        GROUP BY category
                        ORDER BY category
                    """)
                    categories = cursor.fetchall()
                    for cat, cnt in categories:
                        print(f"     └─ {cat}: {cnt}")
                        
            except Exception as e:
                print(f"   - {schema}.{table}: ❌ Помилка ({str(e)[:50]}...)")
        
        # Топ 5 монет за ринковою капіталізацією
        try:
            print("\n🏆 Топ 5 монет за ринковою капіталізацією:")
            cursor.execute("""
                SELECT 
                    name,
                    symbol,
                    current_price,
                    market_cap,
                    market_cap_rank,
                    price_change_percentage_24h
                FROM marts.crypto_daily_metrics
                WHERE extract_date = (SELECT MAX(extract_date) FROM marts.crypto_daily_metrics)
                ORDER BY market_cap_rank
                LIMIT 5
            """)
            
            top_coins = cursor.fetchall()
            for coin in top_coins:
                name, symbol, price, mcap, rank, change = coin
                change_str = f"{change:+.2f}%" if change else "N/A"
                print(f"   {rank}. {name} ({symbol})")
                print(f"      💰 Ціна: ${price:,.2f}")
                print(f"      📊 Капіталізація: ${mcap:,}")
                print(f"      📈 Зміна 24г: {change_str}")
                print()
                
        except Exception as e:
            print(f"⚠️ Не вдалося отримати топ монети: {e}")
        
        # Найбільші зростання та падіння
        try:
            print("📈 Найбільші зростання за 24 години:")
            cursor.execute("""
                SELECT name, symbol, price_change_percentage_24h
                FROM marts.crypto_daily_metrics
                WHERE extract_date = (SELECT MAX(extract_date) FROM marts.crypto_daily_metrics)
                AND price_change_percentage_24h IS NOT NULL
                ORDER BY price_change_percentage_24h DESC
                LIMIT 3
            """)
            
            gainers = cursor.fetchall()
            for name, symbol, change in gainers:
                print(f"   🚀 {name} ({symbol}): +{change:.2f}%")
            
            print("\n📉 Найбільші падіння за 24 години:")
            cursor.execute("""
                SELECT name, symbol, price_change_percentage_24h
                FROM marts.crypto_daily_metrics
                WHERE extract_date = (SELECT MAX(extract_date) FROM marts.crypto_daily_metrics)
                AND price_change_percentage_24h IS NOT NULL
                ORDER BY price_change_percentage_24h ASC
                LIMIT 3
            """)
            
            losers = cursor.fetchall()
            for name, symbol, change in losers:
                print(f"   📉 {name} ({symbol}): {change:.2f}%")
                
        except Exception as e:
            print(f"⚠️ Не вдалося отримати зміни цін: {e}")
        
        # Статистика по категоріях
        try:
            print("\n🏷️ Розподіл за категоріями ринкової капіталізації:")
            cursor.execute("""
                SELECT 
                    market_cap_category,
                    COUNT(*) as count,
                    AVG(current_price) as avg_price
                FROM marts.crypto_daily_metrics
                WHERE extract_date = (SELECT MAX(extract_date) FROM marts.crypto_daily_metrics)
                GROUP BY market_cap_category
                ORDER BY 
                    CASE market_cap_category
                        WHEN 'Large Cap' THEN 1
                        WHEN 'Mid Cap' THEN 2
                        WHEN 'Small Cap' THEN 3
                        WHEN 'Micro Cap' THEN 4
                    END
            """)
            
            categories = cursor.fetchall()
            for cat, count, avg_price in categories:
                avg_price_str = f"${avg_price:,.2f}" if avg_price else "N/A"
                print(f"   📊 {cat}: {count} монет (сер. ціна: {avg_price_str})")
                
        except Exception as e:
            print(f"⚠️ Не вдалося отримати категорії: {e}")
        
        cursor.close()
        conn.close()
        
        print("\n" + "="*60)
        print("✅ Перевірка аналітики завершена!")
        print("="*60)
        
    except Exception as e:
        print(f"❌ Помилка при перевірці аналітики: {e}")

if __name__ == "__main__":
    check_analytics_results()