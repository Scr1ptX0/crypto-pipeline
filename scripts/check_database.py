#!/usr/bin/env python3
"""
Скрипт для перевірки даних в базі PostgreSQL
"""
import psycopg2
from datetime import datetime

def check_crypto_data():
    """Перевірка даних в таблиці crypto_prices"""
    
    try:
        # Підключення до бази даних
        conn = psycopg2.connect(
            host="project-postgres",
            database="crypto_db",
            user="crypto_user",
            password="crypto_pass",
            port=5432
        )
        cursor = conn.cursor()
        
        print("🔍 Перевіряємо дані в базі...")
        print("="*50)
        
        # Загальна кількість записів
        cursor.execute("SELECT COUNT(*) FROM crypto_prices")
        total_count = cursor.fetchone()[0]
        print(f"📊 Загальна кількість записів: {total_count}")
        
        # Унікальні монети
        cursor.execute("SELECT COUNT(DISTINCT coin_id) FROM crypto_prices")
        unique_coins = cursor.fetchone()[0]
        print(f"🪙 Унікальних монет: {unique_coins}")
        
        # Останнє оновлення
        cursor.execute("SELECT MAX(extraction_timestamp) FROM crypto_prices")
        last_update = cursor.fetchone()[0]
        print(f"🕐 Останнє оновлення: {last_update}")
        
        # Топ 5 монет за ринковою капіталізацією
        cursor.execute("""
            SELECT name, symbol, current_price, market_cap, market_cap_rank 
            FROM crypto_prices 
            WHERE extraction_timestamp = (SELECT MAX(extraction_timestamp) FROM crypto_prices)
            ORDER BY market_cap_rank 
            LIMIT 5
        """)
        
        top_coins = cursor.fetchall()
        print("\n🏆 Топ 5 монет за капіталізацією:")
        for coin in top_coins:
            name, symbol, price, market_cap, rank = coin
            print(f"{rank}. {name} ({symbol}): ${price:,.2f} | Cap: ${market_cap:,}")
        
        # Статистика цін
        cursor.execute("""
            SELECT 
                MIN(current_price) as min_price,
                MAX(current_price) as max_price,
                AVG(current_price) as avg_price
            FROM crypto_prices 
            WHERE extraction_timestamp = (SELECT MAX(extraction_timestamp) FROM crypto_prices)
        """)
        
        price_stats = cursor.fetchone()
        min_price, max_price, avg_price = price_stats
        print(f"\n💰 Статистика цін:")
        print(f"   Мінімальна: ${min_price:,.8f}")
        print(f"   Максимальна: ${max_price:,.2f}")
        print(f"   Середня: ${avg_price:,.2f}")
        
        cursor.close()
        conn.close()
        
        print("="*50)
        print("✅ Перевірка завершена успішно!")
        
    except Exception as e:
        print(f"❌ Помилка при перевірці: {e}")

if __name__ == "__main__":
    check_crypto_data()