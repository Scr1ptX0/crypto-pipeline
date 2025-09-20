"""
DAG для генерації HTML дашборду з результатами аналітики
"""
from datetime import datetime, timedelta
import psycopg2
import json
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'crypto_dashboard_generator',
    default_args=default_args,
    description='Генерація HTML дашборду для аналітики криптовалют',
    schedule_interval=timedelta(hours=12),  # Оновлення двічі на день
    catchup=False,
    tags=['dashboard', 'reporting', 'crypto'],
)

def generate_crypto_dashboard(**context):
    """Генерація HTML дашборду"""
    print("📊 Генеруємо криптовалютний дашборд...")
    
    try:
        # Підключення до бази
        conn = psycopg2.connect(
            host="project-postgres",
            database="crypto_db",
            user="crypto_user",
            password="crypto_pass",
            port=5432
        )
        cursor = conn.cursor()
        
        # Збір даних для дашборду
        dashboard_data = {}
        
        # Основна статистика
        cursor.execute("""
            SELECT 
                COUNT(*) as total_coins,
                AVG(current_price) as avg_price,
                SUM(market_cap) as total_market_cap,
                MAX(extraction_timestamp) as last_update
            FROM public.crypto_prices
            WHERE extraction_timestamp = (SELECT MAX(extraction_timestamp) FROM public.crypto_prices)
        """)
        
        stats = cursor.fetchone()
        dashboard_data['overview'] = {
            'total_coins': stats[0] if stats[0] else 0,
            'avg_price': float(stats[1]) if stats[1] else 0,
            'total_market_cap': int(stats[2]) if stats[2] else 0,
            'last_update': str(stats[3]) if stats[3] else 'N/A'
        }
        
        # Топ 10 монет
        try:
            cursor.execute("""
                SELECT 
                    name, symbol, current_price, market_cap, 
                    market_cap_rank, price_change_percentage_24h
                FROM marts.crypto_daily_metrics
                WHERE extract_date = (SELECT MAX(extract_date) FROM marts.crypto_daily_metrics)
                ORDER BY market_cap_rank
                LIMIT 10
            """)
            
            top_coins = []
            for row in cursor.fetchall():
                top_coins.append({
                    'name': row[0],
                    'symbol': row[1],
                    'price': float(row[2]) if row[2] else 0,
                    'market_cap': int(row[3]) if row[3] else 0,
                    'rank': int(row[4]) if row[4] else 0,
                    'change_24h': float(row[5]) if row[5] else 0
                })
            
            dashboard_data['top_coins'] = top_coins
            
        except Exception as e:
            print(f"⚠️ Не вдалося отримати топ монети: {e}")
            dashboard_data['top_coins'] = []
        
        # Топ зростання/падіння
        try:
            cursor.execute("""
                SELECT name, symbol, price_change_percentage_24h
                FROM marts.crypto_daily_metrics
                WHERE extract_date = (SELECT MAX(extract_date) FROM marts.crypto_daily_metrics)
                AND price_change_percentage_24h IS NOT NULL
                ORDER BY price_change_percentage_24h DESC
                LIMIT 5
            """)
            
            gainers = []
            for row in cursor.fetchall():
                gainers.append({
                    'name': row[0],
                    'symbol': row[1],
                    'change': float(row[2])
                })
            
            dashboard_data['top_gainers'] = gainers
            
            cursor.execute("""
                SELECT name, symbol, price_change_percentage_24h
                FROM marts.crypto_daily_metrics
                WHERE extract_date = (SELECT MAX(extract_date) FROM marts.crypto_daily_metrics)
                AND price_change_percentage_24h IS NOT NULL
                ORDER BY price_change_percentage_24h ASC
                LIMIT 5
            """)
            
            losers = []
            for row in cursor.fetchall():
                losers.append({
                    'name': row[0],
                    'symbol': row[1],
                    'change': float(row[2])
                })
            
            dashboard_data['top_losers'] = losers
            
        except Exception as e:
            print(f"⚠️ Не вдалося отримати зміни: {e}")
            dashboard_data['top_gainers'] = []
            dashboard_data['top_losers'] = []
        
        # Розподіл за категоріями
        try:
            cursor.execute("""
                SELECT 
                    market_cap_category,
                    COUNT(*) as count
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
            
            categories = []
            for row in cursor.fetchall():
                categories.append({
                    'category': row[0],
                    'count': int(row[1])
                })
            
            dashboard_data['categories'] = categories
            
        except Exception as e:
            print(f"⚠️ Не вдалося отримати категорії: {e}")
            dashboard_data['categories'] = []
        
        cursor.close()
        conn.close()
        
        # Генерація HTML
        html_content = generate_html_dashboard(dashboard_data)
        
        # Збереження дашборду
        dashboard_path = '/opt/airflow/data/processed/crypto_dashboard.html'
        with open(dashboard_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"✅ Дашборд збережено: {dashboard_path}")
        
        # Також збереження JSON даних
        json_path = '/opt/airflow/data/processed/dashboard_data.json'
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(dashboard_data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ JSON дані збережено: {json_path}")
        
        return "Дашборд згенеровано успішно"
        
    except Exception as e:
        print(f"❌ Помилка генерації дашборду: {e}")
        raise

def generate_html_dashboard(data):
    """Генерація HTML коду дашборду"""
    
    html = f"""
<!DOCTYPE html>
<html lang="uk">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Криптовалютний Дашборд</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: #333;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}
        .header {{
            text-align: center;
            color: white;
            margin-bottom: 30px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: white;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            text-align: center;
        }}
        .stat-value {{
            font-size: 2em;
            font-weight: bold;
            color: #667eea;
        }}
        .stat-label {{
            color: #666;
            margin-top: 5px;
        }}
        .section {{
            background: white;
            margin-bottom: 20px;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .section h2 {{
            margin-top: 0;
            color: #667eea;
            border-bottom: 2px solid #eee;
            padding-bottom: 10px;
        }}
        .coins-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 15px;
        }}
        .coin-card {{
            border: 1px solid #eee;
            border-radius: 8px;
            padding: 15px;
            background: #f9f9f9;
        }}
        .coin-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 10px;
        }}
        .coin-name {{
            font-weight: bold;
            font-size: 1.1em;
        }}
        .coin-symbol {{
            background: #667eea;
            color: white;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 0.8em;
        }}
        .coin-details {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px;
            font-size: 0.9em;
        }}
        .positive {{ color: #4caf50; }}
        .negative {{ color: #f44336; }}
        .update-time {{
            text-align: center;
            color: #666;
            font-size: 0.9em;
            margin-top: 20px;
        }}
        .movers-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
        }}
        .mover-list {{
            background: #f9f9f9;
            border-radius: 8px;
            padding: 15px;
        }}
        .mover-item {{
            display: flex;
            justify-content: space-between;
            padding: 8px 0;
            border-bottom: 1px solid #eee;
        }}
        .mover-item:last-child {{
            border-bottom: none;
        }}
        .categories-chart {{
            display: flex;
            justify-content: space-around;
            align-items: end;
            height: 200px;
            margin: 20px 0;
        }}
        .category-bar {{
            background: #667eea;
            color: white;
            padding: 10px;
            border-radius: 4px 4px 0 0;
            text-align: center;
            min-width: 80px;
            display: flex;
            flex-direction: column;
            justify-content: end;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 Криптовалютний Дашборд</h1>
            <p>Аналітика ринку криптовалют в реальному часі</p>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-value">{data['overview']['total_coins']}</div>
                <div class="stat-label">Загалом монет</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">${data['overview']['avg_price']:,.2f}</div>
                <div class="stat-label">Середня ціна</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">${data['overview']['total_market_cap']:,}</div>
                <div class="stat-label">Загальна капіталізація</div>
            </div>
        </div>
        
        <div class="section">
            <h2>🏆 Топ 10 криптовалют за капіталізацією</h2>
            <div class="coins-grid">
    """
    
    # Топ монети
    for coin in data['top_coins'][:10]:
        change_class = 'positive' if coin['change_24h'] >= 0 else 'negative'
        change_symbol = '+' if coin['change_24h'] >= 0 else ''
        
        html += f"""
                <div class="coin-card">
                    <div class="coin-header">
                        <div class="coin-name">{coin['name']}</div>
                        <div class="coin-symbol">{coin['symbol']}</div>
                    </div>
                    <div class="coin-details">
                        <div><strong>Ціна:</strong> ${coin['price']:,.2f}</div>
                        <div><strong>Ранг:</strong> #{coin['rank']}</div>
                        <div><strong>Капіталізація:</strong> ${coin['market_cap']:,}</div>
                        <div class="{change_class}"><strong>24г:</strong> {change_symbol}{coin['change_24h']:.2f}%</div>
                    </div>
                </div>
        """
    
    html += """
            </div>
        </div>
        
        <div class="section">
            <h2>📈📉 Найбільші рухи за 24 години</h2>
            <div class="movers-grid">
                <div>
                    <h3 style="color: #4caf50;">🚀 Топ зростання</h3>
                    <div class="mover-list">
    """
    
    # Топ зростання
    for gainer in data['top_gainers'][:5]:
        html += f"""
                        <div class="mover-item">
                            <span>{gainer['name']} ({gainer['symbol']})</span>
                            <span class="positive">+{gainer['change']:.2f}%</span>
                        </div>
        """
    
    html += """
                    </div>
                </div>
                <div>
                    <h3 style="color: #f44336;">📉 Топ падіння</h3>
                    <div class="mover-list">
    """
    
    # Топ падіння
    for loser in data['top_losers'][:5]:
        html += f"""
                        <div class="mover-item">
                            <span>{loser['name']} ({loser['symbol']})</span>
                            <span class="negative">{loser['change']:.2f}%</span>
                        </div>
        """
    
    html += """
                    </div>
                </div>
            </div>
        </div>
        
        <div class="section">
            <h2>🏷️ Розподіл за категоріями ринкової капіталізації</h2>
            <div class="categories-chart">
    """
    
    # Категорії
    max_count = max([cat['count'] for cat in data['categories']]) if data['categories'] else 1
    for category in data['categories']:
        height = (category['count'] / max_count) * 150 + 50
        html += f"""
                <div class="category-bar" style="height: {height}px;">
                    <div>{category['count']}</div>
                    <div style="font-size: 0.8em;">{category['category']}</div>
                </div>
        """
    
    html += f"""
            </div>
        </div>
        
        <div class="update-time">
            📅 Останнє оновлення: {data['overview']['last_update']}
        </div>
    </div>
</body>
</html>
    """
    
    return html

# Визначення задач
generate_dashboard_task = PythonOperator(
    task_id='generate_crypto_dashboard',
    python_callable=generate_crypto_dashboard,
    dag=dag,
)

# Єдина задача в цьому DAG
generate_dashboard_task