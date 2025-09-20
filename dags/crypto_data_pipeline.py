"""
ETL Pipeline для збору криптовалютних даних з CoinGecko API
"""
from datetime import datetime, timedelta
import json
import pandas as pd
import requests
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Конфігурація
COINGECKO_API_URL = "https://api.coingecko.com/api/v3"
TOP_CRYPTOS = ['bitcoin', 'ethereum', 'binancecoin', 'solana', 'cardano', 
               'dogecoin', 'polygon', 'avalanche-2', 'chainlink', 'uniswap']

# Default arguments
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'crypto_data_pipeline',
    default_args=default_args,
    description='ETL pipeline для збору криптовалютних даних',
    schedule_interval=timedelta(hours=1),  # Запуск щогодини
    catchup=False,
    tags=['crypto', 'etl', 'production'],
)

def extract_crypto_prices(**context):
    """
    Збір даних про ціни криптовалют з CoinGecko API
    """
    print("🔄 Починаємо збір даних з CoinGecko API...")
    
    try:
        # Запит до API для отримання ринкових даних
        url = f"{COINGECKO_API_URL}/coins/markets"
        params = {
            'vs_currency': 'usd',
            'order': 'market_cap_desc',
            'per_page': 50,
            'page': 1,
            'sparkline': 'false',
            'price_change_percentage': '1h,24h,7d'
        }
        
        print(f"📡 Відправляємо запит до: {url}")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        print(f"✅ Отримано дані для {len(data)} криптовалют")
        
        # Зберігаємо raw дані у файл
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_file_path = f"/opt/airflow/data/raw/crypto_prices_{timestamp}.json"
        
        with open(raw_file_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"💾 Raw дані збережено в: {raw_file_path}")
        
        # Повертаємо шлях до файлу для наступних задач
        return {
            'raw_file_path': raw_file_path,
            'records_count': len(data),
            'extraction_time': timestamp
        }
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Помилка при запиті до API: {e}")
        raise
    except Exception as e:
        print(f"❌ Несподівана помилка: {e}")
        raise

def transform_crypto_data(**context):
    """
    Трансформація та очищення даних
    """
    print("🔄 Починаємо трансформацію даних...")
    
    # Отримуємо дані з попередньої задачі
    ti = context['ti']
    extract_result = ti.xcom_pull(task_ids='extract_crypto_prices')
    raw_file_path = extract_result['raw_file_path']
    
    print(f"📂 Читаємо дані з: {raw_file_path}")
    
    # Читаємо raw дані
    with open(raw_file_path, 'r') as f:
        raw_data = json.load(f)
    
    # Трансформуємо дані
    transformed_records = []
    
    for coin in raw_data:
        try:
            record = {
                'coin_id': coin['id'],
                'symbol': coin['symbol'].upper(),
                'name': coin['name'],
                'current_price': float(coin['current_price']) if coin['current_price'] else None,
                'market_cap': int(coin['market_cap']) if coin['market_cap'] else None,
                'market_cap_rank': int(coin['market_cap_rank']) if coin['market_cap_rank'] else None,
                'total_volume': float(coin['total_volume']) if coin['total_volume'] else None,
                'price_change_24h': float(coin['price_change_24h']) if coin['price_change_24h'] else None,
                'price_change_percentage_24h': float(coin['price_change_percentage_24h']) if coin['price_change_percentage_24h'] else None,
                'price_change_percentage_7d': float(coin.get('price_change_percentage_7d_in_currency')) if coin.get('price_change_percentage_7d_in_currency') else None,
                'circulating_supply': float(coin['circulating_supply']) if coin['circulating_supply'] else None,
                'total_supply': float(coin['total_supply']) if coin['total_supply'] else None,
                'ath': float(coin['ath']) if coin['ath'] else None,
                'ath_date': coin['ath_date'],
                'atl': float(coin['atl']) if coin['atl'] else None,
                'atl_date': coin['atl_date'],
                'last_updated': coin['last_updated'],
                'extraction_timestamp': datetime.now().isoformat()
            }
            transformed_records.append(record)
            
        except (KeyError, ValueError, TypeError) as e:
            print(f"⚠️ Помилка обробки монети {coin.get('id', 'unknown')}: {e}")
            continue
    
    print(f"✅ Трансформовано {len(transformed_records)} записів")
    
    # Зберігаємо трансформовані дані
    timestamp = extract_result['extraction_time']
    processed_file_path = f"/opt/airflow/data/processed/crypto_prices_transformed_{timestamp}.json"
    
    with open(processed_file_path, 'w') as f:
        json.dump(transformed_records, f, indent=2)
    
    print(f"💾 Трансформовані дані збережено в: {processed_file_path}")
    
    return {
        'processed_file_path': processed_file_path,
        'transformed_records_count': len(transformed_records),
        'processing_time': timestamp
    }

def load_to_database(**context):
    """
    Завантаження даних до PostgreSQL
    """
    print("🔄 Завантажуємо дані до PostgreSQL...")
    
    # Отримуємо дані з попередньої задачі
    ti = context['ti']
    transform_result = ti.xcom_pull(task_ids='transform_crypto_data')
    processed_file_path = transform_result['processed_file_path']
    
    # Читаємо трансформовані дані
    with open(processed_file_path, 'r') as f:
        data = json.load(f)
    
    print(f"📊 Завантажуємо {len(data)} записів до бази даних...")
    
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
        
        # Створення таблиці якщо не існує
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS crypto_prices (
            id SERIAL PRIMARY KEY,
            coin_id VARCHAR(100) NOT NULL,
            symbol VARCHAR(10) NOT NULL,
            name VARCHAR(100) NOT NULL,
            current_price DECIMAL(20,8),
            market_cap BIGINT,
            market_cap_rank INTEGER,
            total_volume DECIMAL(20,2),
            price_change_24h DECIMAL(20,8),
            price_change_percentage_24h DECIMAL(10,4),
            price_change_percentage_7d DECIMAL(10,4),
            circulating_supply DECIMAL(25,2),
            total_supply DECIMAL(25,2),
            ath DECIMAL(20,8),
            ath_date TIMESTAMP,
            atl DECIMAL(20,8),
            atl_date TIMESTAMP,
            last_updated TIMESTAMP,
            extraction_timestamp TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cursor.execute(create_table_sql)
        print("✅ Таблиця crypto_prices готова")
        
        # Вставка даних
        insert_sql = """
        INSERT INTO crypto_prices (
            coin_id, symbol, name, current_price, market_cap, market_cap_rank,
            total_volume, price_change_24h, price_change_percentage_24h,
            price_change_percentage_7d, circulating_supply, total_supply,
            ath, ath_date, atl, atl_date, last_updated, extraction_timestamp
        ) VALUES (
            %(coin_id)s, %(symbol)s, %(name)s, %(current_price)s, %(market_cap)s,
            %(market_cap_rank)s, %(total_volume)s, %(price_change_24h)s,
            %(price_change_percentage_24h)s, %(price_change_percentage_7d)s,
            %(circulating_supply)s, %(total_supply)s, %(ath)s, %(ath_date)s,
            %(atl)s, %(atl_date)s, %(last_updated)s, %(extraction_timestamp)s
        )
        """
        
        cursor.executemany(insert_sql, data)
        conn.commit()
        
        print(f"✅ Успішно завантажено {len(data)} записів до таблиці crypto_prices")
        
        # Статистика
        cursor.execute("SELECT COUNT(*) FROM crypto_prices")
        total_records = cursor.fetchone()[0]
        print(f"📊 Загальна кількість записів в таблиці: {total_records}")
        
        cursor.close()
        conn.close()
        
        return {
            'loaded_records': len(data),
            'total_records_in_db': total_records,
            'load_timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        print(f"❌ Помилка завантаження до БД: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        raise

def generate_summary_report(**context):
    """
    Генерація звіту про виконання pipeline
    """
    print("📊 Генеруємо звіт...")
    
    ti = context['ti']
    extract_result = ti.xcom_pull(task_ids='extract_crypto_prices')
    transform_result = ti.xcom_pull(task_ids='transform_crypto_data')
    load_result = ti.xcom_pull(task_ids='load_to_database')
    
    report = {
        'pipeline_execution_time': datetime.now().isoformat(),
        'extraction': {
            'records_extracted': extract_result['records_count'],
            'extraction_time': extract_result['extraction_time']
        },
        'transformation': {
            'records_transformed': transform_result['transformed_records_count'],
            'processing_time': transform_result['processing_time']
        },
        'loading': {
            'records_loaded': load_result['loaded_records'],
            'total_records_in_db': load_result['total_records_in_db'],
            'load_timestamp': load_result['load_timestamp']
        }
    }
    
    print("="*50)
    print("📈 ЗВІТ ВИКОНАННЯ CRYPTO DATA PIPELINE")
    print("="*50)
    print(f"🕐 Час виконання: {report['pipeline_execution_time']}")
    print(f"📥 Зібрано записів: {report['extraction']['records_extracted']}")
    print(f"🔄 Трансформовано: {report['transformation']['records_transformed']}")
    print(f"💾 Завантажено в БД: {report['loading']['records_loaded']}")
    print(f"📊 Загалом в БД: {report['loading']['total_records_in_db']}")
    print("="*50)
    
    # Зберігаємо звіт
    timestamp = extract_result['extraction_time']
    report_file_path = f"/opt/airflow/data/processed/pipeline_report_{timestamp}.json"
    
    with open(report_file_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"📄 Звіт збережено в: {report_file_path}")
    
    return report

# Визначення задач
start_task = BashOperator(
    task_id='start_pipeline',
    bash_command='echo "🚀 Запускаємо Crypto Data Pipeline..."',
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_crypto_prices',
    python_callable=extract_crypto_prices,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_crypto_data',
    python_callable=transform_crypto_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_summary_report',
    python_callable=generate_summary_report,
    dag=dag,
)

end_task = BashOperator(
    task_id='end_pipeline',
    bash_command='echo "✅ Crypto Data Pipeline завершено успішно!"',
    dag=dag,
)

# Визначення залежностей (порядок виконання)
start_task >> extract_task >> transform_task >> load_task >> report_task >> end_task