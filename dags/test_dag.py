"""
Простий тестовий DAG для перевірки налаштування
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

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

# Створення DAG
dag = DAG(
    'test_crypto_setup',
    default_args=default_args,
    description='Тестовий DAG для перевірки налаштування',
    schedule_interval=None,  # Запуск вручну
    catchup=False,
    tags=['test', 'setup'],
)

def check_python_packages():
    """Перевірка Python пакетів"""
    import sys
    print(f"Python версія: {sys.version}")
    
    packages_to_check = ['requests', 'pandas']
    
    for package in packages_to_check:
        try:
            __import__(package)
            print(f"✅ {package} - встановлено")
        except ImportError:
            print(f"❌ {package} - не встановлено")
    
    return "Перевірка завершена"

def test_api_request():
    """Тестовий запит до CoinGecko API"""
    import requests
    
    try:
        url = "https://api.coingecko.com/api/v3/ping"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            print("✅ CoinGecko API доступний!")
            print(f"Відповідь: {response.json()}")
            return response.json()
        else:
            print(f"❌ Помилка API: {response.status_code}")
            return {"error": response.status_code}
            
    except Exception as e:
        print(f"❌ Помилка при запиті: {e}")
        return {"error": str(e)}

def check_database():
    """Перевірка підключення до бази даних"""
    try:
        import psycopg2
        
        # Спроба підключення до project postgres
        conn = psycopg2.connect(
            host="project-postgres",
            database="crypto_db",
            user="crypto_user",
            password="crypto_pass",
            port=5432
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        print("✅ Підключення до PostgreSQL успішне!")
        print(f"Версія: {db_version[0]}")
        return {"status": "success"}
        
    except Exception as e:
        print(f"❌ Помилка підключення до БД: {e}")
        return {"status": "error", "message": str(e)}

# Завдання
check_packages_task = PythonOperator(
    task_id='check_python_packages',
    python_callable=check_python_packages,
    dag=dag,
)

test_api_task = PythonOperator(
    task_id='test_api_request',
    python_callable=test_api_request,
    dag=dag,
)

check_db_task = PythonOperator(
    task_id='check_database',
    python_callable=check_database,
    dag=dag,
)

print_info_task = BashOperator(
    task_id='print_system_info',
    bash_command='echo "🎉 Airflow працює! Дата: $(date)"',
    dag=dag,
)

# Порядок виконання завдань
print_info_task >> check_packages_task >> [test_api_task, check_db_task]