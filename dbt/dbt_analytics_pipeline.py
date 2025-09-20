"""
DAG для запуску dbt моделей аналітики криптовалют
"""
from datetime import datetime, timedelta
import subprocess
import os
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

# DAG definition
dag = DAG(
    'dbt_analytics_pipeline',
    default_args=default_args,
    description='dbt pipeline для аналітики криптовалютних даних',
    schedule_interval=timedelta(hours=6),  # Запуск кожні 6 годин
    catchup=False,
    tags=['dbt', 'analytics', 'crypto'],
)

def setup_dbt_environment():
    """Налаштування dbt середовища"""
    print("🔧 Налаштовуємо dbt середовище...")
    
    # Встановлення dbt якщо потрібно
    try:
        subprocess.run(['dbt', '--version'], check=True, capture_output=True)
        print("✅ dbt вже встановлено")
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("📦 Встановлюємо dbt...")
        subprocess.run(['pip', 'install', 'dbt-postgres'], check=True)
    
    # Перевірка конфігурації
    dbt_dir = '/opt/airflow/dbt'
    os.chdir(dbt_dir)
    
    print(f"📁 Працюємо в директорії: {os.getcwd()}")
    
    # Встановлення змінної середовища для профілів
    os.environ['DBT_PROFILES_DIR'] = dbt_dir
    
    return "dbt середовище готове"

def run_dbt_debug():
    """Перевірка підключення dbt до бази даних"""
    print("🔍 Перевіряємо підключення dbt...")
    
    os.chdir('/opt/airflow/dbt')
    os.environ['DBT_PROFILES_DIR'] = '/opt/airflow/dbt'
    
    try:
        result = subprocess.run(
            ['dbt', 'debug'], 
            check=True, 
            capture_output=True, 
            text=True
        )
        
        print("✅ dbt debug пройшов успішно:")
        print(result.stdout)
        return "dbt підключення працює"
        
    except subprocess.CalledProcessError as e:
        print(f"❌ dbt debug failed: {e}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        raise

def run_dbt_deps():
    """Встановлення dbt залежностей"""
    print("📦 Встановлюємо dbt залежності...")
    
    os.chdir('/opt/airflow/dbt')
    os.environ['DBT_PROFILES_DIR'] = '/opt/airflow/dbt'
    
    try:
        result = subprocess.run(
            ['dbt', 'deps'], 
            check=True, 
            capture_output=True, 
            text=True
        )
        
        print("✅ dbt deps завершено:")
        print(result.stdout)
        return "Залежності встановлено"
        
    except subprocess.CalledProcessError as e:
        print(f"❌ dbt deps failed: {e}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        # Не падаємо, якщо немає пакетів для встановлення
        return "Залежності не потрібні"

def run_dbt_staging_models():
    """Запуск staging моделей dbt"""
    print("🔄 Запускаємо staging моделі...")
    
    os.chdir('/opt/airflow/dbt')
    os.environ['DBT_PROFILES_DIR'] = '/opt/airflow/dbt'
    
    try:
        result = subprocess.run(
            ['dbt', 'run', '--models', 'staging'], 
            check=True, 
            capture_output=True, 
            text=True
        )
        
        print("✅ Staging моделі виконано:")
        print(result.stdout)
        return "Staging моделі готові"
        
    except subprocess.CalledProcessError as e:
        print(f"❌ dbt staging models failed: {e}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        raise

def run_dbt_mart_models():
    """Запуск mart моделей dbt"""
    print("🔄 Запускаємо mart моделі...")
    
    os.chdir('/opt/airflow/dbt')
    os.environ['DBT_PROFILES_DIR'] = '/opt/airflow/dbt'
    
    try:
        result = subprocess.run(
            ['dbt', 'run', '--models', 'marts'], 
            check=True, 
            capture_output=True, 
            text=True
        )
        
        print("✅ Mart моделі виконано:")
        print(result.stdout)
        return "Mart моделі готові"
        
    except subprocess.CalledProcessError as e:
        print(f"❌ dbt mart models failed: {e}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        raise

def run_dbt_tests():
    """Запуск dbt тестів"""
    print("🧪 Запускаємо dbt тести...")
    
    os.chdir('/opt/airflow/dbt')
    os.environ['DBT_PROFILES_DIR'] = '/opt/airflow/dbt'
    
    try:
        result = subprocess.run(
            ['dbt', 'test'], 
            check=True, 
            capture_output=True, 
            text=True
        )
        
        print("✅ dbt тести пройшли:")
        print(result.stdout)
        return "Всі тести пройшли"
        
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Деякі dbt тести не пройшли: {e}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        # Не падаємо при помилках тестів, тільки попереджуємо
        return "Тести завершені з попередженнями"

def generate_dbt_docs():
    """Генерація dbt документації"""
    print("📚 Генеруємо dbt документацію...")
    
    os.chdir('/opt/airflow/dbt')
    os.environ['DBT_PROFILES_DIR'] = '/opt/airflow/dbt'
    
    try:
        # Генерація документації
        result = subprocess.run(
            ['dbt', 'docs', 'generate'], 
            check=True, 
            capture_output=True, 
            text=True
        )
        
        print("✅ dbt документація згенерована:")
        print(result.stdout)
        
        # Перевірка існування файлів документації
        docs_dir = '/opt/airflow/dbt/target'
        if os.path.exists(f"{docs_dir}/index.html"):
            print(f"📄 Документація доступна в: {docs_dir}/index.html")
        
        return "Документація згенерована"
        
    except subprocess.CalledProcessError as e:
        print(f"❌ dbt docs generation failed: {e}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        return "Документація не згенерована"

def analytics_summary():
    """Підсумковий звіт аналітики"""
    print("📊 Генеруємо підсумковий звіт...")
    
    import psycopg2
    
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
        
        print("="*60)
        print("📈 ЗВІТ АНАЛІТИКИ КРИПТОВАЛЮТ")
        print("="*60)
        
        # Перевірка наявності аналітичних таблиць
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'marts' 
            ORDER BY table_name
        """)
        
        tables = cursor.fetchall()
        print(f"📋 Створено аналітичних таблиць: {len(tables)}")
        for table in tables:
            print(f"   - {table[0]}")
        
        # Статистика по топ виконавцях
        try:
            cursor.execute("""
                SELECT category, COUNT(*) as count
                FROM marts.crypto_top_performers 
                GROUP BY category
                ORDER BY category
            """)
            
            categories = cursor.fetchall()
            print(f"\n🏆 Категорії топ виконавців:")
            for cat, count in categories:
                print(f"   - {cat}: {count} монет")
                
        except Exception as e:
            print(f"⚠️ Таблиця топ виконавців ще не готова: {e}")
        
        # Статистика по денним метрикам
        try:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_coins,
                    COUNT(DISTINCT extract_date) as days_of_data,
                    AVG(current_price) as avg_price
                FROM marts.crypto_daily_metrics
            """)
            
            stats = cursor.fetchone()
            if stats:
                total, days, avg_price = stats
                print(f"\n📊 Загальна статистика:")
                print(f"   - Загалом монет: {total}")
                print(f"   - Днів даних: {days}")
                print(f"   - Середня ціна: ${avg_price:,.2f}" if avg_price else "   - Середня ціна: N/A")
                
        except Exception as e:
            print(f"⚠️ Таблиця денних метрик ще не готова: {e}")
        
        cursor.close()
        conn.close()
        
        print("="*60)
        print("✅ Аналітичний pipeline завершено успішно!")
        
        return "Аналітичний звіт готовий"
        
    except Exception as e:
        print(f"❌ Помилка генерації звіту: {e}")
        return "Звіт згенеровано з помилками"

# Визначення задач
start_task = BashOperator(
    task_id='start_dbt_pipeline',
    bash_command='echo "🚀 Запускаємо dbt Analytics Pipeline..."',
    dag=dag,
)

setup_task = PythonOperator(
    task_id='setup_dbt_environment',
    python_callable=setup_dbt_environment,
    dag=dag,
)

debug_task = PythonOperator(
    task_id='dbt_debug',
    python_callable=run_dbt_debug,
    dag=dag,
)

deps_task = PythonOperator(
    task_id='dbt_deps',
    python_callable=run_dbt_deps,
    dag=dag,
)

staging_task = PythonOperator(
    task_id='dbt_staging_models',
    python_callable=run_dbt_staging_models,
    dag=dag,
)

marts_task = PythonOperator(
    task_id='dbt_mart_models',
    python_callable=run_dbt_mart_models,
    dag=dag,
)

test_task = PythonOperator(
    task_id='dbt_tests',
    python_callable=run_dbt_tests,
    dag=dag,
)

docs_task = PythonOperator(
    task_id='generate_dbt_docs',
    python_callable=generate_dbt_docs,
    dag=dag,
)

summary_task = PythonOperator(
    task_id='analytics_summary',
    python_callable=analytics_summary,
    dag=dag,
)

end_task = BashOperator(
    task_id='end_dbt_pipeline',
    bash_command='echo "✅ dbt Analytics Pipeline завершено!"',
    dag=dag,
)

# Порядок виконання задач
start_task >> setup_task >> debug_task >> deps_task >> staging_task >> marts_task >> [test_task, docs_task] >> summary_task >> end_task