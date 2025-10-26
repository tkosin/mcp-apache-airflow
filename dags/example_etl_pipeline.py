"""
Example ETL Pipeline DAG
Demonstrates a typical ETL (Extract, Transform, Load) workflow
"""

from datetime import datetime, timedelta
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def extract_data(**context):
    """Extract: Simulate data extraction"""
    print("Extracting data from source...")
    
    # Simulate extracted data
    raw_data = {
        'users': [
            {'id': 1, 'name': 'Alice', 'age': 30, 'city': 'Bangkok'},
            {'id': 2, 'name': 'Bob', 'age': 25, 'city': 'Chiang Mai'},
            {'id': 3, 'name': 'Charlie', 'age': 35, 'city': 'Phuket'},
            {'id': 4, 'name': 'David', 'age': 28, 'city': 'Bangkok'},
            {'id': 5, 'name': 'Eve', 'age': 32, 'city': 'Pattaya'},
        ],
        'timestamp': str(datetime.now())
    }
    
    print(f"Extracted {len(raw_data['users'])} records")
    
    # Push data to XCom for next task
    context['task_instance'].xcom_push(key='raw_data', value=raw_data)
    return raw_data

def transform_data(**context):
    """Transform: Clean and transform data"""
    print("Transforming data...")
    
    # Pull data from XCom
    ti = context['task_instance']
    raw_data = ti.xcom_pull(task_ids='extract', key='raw_data')
    
    if not raw_data:
        raise ValueError("No data received from extract task")
    
    # Transform: Filter users over 27 and add calculated field
    transformed_data = []
    for user in raw_data['users']:
        if user['age'] > 27:
            user['age_group'] = 'senior' if user['age'] > 30 else 'middle'
            user['name_upper'] = user['name'].upper()
            transformed_data.append(user)
    
    result = {
        'users': transformed_data,
        'count': len(transformed_data),
        'timestamp': raw_data['timestamp']
    }
    
    print(f"Transformed {len(transformed_data)} records")
    print(f"Sample: {json.dumps(transformed_data[0], indent=2)}")
    
    # Push to XCom
    ti.xcom_push(key='transformed_data', value=result)
    return result

def validate_data(**context):
    """Validate: Check data quality"""
    print("Validating data...")
    
    ti = context['task_instance']
    data = ti.xcom_pull(task_ids='transform', key='transformed_data')
    
    if not data or not data['users']:
        raise ValueError("No data to validate")
    
    # Validation rules
    errors = []
    for user in data['users']:
        if not user.get('name'):
            errors.append(f"User {user['id']} missing name")
        if user.get('age', 0) < 18:
            errors.append(f"User {user['id']} age below minimum")
        if not user.get('age_group'):
            errors.append(f"User {user['id']} missing age_group")
    
    if errors:
        print(f"Validation warnings: {errors}")
    else:
        print(f"All {len(data['users'])} records passed validation")
    
    # Push validation result
    validation_result = {
        'status': 'passed' if not errors else 'warnings',
        'errors': errors,
        'validated_count': len(data['users'])
    }
    ti.xcom_push(key='validation_result', value=validation_result)
    
    return validation_result

def load_data(**context):
    """Load: Save data to destination"""
    print("Loading data to destination...")
    
    ti = context['task_instance']
    data = ti.xcom_pull(task_ids='transform', key='transformed_data')
    validation = ti.xcom_pull(task_ids='validate', key='validation_result')
    
    if validation['status'] == 'failed':
        raise ValueError("Cannot load data: validation failed")
    
    # Simulate loading to database/file
    print(f"Loading {data['count']} records...")
    print(f"Data: {json.dumps(data, indent=2)}")
    print("Data loaded successfully!")
    
    # Summary
    summary = {
        'loaded_records': data['count'],
        'timestamp': data['timestamp'],
        'validation_status': validation['status']
    }
    
    return summary

def send_notification(**context):
    """Send completion notification"""
    ti = context['task_instance']
    summary = ti.xcom_pull(task_ids='load')
    
    message = f"""
    ETL Pipeline Completed Successfully!
    
    Records Loaded: {summary['loaded_records']}
    Timestamp: {summary['timestamp']}
    Validation: {summary['validation_status']}
    
    DAG Run ID: {context['dag_run'].run_id}
    """
    
    print(message)
    return message

# Create DAG
with DAG(
    dag_id='example_etl_pipeline',
    default_args=default_args,
    description='Example ETL pipeline with Extract, Transform, Load',
    schedule='@daily',  # Run daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'etl', 'pipeline'],
) as dag:
    
    # Start task
    start = BashOperator(
        task_id='start',
        bash_command='echo "Starting ETL Pipeline..."',
    )
    
    # Extract
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
    )
    
    # Transform
    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
    )
    
    # Validate
    validate = PythonOperator(
        task_id='validate',
        python_callable=validate_data,
    )
    
    # Load
    load = PythonOperator(
        task_id='load',
        python_callable=load_data,
    )
    
    # Notification
    notify = PythonOperator(
        task_id='notify',
        python_callable=send_notification,
    )
    
    # End task
    end = BashOperator(
        task_id='end',
        bash_command='echo "ETL Pipeline completed!"',
    )
    
    # Define dependencies
    start >> extract >> transform >> validate >> load >> notify >> end
