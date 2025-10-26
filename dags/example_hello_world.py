"""
Example Hello World DAG
A simple DAG to test basic Airflow functionality
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_hello(**context):
    """Print hello message with execution date"""
    execution_date = context['execution_date']
    print(f"Hello from Airflow!")
    print(f"Execution date: {execution_date}")
    return "Hello World!"

def print_context(**context):
    """Print DAG run configuration"""
    conf = context.get('dag_run').conf or {}
    print(f"DAG Run Configuration: {conf}")
    print(f"Task Instance: {context['task_instance']}")
    return conf

# Create the DAG
with DAG(
    dag_id='example_hello_world',
    default_args=default_args,
    description='A simple hello world DAG',
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'tutorial'],
) as dag:
    
    # Task 1: Print greeting using Bash
    task_bash_hello = BashOperator(
        task_id='bash_hello',
        bash_command='echo "Hello from Bash!"',
    )
    
    # Task 2: Print hello using Python
    task_python_hello = PythonOperator(
        task_id='python_hello',
        python_callable=print_hello,
    )
    
    # Task 3: Print context information
    task_print_context = PythonOperator(
        task_id='print_context',
        python_callable=print_context,
    )
    
    # Task 4: Final message
    task_goodbye = BashOperator(
        task_id='goodbye',
        bash_command='echo "Goodbye! DAG completed successfully!"',
    )
    
    # Define task dependencies
    task_bash_hello >> task_python_hello >> task_print_context >> task_goodbye
