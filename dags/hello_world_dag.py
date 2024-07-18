'''
The first DAG. Serves as a template for all other DAGs. 
'''

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

# Initializing default arguments for DAG
default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2024, 3, 14),
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

# Instantiate a DAG
hello_world_dag = DAG(
    dag_id='hello_world_dag',
    default_args=default_args,
    description='This is my first DAG',
    schedule_interval='@once',
    catchup=False
)

# Creating a callable function
def print_hello():
    return 'Hello, World!'

# Creating tasks
start_task = EmptyOperator(
    task_id = 'start_task',
    dag = hello_world_dag
)

hello_world_task = PythonOperator(
    task_id = 'hello_world_task',
    python_callable = print_hello,
    dag = hello_world_dag
)

end_task = EmptyOperator(
    task_id = 'end_task',
    dag = hello_world_dag
)

# Setting up dependencies
start_task >> hello_world_task >> end_task




