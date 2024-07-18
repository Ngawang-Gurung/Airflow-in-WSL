'''
This DAG explores how BashOperator can be used to dump and import SQL files. 
'''

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 14),
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(
    'mysql_table_dump',
    default_args=default_args,
    description='A DAG to dump and load a MySQL table',
    schedule_interval='@once',
    catchup=False,
)

dump_command = f"mysqldump -u {DB_USERNAME} -p{DB_PASSWORD} -h {DB_HOST} -P {DB_PORT} customer customer_profile > /home/user/airflow_wsl/dags/dump_file.sql 2>/home/user/airflow_wsl/dags/dump_error.log"


dump_task = BashOperator(
    task_id='mysql_table_dump_task',
    bash_command=dump_command,
    dag=dag 
)

load_command = f"mysql -u {DB_USERNAME} -p{DB_PASSWORD} -h {DB_HOST} -P {DB_PORT} client_rw < /home/user/airflow_wsl/dags/dump_file.sql" 

load_task = BashOperator(
    task_id='mysql_table_load_task',
    bash_command=load_command,
    dag=dag
)

dump_task >> load_task

