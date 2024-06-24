from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'mysql_table_dump',
    default_args=default_args,
    description='A DAG to dump and load a MySQL table',
    schedule_interval='@once',
    catchup=False,
)

dump_command = "mysqldump -u wsl_root -pmysql000 -h 172.24.240.1 -P 3306 customer customer_profile > /home/user/airflow_wsl/dags/dump_file.sql 2>/home/user/airflow_wsl/dags/dump_error.log"

dump_task = BashOperator(
    task_id='mysql_table_dump_task',
    bash_command=dump_command,
    dag=dag 
)

load_command = "mysql -u wsl_root -pmysql000 -h 172.24.240.1 -P 3306 client_rw < /home/user/airflow_wsl/dags/dump_file.sql" 

load_task = BashOperator(
    task_id='mysql_table_load_task',
    bash_command=load_command,
    dag=dag
)

dump_task >> load_task

