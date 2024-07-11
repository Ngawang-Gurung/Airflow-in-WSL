from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.mysql_operator import MySqlOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 14)
}

dag = DAG(
    'mysql_operator_dag',
    default_args=default_args,
    description='A DAG to run MySQL operations',
    schedule_interval='@once',
    catchup=False,
)

mysql_query = """
    CREATE TABLE employee(
        name varchar(255)
    );
"""

mysql_task = MySqlOperator(
    task_id='mysql_task',
    mysql_conn_id='MySQLID', # Connection ID created in Airflow Connnections
    sql=mysql_query,
    database = 'customer',
    dag=dag,
)

mysql_task