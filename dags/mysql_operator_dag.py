'''
This DAG explores how MySQLOperator works. 
'''

from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime, timedelta

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
    CREATE TABLE IF NOT EXISTS new_table(
        name varchar(255)
    );
"""

mysql_task = MySqlOperator(
    task_id='mysql_task',
    sql=mysql_query,
    mysql_conn_id='MySQLID', # Connection ID created in Airflow Connnections
    database = 'mydb',
    dag=dag,
)

mysql_task