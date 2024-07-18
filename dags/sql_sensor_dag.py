from airflow import DAG
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='sql_sensor_dag', 
    start_date=datetime(2024, 3, 14),
    schedule_interval='@once',
    catchup=False
) as dag:
    wait_for_data = SqlSensor(
        task_id = 'wait_for_data',
        conn_id='MySQLID',
        sql='SELECT COUNT(*) FROM employee',
        mode = 'poke',
        timeout = 200,
        poke_interval = 30
    )

    end_task = EmptyOperator(
        task_id= 'end_task'
    )

    wait_for_data >> end_task
