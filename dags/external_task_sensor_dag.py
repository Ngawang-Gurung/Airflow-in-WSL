'''
This DAG is used to verify how Airflow ExternalTaskSensor works.
'''

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2024, 3, 14)
}

external_task_sensor_dag = DAG(
    dag_id = 'external_task_sensor_dag',
    description = 'This DAG waits for completion of task in other dag or other dag itself.',
    default_args = default_args,
    schedule_interval= '@once',
    catchup= False
)

sensor = ExternalTaskSensor(
    task_id = 'sensor',
    external_dag_id= 'hello_world_dag',
    external_task_id= 'hellow_world_task',
    # execution_delta = timedelta(minutes = 10)
    dag = external_task_sensor_dag
)

dependent_task = EmptyOperator(
    task_id = 'dependent_task',
    dag = external_task_sensor_dag
)

sensor >> dependent_task

