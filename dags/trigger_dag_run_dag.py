'''
This DAG is used to verify how TriggerDagRunOperator works.
'''

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2024, 3, 14),
    'owner': 'Airflow'
} 

dag = DAG(
    dag_id= 'trigger_dag_run_dag',
    schedule_interval='@once',
    default_args=default_args
)

trigger_task = TriggerDagRunOperator(
    task_id = 'trigger_task',
    trigger_dag_id='hello_world_dag',
    execution_date= '{{ ds }}',
    reset_dag_run=True,
    wait_for_completion=True,
    poke_interval=30,
    dag = dag
)

end_task = EmptyOperator(
    task_id='end_task',
    dag=dag
)

trigger_task >> end_task