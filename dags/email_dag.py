'''
This DAG is used to schedule email weekly at 1:15 AM UTC i.e. 7:00 AM (NPT).

Note:- This DAG requires [SMTP] to be set on airflow.cfg.
'''

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 14),
    'email': ['ngawanggurung@outlook.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def start_task():
    print("task started")

email_dag = DAG(
    dag_id='emaildag',
    description='Use case of email operator in Airflow',
    default_args=default_args,
    # schedule_interval='@once',
    schedule_interval='15 1 * * *',
    dagrun_timeout=timedelta(minutes=10),
    catchup=False)

start_task = PythonOperator(
    task_id='executetask',
    python_callable=start_task,
    dag=email_dag
)

message = """
<h2>Good Morning, Ngawang Gurung</h2>
"""

send_email = EmailOperator(
    task_id='send_email',
    to='tseringnc707@gmail.com',
    # to = ['tseringnc707@gmail.com','susma.pant@extensodata.com','neupanebishal039@gmail.com', 'anuragkarkikarki79@gmail.com', 'bisheshkafle18@gmail.com', 'kalyanad100@gmail.com'],
    subject='Airflow Message',
    html_content=message,
    files= ['/home/user/airflow/data/hello.txt'],
    dag=email_dag
)

start_task >> send_email
