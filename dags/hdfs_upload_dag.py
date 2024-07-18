'''
This DAG is used to upload a file in HDFS using InsecureClient.

This DAG works in both Windows WSL and Linux
'''

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from hdfs import InsecureClient

def upload_to_hdfs():

    # host_name = "172.24.240.1"
    host_name = "localhost"
    
    # Establish a connection to HDFS 
    hdfs_client = InsecureClient(f'http://{host_name}:9870', user='Ngawang')
    
    # Define local and HDFS file paths
    local_file_path = "dags/email_dag.py"
    hdfs_upload_path = '/mydir'

    # Upload file from local to HDFS
    try:
        hdfs_client.upload(hdfs_upload_path, local_file_path)
        print("File uploaded to HDFS successfully!")
    except Exception as e:
        print(f"Error uploading file: {e}")

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2024, 3, 14)
}

hdfs_upload_dag = DAG(
    dag_id='hdfs_upload_dag',
    default_args=default_args,
    description='This is my HDFS DAG',
    schedule_interval='@once',
    catchup=False
)

upload_task = PythonOperator(
    task_id='upload_task',
    python_callable=upload_to_hdfs,
    dag=hdfs_upload_dag
)

upload_task


