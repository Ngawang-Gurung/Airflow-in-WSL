# This DAG works in both Windows WSL and Linux

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from hdfs import InsecureClient

def upload_to_hdfs():
    # # Establish a connection to HDFS
    # hdfs_client = InsecureClient('http://localhost:9870', user='Ngawang')
    
    # Establish a connection to HDFS from WSL
    hdfs_client = InsecureClient('http://172.24.240.1:9870', user='Ngawang')
    
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
    'start_date': datetime(2024, 3, 14),
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=1)
}

hdfs_dag = DAG(
    dag_id='hdfs_dag',
    default_args=default_args,
    description='This is my HDFS DAG',
    schedule_interval='@once',
    catchup=False
)

upload_task = PythonOperator(
    task_id='upload_task',
    python_callable=upload_to_hdfs,
    dag=hdfs_dag
)

upload_task

# hdfs_command = "hdfs dfs put /opt/airflow/dags/myfile.csv /mydir"

# bash_upload = BashOperator(
#     task_id = 'bash_upload',
#     bash_command= hdfs_command,
#     dag = hdfs_dag
# )

# bash_upload


