# DAG object
from airflow import DAG

# Operators
from airflow.operators.python_operator import PythonOperator

from datetime import timedelta, datetime, date

from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("spark_dataframe").config("spark.jars", "/home/user/airflow_wsl/venv/lib/python3.10/site-packages/pyspark/jars/mysql-connector-j-8.4.0.jar").getOrCreate()
    url = "jdbc:mysql://172.24.240.1:3306/customer"
    properties = {
        "user": "wsl_root",
        "password": "mysql000",
        "driver": "com.mysql.jdbc.Driver"
    }
    df = spark.read.jdbc(url=url, table='customer_profile', properties=properties)
    return df.show()

# Initializing default arguments for DAG
default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2024, 3, 14),
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=1)
}

# Instantiate a DAG
dag = DAG(
    dag_id='mysql_spark_dag',
    default_args=default_args,
    description='This is mysql spark DAG',
    schedule_interval='@once',
    catchup=False
)

mysql_spark_task = PythonOperator(
    task_id = 'mysql_spark_task',
    python_callable = main,
    dag = dag
)




