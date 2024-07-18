'''
This DAG is used to verify if MySQL and PySpark connection works.

mysql-connector-j-8.4.0.jar is placed inside pyspark package's jar directory. 
'''


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime, date

from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

load_dotenv()

DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

def mysql_spark_connection():
    spark = SparkSession.builder.appName("spark_dataframe").getOrCreate()
    url = f"jdbc:mysql://{DB_HOST}:{DB_PORT}/customer"
    properties = {
        "user": DB_USERNAME,
        "password": DB_PASSWORD,
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    df = spark.read.jdbc(url=url, table='customer_profile', properties=properties)
    return df.show()

# Initializing default arguments for DAG
default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2024, 3, 14)
}

# Instantiate a DAG
dag = DAG(
    dag_id='mysql_spark_dag',
    default_args=default_args,
    description='This is mysql spark DAG',
    schedule_interval='@once',
    catchup=False
)

mysql_spark_conn_task = PythonOperator(
    task_id = 'mysql_spark_conn_task',
    python_callable = mysql_spark_connection,
    dag = dag
)




