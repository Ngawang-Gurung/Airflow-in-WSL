# DAG object
from airflow import DAG

# Operators
from airflow.operators.python_operator import PythonOperator

from datetime import timedelta, datetime, date

from pyspark.sql import SparkSession
from pyspark.sql import Row

def func():
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([
    Row(a = 1, b = 2., c='string1', d = date(2000, 1, 1)),
    Row(a = 2, b = 3., c='string2', d = date(2000, 2, 1)),])
    return df.show()

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2024, 3, 14)
}

dag = DAG(
    dag_id='spark_dag',
    default_args=default_args,
    description='This is spark DAG',
    schedule_interval='@once',
    catchup=False
)

spark_task = PythonOperator(
    task_id = 'spark_task',
    python_callable = func,
    dag = dag
)




