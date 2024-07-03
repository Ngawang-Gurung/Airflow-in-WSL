# This DAG is not working in Windows WSL but works in Linux

# DAG object
from airflow import DAG
# Operators
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime, date

from pyspark.sql import SparkSession
from pyspark.sql import Row

import pymysql
import logging

# def func():
#     spark = SparkSession.builder.appName("Incremental_Load").config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000").getOrCreate()

#     df = spark.createDataFrame([
#     Row(a = 1, b = 2., c='string1', d = date(2000, 1, 1)),
#     Row(a = 2, b = 3., c='string2', d = date(2000, 2, 1)),])
#     df.write.mode('append').parquet("hdfs://localhost:9000/mydir/")
#     logging.info("Successful Upload")

def func():
    spark = SparkSession.builder.appName("Incremental_Load").getOrCreate()

    df = spark.createDataFrame([
    Row(a = 1, b = 2., c='string1', d = date(2000, 1, 1)),
    Row(a = 2, b = 3., c='string2', d = date(2000, 2, 1)),])
    df.write.mode('overwrite').parquet("hdfs://172.24.240.1:19000/test/")
    logging.info("Successful Upload")

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2024, 3, 14),
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=1)
}

# Instantiate a DAG
dag = DAG(
    dag_id='hdfs_pyspark_dag',
    default_args=default_args,
    description='This is config driven incremental loading DAG',
    schedule_interval='@once',
    catchup=False
)

upload_task = PythonOperator(
    task_id='upload_task',
    python_callable=func,
    dag=dag
)

upload_task
