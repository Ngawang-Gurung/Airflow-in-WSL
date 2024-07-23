# DAG object
from airflow import DAG
# Operators
from airflow.operators.python import PythonOperator
from datetime import datetime

from pyspark.sql import SparkSession
import pymysql
import logging

import os
from dotenv import load_dotenv

load_dotenv()

DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

def update_date_on_config_table(schema_name, table_name, index, interval_period):
    '''
    Update dates on config table using PyMySQL 
    '''
    pymysql_connection = pymysql.connect(
        host= DB_HOST,
        user= DB_USERNAME,
        password=DB_PASSWORD,
        database= schema_name
    )

    with pymysql_connection.cursor() as cursor:
        exec_date_query = f"UPDATE `{schema_name}`.{table_name} SET execution_date = CURRENT_TIMESTAMP WHERE table_id = {index + 1}"
        cursor.execute(exec_date_query)

        update_startdate_query = f"UPDATE `{schema_name}`.{table_name} SET start_date_time = DATE_ADD(start_date_time, INTERVAL {interval_period} DAY)"
        cursor.execute(update_startdate_query)

        update_enddate_query = f"UPDATE `{schema_name}`.{table_name} SET end_date_time = DATE_ADD(end_date_time, INTERVAL {interval_period} DAY)"
        cursor.execute(update_enddate_query)

        pymysql_connection.commit()


def upload():
    spark = SparkSession.builder.appName("Incremental_Load").getOrCreate()

    def table_df(schema_name, table_name):
        url = f"jdbc:mysql://{DB_HOST}:{DB_PORT}/{schema_name}"
        properties = {
            "user": DB_USERNAME,
            "password": DB_PASSWORD,
            "driver": "com.mysql.cj.jdbc.Driver"
        }
        df = spark.read.jdbc(url=url, table=table_name, properties=properties)
        return df

    def field_mapped_df(cf_db, schema_name, table_name, table_id):
        con = pymysql.connect(
            host= DB_HOST,
            user= DB_USERNAME,
            password=DB_PASSWORD,
            database=cf_db
        )

        with con.cursor() as cursor:
            cursor.callproc(f'{cf_db}.sp_field_mapping', [schema_name, table_name, table_id])
            result = cursor.fetchall()
            fields = [desc[0] for desc in cursor.description]
            df = spark.createDataFrame(result, fields)
            con.commit()

        return df

    df = table_df('config_db', 'cf_etl_table')

    for i, row in zip(range(df.count()), df.collect()):
        is_incremental, table_id, schema, table, location, hdfs_file = row['is_incremental'], row['table_id'], row['schema_name'], row['table_name'], row['hdfs_upload_location'], row['hdfs_file_name']
        hdfs_path = f"{location}{hdfs_file}"

        field_mapped_table = field_mapped_df('config_db', schema, table, table_id)

        if is_incremental:
            start_date, end_date, date_col, interval_period, partition_by = row['start_date_time'], row['end_date_time'], row['inc_field'], row['interval_period'], row['partition_by']

            field_mapped_table.createOrReplaceTempView("incremental_table")
            result = spark.sql(f"SELECT * FROM incremental_table WHERE {date_col} BETWEEN '{start_date}' AND '{end_date}'")
            result.write.mode('append').parquet(hdfs_path, partitionBy=partition_by)

            update_date_on_config_table('config_db', 'cf_etl_table', i, interval_period)
            logging.info("Successful Upload")

        else:
            field_mapped_table.write.mode("overwrite").parquet(hdfs_path)


# Initializing default arguments for DAG
default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2024, 3, 14),
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=1)
}

# Instantiate a DAG
dag = DAG(
    dag_id='config_etl_dag',
    default_args=default_args,
    description='This is config driven incremental loading DAG',
    schedule_interval='@once',
    catchup=False
)

upload_task = PythonOperator(
    task_id='upload_task',
    python_callable=upload,
    dag=dag
)

upload_task
