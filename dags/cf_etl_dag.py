# This DAG is not working

# DAG object
from airflow import DAG

# Operators
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime, date

from pyspark.sql import SparkSession

def get_column_value(df, index, col):
    col_values = df.select(col).collect()
    value = col_values[index][col]
    return value


def update_date_on_config_table(schema_name, table_name, index):

    pymysql_connection = pymysql.connect(
        host='172.24.240.1',
        user='wsl_root',
        password='mysql000',
        database= schema_name
    )

    with pymysql_connection.cursor() as cursor:
        exec_date_query = f"update `{schema_name}`.{table_name} set execution_date = (current_timestamp) where id = {index+1}"
        cursor.execute(exec_date_query)

        update_startdate_query = f"update `{schema_name}`.{table_name}  set start_date_time = date_add(start_date_time, interval 1 day)"
        cursor.execute(update_startdate_query)

        update_enddate_query = f"update `{schema_name}`.{table_name}  set end_date_time = date_add(end_date_time, interval 1 day)"
        cursor.execute(update_enddate_query)
        
        pymysql_connection.commit()


def upload():

    spark = SparkSession.builder.appName("Incremental_Load").getOrCreate()
    
    def table_df(schema_name, table_name):           
        url = f"jdbc:mysql://172.24.240.1/{schema_name}"
        properties = {
            "user": "wsl_root",
            "password": "mysql000",
            "driver": "com.mysql.cj.jdbc.Driver"
        }
        df = spark.read.jdbc(url=url, table=table_name, properties=properties)
        return df

    df = table_df('migration', 'cf_etl_table_wsl')

    for i in range(df.count()):
        
        is_incremental = get_column_value(df, i, 'is_incremental')
        schema = get_column_value(df, i, 'schema_name')
        table = get_column_value(df, i, 'table_name')
        location = get_column_value(df, i, 'hdfs_upload_location')
        hdfs_file = get_column_value(df, i, 'hdfs_file_name')
        hdfs_path = f"{location}{hdfs_file}"

        if is_incremental:
            start_date = get_column_value(df, i, 'start_date_time')
            end_date = get_column_value(df, i, 'end_date_time')
            date_col = get_column_value(df, i, 'inc_field')      
 
            query = f"(SELECT * FROM {schema}.{table} WHERE {date_col} BETWEEN '{start_date}' AND '{end_date}') AS sql_query"
            result = table_df(schema, query)
            result.write.mode('append').parquet(hdfs_path)
            
            update_date_on_config_table('migration', 'cf_etl_table', i)

        elif not is_incremental:
            result = table_df(schema, table)
            result.write.mode("overwrite").parquet(hdfs_path)            

# Initializing default arguments for DAG
default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2024, 3, 14),
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=1)
}

# Instantiate a DAG
dag = DAG(
    dag_id='cf_etl_dag',
    default_args=default_args,
    description='This is config driven incremental loading DAG',
    schedule_interval='@once',
    catchup=False
)

upload_task = PythonOperator(
    task_id = 'upload_task',
    python_callable = upload,
    dag = dag
)




