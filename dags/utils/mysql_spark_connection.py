import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

spark = SparkSession.builder.appName("mysql_spark_connector").getOrCreate()

def table_df(schema_name, table_name):
    url = f"jdbc:mysql://{DB_HOST}:{DB_PORT}/{schema_name}"
    properties = {
        "user": DB_USERNAME,
        "password": DB_PASSWORD,
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    df = spark.read.jdbc(url=url, table=table_name, properties=properties)
    return df

def df_table(df, schema_name, table_name):
    url = f"jdbc:mysql://{DB_HOST}:{DB_PORT}/{schema_name}"
    properties = {
        "user": DB_USERNAME,
        "password": DB_PASSWORD,
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    df.write.jdbc(url=url, table=table_name, mode='overwrite', properties=properties)
