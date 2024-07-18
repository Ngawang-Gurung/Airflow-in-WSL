'''
Producer and Consumer DAG is used to verify how Airflow Dataset works.
'''

from airflow.decorators import dag, task
from airflow import Dataset
from datetime import datetime

my_file = Dataset("/home/user/airflow/data/hello.txt")

@dag(start_date=datetime(2024, 3, 14), schedule=[my_file], catchup=False)
def consumer():

    @task
    def read_file():
        with open(my_file.uri, "r") as f:
            print(f.read())
    
    read_file()

consumer()