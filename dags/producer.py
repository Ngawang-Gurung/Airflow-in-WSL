'''
The producer and consumer DAG is used to verify how Airflow Dataset works.
Taskflow API is also explored
'''

from airflow.decorators import dag, task
from airflow import Dataset
from datetime import datetime

my_file = Dataset("/home/user/airflow/data/hello.txt")

@dag(start_date=datetime(2024, 3, 14), schedule='@daily', catchup=False)
def producer():

    @task(outlets = [my_file])
    def update_file():
        with open(my_file.uri, "a+") as f:
            f.write("Bye")
    
    update_file()

producer()
