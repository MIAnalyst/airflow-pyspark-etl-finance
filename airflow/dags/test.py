from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

def greeting(name,age):
    print(f"Hello, my name is {name} and I'm {age} old")

with DAG(
    dag_id="testing_dag",
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["test"]
) as dag:
    task_1= BashOperator(
        task_id="task_1",
        bash_command="echo This is my first task"
    )

    task_2= BashOperator(
        task_id="task_2",
        bash_command="echo This is my second task"
    )

    task3=PythonOperator(
        task_id="task_3",
        python_callable=greeting,
        op_kwargs={'name':'Ehab','age':39}

    )


task_1 >> task_2
task3