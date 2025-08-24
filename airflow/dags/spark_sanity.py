from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK = "/opt/homebrew/bin/spark-submit"  # 

with DAG("spark_sanity", start_date=datetime(2025,1,1), schedule=None, catchup=False) as dag:
    BashOperator(
        task_id="spark_version",
        bash_command=f"{SPARK} --version"
    )
