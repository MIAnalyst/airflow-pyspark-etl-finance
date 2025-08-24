from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    "dim_date_build",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dim", "date"],
) as dag:

    build_dim_date = BashOperator(
        task_id="build_dim_date",
        bash_command=(
            "set -euo pipefail; "
            "export PYSPARK_PYTHON=$(which python); "
            "export PYTHONPATH={{ var.value.PROJECT_DIR }}:${PYTHONPATH:-}; "
            "cd {{ var.value.PROJECT_DIR }}; "
            "test -f libs.zip || zip -r -q libs.zip libs; "
            "{{ var.value.SPARK_BIN }} --master local[*] "
            "--py-files libs.zip "
            "{{ var.value.JOBS_DIR }}/build_dim_date.py "
            "--config {{ var.value.PROJECT_DIR }}/configs/local.yaml "
            "--start 2011-01-01 --end 2020-12-31"
        ),
    )

    dq_dim_date = BashOperator(
        task_id="dq_dim_date",
        bash_command=(
            "set -euo pipefail; "
            "export PYSPARK_PYTHON=$(which python); "
            "export PYTHONPATH={{ var.value.PROJECT_DIR }}:${PYTHONPATH:-}; "
            "cd {{ var.value.PROJECT_DIR }}; "
            "test -f libs.zip || zip -r -q libs.zip libs; "
            "{{ var.value.SPARK_BIN }} --master local[*] "
            "--py-files libs.zip "
            "{{ var.value.JOBS_DIR }}/check_dim_date_dq.py "
            "--config {{ var.value.PROJECT_DIR }}/configs/local.yaml "
            "--start 2011-01-01 --end 2020-12-31"
        ),
    )

    build_dim_date >> dq_dim_date
