# $AIRFLOW_HOME/dags/store_pipeline.py
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="store_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,            # set a schedule later if you want daily runs
    catchup=False,
    max_active_runs=1,        # avoid overlapping runs of this table
    tags=["store", "bronze", "silver"],
) as dag:

    # 1) SOURCE -> BRONZE
    ingest_store = BashOperator(
        task_id="bronze_ingest_store",
        bash_command=(
            "set -euo pipefail; "
            "export PYSPARK_PYTHON=$(which python); "
            "export PYTHONPATH={{ var.value.PROJECT_DIR }}:${PYTHONPATH:-}; "
            "cd {{ var.value.PROJECT_DIR }}; "
            "rm -f libs.zip; zip -r -q libs.zip libs; "
            # decide the partition date (conf override -> ds)
            "INGESTION_DATE='{{ dag_run.conf.get(\"ingestion_date\", ds) }}'; "
            # sanity-check source exists
            "SRC='{{ var.value.PROJECT_DIR }}/source/Store.csv'; "
            "if [ ! -f \"$SRC\" ]; then echo 'Missing source file: ' \"$SRC\"; exit 2; fi; "
            "{{ var.value.SPARK_BIN }} --master local[*] "
            "--py-files libs.zip "
            "{{ var.value.JOBS_DIR }}/ingest_store.py "
            "--src $SRC "
            "--schema-json {{ var.value.PROJECT_DIR }}/configs/schemas/Store.json "
            "--dst {{ var.value.PROJECT_DIR }}/data/bronze/adventureworks "
            "--ingestion-date ${INGESTION_DATE} "
            "--coalesce 1 "
        ),
    )

    # 2) VALIDATE BRONZE (independent re-read)
    validate_bronze = BashOperator(
        task_id="validate_bronze_store",
        bash_command=(
            "set -euo pipefail; "
            "export PYSPARK_PYTHON=$(which python); "
            "export PYTHONPATH={{ var.value.PROJECT_DIR }}:${PYTHONPATH:-}; "
            "cd {{ var.value.PROJECT_DIR }}; "
            "rm -f libs.zip; zip -r -q libs.zip libs; "
            "INGESTION_DATE='{{ dag_run.conf.get(\"ingestion_date\", ds) }}'; "
            "{{ var.value.SPARK_BIN }} --master local[*] "
            "--py-files libs.zip "
            "{{ var.value.JOBS_DIR }}/dag_validation.py "
            "--root {{ var.value.PROJECT_DIR }}/data/bronze/adventureworks "
            "--table Store "
            "--schema-json {{ var.value.PROJECT_DIR }}/configs/schemas/Store.json "
            "--indestion-date ${INGESTION_DATE} "  # NOTE: typo fixed below
            "--strict-schema true "
        ).replace("--indestion-date", "--ingestion-date"),  # quick typo guard
    )

    # 3) BRONZE -> SILVER
    transform_store = BashOperator(
        task_id="silver_transform_store",
        bash_command=(
            "set -euo pipefail; "
            "export PYSPARK_PYTHON=$(which python); "
            "export PYTHONPATH={{ var.value.PROJECT_DIR }}:${PYTHONPATH:-}; "
            "cd {{ var.value.PROJECT_DIR }}; "
            "rm -f libs.zip; zip -r -q libs.zip libs; "
            "INGESTION_DATE='{{ dag_run.conf.get(\"ingestion_date\", ds) }}'; "
            # optional: assert bronze partition exists for the same date
            "BRONZE_DIR='{{ var.value.PROJECT_DIR }}/data/bronze/adventureworks/Store/ingestion_date='${INGESTION_DATE}; "
            "if [ ! -d \"$BRONZE_DIR\" ]; then "
            "  echo 'Bronze path not found:' \"$BRONZE_DIR\"; "
            "  echo 'Available partitions:'; "
            "  ls -1 {{ var.value.PROJECT_DIR }}/data/bronze/adventureworks/Store/ | sed -n 's/^ingestion_date=//p'; "
            "  exit 2; "
            "fi; "
            "{{ var.value.SPARK_BIN }} --master local[*] "
            "--py-files libs.zip "
            "{{ var.value.JOBS_DIR }}/transform_store_silver.py "
            "--bronze-root {{ var.value.PROJECT_DIR }}/data/bronze/adventureworks "
            "--silver-root {{ var.value.PROJECT_DIR }}/data/silver/adventureworks "
            "--schema-json {{ var.value.PROJECT_DIR }}/configs/schemas/Store_Silver.json "
            "--ingestion-date ${INGESTION_DATE} "
            "--coalesce 1 "
        ),
    )

    # 4) VALIDATE SILVER (independent re-read)
    validate_silver = BashOperator(
        task_id="validate_silver_store",
        bash_command=(
            "set -euo pipefail; "
            "export PYSPARK_PYTHON=$(which python); "
            "export PYTHONPATH={{ var.value.PROJECT_DIR }}:${PYTHONPATH:-}; "
            "cd {{ var.value.PROJECT_DIR }}; "
            "rm -f libs.zip; zip -r -q libs.zip libs; "
            "INGESTION_DATE='{{ dag_run.conf.get(\"ingestion_date\", ds) }}'; "
            "{{ var.value.SPARK_BIN }} --master local[*] "
            "--py-files libs.zip "
            "{{ var.value.JOBS_DIR }}/dag_validation.py "
            "--root {{ var.value.PROJECT_DIR }}/data/silver/adventureworks "
            "--table Store_Silver "
            "--schema-json {{ var.value.PROJECT_DIR }}/configs/schemas/Store_Silver.json "
            "--ingestion-date ${INGESTION_DATE} "
            "--strict-schema true "
        ),
    )

    # Orchestration
    ingest_store >> validate_bronze >> transform_store >> validate_silver
