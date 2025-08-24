# $AIRFLOW_HOME/dags/person_pipeline.py
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="person_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,               
    catchup=False,
    max_active_runs=1,
    tags=["person", "bronze", "silver"],
) as dag:

    # 1) SOURCE -> BRONZE
    ingest_person = BashOperator(
        task_id="bronze_ingest_person",
        bash_command=(
            "set -euo pipefail; "
            "export PYSPARK_PYTHON=$(which python); "
            "export PYTHONPATH={{ var.value.PROJECT_DIR }}:${PYTHONPATH:-}; "
            "cd {{ var.value.PROJECT_DIR }}; "
            "rm -f libs.zip; zip -r -q libs.zip libs; "
            # decide the partition date (conf override -> ds)
            "INGESTION_DATE='{{ dag_run.conf.get(\"ingestion_date\", ds) }}'; "
            # sanity-check source exists
            "SRC='{{ var.value.PROJECT_DIR }}/source/Person.csv'; "
            "if [ ! -f \"$SRC\" ]; then echo 'Missing source file:' \"$SRC\"; exit 2; fi; "
            "{{ var.value.SPARK_BIN }} --master local[*] "
            "--py-files libs.zip "
            "{{ var.value.JOBS_DIR }}/ingest_person.py "
            "--src $SRC "
            "--schema-json {{ var.value.PROJECT_DIR }}/configs/schemas/Person.json "
            "--dst {{ var.value.PROJECT_DIR }}/data/bronze/adventureworks "
            "--ingestion-date ${INGESTION_DATE} "
            "--coalesce 1 "
        ),
    )

    # 2) VALIDATE BRONZE (independent re-read)
    validate_bronze = BashOperator(
        task_id="validate_bronze_person",
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
            "--table Person "
            "--schema-json {{ var.value.PROJECT_DIR }}/configs/schemas/Person.json "
            "--ingestion-date ${INGESTION_DATE} "
        ),
    )

    # 3) BRONZE -> SILVER
    transform_person = BashOperator(
        task_id="silver_transform_person",
        bash_command=(
            "set -euo pipefail; "
            "export PYSPARK_PYTHON=$(which python); "
            "export PYTHONPATH={{ var.value.PROJECT_DIR }}:${PYTHONPATH:-}; "
            "cd {{ var.value.PROJECT_DIR }}; "
            "rm -f libs.zip; zip -r -q libs.zip libs; "
            "INGESTION_DATE='{{ dag_run.conf.get(\"ingestion_date\", ds) }}'; "
            # optional: assert bronze partition exists for the same date
            "BRONZE_DIR='{{ var.value.PROJECT_DIR }}/data/bronze/adventureworks/Person/ingestion_date='${INGESTION_DATE}; "
            "if [ ! -d \"$BRONZE_DIR\" ]; then "
            "  echo 'Bronze path not found:' \"$BRONZE_DIR\"; "
            "  echo 'Available partitions:'; "
            "  ls -1 {{ var.value.PROJECT_DIR }}/data/bronze/adventureworks/Person/ | sed -n 's/^ingestion_date=//p'; "
            "  exit 2; "
            "fi; "
            "{{ var.value.SPARK_BIN }} --master local[*] "
            "--py-files libs.zip "
            "{{ var.value.JOBS_DIR }}/transform_person_silver.py "
            "--bronze-root {{ var.value.PROJECT_DIR }}/data/bronze/adventureworks "
            "--silver-root {{ var.value.PROJECT_DIR }}/data/silver/adventureworks "
            "--schema-json {{ var.value.PROJECT_DIR }}/configs/schemas/Person_Silver.json "
            "--ingestion-date ${INGESTION_DATE} "
            "--coalesce 1 "
        ),
    )

    # 4) VALIDATE SILVER (independent re-read)
    validate_silver = BashOperator(
        task_id="validate_silver_person",
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
            "--table Person_Silver "
            "--schema-json {{ var.value.PROJECT_DIR }}/configs/schemas/Person_Silver.json "
            "--ingestion-date ${INGESTION_DATE} "
        ),
    )

    # Orchestration
    ingest_person >> validate_bronze >> transform_person >> validate_silver
