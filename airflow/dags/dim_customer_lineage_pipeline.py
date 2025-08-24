# $AIRFLOW_HOME/dags/dim_customer_lineage_pipeline.py
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="dim_customer_lineage_pipeline",
    description="Build DimCustomer Dimension",
    start_date=datetime(2025, 1, 1),
    schedule=None,               # run manually or add a cron later
    catchup=False,
    max_active_runs=1,
    tags=["lineage", "bronze", "silver", "gold", "dim_customer"],
) as dag:

    # Reusable shell prelude
    common_env = (
        "set -euo pipefail; "
        "export PYSPARK_PYTHON=$(which python); "
        "export PYTHONPATH={{ var.value.PROJECT_DIR }}:${PYTHONPATH:-}; "
        "cd {{ var.value.PROJECT_DIR }}; "
        "rm -f libs.zip; zip -r -q libs.zip libs; "
        "INGESTION_DATE='{{ dag_run.conf.get(\"ingestion_date\", ds) }}'; "
    )

    # ---------------------------
    # Person pipeline
    # ---------------------------
    with TaskGroup("person") as person_grp:
        ingest_person = BashOperator(
            task_id="bronze_ingest_person",
            bash_command=(
                common_env +
                "SRC='{{ var.value.PROJECT_DIR }}/source/Person.csv'; "
                "if [ ! -f \"$SRC\" ]; then echo 'Missing source file:' \"$SRC\"; exit 2; fi; "
                "{{ var.value.SPARK_BIN }} --master local[*] "
                "--py-files libs.zip {{ var.value.JOBS_DIR }}/ingest_person.py "
                "--src $SRC "
                "--schema-json {{ var.value.PROJECT_DIR }}/configs/schemas/Person.json "
                "--dst {{ var.value.PROJECT_DIR }}/data/bronze/adventureworks "
                "--ingestion-date ${INGESTION_DATE} --coalesce 1 "
            ),
            do_xcom_push=False,
        )

        validate_bronze_person = BashOperator(
            task_id="validate_bronze_person",
            bash_command=(
                common_env +
                "{{ var.value.SPARK_BIN }} --master local[*] --py-files libs.zip "
                "{{ var.value.JOBS_DIR }}/dag_validation.py "
                "--root {{ var.value.PROJECT_DIR }}/data/bronze/adventureworks "
                "--table Person "
                "--schema-json {{ var.value.PROJECT_DIR }}/configs/schemas/Person.json "
                "--ingestion-date ${INGESTION_DATE} "
            ),
            do_xcom_push=False,
        )

        transform_person = BashOperator(
            task_id="silver_transform_person",
            bash_command=(
                common_env +
                "BRONZE_DIR='{{ var.value.PROJECT_DIR }}/data/bronze/adventureworks/Person/ingestion_date='${INGESTION_DATE}; "
                "if [ ! -d \"$BRONZE_DIR\" ]; then "
                "  echo 'Bronze path not found:' \"$BRONZE_DIR\"; "
                "  ls -1 {{ var.value.PROJECT_DIR }}/data/bronze/adventureworks/Person/ | sed -n 's/^ingestion_date=//p'; "
                "  exit 2; fi; "
                "{{ var.value.SPARK_BIN }} --master local[*] --py-files libs.zip "
                "{{ var.value.JOBS_DIR }}/transform_person_silver.py "
                "--bronze-root {{ var.value.PROJECT_DIR }}/data/bronze/adventureworks "
                "--silver-root {{ var.value.PROJECT_DIR }}/data/silver/adventureworks "
                "--schema-json {{ var.value.PROJECT_DIR }}/configs/schemas/Person_Silver.json "
                "--ingestion-date ${INGESTION_DATE} --coalesce 1 "
            ),
            do_xcom_push=False,
        )

        validate_silver_person = BashOperator(
            task_id="validate_silver_person",
            bash_command=(
                common_env +
                "{{ var.value.SPARK_BIN }} --master local[*] --py-files libs.zip "
                "{{ var.value.JOBS_DIR }}/dag_validation.py "
                "--root {{ var.value.PROJECT_DIR }}/data/silver/adventureworks "
                "--table Person_Silver "
                "--schema-json {{ var.value.PROJECT_DIR }}/configs/schemas/Person_Silver.json "
                "--ingestion-date ${INGESTION_DATE} "
            ),
            do_xcom_push=False,
        )

        ingest_person >> validate_bronze_person >> transform_person >> validate_silver_person

    # ---------------------------
    # Customer pipeline
    # ---------------------------
    with TaskGroup("customer") as customer_grp:
        ingest_customer = BashOperator(
            task_id="bronze_ingest_customer",
            bash_command=(
                common_env +
                "SRC='{{ var.value.PROJECT_DIR }}/source/Customer.csv'; "
                "if [ ! -f \"$SRC\" ]; then echo 'Missing source file:' \"$SRC\"; exit 2; fi; "
                "{{ var.value.SPARK_BIN }} --master local[*] --py-files libs.zip "
                "{{ var.value.JOBS_DIR }}/ingest_customer.py "
                "--src $SRC "
                "--schema-json {{ var.value.PROJECT_DIR }}/configs/schemas/Customer.json "
                "--dst {{ var.value.PROJECT_DIR }}/data/bronze/adventureworks "
                "--ingestion-date ${INGESTION_DATE} --coalesce 1 "
            ),
            do_xcom_push=False,
        )

        validate_bronze_customer = BashOperator(
            task_id="validate_bronze_customer",
            bash_command=(
                common_env +
                "{{ var.value.SPARK_BIN }} --master local[*] --py-files libs.zip "
                "{{ var.value.JOBS_DIR }}/dag_validation.py "
                "--root {{ var.value.PROJECT_DIR }}/data/bronze/adventureworks "
                "--table Customer "
                "--schema-json {{ var.value.PROJECT_DIR }}/configs/schemas/Customer.json "
                "--ingestion-date ${INGESTION_DATE} "
            ),
            do_xcom_push=False,
        )

        transform_customer = BashOperator(
            task_id="silver_transform_customer",
            bash_command=(
                common_env +
                "BRONZE_DIR='{{ var.value.PROJECT_DIR }}/data/bronze/adventureworks/Customer/ingestion_date='${INGESTION_DATE}; "
                "if [ ! -d \"$BRONZE_DIR\" ]; then "
                "  echo 'Bronze path not found:' \"$BRONZE_DIR\"; "
                "  ls -1 {{ var.value.PROJECT_DIR }}/data/bronze/adventureworks/Customer/ | sed -n 's/^ingestion_date=//p'; "
                "  exit 2; fi; "
                "{{ var.value.SPARK_BIN }} --master local[*] --py-files libs.zip "
                "{{ var.value.JOBS_DIR }}/transform_customer_silver.py "
                "--bronze-root {{ var.value.PROJECT_DIR }}/data/bronze/adventureworks "
                "--silver-root {{ var.value.PROJECT_DIR }}/data/silver/adventureworks "
                "--schema-json {{ var.value.PROJECT_DIR }}/configs/schemas/Customer_Silver.json "
                "--ingestion-date ${INGESTION_DATE} --coalesce 1 "
            ),
            do_xcom_push=False,
        )

        validate_silver_customer = BashOperator(
            task_id="validate_silver_customer",
            bash_command=(
                common_env +
                "{{ var.value.SPARK_BIN }} --master local[*] --py-files libs.zip "
                "{{ var.value.JOBS_DIR }}/dag_validation.py "
                "--root {{ var.value.PROJECT_DIR }}/data/silver/adventureworks "
                "--table Customer_Silver "
                "--schema-json {{ var.value.PROJECT_DIR }}/configs/schemas/Customer_Silver.json "
                "--ingestion-date ${INGESTION_DATE} "
            ),
            do_xcom_push=False,
        )

        ingest_customer >> validate_bronze_customer >> transform_customer >> validate_silver_customer

    # ---------------------------
    # SalesTerritory pipeline
    # ---------------------------
    with TaskGroup("sales_territory") as st_grp:
        ingest_sales_territory = BashOperator(
            task_id="bronze_ingest_sales_territory",
            bash_command=(
                common_env +
                "SRC='{{ var.value.PROJECT_DIR }}/source/SalesTerritory.csv'; "
                "if [ ! -f \"$SRC\" ]; then echo 'Missing source file:' \"$SRC\"; exit 2; fi; "
                "{{ var.value.SPARK_BIN }} --master local[*] --py-files libs.zip "
                "{{ var.value.JOBS_DIR }}/ingest_sales_territory.py "
                "--src $SRC "
                "--schema-json {{ var.value.PROJECT_DIR }}/configs/schemas/SalesTerritory.json "
                "--dst {{ var.value.PROJECT_DIR }}/data/bronze/adventureworks "
                "--ingestion-date ${INGESTION_DATE} --coalesce 1 "
            ),
            do_xcom_push=False,
        )

        validate_bronze_st = BashOperator(
            task_id="validate_bronze_sales_territory",
            bash_command=(
                common_env +
                "{{ var.value.SPARK_BIN }} --master local[*] --py-files libs.zip "
                "{{ var.value.JOBS_DIR }}/dag_validation.py "
                "--root {{ var.value.PROJECT_DIR }}/data/bronze/adventureworks "
                "--table SalesTerritory "
                "--schema-json {{ var.value.PROJECT_DIR }}/configs/schemas/SalesTerritory.json "
                "--ingestion-date ${INGESTION_DATE} "
            ),
            do_xcom_push=False,
        )

        transform_sales_territory = BashOperator(
            task_id="silver_transform_sales_territory",
            bash_command=(
                common_env +
                "BRONZE_DIR='{{ var.value.PROJECT_DIR }}/data/bronze/adventureworks/SalesTerritory/ingestion_date='${INGESTION_DATE}; "
                "if [ ! -d \"$BRONZE_DIR\" ]; then "
                "  echo 'Bronze path not found:' \"$BRONZE_DIR\"; "
                "  ls -1 {{ var.value.PROJECT_DIR }}/data/bronze/adventureworks/SalesTerritory/ | sed -n 's/^ingestion_date=//p'; "
                "  exit 2; fi; "
                "{{ var.value.SPARK_BIN }} --master local[*] --py-files libs.zip "
                "{{ var.value.JOBS_DIR }}/transform_sales_territory_silver.py "
                "--bronze-root {{ var.value.PROJECT_DIR }}/data/bronze/adventureworks "
                "--silver-root {{ var.value.PROJECT_DIR }}/data/silver/adventureworks "
                "--schema-json {{ var.value.PROJECT_DIR }}/configs/schemas/SalesTerritory_Silver.json "
                "--ingestion-date ${INGESTION_DATE} --coalesce 1 "
            ),
            do_xcom_push=False,
        )

        validate_silver_st = BashOperator(
            task_id="validate_silver_sales_territory",
            bash_command=(
                common_env +
                "{{ var.value.SPARK_BIN }} --master local[*] --py-files libs.zip "
                "{{ var.value.JOBS_DIR }}/dag_validation.py "
                "--root {{ var.value.PROJECT_DIR }}/data/silver/adventureworks "
                "--table SalesTerritory_Silver "
                "--schema-json {{ var.value.PROJECT_DIR }}/configs/schemas/SalesTerritory_Silver.json "
                "--ingestion-date ${INGESTION_DATE} "
            ),
            do_xcom_push=False,
        )

        ingest_sales_territory >> validate_bronze_st >> transform_sales_territory >> validate_silver_st

    # ---------------------------
    # Store pipeline
    # ---------------------------
    with TaskGroup("store") as store_grp:
        ingest_store = BashOperator(
            task_id="bronze_ingest_store",
            bash_command=(
                common_env +
                "SRC='{{ var.value.PROJECT_DIR }}/source/Store.csv'; "
                "if [ ! -f \"$SRC\" ]; then echo 'Missing source file:' \"$SRC\"; exit 2; fi; "
                "{{ var.value.SPARK_BIN }} --master local[*] --py-files libs.zip "
                "{{ var.value.JOBS_DIR }}/ingest_store.py "
                "--src $SRC "
                "--schema-json {{ var.value.PROJECT_DIR }}/configs/schemas/Store.json "
                "--dst {{ var.value.PROJECT_DIR }}/data/bronze/adventureworks "
                "--ingestion-date ${INGESTION_DATE} --coalesce 1 "
            ),
            do_xcom_push=False,
        )

        validate_bronze_store = BashOperator(
            task_id="validate_bronze_store",
            bash_command=(
                common_env +
                "{{ var.value.SPARK_BIN }} --master local[*] --py-files libs.zip "
                "{{ var.value.JOBS_DIR }}/dag_validation.py "
                "--root {{ var.value.PROJECT_DIR }}/data/bronze/adventureworks "
                "--table Store "
                "--schema-json {{ var.value.PROJECT_DIR }}/configs/schemas/Store.json "
                "--ingestion-date ${INGESTION_DATE} "
            ),
            do_xcom_push=False,
        )

        transform_store = BashOperator(
            task_id="silver_transform_store",
            bash_command=(
                common_env +
                "BRONZE_DIR='{{ var.value.PROJECT_DIR }}/data/bronze/adventureworks/Store/ingestion_date='${INGESTION_DATE}; "
                "if [ ! -d \"$BRONZE_DIR\" ]; then "
                "  echo 'Bronze path not found:' \"$BRONZE_DIR\"; "
                "  ls -1 {{ var.value.PROJECT_DIR }}/data/bronze/adventureworks/Store/ | sed -n 's/^ingestion_date=//p'; "
                "  exit 2; fi; "
                "{{ var.value.SPARK_BIN }} --master local[*] --py-files libs.zip "
                "{{ var.value.JOBS_DIR }}/transform_store_silver.py "
                "--bronze-root {{ var.value.PROJECT_DIR }}/data/bronze/adventureworks "
                "--silver-root {{ var.value.PROJECT_DIR }}/data/silver/adventureworks "
                "--schema-json {{ var.value.PROJECT_DIR }}/configs/schemas/Store_Silver.json "
                "--ingestion-date ${INGESTION_DATE} --coalesce 1 "
            ),
            do_xcom_push=False,
        )

        validate_silver_store = BashOperator(
            task_id="validate_silver_store",
            bash_command=(
                common_env +
                "{{ var.value.SPARK_BIN }} --master local[*] --py-files libs.zip "
                "{{ var.value.JOBS_DIR }}/dag_validation.py "
                "--root {{ var.value.PROJECT_DIR }}/data/silver/adventureworks "
                "--table Store_Silver "
                "--schema-json {{ var.value.PROJECT_DIR }}/configs/schemas/Store_Silver.json "
                "--ingestion-date ${INGESTION_DATE} "
            ),
        )

        ingest_store >> validate_bronze_store >> transform_store >> validate_silver_store

    # ---------------------------
    # Gold: DimCustomer (depends on all four silver validations)
    # ---------------------------
    build_dim_customer = BashOperator(
        task_id="gold_build_dim_customer",
        bash_command=(
            common_env +
            "{{ var.value.SPARK_BIN }} --master local[*] --py-files libs.zip "
            "{{ var.value.JOBS_DIR }}/build_dim_customer.py "
            "--silver-root {{ var.value.PROJECT_DIR }}/data/silver/adventureworks "
            "--gold-root   {{ var.value.PROJECT_DIR }}/data/gold/adventureworks "
            "--schema-json {{ var.value.PROJECT_DIR }}/configs/schemas/DimCustomer.json "
            "--snapshot-mode common "  # use the same snapshot date the DAG is running with
            "--ingestion-date ${INGESTION_DATE} "
            "--coalesce 1 "
        ),
        do_xcom_push=False,
    )

    validate_gold = BashOperator(
        task_id="validate_gold_dim_customer",
        bash_command=(
            common_env +
            "{{ var.value.SPARK_BIN }} --master local[*] --py-files libs.zip "
            "{{ var.value.JOBS_DIR }}/dag_validation.py "
            "--root {{ var.value.PROJECT_DIR }}/data/gold/adventureworks "
            "--table DimCustomer "
            "--schema-json {{ var.value.PROJECT_DIR }}/configs/schemas/DimCustomer.json "
            "--ingestion-date ${INGESTION_DATE} "
        ),
        do_xcom_push=False,
    )

    # Fan-in → Gold
    [person_grp, customer_grp, st_grp, store_grp] >> build_dim_customer >> validate_gold
