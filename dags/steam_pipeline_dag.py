from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import sys

sys.path.append("/opt/airflow/scripts")
from fetch_player_counts import fetch_all_and_save as fetch_player_counts_fn
from fetch_store_details import fetch_all_and_save as fetch_store_details_fn
from transform_silver import run_silver_transform
from build_gold import run_gold_build
from build_star_schema import run_star_schema_build

default_args = {
    "owner": "vishal",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="steam_pipeline_dag",
    default_args=default_args,
    start_date=datetime(2026, 9, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["steam", "portfolio"],
) as dag:

    # ---- Daily: always runs ----
    fetch_player_counts = PythonOperator(
        task_id="fetch_player_counts",
        python_callable=fetch_player_counts_fn,
    )

    # ---- Weekly branch check ----
    def check_weekly_refresh(**context):
        return "fetch_store_details"
        # if context["logical_date"].weekday() == 0:  # Monday
        #     return "fetch_store_details"
        # return "skip_store_details"

    branch_weekly = BranchPythonOperator(
        task_id="branch_weekly_check",
        python_callable=check_weekly_refresh,
    )

    fetch_store_details = PythonOperator(
        task_id="fetch_store_details",
        python_callable=fetch_store_details_fn,
    )

    skip_store_details = EmptyOperator(task_id="skip_store_details")

    join_after_branch = EmptyOperator(
        task_id="join_after_branch",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ---- Bronze landing ----
    def land_bronze_fn(**context):
        import shutil, glob, os
        ds = context["ds"]
        src_dir = "/opt/airflow/scripts/output"
        bronze_dir = f"/opt/airflow/bronze/dt={ds}"
        os.makedirs(bronze_dir, exist_ok=True)

        moved = 0
        for pattern in ["player_counts_*.json", "store_details_*.json"]:
            for filepath in glob.glob(os.path.join(src_dir, pattern)):
                shutil.move(filepath, bronze_dir)
                moved += 1

        print(f"Landed {moved} file(s) into {bronze_dir}")

    land_bronze = PythonOperator(
        task_id="land_bronze",
        python_callable=land_bronze_fn,
    )

    # ---- Silver transform ----
    def transform_silver_fn(**context):
        ds = context["ds"]
        run_silver_transform(ds)

    transform_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=transform_silver_fn,
    )

        # ---- Gold build ----
    def build_gold_fn(**context):
        ds = context["ds"]
        run_gold_build(ds)

    build_gold = PythonOperator(
        task_id="build_gold",
        python_callable=build_gold_fn,
    )
    # ---- Star schema build ----
    def build_star_schema_fn(**context):
        ds = context["ds"]
        run_star_schema_build(ds)

    build_star_schema = PythonOperator(
        task_id="build_star_schema",
        python_callable=build_star_schema_fn,
    )

    # ---- Dependencies ----
    branch_weekly >> [fetch_store_details, skip_store_details] >> join_after_branch
    [fetch_player_counts, join_after_branch] >> land_bronze >> transform_silver >> build_gold >> build_star_schema
   