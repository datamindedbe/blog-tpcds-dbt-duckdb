from airflow import DAG
from conveyor.factories import ConveyorDbtTaskFactory
from conveyor.operators import ConveyorContainerOperatorV2
from datetime import datetime, timedelta


default_args = {
    "owner": "Conveyor",
    "depends_on_past": False,
    "start_date": datetime(year=2023, month=1, day=24),
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG(
    "dbt-duckdb-tpcds", default_args=default_args, schedule_interval="@daily", max_active_runs=1, concurrency=10
)

def run_tpcds_benchmark_for_model(modelName):
  ConveyorContainerOperatorV2(
      dag=dag,
      task_id=f"dbt-tpcds-m2xlarge-{modelName}",
      arguments=["build", "--profiles-dir", "/app/dbt", "--project-dir", "/app/dbt/dbt_duckdb_tpcds", "--target", "dev", "--select", f"models/normal/tpcds_{modelName}.sql"],
      aws_role="dbt-duckdb-tpcds-{{ macros.datafy.env() }}",
      instance_type="mx.2xlarge",
      instance_life_cycle="on-demand",
      disk_size=100,
      disk_mount_path="/var/data",
  )

for model in range(1,100):
  if model < 10:
    run_tpcds_benchmark_for_model(f"q0{model}")
  else:
    run_tpcds_benchmark_for_model(f"q{model}")

