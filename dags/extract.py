import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

with DAG(
    "extract",
    default_args={"depends_on_past": True},
    start_date=datetime(2021, 6, 1),
    end_date=datetime(2021, 12, 31),
    schedule_interval="@monthly",
) as dag:
    object_name = "yellow_tripdata_" + "{{ ds[:7] }}.parquet"
    trip_data_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{object_name}"
    filename = f"{AIRFLOW_HOME}/data/bronze/{object_name}"

    curl_trip_data_task = BashOperator(
        task_id="curl_trip_data",
        bash_command=f"curl {trip_data_url} > {filename}",
    )

    curl_trip_data_task
