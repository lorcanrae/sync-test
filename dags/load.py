import os
from datetime import datetime

from airflow import DAG

from airflow.sensors.external_task import ExternalTaskSensor

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

with DAG(
    "load",
    default_args={"depends_on_past": True},
    start_date=datetime(2021, 6, 1),
    end_date=datetime(2021, 12, 31),
    schedule_interval="@monthly",
) as dag:
    date = "{{ ds[:7] }}"
    filtered_data_file = f"{AIRFLOW_HOME}/data/silver/yellow_tripdata_{date}.csv"
    wait_for_transform_task = ExternalTaskSensor(
        task_id="transform_sensor",
        external_dag_id="transform",
        allowed_states=["success"],
        poke_interval=10,
        timeout=60 * 10,
    )

    upload_local_file_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_local_file_to_gcs",
        gcp_conn_id="google_cloud_connection",
        src=filtered_data_file,
        dst=f"yellow_tripdata_{date}.csv",
        bucket="de_airflow_taxi_silver",
    )

    create_dataset_task = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        gcp_conn_id="google_cloud_connection",
        dataset_id="de_airflow_taxi_gold",
    )

    create_table_task = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        gcp_conn_id="google_cloud_connection",
        dataset_id="de_airflow_taxi_gold",
        table_id="trips",
        schema_fields=[
            {"name": "date", "type": "STRING"},
            {"name": "trip_distance", "type": "FLOAT"},
            {"name": "total_amount", "type": "FLOAT"},
        ],
    )

    remove_existing_data_task = BigQueryInsertJobOperator(
        task_id="remove_existing_data",
        gcp_conn_id="google_cloud_connection",
        configuration={
            "query": {
                "query": f"DELETE FROM de_airflow_taxi_gold.trips WHERE date = '{date}'",
                "useLegacySql": False,
            }
        },
    )

    load_to_bigquery_task = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket="de_airflow_taxi_silver",
        source_objects=f"yellow_tripdata_{date}.csv",
        gcp_conn_id="google_cloud_connection",
        destination_project_dataset_table="de_airflow_taxi_gold.trips",
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
    )

    (
        wait_for_transform_task
        >> upload_local_file_to_gcs_task
        >> create_dataset_task
        >> create_table_task
        >> remove_existing_data_task
        >> load_to_bigquery_task
    )
