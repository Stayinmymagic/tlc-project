import os
import logging
from datetime import datetime
import datetime as dt
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
AIRFLOW_HOME = '/opt/airflow/'
# Yellow
YELLOW_TAXI_URL_TEMPLATE = URL_PREFIX + "yellow_tripdata_{{logical_date.strftime(\'%Y-%m\')}}.parquet"
YELLOW_TAXI_AIRFLOW_TEMPLATE = AIRFLOW_HOME + "yellow_tripdata_{{logical_date.strftime(\'%Y-%m\')}}.parquet"
YELLOW_TAXI_GCS_TEMPLATE = "raw/yellow_tripdata/{{ logical_date.strftime(\'%Y\') }}/yellow_tripdata_{{ logical_date.strftime(\'%Y-%m\') }}.parquet"
# Green
GREEN_TAXI_URL_TEMPLATE = URL_PREFIX + "green_tripdata_{{logical_date.strftime(\'%Y-%m\')}}.parquet"
GREEN_TAXI_AIRFLOW_TEMPLATE = AIRFLOW_HOME + "green_tripdata_{{logical_date.strftime(\'%Y-%m\')}}.parquet"
GREEN_TAXI_GCS_TEMPLATE = "raw/green_tripdata/{{ logical_date.strftime(\'%Y\') }}/green_tripdata_{{ logical_date.strftime(\'%Y-%m\') }}.parquet"
# HVFHV
HVFHV_TAXI_URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{{ logical_date.strftime(\'%Y-%m\')}}.parquet"
# HVFHV_TAXI_URL_TEMPLATE = URL_PREFIX + "fhvhv_tripdata_{{ logical_date.strftime(\'%Y-%m\')}}.parquet"
HVFHV_TAXI_AIRFLOW_TEMPLATE =  AIRFLOW_HOME + "fhvhv_tripdata_{{logical_date.strftime(\'%Y-%m\')}}.parquet"
HVFHV_TAXI_GCS_TEMPLATE = "raw/HVfhv_tripdata/{{ logical_date.strftime(\'%Y\') }}/fhvhv_tripdata_{{ logical_date.strftime(\'%Y-%m\') }}.parquet"
# Zone
ZONES_URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
ZONES_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/taxi_zone_lookup.csv'
ZONES_GCS_PATH_TEMPLATE = "raw/taxi_zone/taxi_zone_lookup.csv"

default_args = {
    "owner":"airflow",
    "start_date": datetime(2019, 1, 1),
    "depends_on_past":False,
    "retries":1,
    'retry_delay': dt.timedelta(minutes=1)
}

def upload_to_gcs(bucket,object_name,local_path):
    client = storage.Client()
    bucket = client.bucket(bucket)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # blob is the unique path of the object in the bucket.
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_path)

def donwload_and_upload_dag(dag, taxi_url_template, taxi_airflow_template, taxi_gcs_template):

    with dag:

        # download_dataset_task
        download_dataset_task = BashOperator(
            task_id = "download_dataset_task",
            bash_command = f" curl -sSLf {taxi_url_template} > {taxi_airflow_template}"
        )

        # local_to_gcs_task
        local_to_gcs_task = PythonOperator(
            task_id = "local_to_gcs_task",
            python_callable = upload_to_gcs,
            op_kwargs = {
                "bucket" : BUCKET,
                "object_name":taxi_gcs_template,
                "local_path":taxi_airflow_template
            }
        )

        # bigquery_external_table_task


        # remove local data task
        rm_local_data_task = BashOperator(
            task_id = "rm_local_data_task",
            bash_command = f"rm {taxi_airflow_template}"
        )
    
        download_dataset_task >> local_to_gcs_task >> rm_local_data_task

# yellow
yellow_taxi_data_dag = DAG(
    dag_id = "yellow_taxi_data_dag",
    # 每個月2號 7 am
    schedule_interval = "0 7 2 * *",
    start_date = datetime(2019,1,1),
    default_args = default_args,
    catchup = True, 
    max_active_runs = 2,
    tags = ['tlc-project']

)
donwload_and_upload_dag(
    yellow_taxi_data_dag,
    YELLOW_TAXI_URL_TEMPLATE, 
    YELLOW_TAXI_AIRFLOW_TEMPLATE, 
    YELLOW_TAXI_GCS_TEMPLATE
)
# green
green_taxi_data_dag = DAG(
    dag_id = "green_taxi_data_dag",
    schedule_interval = "0 7 2 * *",
    start_date = datetime(2019,1,1),
    default_args = default_args,
    catchup = True,
    max_active_runs = 2, 
    tags = ['tlc-project']

)
donwload_and_upload_dag(
    green_taxi_data_dag,
    GREEN_TAXI_URL_TEMPLATE, 
    GREEN_TAXI_AIRFLOW_TEMPLATE, 
    GREEN_TAXI_GCS_TEMPLATE
)
# HVfhv
HVfhv_taxi_data_dag = DAG(
    dag_id = "HVfhv_taxi_data_dag",
    schedule_interval = "0 7 2 * *",
    start_date = datetime(2019,2,1),
    default_args = default_args,
    catchup = True,
    max_active_runs = 2,
    tags = ['tlc-project']

)
donwload_and_upload_dag(
    HVfhv_taxi_data_dag,
    HVFHV_TAXI_URL_TEMPLATE, 
    HVFHV_TAXI_AIRFLOW_TEMPLATE, 
    HVFHV_TAXI_GCS_TEMPLATE
)
#zone
zone_data_dag = DAG(
    dag_id  = "zone_data_dag",
    schedule_interval = "@once",
    start_date = days_ago(1),
    default_args = default_args,
    catchup = False,
    max_active_runs = 2,
    tags = ['tlc-project']
)
donwload_and_upload_dag(
    zone_data_dag,
    ZONES_URL_TEMPLATE, 
    ZONES_CSV_FILE_TEMPLATE, 
    ZONES_GCS_PATH_TEMPLATE
)