import os
import logging
from datetime import datetime
import datetime as dt
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from geopy.geocoders import Nominatim
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryCreateEmptyTableOperator
import pandas as pd
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
DATASET_NAME = "staging"
AIRFLOW_HOME = '/opt/airflow/'
ZONES_GCS_PATH_TEMPLATE = "raw/taxi_zone/taxi_zone_lookup.csv"
default_args = {
    "owner":"airflow",
    "start_date": datetime(2023, 1, 1),
    "depends_on_past":False,
    "retries":1,
    'retry_delay': dt.timedelta(seconds=5)
}

def transform_address(zone):
    loc = Nominatim(user_agent = "GetLoc")
    address = str(zone) + ", New York"
    try:
        getLoc = loc.geocode(address)
        return [getLoc.latitude, getLoc.longitude]
    except:
        # 抓不到的用google map api補
        return [None, None]

def add_lat_long():
    # load data from gcs
    df = pd.read_csv('gs://{}/{}'.format(BUCKET,ZONES_GCS_PATH_TEMPLATE))
    # add lat long
    df[['lat', 'long']] = df.apply(lambda x: transform_address(x['Zone']), axis = 1, result_type='expand')
    df.to_csv(AIRFLOW_HOME+"taxi_zone_lookup_new.csv", index=False)

def upload_to_gcs(bucket,object_name,local_path):
    client = storage.Client()
    bucket = client.bucket(bucket)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # blob is the unique path of the object in the bucket.
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_path)


with DAG(
    dag_id = "data_pipe_dag",
    # 每個月3號 7 am
    schedule_interval = "@once",
    default_args = default_args,
    catchup = False, 
    max_active_runs = 2,
    tags = ['tlc-project']
) as dag:
    # add_lat_long_task = PythonOperator(
    #     task_id = "add_lat_long_task",
    #     python_callable=add_lat_long
    # )
    # upload_to_gcs_task = PythonOperator(
    #         task_id = "upload_to_gcs_task",
    #         python_callable = upload_to_gcs,
    #         op_kwargs = {
    #             "bucket" : BUCKET,
    #             "object_name": "raw/taxi_zone/taxi_zone_lookup_new.csv",
    #             "local_path": AIRFLOW_HOME+"taxi_zone_lookup_new.csv"
    #         }
    #     )
    create_zone_table_task = BigQueryCreateExternalTableOperator(
        task_id="create_zone_externaltable_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                # "datasetId": DATASET_NAME,
                "datasetId": "staging",
                "tableId": "zone_external_table",
            },
            "schema": {
                "fields": [
                    {"name": "locationID", "type": "INTEGER"},
                    {"name": "borough", "type": "STRING"},
                    {"name": "zone", "type": "STRING"},
                    {"name": "service_zone", "type": "STRING"},
                    {"name": "lat", "type": "FLOAT"},
                    {"name": "long", "type": "FLOAT"},
                    
                ]
            },
            "externalDataConfiguration": {
                "sourceFormat": "CSV",
                "compression": "NONE",
                "csvOptions": {"skipLeadingRows": 1},
                "sourceUris": [f'gs://{BUCKET}/raw/taxi_zone/taxi_zone_lookup_new.csv'],
            },
        }
        
    )
    # create table
    # delete external table
    # add_lat_long_task >> upload_to_gcs_task >> create_zone_table_task
    # create_zone_table_task