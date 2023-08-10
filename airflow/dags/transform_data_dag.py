import os
import logging
from datetime import datetime
import datetime as dt
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator, BigQueryCreateEmptyTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

# MACRO VARIABLES
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

GCS_DATASET = "tripdata"
BIGQUERY_DATASET = "trips_data_all"

INPUT_PART = "raw"
INPUT_FILETYPE = "parquet"
path_to_local_home = "/opt/airflow"
COLOR_RANGE = {'yellow':'tpep_pickup_datetime' ,'green':'lpep_pickup_datetime', 'HVfhv':'pickup_datetime'}
COLOR_MAP_VIEW = {'yellow':'vendorid' ,'green':'vendorid', 'HVfhv':'driver_license_number'}
# AIRFLOW ARGS
default_args = {
    "owner":'Roey',
    "start_date" : days_ago(1),
    "depends_on_past": False,
    "retries":1
}



# DAG
with DAG(
    dag_id = "transform_data_dag",
    schedule_interval = "0 8 2 * *",
    start_date = days_ago(1),
    default_args = default_args,
    catchup = False,
    max_active_runs = 1,
    tags = ['tlc-project']) as dag:
    with TaskGroup('dynamic_tasks_group',
                   prefix_group_id=False,
                   ) as dynamic_tasks_group:
        for color, ds_col in COLOR_RANGE.items():
            ## bigquery_external_table_task
            bigquery_external_table_task = BigQueryCreateExternalTableOperator(
                task_id = f"bq_{color}_external_task",
                table_resource = {
                    "tableReference":{
                        "projectId": PROJECT_ID,
                        "datasetId": BIGQUERY_DATASET,
                        "tableId": f"{color}_external_table"
                    },
                    "externalDataConfiguration":{
                        "autodetect":"True",
                        "sourceFormat":f"{INPUT_FILETYPE.upper()}",
                        "sourceUris" : [f"gs://{BUCKET}/raw/{color}_tripdata/2021/*"]
                    }
                }
            )

            ## create partitioned by date table
            # CREATE_BQ_TBL_QUERY = (
            #     f"CREATE OR REPLACE TABLE staging.{color}_tripdata \
            #      PARTITION BY DATE({ds_col}) \
            #      AS SELECT * FROM {BIGQUERY_DATASET}.{color}_external_table"
            # )
            CREATE_BQ_TBL_QUERY = open(f'/opt/airflow/dags/sql/create_partitioned_{color}.sql', 'r').read()
            bq_create_partitioned_table_task = BigQueryInsertJobOperator(
                task_id = f"bq_create_{color}_partitioned_table_task",
                configuration = {
                    "query":{
                        "query": CREATE_BQ_TBL_QUERY,
                        "useLegacySql":False
                    }
                }

            )
            ## create view in staging dataset : 避免重複data
            create_view_task = BigQueryCreateEmptyTableOperator(
                task_id = f"bq_create_{color}_view",
                dataset_id = 'staging',
                table_id = f"{color}_view",
                view = {
                    "query" : f"""WITH tripdata as (
                                SELECT *, row_number() over (partition by {COLOR_MAP_VIEW[color]}, pickup_datetime) as rn from `staging.{color}_tripdata`
                                WHERE {COLOR_MAP_VIEW[color]} is not null and trip_distance > 0
                                )
                                SELECT * from tripdata WHERE rn = 1;""",
                    "useLegacySql": False
                }
            )
            bigquery_external_table_task >> bq_create_partitioned_table_task >> create_view_task
    with TaskGroup('create_fact_tasks_group',
                   prefix_group_id=False,
                   ) as create_fact_tasks_group:
        create_core_fact_table_task = BigQueryCreateEmptyTableOperator(
            task_id = "create_core_fact_table",
            dataset_id = 'core',
            table_id = 'yg_fact_table',
            )	
        create_core_hvfhv_fact_table_task = BigQueryCreateEmptyTableOperator(
            task_id = "create_core_hvfhv_fact_table",
            dataset_id = 'core',
            table_id = 'hvfhv_fact_table',
            )	
        
        CREATE_FACT_TB_QUERY = open(f'/opt/airflow/dags/sql/create_fact_trips.sql', 'r').read()
        ## create fact table by view
        insert_yg_fact_table_task = BigQueryInsertJobOperator(
            task_id="insert_yg_fact_table",
            trigger_rule='none_failed',
            configuration = {
                "query" :{
                    "query": CREATE_FACT_TB_QUERY,
                    "useLegacySql":False,
                    # "writeDispositionTable": "WRITE_EMPTY",
                    # "destinationTable": {
                    #     'projectId': PROJECT_ID,
                    #     'datasetId': 'core',
                    #     'tableId':'fact_trips'
                    # }
                }
            }
        )

        CREATE_HVFHV_FACT_TB_QUERY = open(f'/opt/airflow/dags/sql/create_hvfhv_fact_trips.sql', 'r').read()
        ## create fact table by view
        insert_hvfhv_fact_table_task = BigQueryInsertJobOperator(
            task_id="insert_hvfhv_fact_table",
            trigger_rule='none_failed',
            configuration = {
                "query" :{
                    "query": CREATE_HVFHV_FACT_TB_QUERY,
                    "useLegacySql":False,
                    # "writeDispositionTable": "WRITE_EMPTY",
                    # "destinationTable": {
                    #     'projectId': PROJECT_ID,
                    #     'datasetId': 'core',
                    #     'tableId':'hvfhv_fact_trips'
                    # }
                }
            }
        )
        create_core_fact_table_task >> insert_yg_fact_table_task
        create_core_hvfhv_fact_table_task >> insert_hvfhv_fact_table_task
    create_monthly_revenue_table = BigQueryCreateEmptyTableOperator(
            task_id = "create_monthly_revenue_table",
            dataset_id = 'core',
            table_id = 'monthly_revenue_table',
            )	
    ## create monthly revenue table
    CREATE_MONTHLY_TB_QUERY = open(f'/opt/airflow/dags/sql/create_monthly_revenue.sql', 'r').read()
    ## create fact table by view
    insert_monthly_revenue_table_task = BigQueryInsertJobOperator(
        task_id="insert_monthly_revenue_table_task",
        trigger_rule='none_failed',
        configuration = {
            "query" :{
                "query": CREATE_MONTHLY_TB_QUERY,
                "useLegacySql":False,
                # "writeDispositionTable": "WRITE_EMPTY",
                # "destinationTable": {
                #     'projectId': PROJECT_ID,
                #     'datasetId': 'core',
                #     'tableId':'monthly_revenue_table'
                # }
            }
        }
    )
    ## 把task 連接起來
    # dynamic_tasks_group >> create_fact_tasks_group >> create_monthly_revenue_table_task
    dynamic_tasks_group >> create_fact_tasks_group >>  create_monthly_revenue_table >> insert_monthly_revenue_table_task
        

        